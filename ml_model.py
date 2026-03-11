import psycopg2
import psycopg2.pool
import os
import logging
import pickle
import numpy as np
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.model_selection import cross_val_score, StratifiedKFold, train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score
from sklearn.utils.class_weight import compute_sample_weight
from xgboost import XGBClassifier

load_dotenv('/root/solana_bot/.env')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('/var/log/solana_bot/ml_model.log'),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

pool = psycopg2.pool.ThreadedConnectionPool(
    1, 5,
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST")
)

# ── CONFIGURACIÓN ─────────────────────────────────────
TARGET_GAIN    = 0.30   # Ganador si sube 30%+ (antes 50% → pocos positivos)
MIN_SNAPSHOTS  = 5      # Mínimo snapshots (antes 10 → descartaba demasiado)
MODEL_PATH     = '/root/solana_bot/model.pkl'
FEATURES_PATH  = '/root/solana_bot/features.pkl'

def load_training_data():
    """
    Carga TODO el histórico disponible (sin límite de días).
    Target binario: 1 = token subió TARGET_GAIN%+ desde precio mínimo inicial.
    """
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                dt.mint,
                dt.buys_5m,
                dt.sells_5m,
                dt.market_cap,
                dt.volume_24h,
                COALESCE(dt.volume_1h, 0)  as volume_1h,
                COALESCE(dt.volume_5m, 0)  as volume_5m,
                dt.survival_score,
                dt.narrative,
                dt.buys_1h,
                dt.sells_1h,
                COALESCE(dt.buys_24h, 0)  as buys_24h,
                COALESCE(dt.sells_24h, 0) as sells_24h,
                COALESCE(dt.rug_score, 50)              as rug_score,
                COALESCE(dt.holder_count, 10)           as holder_count,
                COALESCE(dt.top10_concentration, 95)    as top10_concentration,
                COALESCE(dt.dev_sold, FALSE)            as dev_sold,
                (dt.twitter  IS NOT NULL)               as has_twitter,
                (dt.telegram IS NOT NULL)               as has_telegram,
                (dt.website  IS NOT NULL)               as has_website,
                EXTRACT(HOUR FROM dt.created_at)        as launch_hour,
                COALESCE(dt.liquidity_usd, 0)           as liquidity_usd,
                COALESCE(dt.fdv, 0)                     as fdv,
                MIN(ps.price_usd)                       as price_entry,
                MAX(ps.price_usd)                       as price_max,
                COUNT(ps.id)                            as snapshot_count,
                CASE
                    WHEN MAX(ps.price_usd) >= MIN(ps.price_usd) * (1 + %s)
                    THEN 1 ELSE 0
                END as target
            FROM discovered_tokens dt
            JOIN price_snapshots ps ON ps.mint = dt.mint
            WHERE dt.buys_5m       IS NOT NULL
              AND dt.survival_score IS NOT NULL
              AND dt.market_cap     IS NOT NULL
              AND dt.market_cap     > 0
            GROUP BY
                dt.mint, dt.buys_5m, dt.sells_5m, dt.market_cap,
                dt.volume_24h, dt.volume_1h, dt.volume_5m,
                dt.survival_score, dt.narrative,
                dt.buys_1h, dt.sells_1h, dt.buys_24h, dt.sells_24h,
                dt.rug_score, dt.holder_count, dt.top10_concentration,
                dt.dev_sold, dt.twitter, dt.telegram, dt.website,
                dt.created_at, dt.liquidity_usd, dt.fdv
            HAVING COUNT(ps.id) >= %s
        """, (TARGET_GAIN, MIN_SNAPSHOTS))

        rows = cur.fetchall()
        cur.close()

        columns = [
            'mint', 'buys_5m', 'sells_5m', 'market_cap', 'volume_24h',
            'volume_1h', 'volume_5m', 'survival_score', 'narrative',
            'buys_1h', 'sells_1h', 'buys_24h', 'sells_24h',
            'rug_score', 'holder_count', 'top10_concentration',
            'dev_sold', 'has_twitter', 'has_telegram', 'has_website',
            'launch_hour', 'liquidity_usd', 'fdv',
            'price_entry', 'price_max', 'snapshot_count', 'target'
        ]

        df = pd.DataFrame(rows, columns=columns)
        log.info(f"✅ {len(df)} tokens cargados para entrenamiento (histórico completo)")
        log.info(f"   Ganadores (target=1): {df['target'].sum()} ({df['target'].mean()*100:.1f}%)")
        log.info(f"   Perdedores (target=0): {(1-df['target']).sum()} ({(1-df['target']).mean()*100:.1f}%)")
        return df
    finally:
        pool.putconn(conn)

def engineer_features(df):
    """Features derivadas del dataset de entrenamiento o de un token individual."""

    # Ratios compras/ventas
    df['buy_sell_ratio']     = df['buys_5m']  / (df['sells_5m']  + 1)
    df['buy_sell_ratio_1h']  = df['buys_1h']  / (df['sells_1h']  + 1)
    df['buy_sell_ratio_24h'] = df['buys_24h'] / (df['sells_24h'] + 1)

    # Presión neta
    df['net_buy_pressure'] = df['buys_5m'] - df['sells_5m']

    # Volumen relativo
    df['vol_to_mcap']    = df['volume_24h'] / (df['market_cap'] + 1)
    df['vol5m_to_mcap']  = df['volume_5m']  / (df['market_cap'] + 1)

    # Logaritmos (neutraliza outliers extremos)
    df['log_mcap']      = np.log1p(df['market_cap'])
    df['log_volume']    = np.log1p(df['volume_24h'])
    df['log_volume_1h'] = np.log1p(df['volume_1h'])
    df['log_volume_5m'] = np.log1p(df['volume_5m'])

    # Score normalizado
    df['score_normalized'] = df['survival_score'] / 100.0

    # Riesgo de holder
    df['holder_risk'] = df['top10_concentration'] / 100.0

    # Narrativa codificada
    le = LabelEncoder()
    df['narrative_encoded'] = le.fit_transform(df['narrative'].fillna('OTHER'))

    # Señales binarias
    df['zero_sells']   = (df['sells_5m'] == 0).astype(int)
    df['dev_sold']     = df['dev_sold'].astype(int)
    df['has_twitter']  = df['has_twitter'].astype(int)
    df['has_telegram'] = df['has_telegram'].astype(int)
    df['has_website']  = df['has_website'].astype(int)

    # MCap en rango óptimo ($1K–$10K)
    df['optimal_mcap'] = ((df['market_cap'] >= 1000) & (df['market_cap'] <= 10000)).astype(int)

    # Hora de lanzamiento (mercado más activo 14h–22h UTC)
    df['peak_hours'] = df['launch_hour'].apply(
        lambda h: 1 if 14 <= h <= 22 else 0
    )

    # Liquidez — una de las señales más fuertes contra rugs
    df['log_liquidity']      = np.log1p(df['liquidity_usd'])
    df['liquidity_to_mcap']  = df['liquidity_usd'] / (df['market_cap'] + 1)
    df['has_liquidity']      = (df['liquidity_usd'] > 1000).astype(int)
    df['log_fdv']            = np.log1p(df['fdv'])
    df['fdv_to_mcap']        = df['fdv'] / (df['market_cap'] + 1)

    return df, le

def get_feature_columns():
    return [
        # Raw
        'buys_5m', 'sells_5m', 'market_cap', 'volume_24h',
        'volume_1h', 'volume_5m', 'survival_score',
        'buys_1h', 'sells_1h', 'buys_24h', 'sells_24h',
        'rug_score', 'holder_count', 'top10_concentration',
        'launch_hour',
        # Engineered ratios
        'buy_sell_ratio', 'buy_sell_ratio_1h', 'buy_sell_ratio_24h',
        'net_buy_pressure', 'vol_to_mcap', 'vol5m_to_mcap',
        'log_mcap', 'log_volume', 'log_volume_1h', 'log_volume_5m',
        'score_normalized', 'holder_risk',
        # Categoricals
        'narrative_encoded',
        # Binary flags
        'zero_sells', 'optimal_mcap', 'peak_hours',
        'dev_sold', 'has_twitter', 'has_telegram', 'has_website',
        'has_liquidity',
        # Liquidity
        'liquidity_usd', 'log_liquidity', 'liquidity_to_mcap',
        'fdv', 'log_fdv', 'fdv_to_mcap',
    ]

def train_models(X, y):
    """Entrena múltiples modelos balanceados y elige el mejor por AUC-ROC."""

    pos   = (y == 1).sum()
    neg   = (y == 0).sum()
    ratio = neg / max(pos, 1)
    sample_weights = compute_sample_weight('balanced', y)

    models = {
        'RandomForest': (
            RandomForestClassifier(
                n_estimators=300,
                max_depth=8,
                min_samples_leaf=3,
                class_weight='balanced',
                random_state=42,
                n_jobs=-1
            ),
            {}   # fit_params
        ),
        'GradientBoosting': (
            GradientBoostingClassifier(
                n_estimators=300,
                max_depth=4,
                learning_rate=0.05,
                subsample=0.8,
                random_state=42
            ),
            {'sample_weight': sample_weights}
        ),
        'XGBoost': (
            XGBClassifier(
                n_estimators=300,
                max_depth=4,
                learning_rate=0.05,
                scale_pos_weight=ratio,
                subsample=0.8,
                colsample_bytree=0.8,
                random_state=42,
                eval_metric='logloss',
                verbosity=0
            ),
            {}
        ),
    }

    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    best_model      = None
    best_score      = 0
    best_name       = ""
    best_fit_params = {}

    print("\n" + "="*60)
    print("🤖 ENTRENANDO MODELOS ML")
    print(f"   Dataset: {len(y)} samples | Positivos: {pos} ({pos/len(y)*100:.1f}%) | Negativos: {neg}")
    print("="*60)

    for name, (model, fit_params) in models.items():
        # cross_val_score no soporta fit_params con sample_weight por defecto
        # → lo evaluamos con split manual para GradientBoosting
        if fit_params:
            fold_scores = []
            for train_idx, val_idx in cv.split(X, y):
                X_tr, X_val = X[train_idx], X[val_idx]
                y_tr, y_val = y[train_idx], y[val_idx]
                sw_tr = sample_weights[train_idx]
                model.fit(X_tr, y_tr, sample_weight=sw_tr)
                proba = model.predict_proba(X_val)[:, 1]
                fold_scores.append(roc_auc_score(y_val, proba))
            scores = np.array(fold_scores)
        else:
            scores = cross_val_score(model, X, y, cv=cv, scoring='roc_auc', n_jobs=-1)

        mean_score = scores.mean()
        std_score  = scores.std()
        print(f"\n  {name}:")
        print(f"    AUC-ROC: {mean_score:.3f} ± {std_score:.3f}")

        if mean_score > best_score:
            best_score      = mean_score
            best_model      = model
            best_name       = name
            best_fit_params = fit_params

    print(f"\n🏆 Mejor modelo: {best_name} (AUC: {best_score:.3f})")

    # Entrenar el mejor con todos los datos
    if best_fit_params:
        best_model.fit(X, y, **best_fit_params)
    else:
        best_model.fit(X, y)

    # Feature importance
    if hasattr(best_model, 'feature_importances_'):
        feature_cols = get_feature_columns()
        importances = pd.Series(
            best_model.feature_importances_,
            index=feature_cols
        ).sort_values(ascending=False)

        print("\n📊 FEATURES MÁS IMPORTANTES:")
        for feat, imp in importances.head(10).items():
            bar = "█" * int(imp * 50)
            print(f"  {feat:<28} {bar} {imp:.3f}")

    return best_model, best_name, best_score

def evaluate_model(model, X, y):
    """Evaluación detallada con reporte y simulación de trading."""
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    model.fit(X_train, y_train)
    y_pred  = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1]

    print("\n📋 REPORTE DE CLASIFICACIÓN (test 20%):")
    print(classification_report(y_test, y_pred, target_names=['Perdedor', 'Ganador']))

    cm = confusion_matrix(y_test, y_pred)
    print("🔢 MATRIZ DE CONFUSIÓN:")
    print(f"  Verdaderos Negativos: {cm[0][0]:4d} | Falsos Positivos: {cm[0][1]:4d}")
    print(f"  Falsos Negativos:     {cm[1][0]:4d} | Verdaderos Positivos: {cm[1][1]:4d}")

    # Simulación de trading
    print(f"\n💰 SIMULACIÓN (threshold 65%):")
    capital   = 1000.0
    bet       = 20.0
    threshold = 0.65
    trades = wins = 0
    for prob, actual in zip(y_proba, y_test):
        if prob >= threshold:
            trades += 1
            if actual == 1:
                capital += bet * TARGET_GAIN
                wins += 1
            else:
                capital -= bet * 0.30   # stop-loss simulado -30%
    if trades > 0:
        print(f"  Trades (prob≥{threshold}): {trades} | Win rate: {wins/trades*100:.1f}%")
        print(f"  Capital: $1000 → ${capital:.2f} ({(capital-1000)/1000*100:+.1f}%)")
    else:
        print("  Sin trades con ese threshold en el test set.")

    return roc_auc_score(y_test, y_proba)

def save_model(model, le, feature_cols):
    with open(MODEL_PATH, 'wb') as f:
        pickle.dump(model, f)
    with open(FEATURES_PATH, 'wb') as f:
        pickle.dump({'label_encoder': le, 'features': feature_cols}, f)
    log.info(f"✅ Modelo guardado en {MODEL_PATH}")

def save_metrics(name, auc, n_samples, n_positives):
    """Guarda métricas del entrenamiento en la DB para tracking."""
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ml_training_log (
                id          SERIAL PRIMARY KEY,
                trained_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                model_name  TEXT,
                auc_roc     NUMERIC(5,4),
                n_samples   INTEGER,
                n_positives INTEGER,
                target_gain NUMERIC(4,2)
            )
        """)
        cur.execute("""
            INSERT INTO ml_training_log (model_name, auc_roc, n_samples, n_positives, target_gain)
            VALUES (%s, %s, %s, %s, %s)
        """, (name, round(auc, 4), n_samples, n_positives, TARGET_GAIN))
        conn.commit()
        cur.close()
        log.info(f"📈 Métricas guardadas: {name} AUC={auc:.3f} n={n_samples}")
    except Exception as e:
        conn.rollback()
        log.warning(f"No se pudieron guardar métricas: {e}")
    finally:
        pool.putconn(conn)

def predict_token(mint):
    """Predice la probabilidad de éxito de un token en tiempo real."""
    if not os.path.exists(MODEL_PATH):
        return None

    with open(MODEL_PATH, 'rb') as f:
        model = pickle.load(f)
    with open(FEATURES_PATH, 'rb') as f:
        saved = pickle.load(f)

    le = saved['label_encoder']

    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT buys_5m, sells_5m, market_cap, volume_24h,
                   COALESCE(volume_1h, 0), COALESCE(volume_5m, 0),
                   survival_score, narrative, buys_1h, sells_1h,
                   COALESCE(buys_24h, 0), COALESCE(sells_24h, 0),
                   COALESCE(rug_score, 50), COALESCE(holder_count, 10),
                   COALESCE(top10_concentration, 95),
                   COALESCE(dev_sold, FALSE),
                   (twitter  IS NOT NULL),
                   (telegram IS NOT NULL),
                   (website  IS NOT NULL),
                   EXTRACT(HOUR FROM created_at),
                   COALESCE(liquidity_usd, 0),
                   COALESCE(fdv, 0)
            FROM discovered_tokens WHERE mint = %s
        """, (mint,))
        row = cur.fetchone()
        cur.close()

        if not row:
            return None

        df = pd.DataFrame([row], columns=[
            'buys_5m', 'sells_5m', 'market_cap', 'volume_24h',
            'volume_1h', 'volume_5m', 'survival_score', 'narrative',
            'buys_1h', 'sells_1h', 'buys_24h', 'sells_24h',
            'rug_score', 'holder_count', 'top10_concentration',
            'dev_sold', 'has_twitter', 'has_telegram', 'has_website',
            'launch_hour', 'liquidity_usd', 'fdv'
        ])

        df, _ = engineer_features(df)
        try:
            df['narrative_encoded'] = le.transform(df['narrative'].fillna('OTHER'))
        except ValueError:
            df['narrative_encoded'] = 0

        X = df[get_feature_columns()].fillna(0)
        proba = model.predict_proba(X)[0][1]
        return round(proba * 100, 1)
    finally:
        pool.putconn(conn)

def run():
    log.info("🤖 ML Model Training iniciando...")

    df = load_training_data()

    if len(df) < 50:
        log.error(f"❌ Solo {len(df)} tokens — necesitamos mínimo 50 para entrenar")
        return

    for col in df.columns:
        try:
            df[col] = pd.to_numeric(df[col])
        except (ValueError, TypeError):
            pass
    df, le = engineer_features(df)
    feature_cols = get_feature_columns()

    X = df[feature_cols].fillna(0).values
    y = df['target'].values

    log.info(f"📊 Dataset: {X.shape[0]} samples, {X.shape[1]} features")

    model, name, cv_auc = train_models(X, y)
    test_auc = evaluate_model(model, X, y)
    save_model(model, le, feature_cols)
    save_metrics(name, test_auc, len(y), int(y.sum()))

    log.info(f"✅ {name} | CV AUC: {cv_auc:.3f} | Test AUC: {test_auc:.3f}")
    log.info(f"   Target: +{TARGET_GAIN*100:.0f}% | Samples: {len(y)} | Features: {X.shape[1]}")

if __name__ == "__main__":
    run()
