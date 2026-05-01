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
from sklearn.model_selection import cross_val_score, StratifiedKFold, TimeSeriesSplit, train_test_split
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
    Carga datos de entrenamiento SIN leakage temporal ni de target.

    Fuente: token_features_at_signal — foto de las features en el instante exacto
    del primer BUY/STRONG_BUY, antes de que el precio se mueva.

    Target correcto: ¿subió TARGET_GAIN%+ en las 2 horas siguientes al signal,
    partiendo del precio registrado en ese momento (no del mínimo histórico)?

    Fallback al método antiguo solo si la tabla nueva tiene < 50 registros válidos.
    """
    conn = pool.getconn()
    try:
        cur = conn.cursor()

        # ── Contar datos limpios disponibles ───────────────────────────────
        cur.execute("""
            SELECT COUNT(*) FROM token_features_at_signal tfs
            WHERE tfs.price_at_signal > 0
              AND tfs.liquidity_usd > 0
              AND EXISTS (
                SELECT 1 FROM price_snapshots ps
                WHERE ps.mint = tfs.mint
                  AND ps.snapshot_at > tfs.captured_at
                  AND ps.snapshot_at <= tfs.captured_at + INTERVAL '120 minutes'
              )
        """)
        clean_count = cur.fetchone()[0]
        log.info(f"token_features_at_signal: {clean_count} registros con label válido")

        if clean_count >= 50:
            # ── MÉTODO LIMPIO: features al momento del signal, target futuro ──
            log.info("✅ Usando datos sin leakage (token_features_at_signal)")
            cur.execute("""
                WITH future_max AS (
                    SELECT tfs.mint,
                           MAX(ps.price_usd)  AS max_future_price,
                           COUNT(ps.id)       AS future_snaps
                    FROM token_features_at_signal tfs
                    JOIN price_snapshots ps ON ps.mint = tfs.mint
                      AND ps.snapshot_at >  tfs.captured_at
                      AND ps.snapshot_at <= tfs.captured_at + INTERVAL '120 minutes'
                    GROUP BY tfs.mint
                    HAVING COUNT(ps.id) >= 2
                )
                SELECT
                    tfs.buys_5m,
                    tfs.sells_5m,
                    tfs.market_cap,
                    tfs.volume_24h,
                    COALESCE(tfs.volume_1h, 0),
                    COALESCE(tfs.volume_5m, 0),
                    tfs.survival_score,
                    tfs.narrative,
                    tfs.buys_1h,
                    tfs.sells_1h,
                    COALESCE(tfs.buys_24h, 0),
                    COALESCE(tfs.sells_24h, 0),
                    COALESCE(tfs.rug_score, 50),
                    COALESCE(tfs.holder_count, 10),
                    COALESCE(tfs.top10_concentration, 95),
                    COALESCE(tfs.dev_sold, FALSE),
                    tfs.has_twitter,
                    tfs.has_telegram,
                    tfs.has_website,
                    EXTRACT(HOUR FROM tfs.captured_at)  AS launch_hour,
                    COALESCE(tfs.liquidity_usd, 0),
                    COALESCE(tfs.fdv, 0),
                    COALESCE(tfs.smart_money_bought, 0),
                    COALESCE(tfs.narrative_momentum, 0),
                    COALESCE(tfs.deployer_prior_tokens, 0),
                    COALESCE(tfs.deployer_rugged_count, 0),
                    COALESCE(tfs.deployer_rug_rate, 0.5),
                    COALESCE(tfs.is_known_rugger, 0),
                    COALESCE(tfs.deployer_known, 0),
                    tfs.captured_at,
                    CASE WHEN fm.max_future_price >= tfs.price_at_signal * (1 + %s)
                         THEN 1 ELSE 0 END  AS target
                FROM token_features_at_signal tfs
                JOIN future_max fm ON fm.mint = tfs.mint
                WHERE tfs.price_at_signal > 0
                  AND tfs.buys_5m IS NOT NULL
                  AND tfs.market_cap IS NOT NULL
                  AND tfs.market_cap > 0
            """, (TARGET_GAIN,))
            source = "LIMPIO"

        else:
            # ── FALLBACK: método antiguo mientras se acumulan datos limpios ──
            log.warning(f"⚠️  Solo {clean_count} registros limpios — usando método legacy (con leakage)")
            cur.execute("""
                SELECT
                    dt.buys_5m, dt.sells_5m, dt.market_cap, dt.volume_24h,
                    COALESCE(dt.volume_1h, 0), COALESCE(dt.volume_5m, 0),
                    dt.survival_score, dt.narrative, dt.buys_1h, dt.sells_1h,
                    COALESCE(dt.buys_24h, 0), COALESCE(dt.sells_24h, 0),
                    COALESCE(dt.rug_score, 50), COALESCE(dt.holder_count, 10),
                    COALESCE(dt.top10_concentration, 95),
                    COALESCE(dt.dev_sold, FALSE),
                    (dt.twitter IS NOT NULL), (dt.telegram IS NOT NULL), (dt.website IS NOT NULL),
                    EXTRACT(HOUR FROM dt.created_at),
                    COALESCE(dt.liquidity_usd, 0), COALESCE(dt.fdv, 0),
                    CASE WHEN EXISTS (
                        SELECT 1 FROM wallet_activity wa
                        JOIN smart_wallets sw ON sw.wallet = wa.wallet
                        WHERE wa.mint = dt.mint AND sw.is_smart = TRUE
                    ) THEN 1 ELSE 0 END,
                    COALESCE((
                        SELECT ns.momentum FROM narrative_stats ns
                        WHERE ns.narrative = dt.narrative AND ns.window_hours = 24
                        ORDER BY ns.calculated_at DESC LIMIT 1
                    ), 0),
                    COALESCE(ds.total_tokens, 0), COALESCE(ds.rugged_count, 0),
                    COALESCE(ds.rug_rate, 0.5), COALESCE(ds.is_serial_rugger::int, 0),
                    (dt.deployer_wallet IS NOT NULL)::int,
                    dt.created_at,
                    CASE WHEN MAX(ps.price_usd) >= MIN(ps.price_usd) * (1 + %s)
                         THEN 1 ELSE 0 END AS target
                FROM discovered_tokens dt
                JOIN price_snapshots ps ON ps.mint = dt.mint
                LEFT JOIN deployer_stats ds ON ds.wallet = dt.deployer_wallet
                WHERE dt.buys_5m IS NOT NULL
                  AND dt.survival_score IS NOT NULL
                  AND dt.market_cap IS NOT NULL
                  AND dt.market_cap > 0
                GROUP BY
                    dt.mint, dt.buys_5m, dt.sells_5m, dt.market_cap,
                    dt.volume_24h, dt.volume_1h, dt.volume_5m,
                    dt.survival_score, dt.narrative,
                    dt.buys_1h, dt.sells_1h, dt.buys_24h, dt.sells_24h,
                    dt.rug_score, dt.holder_count, dt.top10_concentration,
                    dt.dev_sold, dt.twitter, dt.telegram, dt.website,
                    dt.created_at, dt.liquidity_usd, dt.fdv,
                    dt.deployer_wallet, ds.total_tokens, ds.rugged_count,
                    ds.rug_rate, ds.is_serial_rugger
                HAVING COUNT(ps.id) >= %s
            """, (TARGET_GAIN, MIN_SNAPSHOTS))
            source = "LEGACY"

        rows = cur.fetchall()
        cur.close()

        columns = [
            'buys_5m', 'sells_5m', 'market_cap', 'volume_24h',
            'volume_1h', 'volume_5m', 'survival_score', 'narrative',
            'buys_1h', 'sells_1h', 'buys_24h', 'sells_24h',
            'rug_score', 'holder_count', 'top10_concentration',
            'dev_sold', 'has_twitter', 'has_telegram', 'has_website',
            'launch_hour', 'liquidity_usd', 'fdv',
            'smart_money_bought', 'narrative_momentum',
            'deployer_prior_tokens', 'deployer_rugged_count',
            'deployer_rug_rate', 'is_known_rugger', 'deployer_known',
            'captured_at', 'target'
        ]

        df = pd.DataFrame(rows, columns=columns)
        log.info(f"✅ [{source}] {len(df)} tokens cargados")
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

    # Velocidad de actividad — aceleración vs promedio histórico de 24h
    # buys_growth > 1 significa que los 5m actuales son más activos que la media
    df['buys_growth']      = df['buys_5m']  / (df['buys_24h']  / 288.0 + 1)
    df['sells_growth']     = df['sells_5m'] / (df['sells_24h'] / 288.0 + 1)
    df['buy_acceleration'] = df['buys_1h']  / (df['buys_24h']  / 24.0  + 1)
    df['vol_growth_1h']    = df['volume_1h'] / (df['volume_24h'] / 24.0  + 1)
    df['vol_growth_5m']    = df['volume_5m'] / (df['volume_24h'] / 288.0 + 1)

    # Smart money y narrativa (ya vienen de la query, solo aseguramos tipos)
    df['smart_money_bought']  = df['smart_money_bought'].fillna(0).astype(int)
    df['narrative_momentum']  = pd.to_numeric(df['narrative_momentum'], errors='coerce').fillna(0)

    # ── Deployer features (mejora 15) ──────────────────
    # deployer_rug_rate: 0.5 = desconocido (neutro), 0 = limpio, 1 = rugger total
    df['deployer_prior_tokens'] = np.log1p(df['deployer_prior_tokens'].fillna(0))
    df['deployer_rugged_count'] = df['deployer_rugged_count'].fillna(0).astype(int)
    df['deployer_rug_rate']     = pd.to_numeric(df['deployer_rug_rate'], errors='coerce').fillna(0.5).clip(0, 1)
    df['is_known_rugger']       = df['is_known_rugger'].fillna(0).astype(int)
    df['deployer_known']        = df['deployer_known'].fillna(0).astype(int)

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
        # Velocidad de actividad
        'buys_growth', 'sells_growth', 'buy_acceleration',
        'vol_growth_1h', 'vol_growth_5m',
        # Señales externas
        'smart_money_bought', 'narrative_momentum',
        # Historial del deployer
        'deployer_prior_tokens', 'deployer_rugged_count',
        'deployer_rug_rate', 'is_known_rugger', 'deployer_known',
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
                max_depth=6,          # reducido de 8 → menos overfitting
                min_samples_leaf=5,   # aumentado de 3 → más generalización
                max_features='sqrt',
                class_weight='balanced',
                random_state=42,
                n_jobs=-1
            ),
            {}   # fit_params
        ),
        'GradientBoosting': (
            GradientBoostingClassifier(
                n_estimators=200,
                max_depth=3,          # reducido de 4 → menos overfitting
                learning_rate=0.05,
                subsample=0.7,
                min_samples_leaf=5,
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
                reg_alpha=0.1,        # L1 regularización (nuevo)
                reg_lambda=1.5,       # L2 regularización (aumentado)
                min_child_weight=3,   # mínimo muestras por hoja (nuevo)
                random_state=42,
                eval_metric='logloss',
                verbosity=0
            ),
            {}
        ),
    }

    # Walk-forward: entrena en pasado, valida en futuro (datos ya ordenados por tiempo)
    cv          = TimeSeriesSplit(n_splits=5)
    best_score  = 0
    best_name   = ""
    auc_scores  = {}
    model_names = list(models.keys())

    print("\n" + "="*60)
    print("🤖 ENTRENANDO ENSEMBLE ML (walk-forward CV)")
    print(f"   Dataset: {len(y)} samples | Positivos: {pos} ({pos/len(y)*100:.1f}%) | Negativos: {neg}")
    print("="*60)

    for name, (model, fit_params) in models.items():
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

        mean_score        = scores.mean()
        auc_scores[name]  = mean_score
        print(f"\n  {name}:")
        print(f"    AUC-ROC: {mean_score:.3f} ± {scores.std():.3f}")

        if mean_score > best_score:
            best_score = mean_score
            best_name  = name

    print(f"\n🏆 Mejor modelo: {best_name} (AUC: {best_score:.3f})")

    # Entrenar TODOS los modelos en datos completos y construir ensemble
    feature_cols = get_feature_columns()
    ensemble = {'models': [], 'weights': [], 'feature_cols': feature_cols}

    print("\n🔧 Entrenando ensemble en datos completos...")
    for name, (model, fit_params) in models.items():
        if fit_params:
            model.fit(X, y, **fit_params)
        else:
            model.fit(X, y)
        ensemble['models'].append(model)
        ensemble['weights'].append(auc_scores.get(name, 0.5))
        print(f"  ✅ {name} (peso AUC={auc_scores.get(name, 0.5):.3f})")

    # Normalizar pesos por AUC para soft voting
    total_w = sum(ensemble['weights'])
    ensemble['weights'] = [w / total_w for w in ensemble['weights']]
    print(f"  📊 Pesos normalizados: " +
          " | ".join(f"{n}={w:.3f}" for n, w in zip(model_names, ensemble['weights'])))

    # Feature importance del mejor modelo (para logging)
    best_idx = model_names.index(best_name)
    best_fitted = ensemble['models'][best_idx]
    if hasattr(best_fitted, 'feature_importances_'):
        importances = pd.Series(
            best_fitted.feature_importances_, index=feature_cols
        ).sort_values(ascending=False)
        print("\n📊 FEATURES MÁS IMPORTANTES (mejor modelo):")
        for feat, imp in importances.head(10).items():
            bar = "█" * int(imp * 50)
            print(f"  {feat:<28} {bar} {imp:.3f}")

    return ensemble, best_name, best_score

def evaluate_model(ensemble, X, y):
    """Evaluación detallada con reporte y simulación de trading."""
    split  = int(len(y) * 0.8)
    X_test = X[split:]
    y_test = y[split:]

    if isinstance(ensemble, dict):
        # Ensemble ya entrenado en full data — predecir sobre el 20% más reciente
        print("\n📋 REPORTE ENSEMBLE (últimas 20% muestras temporales):")
        y_proba = np.zeros(len(y_test))
        for model, weight in zip(ensemble['models'], ensemble['weights']):
            y_proba += weight * model.predict_proba(X_test)[:, 1]
    else:
        X_train, y_train = X[:split], y[:split]
        ensemble.fit(X_train, y_train)
        print("\n📋 REPORTE DE CLASIFICACIÓN (test 20%):")
        y_proba = ensemble.predict_proba(X_test)[:, 1]

    y_pred = (y_proba >= 0.5).astype(int)
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

def save_model(ensemble, le, feature_cols, X_train_df=None):
    with open(MODEL_PATH, 'wb') as f:
        pickle.dump(ensemble, f)
    # Guardar training_stats para feature drift detection (5 features clave)
    drift_cols = ['buys_5m', 'sells_5m', 'market_cap', 'liquidity_usd', 'survival_score']
    training_stats = {}
    if X_train_df is not None:
        for col in drift_cols:
            if col in X_train_df.columns:
                training_stats[col] = X_train_df[col].dropna().values.tolist()
    with open(FEATURES_PATH, 'wb') as f:
        pickle.dump({'label_encoder': le, 'features': feature_cols,
                     'training_stats': training_stats}, f)
    n = len(ensemble['models']) if isinstance(ensemble, dict) else 1
    log.info(f"✅ Ensemble guardado ({n} modelos) en {MODEL_PATH}")


def check_feature_drift() -> list:
    """
    KS test entre distribución de features en training vs últimas 48h.
    Retorna lista de features con drift significativo (p < 0.05).
    """
    try:
        from scipy.stats import ks_2samp
    except ImportError:
        log.warning("scipy no disponible — feature drift detection omitida")
        return []

    if not os.path.exists(FEATURES_PATH):
        return []

    with open(FEATURES_PATH, 'rb') as f:
        saved = pickle.load(f)
    training_stats = saved.get('training_stats', {})
    if not training_stats:
        return []

    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT buys_5m, sells_5m, market_cap, liquidity_usd, survival_score
            FROM discovered_tokens
            WHERE created_at > NOW() - INTERVAL '48 hours'
              AND signal IN ('STRONG_BUY', 'BUY')
              AND buys_5m IS NOT NULL
            LIMIT 500
        """)
        rows = cur.fetchall()
        cur.close()
    finally:
        pool.putconn(conn)

    if len(rows) < 20:
        return []

    recent = pd.DataFrame(rows,
        columns=['buys_5m', 'sells_5m', 'market_cap', 'liquidity_usd', 'survival_score'])

    drifted = []
    for col in training_stats:
        if col not in recent.columns:
            continue
        train_arr  = np.array(training_stats[col])
        recent_arr = recent[col].dropna().values
        if len(recent_arr) < 10:
            continue
        _, p_value = ks_2samp(train_arr, recent_arr)
        if p_value < 0.05:
            drifted.append(f"{col}(p={p_value:.3f})")

    if drifted:
        log.warning(f"⚠️  Feature drift en {len(drifted)} features: {drifted}")
    else:
        log.info("✅ Sin feature drift detectado")
    return drifted

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

    # Ordenar cronológicamente antes del walk-forward split
    if 'captured_at' in df.columns:
        df = df.sort_values('captured_at').reset_index(drop=True)
        df = df.drop(columns=['captured_at'])

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

    ensemble, best_name, cv_auc = train_models(X, y)
    test_auc = evaluate_model(ensemble, X, y)
    save_model(ensemble, le, feature_cols, X_train_df=df[['buys_5m','sells_5m','market_cap','liquidity_usd','survival_score']])
    save_metrics(best_name, test_auc, len(y), int(y.sum()))

    weights_str = " | ".join(
        f"{n}={w:.3f}" for n, w in zip(
            ['RF', 'GB', 'XGB'], ensemble.get('weights', [])
        )
    )
    log.info(f"✅ Ensemble | Best: {best_name} | CV AUC: {cv_auc:.3f} | Test AUC: {test_auc:.3f}")
    log.info(f"   Pesos: {weights_str} | Target: +{TARGET_GAIN*100:.0f}% | Samples: {len(y)}")

    # ── FEATURE DRIFT DETECTION ────────────────────────
    drifted = check_feature_drift()
    if len(drifted) >= 3:
        from telegram_bot import alert_feature_drift
        alert_feature_drift(drifted)

    # ── BACKTEST AUTOMÁTICO POST-ENTRENAMIENTO ─────────
    log.info("🔬 Lanzando backtest con nuevo modelo...")
    try:
        import backtester
        backtester.run_backtest()
    except Exception as e:
        log.warning(f"Backtest post-training falló: {e}")

if __name__ == "__main__":
    run()
