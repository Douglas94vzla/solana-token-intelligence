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
from sklearn.model_selection import cross_val_score, StratifiedKFold
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import classification_report, confusion_matrix
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
TARGET_GAIN = 0.5        # Token "ganador" si sube 50%+
MIN_SNAPSHOTS = 10       # Mínimo de snapshots para incluir
MODEL_PATH = '/root/solana_bot/model.pkl'
FEATURES_PATH = '/root/solana_bot/features.pkl'

def load_training_data():
    """
    Carga datos de entrenamiento con target binario:
    1 = token subió 50%+ desde detección
    0 = token no subió 50%
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
                dt.survival_score,
                dt.narrative,
                dt.buys_1h,
                dt.sells_1h,
                COALESCE(dt.rug_score, 50) as rug_score,
                COALESCE(dt.holder_count, 10) as holder_count,
                COALESCE(dt.top10_concentration, 95) as top10_concentration,
                -- Target: precio máximo vs precio inicial
                MIN(ps.price_usd) as price_entry,
                MAX(ps.price_usd) as price_max,
                COUNT(ps.id) as snapshot_count,
                -- Calcular si ganó
                CASE 
                    WHEN MAX(ps.price_usd) >= MIN(ps.price_usd) * (1 + %s)
                    THEN 1 ELSE 0 
                END as target
            FROM discovered_tokens dt
            JOIN price_snapshots ps ON ps.mint = dt.mint
            WHERE dt.buys_5m IS NOT NULL
            AND dt.survival_score IS NOT NULL
            AND dt.market_cap IS NOT NULL
            AND dt.market_cap > 0
            AND dt.created_at > NOW() - INTERVAL '7 days'
            GROUP BY dt.mint, dt.buys_5m, dt.sells_5m, dt.market_cap,
                     dt.volume_24h, dt.survival_score, dt.narrative,
                     dt.buys_1h, dt.sells_1h, dt.rug_score,
                     dt.holder_count, dt.top10_concentration
            HAVING COUNT(ps.id) >= %s
        """, (TARGET_GAIN, MIN_SNAPSHOTS))
        
        rows = cur.fetchall()
        cur.close()
        
        columns = [
            'mint', 'buys_5m', 'sells_5m', 'market_cap', 'volume_24h',
            'survival_score', 'narrative', 'buys_1h', 'sells_1h',
            'rug_score', 'holder_count', 'top10_concentration',
            'price_entry', 'price_max', 'snapshot_count', 'target'
        ]
        
        df = pd.DataFrame(rows, columns=columns)
        log.info(f"✅ {len(df)} tokens cargados para entrenamiento")
        log.info(f"   Ganadores (target=1): {df['target'].sum()} ({df['target'].mean()*100:.1f}%)")
        log.info(f"   Perdedores (target=0): {(1-df['target']).sum()} ({(1-df['target']).mean()*100:.1f}%)")
        return df
    finally:
        pool.putconn(conn)

def engineer_features(df):
    """Crea features derivadas más poderosas"""
    
    # Ratio compras/ventas
    df['buy_sell_ratio'] = df['buys_5m'] / (df['sells_5m'] + 1)
    df['buy_sell_ratio_1h'] = df['buys_1h'] / (df['sells_1h'] + 1)
    
    # Presión compradora neta
    df['net_buy_pressure'] = df['buys_5m'] - df['sells_5m']
    
    # Volumen relativo al market cap
    df['vol_to_mcap'] = df['volume_24h'] / (df['market_cap'] + 1)
    
    # Market cap en rangos logarítmicos
    df['log_mcap'] = np.log1p(df['market_cap'])
    df['log_volume'] = np.log1p(df['volume_24h'])
    
    # Score normalizado
    df['score_normalized'] = df['survival_score'] / 100.0
    
    # Concentración de holders (riesgo)
    df['holder_risk'] = df['top10_concentration'] / 100.0
    
    # Narrativa codificada
    le = LabelEncoder()
    df['narrative_encoded'] = le.fit_transform(df['narrative'].fillna('OTHER'))
    
    # Señal de 0 sells (muy fuerte)
    df['zero_sells'] = (df['sells_5m'] == 0).astype(int)
    
    # MCap en rango óptimo ($1K-$5K)
    df['optimal_mcap'] = ((df['market_cap'] >= 1000) & (df['market_cap'] <= 5000)).astype(int)
    
    return df, le

def get_feature_columns():
    return [
        'buys_5m', 'sells_5m', 'market_cap', 'volume_24h',
        'survival_score', 'buys_1h', 'sells_1h',
        'rug_score', 'holder_count', 'top10_concentration',
        'buy_sell_ratio', 'buy_sell_ratio_1h', 'net_buy_pressure',
        'vol_to_mcap', 'log_mcap', 'log_volume', 'score_normalized',
        'holder_risk', 'narrative_encoded', 'zero_sells', 'optimal_mcap'
    ]

def train_models(X, y):
    """Entrena múltiples modelos y elige el mejor"""
    
    models = {
        'RandomForest': RandomForestClassifier(
            n_estimators=200,
            max_depth=8,
            min_samples_leaf=3,
            class_weight='balanced',
            random_state=42
        ),
        'GradientBoosting': GradientBoostingClassifier(
            n_estimators=200,
            max_depth=4,
            learning_rate=0.05,
            random_state=42
        ),
        'XGBoost': XGBClassifier(
            n_estimators=200,
            max_depth=4,
            learning_rate=0.05,
            scale_pos_weight=len(y[y==0])/max(len(y[y==1]), 1),
            random_state=42,
            eval_metric='logloss',
            verbosity=0
        )
    }
    
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    best_model = None
    best_score = 0
    best_name = ""
    
    print("\n" + "="*60)
    print("🤖 ENTRENANDO MODELOS ML")
    print("="*60)
    
    for name, model in models.items():
        scores = cross_val_score(model, X, y, cv=cv, scoring='roc_auc')
        mean_score = scores.mean()
        std_score = scores.std()
        
        print(f"\n  {name}:")
        print(f"    AUC-ROC: {mean_score:.3f} ± {std_score:.3f}")
        
        if mean_score > best_score:
            best_score = mean_score
            best_model = model
            best_name = name
    
    print(f"\n🏆 Mejor modelo: {best_name} (AUC: {best_score:.3f})")
    
    # Entrenar el mejor modelo con todos los datos
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
            print(f"  {feat:<25} {bar} {imp:.3f}")
    
    return best_model, best_name, best_score

def evaluate_model(model, X, y):
    """Evaluación detallada del modelo"""
    from sklearn.model_selection import train_test_split
    
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1]
    
    print("\n📋 REPORTE DE CLASIFICACIÓN:")
    print(classification_report(y_test, y_pred, target_names=['Perdedor', 'Ganador']))
    
    print("🔢 MATRIZ DE CONFUSIÓN:")
    cm = confusion_matrix(y_test, y_pred)
    print(f"  Verdaderos Negativos: {cm[0][0]} | Falsos Positivos: {cm[0][1]}")
    print(f"  Falsos Negativos:     {cm[1][0]} | Verdaderos Positivos: {cm[1][1]}")
    
    # Simulación de trading con el modelo
    print("\n💰 SIMULACIÓN DE TRADING:")
    capital = 100.0
    bet = 10.0
    threshold = 0.6  # Solo entrar si modelo dice 60%+ de probabilidad
    
    trades = 0
    wins = 0
    for prob, actual in zip(y_proba, y_test):
        if prob >= threshold:
            trades += 1
            if actual == 1:
                capital += bet * 0.5  # +50% ganancia
                wins += 1
            else:
                capital -= bet * 0.5  # -50% pérdida
    
    if trades > 0:
        print(f"  Trades ejecutados (prob≥{threshold}): {trades}")
        print(f"  Win rate: {wins/trades*100:.1f}%")
        print(f"  Capital final: ${capital:.2f}")
        print(f"  ROI: {(capital-100)/100*100:+.1f}%")

def save_model(model, le, feature_cols):
    with open(MODEL_PATH, 'wb') as f:
        pickle.dump(model, f)
    with open(FEATURES_PATH, 'wb') as f:
        pickle.dump({'label_encoder': le, 'features': feature_cols}, f)
    log.info(f"✅ Modelo guardado en {MODEL_PATH}")

def predict_token(mint):
    """Predice la probabilidad de éxito de un token en tiempo real"""
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
                   survival_score, narrative, buys_1h, sells_1h,
                   COALESCE(rug_score, 50), COALESCE(holder_count, 10),
                   COALESCE(top10_concentration, 95)
            FROM discovered_tokens WHERE mint = %s
        """, (mint,))
        row = cur.fetchone()
        cur.close()
        
        if not row:
            return None
        
        df = pd.DataFrame([row], columns=[
            'buys_5m', 'sells_5m', 'market_cap', 'volume_24h',
            'survival_score', 'narrative', 'buys_1h', 'sells_1h',
            'rug_score', 'holder_count', 'top10_concentration'
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
    
    df = df.apply(lambda col: pd.to_numeric(col, errors="ignore"))
    df, le = engineer_features(df)
    feature_cols = get_feature_columns()
    
    X = df[feature_cols].fillna(0)
    y = df['target'].values
    
    log.info(f"📊 Dataset: {X.shape[0]} samples, {X.shape[1]} features")
    
    # Entrenar y evaluar
    model, name, score = train_models(X, y)
    evaluate_model(model, X, y)
    
    # Guardar
    save_model(model, le, feature_cols)
    
    print(f"\n✅ Modelo {name} entrenado con AUC: {score:.3f}")
    print(f"   Guardado en {MODEL_PATH}")
    print(f"   Listo para predecir tokens en tiempo real\n")

if __name__ == "__main__":
    run()
