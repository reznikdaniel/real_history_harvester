import pandas as pd
import numpy as np
import joblib
from tensorflow.keras.models import load_model
import os

print("🧠 Запуск MLOps Pipeline: Continuous Training")

FILE_NAME = "real_ground_history.csv"
MODEL_NAME = "forecast_ai_model.h5"
SCALER_NAME = "forecast_scaler.pkl"

def retrain():
    if not os.path.exists(FILE_NAME) or os.path.getsize(FILE_NAME) == 0:
        print("❌ Data Lake порожній. Немає на чому вчитися.")
        return
        
    print("📥 Завантаження даних з Озера...")
    df = pd.read_csv(FILE_NAME)
    df['time'] = pd.to_datetime(df['time'])
    
    print("⚙️ Завантаження існуючої нейромережі та скейлера...")
    try:
        model = load_model(MODEL_NAME, compile=True) # compile=True важливо для навчання
        scaler = joblib.load(SCALER_NAME)
    except Exception as e:
        print(f"❌ Помилка завантаження моделі: {e}")
        return

    # Підготовка даних (нарізка на вікна 24->2)
    X_train, y_train = [], []
    
    # Групуємо по унікальним станціям, щоб не змішувати дані різних міст
    for uid, group in df.groupby('uid'):
        group = group.sort_values('time')
        values = group[['pm2.5', 'pm10', 'no2']].values
        
        # Нам треба мінімум 26 годин (24 історія + 2 прогноз)
        if len(values) < 26:
            continue
            
        # Масштабуємо дані тим самим скейлером
        scaled_values = scaler.transform(values)
        
        for i in range(len(scaled_values) - 25):
            # Вхід: 24 години
            X_train.append(scaled_values[i:i+24])
            
            # Вихід: 4 значення (pm2.5_1h, pm10_1h, pm2.5_2h, pm10_2h)
            y_train.append([
                scaled_values[i+24, 0], # pm2.5 через 1 годину
                scaled_values[i+24, 1], # pm10 через 1 годину
                scaled_values[i+25, 0], # pm2.5 через 2 години
                scaled_values[i+25, 1]  # pm10 через 2 години
            ])

    if not X_train:
        print("⚠️ Недостатньо безперервних даних для навчання (потрібно більше часу збору).")
        return

    X_train = np.array(X_train)
    y_train = np.array(y_train)
    
    print(f"🎓 Починаю донавчання на {len(X_train)} нових патернах...")
    
    # Дообучаємо модель (epochs=3 достатньо, щоб не забути старе, але вивчити нове)
    model.fit(X_train, y_train, epochs=3, batch_size=32, verbose=1)
    
    print("💾 Збереження позумнішалої моделі...")
    model.save(MODEL_NAME)
    print("✅ MLOps Pipeline успішно завершено!")

if __name__ == "__main__":
    retrain()
