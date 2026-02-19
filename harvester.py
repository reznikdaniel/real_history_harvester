import requests
import pandas as pd
import time
from datetime import datetime
import os

print("🌍 Big Data Harvester (Східна Європа) запущено!")
print("Скануємо Україну та сусідів...")

API_TOKEN = "89bb553f1a69e42b0a03af6dd05c6b3a26aa2a70"
FILE_NAME = "real_ground_history.csv"

# Координати прямокутника (lat_min, lon_min, lat_max, lon_max)
# Цей квадрат накриває Україну, Польщу, Румунію, Молдову, Угорщину та Словаччину
BOUNDS = "43.0,20.0,54.0,40.0"

# Країни, дані з яких ми принципово не збираємо
BANNED_WORDS = ["Russia", "Belarus", "РФ", "Беларусь", "Russian Federation"]

def fetch_and_save():
    new_data = []
    current_time = datetime.now().strftime('%Y-%m-%d %H:00:00')
    print(f"\n[{current_time}] 📡 Пошук датчиків у заданому квадраті...")
    
    try:
        # 1. Отримуємо список УСІХ станцій в нашому квадраті
        map_url = f"https://api.waqi.info/map/bounds/?latlng={BOUNDS}&token={API_TOKEN}"
        stations_req = requests.get(map_url, timeout=10).json()
        
        if stations_req.get('status') != 'ok':
            print("❌ Помилка отримання карти.")
            return

        stations = stations_req['data']
        print(f"Знайдено {len(stations)} активних станцій. Починаю детальний збір...")

        # 2. Опитуємо кожну станцію персонально
        for s in stations:
            uid = s['uid']
            name = s['station']['name']
            
            # Фільтр від небажаних країн
            if any(banned.lower() in name.lower() for banned in BANNED_WORDS):
                continue
                
            try:
               
                details = requests.get(f"https://api.waqi.info/feed/@{uid}/?token={API_TOKEN}", timeout=5).json()
                
                if details.get('status') == 'ok':
                    iaqi = details['data'].get('iaqi', {})
                    pm25 = float(iaqi.get('pm25', {}).get('v', 0))
                    pm10 = float(iaqi.get('pm10', {}).get('v', 0))
                    no2 = float(iaqi.get('no2', {}).get('v', 0))
                    
                    # Зберігаємо тільки якщо датчик реально щось міряє (не вимкнений)
                    if pm25 > 0 or pm10 > 0:
                        new_data.append({
                            'time': current_time,
                            'station': name,
                            'uid': uid,
                            'pm2.5': pm25,
                            'pm10': pm10,
                            'no2': no2
                        })
            except Exception as e:
                pass # Якщо одна станція "відвалилась", просто йдемо до наступної
            
            # 🛡️ АНТИ-БАН СИСТЕМА: 
           
            time.sleep(0.1)

        # 3. Збереження масиву даних
        if new_data:
            df_new = pd.DataFrame(new_data)
            if os.path.exists(FILE_NAME):
                df_existing = pd.read_csv(FILE_NAME)
                # Зшиваємо нове зі старим, унікальність перевіряємо по часу та ID станції
                df_combined = pd.concat([df_existing, df_new]).drop_duplicates(subset=['time', 'uid'], keep='last')
                df_combined.to_csv(FILE_NAME, index=False)
            else:
                df_new.to_csv(FILE_NAME, index=False)
            
            print(f"✅ Успішно завантажено {len(new_data)} записів у Data Lake.")

    except Exception as e:
        print(f"❌ Критична помилка циклу: {e}")

if __name__ == "__main__":
    fetch_and_save()