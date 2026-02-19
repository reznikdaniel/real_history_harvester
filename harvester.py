import requests
import pandas as pd
import time
from datetime import datetime
import os

print("🌍 Big Data Harvester (Східна Європа) запущено!")
print("Скануємо Україну та сусідів...")

API_TOKEN = "89bb553f1a69e42b0a03af6dd05c6b3a26aa2a70"
FILE_NAME = "real_ground_history.csv"
BOUNDS = "43.0,20.0,54.0,40.0"
BANNED_WORDS = ["Russia", "Belarus", "РФ", "Беларусь", "Russian Federation"]

def fetch_and_save():
    new_data = []
    current_time = datetime.now().strftime('%Y-%m-%d %H:00:00')
    print(f"\n[{current_time}] 📡 Пошук датчиків у заданому квадраті...")
    
    try:
        map_url = f"https://api.waqi.info/map/bounds/?latlng={BOUNDS}&token={API_TOKEN}"
        stations_req = requests.get(map_url, timeout=10).json()
        
        if stations_req.get('status') != 'ok':
            print("❌ Помилка отримання карти.")
            return

        stations = stations_req['data']
        print(f"Знайдено {len(stations)} активних станцій. Починаю детальний збір...")

        for s in stations:
            uid = s['uid']
            name = s['station']['name']
            
            if any(banned.lower() in name.lower() for banned in BANNED_WORDS):
                continue
                
            try:
                details = requests.get(f"https://api.waqi.info/feed/@{uid}/?token={API_TOKEN}", timeout=5).json()
                
                if details.get('status') == 'ok':
                    iaqi = details['data'].get('iaqi', {})
                    pm25 = float(iaqi.get('pm25', {}).get('v', 0))
                    pm10 = float(iaqi.get('pm10', {}).get('v', 0))
                    no2 = float(iaqi.get('no2', {}).get('v', 0))
                    
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
                pass 
            
            time.sleep(0.1)

        if new_data:
            df_new = pd.DataFrame(new_data)
            
            # 🌟 ВИПРАВЛЕННЯ: Перевіряємо не тільки існування файлу, але і чи він НЕ порожній (> 0 байт)
            if os.path.exists(FILE_NAME) and os.path.getsize(FILE_NAME) > 0:
                try:
                    df_existing = pd.read_csv(FILE_NAME)
                    df_combined = pd.concat([df_existing, df_new]).drop_duplicates(subset=['time', 'uid'], keep='last')
                    df_combined.to_csv(FILE_NAME, index=False)
                except pd.errors.EmptyDataError:
                    # Якщо файл якось поламався, створюємо поверх нього новий
                    df_new.to_csv(FILE_NAME, index=False)
            else:
                # Якщо файлу немає або він порожній (0 байт)
                df_new.to_csv(FILE_NAME, index=False)
            
            print(f"✅ Успішно завантажено {len(new_data)} записів у Data Lake.")

    except Exception as e:
        print(f"❌ Критична помилка циклу: {e}")

if __name__ == "__main__":
    fetch_and_save()
        print(f"❌ Критична помилка циклу: {e}")

if __name__ == "__main__":

    fetch_and_save()
