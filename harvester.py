import requests
import pandas as pd
import time
from datetime import datetime
import os

print("🌍 Big Data Harvester (Гібридний режим) запущено!")

API_TOKEN = "89bb553f1a69e42b0a03af6dd05c6b3a26aa2a70"
FILE_NAME = "real_ground_history.csv"

# Широкий квадрат для сусідів
BOUNDS = "43.0,20.0,54.0,40.0"

# Примусовий пошук для України (щоб обійти ліміти API)
UKRAINE_CITIES = ["Ukraine", "Kyiv", "Odesa", "Izmail", "Lviv", "Dnipro", "Kharkiv", "Mykolaiv", "Zaporizhzhia"]

# Фільтр ворожих станцій
BANNED_WORDS = ["Russia", "Belarus", "РФ", "Беларусь", "Russian Federation", "Moscow"]

def fetch_and_save():
    new_data = []
    current_time = datetime.now().strftime('%Y-%m-%d %H:00:00')
    print(f"\n[{current_time}] 📡 Формування списку цілей...")
    
    raw_stations = []
    
    # 1. Запит по квадрату (загальний)
    try:
        req1 = requests.get(f"https://api.waqi.info/map/bounds/?latlng={BOUNDS}&token={API_TOKEN}", timeout=10).json()
        if req1.get('status') == 'ok':
            raw_stations.extend(req1['data'])
    except Exception as e:
        print(f"Помилка карти: {e}")
        
    # 2. Примусові точкові запити по Україні
    for city in UKRAINE_CITIES:
        try:
            req2 = requests.get(f"https://api.waqi.info/search/?keyword={city}&token={API_TOKEN}", timeout=5).json()
            if req2.get('status') == 'ok':
                for s in req2['data']:
                    # Формат пошуку трохи відрізняється від формату карти, тому уніфікуємо
                    raw_stations.append({
                        'uid': s['uid'],
                        'station': {'name': s['station']['name']}
                    })
        except Exception as e:
            pass
        time.sleep(0.1) # Пауза проти бану
        
    # 3. Видалення дублікатів (за унікальним ID станції)
    unique_stations = {}
    for s in raw_stations:
        unique_stations[s['uid']] = s
        
    stations_list = list(unique_stations.values())
    print(f"🎯 Знайдено {len(stations_list)} унікальних станцій. Починаю детальний збір...")

    # 4. Збір показників повітря
    for s in stations_list:
        uid = s['uid']
        name = s.get('station', {}).get('name', 'Unknown')
        
        # Перевірка на бан-лист
        if any(banned.lower() in name.lower() for banned in BANNED_WORDS):
            continue
            
        try:
            details = requests.get(f"https://api.waqi.info/feed/@{uid}/?token={API_TOKEN}", timeout=5).json()
            
            if details.get('status') == 'ok':
                # 🛡️ ВАКЦИНА ВІД ЗОМБІ: Перевіряємо реальний час станції
                station_time_str = details['data'].get('time', {}).get('s')
                if station_time_str:
                    station_time = datetime.strptime(station_time_str, "%Y-%m-%d %H:%M:%S")
                    hours_dead = (datetime.now() - station_time).total_seconds() / 3600
                    if hours_dead > 12:
                        continue # Станція мертва, ігноруємо її і йдемо до наступної
                        
                iaqi = details['data'].get('iaqi', {})
                pm25 = float(iaqi.get('pm25', {}).get('v', 0))
                pm10 = float(iaqi.get('pm10', {}).get('v', 0))
                no2 = float(iaqi.get('no2', {}).get('v', 0))
                
                # Зберігаємо лише "живі" станції
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

    # 5. Запис у Data Lake
    if new_data:
        df_new = pd.DataFrame(new_data)
        
        if os.path.exists(FILE_NAME) and os.path.getsize(FILE_NAME) > 0:
            try:
                df_existing = pd.read_csv(FILE_NAME)
                df_combined = pd.concat([df_existing, df_new]).drop_duplicates(subset=['time', 'uid'], keep='last')
                df_combined.to_csv(FILE_NAME, index=False)
            except pd.errors.EmptyDataError:
                df_new.to_csv(FILE_NAME, index=False)
        else:
            df_new.to_csv(FILE_NAME, index=False)
        
        print(f"✅ Успішно завантажено {len(new_data)} записів. Місія виконана.")

if __name__ == "__main__":
    fetch_and_save()

