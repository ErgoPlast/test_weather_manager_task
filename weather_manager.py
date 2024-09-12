import requests
import schedule
import time
import threading
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, Float, DateTime, String
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.sql import func
from datetime import datetime, timezone

API_URL = 'https://api.open-meteo.com/v1/forecast'

SQLITE_URL = 'sqlite:///weather_data.db'

COEF_PRESSURE = 0.7506

WEATHER_TIMER_MIN = 3

PARAMS = {
	    "latitude": 55.691830566,
	    "longitude": 37.354665248,
	    "current": ["temperature_2m", "surface_pressure", "wind_speed_10m", 
                 "wind_direction_10m", "rain", "showers", "snowfall"],
        "wind_speed_unit": "ms"
    }

Base = declarative_base()

class WeatherData(Base):
    __tablename__ = 'weather_data'
    id = Column('id', Integer, primary_key=True, nullable=False) 
    temperature = Column('temperature', Float)
    windspeed = Column('windspeed', Float)
    precipitation = Column('precipitation', Float)
    precipitation_type = Column('percipitation_type', String)
    surface_pressure = Column('surface_pressure', Float)
    wind_direction = Column('wind_direction', String)
    date = Column('date', DateTime(timezone=False), server_default=func.now())

engine = create_engine(SQLITE_URL, echo=False)
Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()

def fetch_weather() -> None:

    def degToCompass(num: float) -> str:
        """Функция для сопоставления градусов направления ветра с текстовыми значениями направлений."""
        val=int((num/22.5)+.5)
        arr=["С","ССВ","СВ","ВСВ","В","ВЮВ", "ЮВ", "ЮЮВ","Ю","ЮЮЗ","ЮЗ","ЗЮЗ","З","ЗСЗ","СЗ","ССЗ"]
        return arr[(val % 16)]
    
    """Функция для получения данных о погоде и записи их в базу данных."""
    try:
        response = requests.get(API_URL, params=PARAMS)
        data = response.json()
        if 'current' in data:
            temperature = data['current']['temperature_2m']
            windspeed = data['current']['wind_speed_10m']
            surface_pressure = round(data['current']['surface_pressure'] * COEF_PRESSURE, 2)
            wind_direction = degToCompass(data['current']['wind_direction_10m'])
            rain = data['current']['rain']
            showers = data['current']['showers']
            snowfall = data['current']['snowfall']
            if rain != 0.0:
                precipitation = rain
                precipitation_type = 'rain'
            elif showers != 0.0:
                precipitation = showers
                precipitation_type = 'showers'
            elif snowfall != 0.0:
                precipitation = snowfall
                precipitation_type = 'snowfall'
            else:
                precipitation_type = 'no precipitation'
                precipitation = 0.0

            # Запись данных в базу
            weather_entry = WeatherData(temperature=temperature, windspeed=windspeed,
                                        precipitation=precipitation, surface_pressure=surface_pressure, 
                                        wind_direction=wind_direction, precipitation_type=precipitation_type)
            
            session.add(weather_entry)
            session.commit()
            print("Данные записаны.")
        else:
            print("Ошибка в получении данных.")
    except Exception as e:
        print(f"Ошибка получения данных о погоде: {e}")


def export_to_excel() -> None:
    """Функция для экспорта данных из базы данных в Excel файл."""
    try:
        offset = datetime.now(timezone.utc).astimezone().utcoffset()
        query = session.query(WeatherData).all()
        data = [{
            'id': item.id,
            'temperature °C': item.temperature,
            'windspeed m/s': item.windspeed,
            'wind_direction': item.wind_direction,
            'precipitation mm': item.precipitation,
            'precipitation_type': item.precipitation_type,
            'surface_pressure mmHg': item.surface_pressure,
            'date': item.date + offset,
        } for item in query]

        df = pd.DataFrame(data)
        df.to_excel('weather_data.xlsx', index=False)
        print("Данные успешно экспортированы в weather_data.xlsx")
    except Exception as e:
        print(f"Ошибка экспорта данных: {e}")


def run_schedule() -> None:
    """Запуск расписания для сбора данных о погоде."""
    schedule.every(WEATHER_TIMER_MIN).minutes.do(fetch_weather)  # Периодичность сбора данных

    while True:
        schedule.run_pending()
        time.sleep(1)


def handle_console_commands():
    """Обработка команд из консоли."""
    while True:
        command = input("Введите команду: ")
        if command == "export":
            export_to_excel()
        elif command == "exit":
            print("Завершение работы...")
            session.close()
            break
        else:
            print("Неизвестная команда. Доступные команды: export, exit")


# Запуск двух параллельных потоков: для сбора данных и для команд из консоли
if __name__ == "__main__":
    # Поток для расписания
    schedule_thread = threading.Thread(target=run_schedule, daemon=True)
    schedule_thread.start()

    # Основной поток для работы с консолью
    handle_console_commands()