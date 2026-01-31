import requests
import psycopg2

API_KEY = "491aa7d49972f473f438afcbdea7e9ad"
CITY = "Kolkata"

def fetch_and_load():
    url = f"https://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    temperature_c = data.get("main", {}).get("temp")
    humidity = data.get("main", {}).get("humidity")

    weather_list = data.get("weather", [])
    weather_desc = weather_list[0]["description"] if len(weather_list) > 0 else None

    conn = psycopg2.connect(
        host="postgres",
        dbname="weather_db",
        user="airflow",
        password="airflow",
        port=5432
    )

    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO weather_raw (city, temperature_c, humidity, weather)
        VALUES (%s, %s, %s, %s)
        """,
        (CITY, temperature_c, humidity, weather_desc)
    )

    conn.commit()
    cur.close()
    conn.close()