import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Подключение к базе данных
conn = sqlite3.connect('database/data.db')

# Чтение данных
finance_data = pd.read_sql_query("SELECT * FROM finance_data", conn)
weather_data = pd.read_sql_query("SELECT * FROM weather_data", conn)
osm_data = pd.read_sql_query("SELECT * FROM osm_data", conn)

# Анализ финансовых данных
finance_data['change'] = ((finance_data['close'] - finance_data['open']) / finance_data['open']) * 100
average_prices = finance_data[['open', 'close']].mean()
correlation = finance_data[['open', 'close', 'volume']].corr()

# Графики для финансовых данных
plt.figure(figsize=(12, 6))
sns.lineplot(data=finance_data, x='date', y='open', label='Open Price')
sns.lineplot(data=finance_data, x='date', y='close', label='Close Price')
plt.title('Financial Data: Open vs Close')
plt.xlabel('Date')
plt.ylabel('Price')
plt.legend()
plt.savefig('data/processed/financial_analysis.png')

# Анализ климатических данных
average_temp = weather_data['temperature'].mean()
average_humidity = weather_data['humidity'].mean()

# График температуры
plt.figure(figsize=(12, 6))
sns.barplot(data=weather_data, x='city', y='temperature')
plt.title('Average Temperature by City')
plt.xlabel('City')
plt.ylabel('Temperature (K)')
plt.savefig('data/processed/weather_analysis.png')

# Анализ OSM данных
osm_avg_length = osm_data['average_length'].mean()

print(f"Average Prices:\n{average_prices}")
print(f"Correlation:\n{correlation}")
print(f"Average Temperature: {average_temp}")
print(f"Average Humidity: {average_humidity}")
print(f"OSM Average Road Length: {osm_avg_length}")

conn.close()