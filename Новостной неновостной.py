import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

TIKER = 'LKOH'
START_DAY = '2025-10-01'
END_DAY = '2025-11-09'
END_NEWS = '2025-11-10'
KEY_WORDS = ['LKOH','нефть']
NUMBER = 10


df_volumes = pd.read_excel(f'/Users/ilyalebedev/Desktop/Работа/Аналитика/Анализ объемов торгов в течение дня/volume_{TIKER}_{START_DAY}-{END_DAY}.xlsx')
df_news = pd.read_excel(f'/Users/ilyalebedev/Desktop/Работа/Аналитика/Анализ объемов торгов в течение дня/news_{START_DAY}-{END_NEWS}.xlsx')




df_news['has_keyword'] = df_news['text'].str.contains('|'.join(KEY_WORDS), case=False, na=False)
result_days = (df_news.groupby('date_only')['has_keyword']
               .sum()  
               .loc[lambda x: x > NUMBER]  
               .index)

result_days_dates = [pd.to_datetime(date).date() for date in result_days]

df_volumes['date_only'] = pd.to_datetime(df_volumes['date_only']).dt.date
df_keyword_days = df_volumes[df_volumes['date_only'].isin(result_days_dates)].copy()
df_normal_days = df_volumes[~df_volumes['date_only'].isin(result_days_dates)].copy()

print(f"Дней с ключевыми словами: {len(df_keyword_days['date_only'].unique())}")
print(f"Обычных дней: {len(df_normal_days['date_only'].unique())}")






def create_average_volume(df, group_name):

    if df.empty:
        print(f"Нет данных для группы: {group_name}")
        return pd.Series(dtype=float)
    
    full_df = pd.DataFrame()

    for i in df['time_only'].unique():

        minute_avg = df[df['time_only'] == i]['volume'].mean()

        full_df = pd.concat([full_df, pd.DataFrame({
            'time_only': [i],
            'avg_volume': [minute_avg]
        })], ignore_index=True)

    return full_df
    

keyword_df = create_average_volume(df_keyword_days, "Ключевые дни")
normal_df = create_average_volume(df_normal_days, "Обычные дни")

print('Период: ' + START_DAY + '  ' + END_DAY)
print('Ключевые дни: ' + str(keyword_df['avg_volume'].sum()))
print('Обычные дни: ' + str(normal_df['avg_volume'].sum()))
print('Ключевое слово: ' + KEY_WORDS[0])
print('Кол-во вхождений: ' + str(NUMBER))


# Создаем кумулятивные суммы
keyword_df = keyword_df.sort_values('time_only')
normal_df = normal_df.sort_values('time_only')

keyword_df['cumulative_volume'] = keyword_df['avg_volume'].cumsum()
normal_df['cumulative_volume'] = normal_df['avg_volume'].cumsum()

# Преобразуем время в числовой формат для построения графика
keyword_df['time_numeric'] = pd.to_datetime(keyword_df['time_only']).dt.hour + pd.to_datetime(keyword_df['time_only']).dt.minute / 60 + pd.to_datetime(keyword_df['time_only']).dt.second / 3600
normal_df['time_numeric'] = pd.to_datetime(normal_df['time_only']).dt.hour + pd.to_datetime(normal_df['time_only']).dt.minute / 60 + pd.to_datetime(normal_df['time_only']).dt.second / 3600
plt.figure(figsize=(14, 8))

plt.plot(keyword_df['time_numeric'], 
         keyword_df['cumulative_volume'], 
         marker='o', 
         linewidth=2, 
         markersize=3, 
         color='red',
         label='Дни с ключевыми словами',
         alpha=0.8)

plt.plot(normal_df['time_numeric'], 
         normal_df['cumulative_volume'], 
         marker='o', 
         linewidth=2, 
         markersize=3, 
         color='blue',
         label='Обычные дни',
         alpha=0.8)

plt.title('Сравнение кумулятивных объемов: дни с новостями vs обычные дни')
plt.xlabel('Время (часы)')
plt.ylabel('Накопленная сумма среднего объема')
plt.grid(True, alpha=0.3)
plt.legend(title='Тип дней')
plt.xticks(range(0, 25, 2))  # Метки каждые 2 часа
plt.tight_layout()
plt.show()

print(f"Итоговый объем в дни с ключевыми словами: {keyword_df['cumulative_volume'].iloc[-1]:.2f}")
print(f"Итоговый объем в обычные дни: {normal_df['cumulative_volume'].iloc[-1]:.2f}")