import asyncio
from telethon import TelegramClient
import pandas as pd
from datetime import datetime, timedelta
import pytz

class TelegramParser:
    def __init__(self, api_id, api_hash, start_day=None, end_day=None):
        self.api_id = api_id
        self.api_hash = api_hash
        
        # Делаем все даты "aware" (с часовым поясом)
        tz = pytz.UTC
        self.start_day = start_day or (datetime.now(tz) - timedelta(days=7))
        self.end_day = end_day or datetime.now(tz)
        
        # Если передали "naive" даты - конвертируем в "aware"
        if self.start_day.tzinfo is None:
            self.start_day = tz.localize(self.start_day)
        if self.end_day.tzinfo is None:
            self.end_day = tz.localize(self.end_day)
            
        self.client = None
    
    async def _parsing_tg(self, channel_name, limit=None):
        if not self.client:
            self.client = TelegramClient('session', self.api_id, self.api_hash)
            await self.client.start()
            print("Подключение установлено!")
        
        messages_data = []
        
        try:
            # Ищем сообщения в нужном диапазоне дат
            async for message in self.client.iter_messages(
                channel_name,
                offset_date=self.end_day,  # Начинаем с конца периода
                reverse=False  # Идем от новых к старым
            ):
                # Если сообщение старше начальной даты - прерываем
                if message.date < self.start_day:
                    break
                
                # Если сообщение в нужном диапазоне - добавляем
                if self.start_day <= message.date <= self.end_day:
                    if message.text:
                        messages_data.append({
                            'id': message.id,
                            'date': message.date,
                            'text': message.text
                        })
                
                # Ограничение по количеству
                if limit and len(messages_data) >= limit:
                    break
                        
        except Exception as e:
            print(f"Ошибка при парсинге: {e}")
        
        return messages_data
    
    def get_df(self, messages):
        if not messages:
            return pd.DataFrame()
        
        df = pd.DataFrame(messages)
        df['date_only'] = df['date'].dt.date
        df['time_only'] = df['date'].dt.time
        df = df.sort_values('date', ascending=False).reset_index(drop=True)
        
        # Преобразуем даты в наивные (без часового пояса) для Excel
        df['date'] = df['date'].dt.tz_localize(None)
        
        return df
    
    async def stop(self):
        if self.client:
            await self.client.disconnect()
            print("Отключились!")

async def main():
    API_ID = 28528337
    API_HASH = "4dcc884fc5246582747ef00436ba534f"
    
    start_date = datetime.now() - timedelta(days=30)
    end_date = datetime.now()
    
    parser = TelegramParser(
        api_id=API_ID, 
        api_hash=API_HASH,
        start_day=start_date,
        end_day=end_date
    )
    
    try:
        messages = await parser._parsing_tg("@breakingmash")
        df = parser.get_df(messages)
        print(f"Сообщений за период: {len(df)}")
        print(df.head())
        
    finally:
        await parser.stop()

async def quick_parse(channel, start_date, end_date, api_id, api_hash, limit=None):
    parser = TelegramParser(api_id, api_hash, start_date, end_date)
    messages = await parser._parsing_tg(channel, limit=limit)
    df = parser.get_df(messages)
    await parser.stop()
    return df


if __name__ == "__main__":
    # Пример с конкретными датами (исправлены даты на прошедший период)
    start = datetime(2025, 10, 1)  
    end = datetime(2025, 11, 10) 
    
    df = asyncio.run(quick_parse(
        channel="@markettwits",
        start_date=start,
        end_date=end,
        api_id=28528337,
        api_hash="4dcc884fc5246582747ef00436ba534f"
    ))
    print(df)
    
    # Форматируем даты для имени файла
    start_str = start.strftime("%Y-%m-%d")
    end_str = end.strftime("%Y-%m-%d")
    df.to_excel(f'news_{start_str}-{end_str}.xlsx', index=False)
    print(f"Файл сохранен: news_{start_str}-{end_str}.xlsx")