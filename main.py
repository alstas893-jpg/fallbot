import asyncio
import aiohttp
import pandas as pd
from datetime import datetime, timedelta
import os
from telegram import Bot
from dotenv import load_dotenv

# ---------- Загрузка настроек ----------
load_dotenv()

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = int(os.getenv("TELEGRAM_USER_ID", "0"))

DROP_PERCENT = float(os.getenv("DROP_PERCENT", "10"))
DAYS_BACK = int(os.getenv("DAYS_BACK", "3"))
MIN_VOLUME = int(os.getenv("MIN_VOLUME", "10000000"))
STOP_LOSS_PERCENT = float(os.getenv("STOP_LOSS_PERCENT", "2"))

BASE = "https://iss.moex.com/iss"

# ---------- API ----------
class MoexAPI:
    def __init__(self):
        self.session = None

    async def get_session(self):
        if not self.session:
            self.session = aiohttp.ClientSession()
        return self.session

    async def request(self, url):
        s = await self.get_session()
        try:
            async with s.get(url) as r:
                if r.status == 200:
                    return await r.json()
        except:
            return None

    async def get_tickers(self):
        sec = await self.request(f"{BASE}/engines/stock/markets/shares/boards/TQBR/securities.json")
        mkt = await self.request(f"{BASE}/engines/stock/markets/shares/boards/TQBR/securities.json?iss.only=marketdata")

        df_sec = pd.DataFrame(sec['securities']['data'], columns=sec['securities']['columns'])
        df_mkt = pd.DataFrame(mkt['marketdata']['data'], columns=mkt['marketdata']['columns'])

        df = df_sec[['SECID', 'SHORTNAME']].merge(
            df_mkt[['SECID', 'LAST', 'VALTODAY', 'VOLTODAY']],
            on='SECID',
            how='left'
        )

        df['LAST'] = pd.to_numeric(df['LAST'], errors='coerce')
        df['VALTODAY'] = pd.to_numeric(df['VALTODAY'], errors='coerce').fillna(0)
        df['VOLTODAY'] = pd.to_numeric(df['VOLTODAY'], errors='coerce').fillna(0)

        df.rename(columns={
            'VALTODAY': 'VOLUME_RUB',
            'VOLTODAY': 'VOLUME_QTY'
        }, inplace=True)

        return df

    async def get_history(self, ticker):
        till = datetime.now()
        frm = till - timedelta(days=DAYS_BACK + 5)

        url = f"{BASE}/engines/stock/markets/shares/securities/{ticker}/candles.json?from={frm:%Y-%m-%d}&till={till:%Y-%m-%d}&interval=24"

        data = await self.request(url)
        if not data or not data.get("candles"):
            return None

        df = pd.DataFrame(data['candles']['data'], columns=data['candles']['columns'])
        df['close'] = pd.to_numeric(df['close'], errors='coerce')
        return df.dropna()

# ---------- Логика ----------
def calculate_stop_loss(price):
    sl = price * (1 - STOP_LOSS_PERCENT / 100)
    return round(sl, 2), round(price - sl, 2)

async def analyze(api, row):
    ticker = row['SECID']
    name = row['SHORTNAME']
    volume = row['VOLUME_RUB']
    last_price = row['LAST']

    if pd.isna(last_price) or last_price <= 0:
        return None

    df = await api.get_history(ticker)
    if df is None or len(df) < 2:
        return None

    old_price = df.iloc[0]['close']
    change = (last_price - old_price) / old_price * 100

    if change <= -DROP_PERCENT:
        sl_price, risk = calculate_stop_loss(last_price)

        return {
            'ticker': ticker,
            'name': name,
            'change': round(change, 2),
            'price': round(last_price, 2),
            'old_price': round(old_price, 2),
            'sl': sl_price,
            'risk': risk,
            'volume': volume
        }

    return None

# ---------- Скан ----------
async def scan():
    api = MoexAPI()
    df = await api.get_tickers()

    df = df[
        (df['VOLUME_RUB'] >= MIN_VOLUME) &
        (df['VOLUME_QTY'] > 0) &
        (df['LAST'] > 0)
    ]

    tasks = [analyze(api, row) for _, row in df.iterrows()]
    results = await asyncio.gather(*tasks)

    return [r for r in results if r]

# ---------- Telegram ----------
async def send(results):
    bot = Bot(token=TOKEN)

    if not results:
        await bot.send_message(chat_id=CHAT_ID, text="Нет сигналов")
        return

    for r in sorted(results, key=lambda x: x['change']):
        text = (
            f"🔻 {r['ticker']} ({r['name']})\n"
            f"Падение: {r['change']}%\n"
            f"{r['old_price']} → {r['price']}\n"
            f"SL: {r['sl']} (риск {r['risk']})\n"
            f"Объем: {int(r['volume']):,} ₽"
        )
        await bot.send_message(chat_id=CHAT_ID, text=text)

# ---------- Запуск ----------
async def main():
    results = await scan()
    await send(results)

if __name__ == "__main__":
    asyncio.run(main())