import asyncio
import aiohttp
import pandas as pd
import aiosqlite
from datetime import datetime, timedelta, date, time
from typing import List, Dict, Optional, Tuple
import logging
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.error import TelegramError
import json
from urllib.parse import quote
from newsapi import NewsApiClient
import os
import re
import sys
import pytz
from dotenv import load_dotenv

# Настройка event loop для Windows
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# ---------- Загрузка .env ----------
load_dotenv()

# ---------- БЕЗОПАСНОСТЬ ----------
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)
logging.getLogger("aiohttp").setLevel(logging.WARNING)

class TokenMaskingFilter(logging.Filter):
    def filter(self, record):
        if hasattr(record, 'msg') and isinstance(record.msg, str):
            record.msg = re.sub(r'bot\d+:[A-Za-z0-9_-]+', 'bot***:HIDDEN', record.msg)
        return True

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('falling_stocks.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

for handler in logging.root.handlers:
    handler.addFilter(TokenMaskingFilter())

logger = logging.getLogger(__name__)

# ---------- НАСТРОЙКИ ----------
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_USER_ID = int(os.getenv("TELEGRAM_USER_ID", "0"))
NEWS_API_KEY = os.getenv("NEWS_API_KEY", "")

if not TELEGRAM_BOT_TOKEN or TELEGRAM_BOT_TOKEN in ["YOUR_BOT_TOKEN", ""]:
    raise ValueError("❌ TELEGRAM_BOT_TOKEN не настроен в .env!")
if TELEGRAM_USER_ID == 0:
    raise ValueError("❌ TELEGRAM_USER_ID не настроен в .env!")

# Параметры сканирования
DROP_PERCENT = float(os.getenv("DROP_PERCENT", "10.0"))
DAYS_BACK = int(os.getenv("DAYS_BACK", "3"))
MIN_VOLUME = int(os.getenv("MIN_VOLUME", "10000000"))
STOP_LOSS_PERCENT = float(os.getenv("STOP_LOSS_PERCENT", "2.0"))
CACHE_DB = "moex_cache.db"
CACHE_TTL_DAYS = int(os.getenv("CACHE_TTL_DAYS", "1"))
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", "10"))

# ---------- MOEX API (ИСПРАВЛЕНО) ----------
class MoexAPI:
    BASE = 'https://iss.moex.com/iss'
    
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self._lock = asyncio.Lock()
    
    async def get_session(self) -> aiohttp.ClientSession:
        async with self._lock:
            if self.session is None or self.session.closed:
                timeout = aiohttp.ClientTimeout(total=30, connect=10)
                connector = aiohttp.TCPConnector(
                    limit=50,
                    limit_per_host=10,
                    ttl_dns_cache=300,
                    force_close=True,
                    enable_cleanup_closed=True
                )
                self.session = aiohttp.ClientSession(
                    timeout=timeout,
                    connector=connector,
                    headers={
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                        'Accept': 'application/json'
                    }
                )
            return self.session
    
    async def close(self):
        async with self._lock:
            if self.session and not self.session.closed:
                await self.session.close()
                self.session = None
    
    async def request(self, url: str, retries: int = 3) -> Optional[dict]:
        """Запрос с повторными попытками"""
        for attempt in range(retries):
            try:
                s = await self.get_session()
                async with s.get(url, ssl=False) as r:
                    if r.status == 200:
                        return await r.json()
                    elif r.status == 429:
                        wait_time = 2 ** attempt
                        logger.warning(f"⏳ Rate limit, ждем {wait_time}с...")
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        logger.warning(f"HTTP {r.status} для {url[:100]}")
                        if attempt < retries - 1:
                            await asyncio.sleep(1)
                            continue
            except asyncio.TimeoutError:
                logger.error(f"⏰ Таймаут запроса (попытка {attempt+1}/{retries})")
                if attempt < retries - 1:
                    await asyncio.sleep(1)
            except aiohttp.ClientError as e:
                logger.error(f"🌐 Сетевая ошибка (попытка {attempt+1}/{retries}): {e}")
                if attempt < retries - 1:
                    await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"❌ Ошибка запроса: {e}")
                break
        return None
    
    async def get_all_tickers(self) -> Optional[pd.DataFrame]:
        logger.info("📋 Загружаем список акций...")
        
        sec_url = f"{self.BASE}/engines/stock/markets/shares/boards/TQBR/securities.json?iss.meta=off"
        mkt_url = f"{self.BASE}/engines/stock/markets/shares/boards/TQBR/securities.json?iss.only=marketdata&iss.meta=off"
        
        sec_data, mkt_data = await asyncio.gather(
            self.request(sec_url),
            self.request(mkt_url)
        )
        
        if not sec_data or 'securities' not in sec_data:
            logger.error("❌ Не удалось получить список бумаг")
            return None
        
        if not mkt_data or 'marketdata' not in mkt_data:
            logger.error("❌ Не удалось получить рыночные данные")
            return None
        
        try:
            sec_cols = sec_data['securities']['columns']
            sec_rows = sec_data['securities']['data']
            df_sec = pd.DataFrame(sec_rows, columns=sec_cols)
            
            mkt_cols = mkt_data['marketdata']['columns']
            mkt_rows = mkt_data['marketdata']['data']
            df_mkt = pd.DataFrame(mkt_rows, columns=mkt_cols)
            
            df = df_sec[['SECID', 'SHORTNAME']].merge(
                df_mkt[['SECID', 'LAST', 'VALTODAY']], on='SECID', how='left'
            )
            df = df.rename(columns={'VALTODAY': 'VOLUME_RUB'})
            
            logger.info(f"✅ Загружено {len(df)} акций")
            return df
            
        except Exception as e:
            logger.error(f"❌ Ошибка обработки данных: {e}")
            return None
    
    async def get_history(self, ticker: str, days_back: int = 30) -> Optional[pd.DataFrame]:
        till = datetime.now()
        frm = till - timedelta(days=days_back)
        
        url = (f"{self.BASE}/engines/stock/markets/shares/boards/TQBR/securities/{ticker}/candles.json"
               f"?from={frm.strftime('%Y-%m-%d')}&till={till.strftime('%Y-%m-%d')}"
               f"&interval=24&iss.meta=off&iss.only=candles")
        
        data = await self.request(url)
        if not data or 'candles' not in data:
            return None
        
        rows = data['candles']['data']
        cols = data['candles']['columns']
        
        if not rows:
            return None
        
        df = pd.DataFrame(rows, columns=cols)
        df = df.rename(columns={'begin': 'date'})
        df['date'] = pd.to_datetime(df['date']).dt.date
        
        for col in ['open', 'high', 'low', 'close', 'volume']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        df = df.dropna(subset=['close']).sort_values('date')
        return df

# ---------- КЭШ ----------
async def init_cache():
    try:
        async with aiosqlite.connect(CACHE_DB) as db:
            await db.execute('''
                CREATE TABLE IF NOT EXISTS history_cache (
                    ticker TEXT PRIMARY KEY,
                    data TEXT NOT NULL,
                    updated_at REAL NOT NULL
                )
            ''')
            await db.commit()
        logger.info("✅ Кэш инициализирован")
    except Exception as e:
        logger.error(f"❌ Ошибка кэша: {e}")

async def get_cached_prices(ticker: str) -> Optional[Dict]:
    try:
        async with aiosqlite.connect(CACHE_DB) as db:
            async with db.execute(
                "SELECT data, updated_at FROM history_cache WHERE ticker = ?", (ticker,)
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    data_json, updated_at = row
                    if datetime.now().timestamp() - updated_at < CACHE_TTL_DAYS * 24 * 3600:
                        return json.loads(data_json)
    except Exception as e:
        logger.error(f"❌ Ошибка чтения кэша: {e}")
    return None

async def set_cached_prices(ticker: str, data: Dict):
    try:
        async with aiosqlite.connect(CACHE_DB) as db:
            await db.execute(
                "INSERT OR REPLACE INTO history_cache VALUES (?, ?, ?)",
                (ticker, json.dumps(data), datetime.now().timestamp())
            )
            await db.commit()
    except Exception as e:
        logger.error(f"❌ Ошибка сохранения кэша: {e}")

# ---------- TRADINGVIEW ----------
def get_tradingview_link(ticker: str) -> str:
    symbol = f"MOEX:{ticker}"
    encoded = quote(symbol, safe='')
    return f"https://ru.tradingview.com/chart/?symbol={encoded}"

def create_tradingview_keyboard(ticker: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(f"📊 {ticker} на TradingView", url=get_tradingview_link(ticker))
    ]])

# ---------- NEWS ----------
def init_newsapi():
    if NEWS_API_KEY and NEWS_API_KEY != "YOUR_NEWS_API_KEY":
        try:
            return NewsApiClient(api_key=NEWS_API_KEY)
        except Exception as e:
            logger.warning(f"⚠️ NewsAPI не инициализирован: {e}")
    return None

newsapi = init_newsapi()

def get_news_for_ticker(ticker: str, name: str) -> Optional[Tuple[str, str]]:
    if not newsapi:
        return None
    
    try:
        query = f'"{ticker}" OR "{name}"'
        from_date = (datetime.now() - timedelta(days=DAYS_BACK + 1)).strftime('%Y-%m-%d')
        
        all_articles = newsapi.get_everything(
            q=query,
            language='ru',
            from_param=from_date,
            sort_by='relevancy',
            page_size=3
        )
        
        if all_articles.get('articles'):
            article = all_articles['articles'][0]
            title = article.get('title', '')
            url = article.get('url', '')
            
            if ' - ' in title:
                title = title.split(' - ')[0]
            if len(title) > 70:
                title = title[:67] + "..."
            
            return (title, url)
    except Exception as e:
        logger.debug(f"Новости для {ticker}: {e}")
    
    return None

# ---------- РАСЧЕТ СТОП-ЛОССА ----------
def calculate_stop_loss(current_price: float, stop_loss_percent: float) -> Dict:
    """
    Правильный расчет стоп-лосса для LONG позиции:
    - stop_loss_price = current_price * (1 - stop_loss_percent / 100)
    - Риск на акцию = current_price - stop_loss_price
    - Тейк-профит = current_price * (1 + (stop_loss_percent * 2) / 100)  (R/R = 1:2)
    
    Пример: цена 100₽, SL 2%
    SL цена = 100 * 0.98 = 98₽
    Риск = 2₽
    TP = 100 * 1.04 = 104₽
    """
    if current_price <= 0 or stop_loss_percent <= 0:
        return {
            'current_price': 0,
            'stop_loss_percent': 0,
            'stop_loss_price': 0,
            'stop_loss_amount': 0,
            'risk_per_share': 0,
            'take_profit_price': 0,
            'potential_profit_per_share': 0,
            'risk_reward_ratio': 'N/A'
        }
    
    sl_price = round(current_price * (1 - stop_loss_percent / 100), 2)
    risk_per_share = round(current_price - sl_price, 2)
    take_profit_price = round(current_price * (1 + (stop_loss_percent * 2) / 100), 2)
    potential_profit_per_share = round(take_profit_price - current_price, 2)
    
    return {
        'current_price': current_price,
        'stop_loss_percent': stop_loss_percent,
        'stop_loss_price': sl_price,
        'stop_loss_amount': risk_per_share,
        'risk_per_share': risk_per_share,
        'take_profit_price': take_profit_price,
        'potential_profit_per_share': potential_profit_per_share,
        'risk_reward_ratio': f'1:{round(potential_profit_per_share/risk_per_share, 1) if risk_per_share > 0 else 0}'
    }

# ---------- АНАЛИЗ ----------
async def analyze_ticker(api: MoexAPI, ticker: str, name: str, volume: float) -> Optional[Dict]:
    try:
        cached = await get_cached_prices(ticker)
        
        if cached:
            records = []
            for date_str, close_val in cached.items():
                try:
                    d = datetime.strptime(date_str, '%Y-%m-%d').date()
                    records.append({'date': d, 'close': float(close_val)})
                except (ValueError, TypeError):
                    continue
            
            if not records:
                return None
            df = pd.DataFrame(records)
        else:
            df = await api.get_history(ticker, days_back=DAYS_BACK + 10)
            
            if df is not None and not df.empty:
                cache_data = {}
                for _, row in df.iterrows():
                    try:
                        d = row['date']
                        if hasattr(d, 'strftime'):
                            cache_data[d.strftime('%Y-%m-%d')] = float(row['close'])
                        else:
                            cache_data[str(d)] = float(row['close'])
                    except Exception:
                        continue
                if cache_data:
                    await set_cached_prices(ticker, cache_data)
        
        if df is None or df.empty:
            return None
        
        df = df.sort_values('date').reset_index(drop=True)
        
        if len(df) < 2:
            return None
        
        latest = df.iloc[-1]
        latest_price = float(latest['close'])
        latest_date = latest['date']
        
        if hasattr(latest_date, 'date'):
            latest_date = latest_date.date()
        
        if latest_price <= 0:
            return None
        
        target_date = latest_date - timedelta(days=DAYS_BACK)
        old_candidates = df[df['date'] <= target_date].copy()
        
        if old_candidates.empty:
            if len(df) >= 2:
                old_candidates = df.head(1)
            else:
                return None
        
        old = old_candidates.iloc[-1]
        old_price = float(old['close'])
        old_date = old['date']
        
        if hasattr(old_date, 'date'):
            old_date = old_date.date()
        
        if old_price <= 0:
            return None
        
        change_pct = ((latest_price - old_price) / old_price) * 100
        
        if change_pct <= -DROP_PERCENT:
            news_info = get_news_for_ticker(ticker, name)
            stop_loss = calculate_stop_loss(latest_price, STOP_LOSS_PERCENT)
            
            result = {
                'ticker': ticker,
                'name': name,
                'change_pct': round(change_pct, 2),
                'date_from': str(old_date),
                'price_from': round(old_price, 2),
                'date_to': str(latest_date),
                'price_to': round(latest_price, 2),
                'volume_rub': volume,
                'stop_loss': stop_loss,
                'news_title': news_info[0] if news_info else None,
                'news_url': news_info[1] if news_info else None
            }
            
            logger.info(
                f"🔻 {ticker}: {change_pct:+.2f}% "
                f"({old_date} {old_price}₽ → {latest_date} {latest_price}₽) "
                f"| SL: {stop_loss['stop_loss_price']}₽ | V: {volume:,.0f}₽"
            )
            return result
    
    except Exception as e:
        logger.error(f"🔥 Ошибка анализа {ticker}: {e}")
    
    return None

# ---------- СКАНИРОВАНИЕ ----------
async def scan_market() -> List[Dict]:
    await init_cache()
    logger.info(f"🔍 Сканирование: падение ≥{DROP_PERCENT}% за {DAYS_BACK} дн., объем ≥{MIN_VOLUME/1e6:.0f} млн ₽")
    
    api = MoexAPI()
    
    try:
        all_shares = await api.get_all_tickers()
        if all_shares is None or all_shares.empty:
            logger.error("❌ Не удалось загрузить список акций")
            return []
        
        all_shares['VOLUME_RUB'] = pd.to_numeric(all_shares['VOLUME_RUB'], errors='coerce').fillna(0)
        liquid = all_shares[all_shares['VOLUME_RUB'] >= MIN_VOLUME].copy()
        
        logger.info(f"✅ Ликвидных (≥{MIN_VOLUME/1e6:.0f} млн ₽): {len(liquid)}/{len(all_shares)}")
        
        if liquid.empty:
            logger.warning("⚠️ Нет акций с достаточным объемом торгов")
            return []
        
        sem = asyncio.Semaphore(MAX_CONCURRENT)
        
        async def analyze_with_limit(row):
            async with sem:
                return await analyze_ticker(
                    api, row['SECID'], row['SHORTNAME'], row['VOLUME_RUB']
                )
        
        tasks = [analyze_with_limit(row) for _, row in liquid.iterrows()]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        falling = [r for r in results if r is not None and not isinstance(r, Exception)]
        falling.sort(key=lambda x: x['change_pct'])
        
        logger.info(f"🎯 Найдено падающих акций: {len(falling)}")
        return falling[:15]
        
    finally:
        await api.close()

# ---------- ФОРМАТИРОВАНИЕ ----------
def escape_html(text: str) -> str:
    if text is None:
        return ""
    return str(text).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

# ---------- ОТПРАВКА В TELEGRAM ----------
async def send_results_to_telegram(results: List[Dict], context: ContextTypes.DEFAULT_TYPE = None):
    """Отправка результатов в Telegram"""
    try:
        bot = Bot(token=TELEGRAM_BOT_TOKEN)
        
        if not results:
            text = (
                f"📉 <b>Отчет о падающих акциях</b>\n\n"
                f"😞 Акций с падением ≥{DROP_PERCENT}% не найдено\n"
                f"💰 Мин. объем: {MIN_VOLUME/1e6:.0f} млн ₽\n"
                f"🛑 Стоп-лосс: {STOP_LOSS_PERCENT}%\n\n"
                f"🕒 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} МСК"
            )
            await bot.send_message(
                chat_id=TELEGRAM_USER_ID,
                text=text,
                parse_mode='HTML'
            )
            return
        
        header = (
            f"📊 <b>ПАДАЮЩИЕ АКЦИИ MOEX</b>\n"
            f"{'=' * 30}\n"
            f"Найдено: <b>{len(results)}</b> акций\n"
            f"Падение ≥{DROP_PERCENT}% за {DAYS_BACK} дн.\n"
            f"Мин. объем: {MIN_VOLUME/1e6:.0f} млн ₽\n"
            f"Стоп-лосс: <b>{STOP_LOSS_PERCENT}%</b>\n"
            f"Соотношение R/R: 1:2\n"
        )
        await bot.send_message(
            chat_id=TELEGRAM_USER_ID,
            text=header,
            parse_mode='HTML'
        )
        
        for i, r in enumerate(results, 1):
            sl = r['stop_loss']
            text = (
                f"🔻 <b>#{i} {r['ticker']}</b> — {escape_html(r['name'])}\n"
                f"{'─' * 25}\n"
                f"📉 Падение: <b>{r['change_pct']}%</b>\n"
                f"💰 Цена входа: <b>{r['price_to']}₽</b>\n"
                f"🛑 Стоп-лосс: <b>{sl['stop_loss_price']}₽</b> (-{STOP_LOSS_PERCENT}%)\n"
                f"⚠️ Риск на акцию: <b>{sl['risk_per_share']}₽</b>\n"
                f"🎯 Тейк-профит: <b>{sl['take_profit_price']}₽</b> (+{sl['potential_profit_per_share']}₽)\n"
                f"📊 R/R: <b>{sl['risk_reward_ratio']}</b>\n"
                f"📅 Период: {r['date_from']} → {r['date_to']}\n"
                f"💵 Объем: {r['volume_rub']:,.0f}₽"
            )
            
            if r['news_title'] and r['news_url']:
                text += f"\n📰 <a href='{r['news_url']}'>{escape_html(r['news_title'])}</a>"
            
            await bot.send_message(
                chat_id=TELEGRAM_USER_ID,
                text=text,
                parse_mode='HTML',
                reply_markup=create_tradingview_keyboard(r['ticker'])
            )
            await asyncio.sleep(0.3)
        
        footer = f"✅ Отчет завершен\n🕒 {datetime.now().strftime('%H:%M:%S')} МСК"
        await bot.send_message(
            chat_id=TELEGRAM_USER_ID,
            text=footer,
            parse_mode='HTML'
        )
        
        logger.info(f"✅ Отправлено {len(results)} сигналов")
        
    except TelegramError as e:
        logger.error(f"🔥 Ошибка Telegram: {e}")

# ---------- КОМАНДЫ БОТА ----------
async def start_cmd(update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "🚀 <b>БОТ ПАДАЮЩИХ АКЦИЙ MOEX</b>\n\n"
        f"🔍 Падение ≥{DROP_PERCENT}% за {DAYS_BACK} дня\n"
        f"💰 Мин. объем: {MIN_VOLUME/1e6:.0f} млн ₽\n"
        f"🛑 Стоп-лосс: {STOP_LOSS_PERCENT}%\n"
        f"📊 R/R: 1:2\n\n"
        "<b>Команды:</b>\n"
        "/scan — сканирование сейчас\n"
        "/status — статус\n"
        "/help — помощь\n\n"
        "🕐 Автоуведомления: <b>17:30 МСК</b>"
    )
    await update.message.reply_text(text, parse_mode='HTML')

async def scan_cmd(update, context: ContextTypes.DEFAULT_TYPE):
    msg = await update.message.reply_text(
        "🔍 <b>Запускаю сканирование...</b>\n⏳ Подождите 1-2 минуты",
        parse_mode='HTML'
    )
    
    try:
        results = await scan_market()
        await msg.delete()
        await send_results_to_telegram(results, context)
    except Exception as e:
        logger.error(f"Ошибка сканирования: {e}")
        await msg.edit_text(f"❌ Ошибка: {escape_html(str(e)[:200])}")

async def status_cmd(update, context: ContextTypes.DEFAULT_TYPE):
    msk_tz = pytz.timezone('Europe/Moscow')
    now_msk = datetime.now(msk_tz)
    
    next_run = now_msk.replace(hour=17, minute=30, second=0, microsecond=0)
    if now_msk > next_run:
        next_run += timedelta(days=1)
    
    time_to_next = next_run - now_msk
    hours, remainder = divmod(time_to_next.seconds, 3600)
    minutes = remainder // 60
    
    text = (
        "📊 <b>СТАТУС БОТА</b>\n\n"
        f"✅ Бот активен\n"
        f"📅 Следующее сканирование: <b>сегодня в 17:30 МСК</b>\n"
        f"⏳ Через: {hours} ч. {minutes} мин.\n\n"
        "<b>Настройки:</b>\n"
        f"• Падение: ≥{DROP_PERCENT}%\n"
        f"• Период: {DAYS_BACK} дня\n"
        f"• Мин. объем: {MIN_VOLUME/1e6:.0f} млн ₽\n"
        f"• Стоп-лосс: {STOP_LOSS_PERCENT}%\n"
        f"• R/R: 1:2\n\n"
        "<b>Формула SL:</b>\n"
        f"SL = Цена × (1 - {STOP_LOSS_PERCENT}/100)\n"
        "Пример: 100₽ - 2% = 98₽"
    )
    await update.message.reply_text(text, parse_mode='HTML')

async def help_cmd(update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "📚 <b>ПОМОЩЬ</b>\n\n"
        "<b>Как работает:</b>\n"
        "1. Сканирование в 17:30 МСК\n"
        "2. Анализ акций MOEX\n"
        "3. Поиск падения ≥10% за 3 дня\n"
        "4. Расчет стоп-лосса и тейк-профита\n\n"
        "<b>Стоп-лосс:</b>\n"
        f"SL = Цена × (1 - {STOP_LOSS_PERCENT}/100)\n"
        "Пример: 100₽ × 0.98 = 98₽\n"
        "Риск: 2₽ на акцию\n\n"
        "<b>Команды:</b>\n"
        "/scan — ручной запуск\n"
        "/status — статус\n"
        "/start — главная"
    )
    await update.message.reply_text(text, parse_mode='HTML')

# ---------- АВТОСКАНИРОВАНИЕ ----------
async def scheduled_scan_1730(context: ContextTypes.DEFAULT_TYPE):
    logger.info("=" * 50)
    logger.info("🕔 ЗАПУСК АВТОСКАНИРОВАНИЯ (17:30 МСК)")
    logger.info("=" * 50)
    
    try:
        results = await scan_market()
        await send_results_to_telegram(results, context)
        logger.info("✅ Автосканирование завершено")
    except Exception as e:
        logger.error(f"💥 Ошибка автосканирования: {e}", exc_info=True)
        try:
            bot = Bot(token=TELEGRAM_BOT_TOKEN)
            await bot.send_message(
                chat_id=TELEGRAM_USER_ID,
                text=f"❌ <b>Ошибка!</b>\n{escape_html(str(e)[:300])}",
                parse_mode='HTML'
            )
        except:
            pass

# ---------- ЗАПУСК ----------
def main():
    if not TELEGRAM_BOT_TOKEN:
        logger.error("❌ Токен не найден!")
        sys.exit(1)
    
    if TELEGRAM_USER_ID == 0:
        logger.error("❌ USER_ID не найден!")
        sys.exit(1)
    
    logger.info("=" * 50)
    logger.info("🚀 БОТ ПАДАЮЩИХ АКЦИЙ ЗАПУСКАЕТСЯ")
    logger.info("=" * 50)
    logger.info(f"📊 Падение: ≥{DROP_PERCENT}% за {DAYS_BACK} дн.")
    logger.info(f"💰 Мин. объем: {MIN_VOLUME/1e6:.0f} млн ₽")
    logger.info(f"🛑 Стоп-лосс: {STOP_LOSS_PERCENT}%")
    logger.info(f"📊 R/R: 1:2")
    
    demo_price = 100
    demo_sl = calculate_stop_loss(demo_price, STOP_LOSS_PERCENT)
    logger.info(f"💡 Пример SL: цена {demo_price}₽ → SL {demo_sl['stop_loss_price']}₽ → TP {demo_sl['take_profit_price']}₽")
    
    # Создаем приложение
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    
    # Добавляем команды
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("scan", scan_cmd))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(CommandHandler("help", help_cmd))
    
    # Планировщик без JobQueue (чтобы избежать ошибок с зависимостями)
    async def scheduler_loop():
        """Фоновый планировщик"""
        msk_tz = pytz.timezone('Europe/Moscow')
        
        while True:
            now = datetime.now(msk_tz)
            next_run = now.replace(hour=17, minute=30, second=0, microsecond=0)
            
            if now >= next_run:
                next_run += timedelta(days=1)
            
            wait_seconds = (next_run - now).total_seconds()
            logger.info(f"⏰ Следующее сканирование через {wait_seconds/3600:.1f} ч.")
            
            await asyncio.sleep(wait_seconds)
            
            logger.info("🕔 Запуск автосканирования (17:30 МСК)")
            try:
                results = await scan_market()
                
                bot = Bot(token=TELEGRAM_BOT_TOKEN)
                
                if results:
                    header = (
                        f"📊 <b>АВТООТЧЕТ: ПАДАЮЩИЕ АКЦИИ</b>\n"
                        f"📅 {datetime.now(msk_tz).strftime('%d.%m.%Y')}\n"
                        f"⏰ 17:30 МСК\n"
                        f"Найдено: <b>{len(results)}</b> акций"
                    )
                    await bot.send_message(chat_id=TELEGRAM_USER_ID, text=header, parse_mode='HTML')
                    
                    for i, r in enumerate(results, 1):
                        sl = r['stop_loss']
                        text = (
                            f"🔻 <b>#{i} {r['ticker']}</b> — {escape_html(r['name'])}\n"
                            f"📉 <b>{r['change_pct']}%</b> | 💰 {r['price_to']}₽\n"
                            f"🛑 SL: <b>{sl['stop_loss_price']}₽</b> | 🎯 TP: <b>{sl['take_profit_price']}₽</b>"
                        )
                        await bot.send_message(
                            chat_id=TELEGRAM_USER_ID,
                            text=text,
                            parse_mode='HTML',
                            reply_markup=create_tradingview_keyboard(r['ticker'])
                        )
                        await asyncio.sleep(0.5)
                    
                    await bot.send_message(chat_id=TELEGRAM_USER_ID, text="✅ Отчет завершен")
                else:
                    await bot.send_message(
                        chat_id=TELEGRAM_USER_ID,
                        text=f"📉 Акций с падением ≥{DROP_PERCENT}% не найдено"
                    )
                
            except Exception as e:
                logger.error(f"💥 Ошибка: {e}", exc_info=True)
    
    # Запускаем планировщик в фоне
    asyncio.create_task(scheduler_loop())
    
    print("\n" + "=" * 50)
    print("✅ Бот запущен!")
    print("=" * 50)
    print(f"📊 Падение ≥{DROP_PERCENT}% за {DAYS_BACK} дн.")
    print(f"🛑 SL: {STOP_LOSS_PERCENT}% | R/R: 1:2")
    print("🕐 Автоуведомления: 17:30 МСК")
    print("📋 Команды: /start, /scan, /status, /help")
    print("=" * 50)
    
    try:
        app.run_polling(allowed_updates=["message"])
    except KeyboardInterrupt:
        logger.info("👋 Бот остановлен")

if __name__ == '__main__':
    main()