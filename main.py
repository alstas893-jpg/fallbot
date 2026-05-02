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
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", "20"))

# ---------- MOEX API ----------
class MoexAPI:
    BASE = 'https://iss.moex.com/iss'
    
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def get_session(self) -> aiohttp.ClientSession:
        if not self.session or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=60)
            connector = aiohttp.TCPConnector(force_close=True)
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
        if self.session and not self.session.closed:
            await self.session.close()
    
    async def request(self, url: str) -> Optional[dict]:
        s = await self.get_session()
        try:
            async with s.get(url) as r:
                if r.status == 200:
                    return await r.json()
                else:
                    logger.warning(f"HTTP {r.status} для {url[:100]}")
        except asyncio.TimeoutError:
            logger.error(f"⏰ Таймаут запроса: {url[:100]}")
        except Exception as e:
            logger.error(f"Ошибка запроса: {e}")
        return None
    
    async def get_all_tickers(self) -> Optional[pd.DataFrame]:
        logger.info("📋 Загружаем список акций...")
        
        sec_url = f"{self.BASE}/engines/stock/markets/shares/boards/TQBR/securities.json?iss.meta=off"
        sec_data = await self.request(sec_url)
        
        if not sec_data or 'securities' not in sec_data:
            logger.error("❌ Не удалось получить список бумаг")
            return None
        
        mkt_url = f"{self.BASE}/engines/stock/markets/shares/boards/TQBR/securities.json?iss.only=marketdata&iss.meta=off"
        mkt_data = await self.request(mkt_url)
        
        if not mkt_data or 'marketdata' not in mkt_data:
            logger.error("❌ Не удалось получить рыночные данные")
            return None
        
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
def calculate_stop_loss(price: float, stop_loss_percent: float) -> Dict:
    """
    Расчет уровней стоп-лосса
    Возвращает словарь с уровнями
    """
    if price <= 0 or stop_loss_percent <= 0:
        return {
            'current_price': 0,
            'stop_loss_percent': 0,
            'stop_loss_price': 0,
            'stop_loss_amount': 0,
            'risk_per_share': 0
        }
    
    sl_price = round(price * (1 - stop_loss_percent / 100), 2)
    sl_amount = round(price - sl_price, 2)
    
    return {
        'current_price': price,
        'stop_loss_percent': stop_loss_percent,
        'stop_loss_price': sl_price,
        'stop_loss_amount': sl_amount,
        'risk_per_share': sl_amount
    }

# ---------- АНАЛИЗ (ИСПРАВЛЕНО) ----------
async def analyze_ticker(api: MoexAPI, ticker: str, name: str, volume: float) -> Optional[Dict]:
    try:
        # 1. Получаем данные (из кэша или API)
        cached = await get_cached_prices(ticker)
        
        if cached:
            # Преобразуем словарь в DataFrame
            records = []
            for date_str, close_val in cached.items():
                try:
                    d = datetime.strptime(date_str, '%Y-%m-%d').date()
                    records.append({'date': d, 'close': float(close_val)})
                except (ValueError, TypeError):
                    continue
            
            if not records:
                logger.debug(f"{ticker}: нет валидных записей в кэше")
                return None
            df = pd.DataFrame(records)
        else:
            # Запрашиваем с запасом +10 дней для надежности
            df = await api.get_history(ticker, days_back=DAYS_BACK + 10)
            
            if df is not None and not df.empty:
                # Сохраняем в кэш
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
        
        # 2. Проверки данных
        if df is None or df.empty:
            logger.debug(f"{ticker}: нет данных для анализа")
            return None
        
        df = df.sort_values('date').reset_index(drop=True)
        
        if len(df) < 2:
            logger.debug(f"{ticker}: недостаточно данных (только {len(df)} точка)")
            return None
        
        # 3. Последняя цена и дата
        latest = df.iloc[-1]
        latest_price = float(latest['close'])
        latest_date = latest['date']
        
        # Приводим дату к date, если это datetime
        if hasattr(latest_date, 'date'):
            latest_date = latest_date.date()
        
        if latest_price <= 0:
            logger.warning(f"{ticker}: некорректная последняя цена {latest_price}")
            return None
        
        # 4. Целевая дата (DAYS_BACK календарных дней назад)
        target_date = latest_date - timedelta(days=DAYS_BACK)
        
        # 5. Ищем старую цену по дате, а не по индексу!
        old_candidates = df[df['date'] <= target_date].copy()
        
        if old_candidates.empty:
            # Если нет данных за целевой период — берем самую старую доступную запись
            if len(df) >= 2:
                # Берем самую первую запись
                old_candidates = df.head(1)
                logger.debug(
                    f"{ticker}: нет данных за {target_date}, "
                    f"использую самую старую: {old_candidates.iloc[0]['date']}"
                )
            else:
                return None
        
        old = old_candidates.iloc[-1]  # Самая поздняя запись среди подходящих
        old_price = float(old['close'])
        old_date = old['date']
        
        # Приводим дату к date
        if hasattr(old_date, 'date'):
            old_date = old_date.date()
        
        if old_price <= 0:
            logger.warning(f"{ticker}: некорректная старая цена {old_price}")
            return None
        
        # 6. Считаем процент изменения
        change_pct = ((latest_price - old_price) / old_price) * 100
        
        # 7. Проверяем порог падения
        if change_pct <= -DROP_PERCENT:
            # Получаем новости
            news_info = get_news_for_ticker(ticker, name)
            
            # Рассчитываем стоп-лосс
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
        else:
            logger.debug(f"{ticker}: падение {change_pct:+.2f}% (порог: -{DROP_PERCENT}%)")
    
    except Exception as e:
        logger.error(f"🔥 Ошибка анализа {ticker}: {e}", exc_info=True)
    
    return None

# ---------- СКАНИРОВАНИЕ ----------
async def scan_market() -> List[Dict]:
    await init_cache()
    logger.info(f"🔍 Сканирование: падение ≥{DROP_PERCENT}% за {DAYS_BACK} дн., объем ≥{MIN_VOLUME/1e6:.0f} млн ₽, SL={STOP_LOSS_PERCENT}%")
    
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

def format_results(results: List[Dict]) -> str:
    if not results:
        return "😞 Акций с заданными критериями не найдено."
    
    msg = f"📊 <b>ПАДАЮЩИЕ АКЦИИ (≥{DROP_PERCENT}% за {DAYS_BACK} дн.)</b>\n"
    msg += f"💰 Мин. объем: {MIN_VOLUME/1e6:.0f} млн ₽ | 🛑 SL: {STOP_LOSS_PERCENT}%\n"
    msg += "=" * 30 + "\n"
    
    for r in results:
        sl = r['stop_loss']
        msg += f"\n🔻 <b>{r['ticker']}</b> ({escape_html(r['name'])})\n"
        msg += f"   📉 Падение: <b>{r['change_pct']}%</b>\n"
        msg += f"   💰 Цена: {r['price_from']}₽ → {r['price_to']}₽\n"
        msg += f"   🛑 Стоп-лосс: <b>{sl['stop_loss_price']}₽</b> (-{STOP_LOSS_PERCENT}%)\n"
        msg += f"   ⚠️ Риск на акцию: {sl['risk_per_share']}₽\n"
        msg += f"   📅 Период: {r['date_from']} → {r['date_to']}\n"
        msg += f"   📊 Объем: {r['volume_rub']:,.0f}₽\n"
        
        if r['news_title'] and r['news_url']:
            msg += f"   📰 <a href='{r['news_url']}'>{escape_html(r['news_title'])}</a>\n"
        elif r['news_title']:
            msg += f"   📰 {escape_html(r['news_title'])}\n"
    
    msg += "\n" + "=" * 30
    msg += f"\n🎯 Всего найдено: {len(results)}"
    msg += f"\n🕒 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    return msg

# ---------- ОТПРАВКА В TELEGRAM ----------
async def send_results_to_telegram(results: List[Dict], context: ContextTypes.DEFAULT_TYPE = None):
    """Отправка результатов в Telegram"""
    try:
        bot = Bot(token=TELEGRAM_BOT_TOKEN)
        
        if not results:
            text = (
                f"📉 <b>Ежедневный отчет о падающих акциях</b>\n\n"
                f"😞 Акций с падением ≥{DROP_PERCENT}% за {DAYS_BACK} дн. не найдено.\n"
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
        
        # Отправляем заголовок
        header = (
            f"📊 <b>ЕЖЕДНЕВНЫЙ ОТЧЕТ О ПАДАЮЩИХ АКЦИЯХ</b>\n"
            f"📅 {datetime.now().strftime('%d.%m.%Y')}\n"
            f"⏰ 17:30 МСК\n"
            f"{'=' * 30}\n"
            f"Найдено: <b>{len(results)}</b> акций\n"
            f"Критерии: падение ≥{DROP_PERCENT}% за {DAYS_BACK} дн.\n"
            f"Мин. объем: {MIN_VOLUME/1e6:.0f} млн ₽\n"
            f"Стоп-лосс: <b>{STOP_LOSS_PERCENT}%</b>\n"
        )
        await bot.send_message(
            chat_id=TELEGRAM_USER_ID,
            text=header,
            parse_mode='HTML'
        )
        
        # Отправляем каждый результат отдельно с кнопкой
        for i, r in enumerate(results, 1):
            sl = r['stop_loss']
            text = (
                f"🔻 <b>#{i} {r['ticker']} ({escape_html(r['name'])})</b>\n"
                f"{'─' * 25}\n"
                f"📉 Падение: <b>{r['change_pct']}%</b>\n"
                f"💰 {r['price_from']}₽ → {r['price_to']}₽\n"
                f"🛑 <b>Стоп-лосс: {sl['stop_loss_price']}₽</b> (-{STOP_LOSS_PERCENT}%)\n"
                f"⚠️ Риск на акцию: {sl['risk_per_share']}₽\n"
                f"📅 {r['date_from']} → {r['date_to']}\n"
                f"📊 Объем: {r['volume_rub']:,.0f}₽"
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
        
        # Итоговое сообщение
        footer = f"\n✅ Отчет завершен\n🕒 {datetime.now().strftime('%H:%M:%S')} МСК"
        await bot.send_message(
            chat_id=TELEGRAM_USER_ID,
            text=footer,
            parse_mode='HTML'
        )
        
        logger.info(f"✅ Отправлено {len(results)} сигналов")
        
    except TelegramError as e:
        logger.error(f"🔥 Ошибка Telegram: {e}")
        if "chat not found" in str(e).lower():
            logger.error("💡 Напишите боту /start в Telegram!")

# ---------- КОМАНДЫ БОТА ----------
async def start_cmd(update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /start"""
    text = (
        "🚀 <b>БОТ ПАДАЮЩИХ АКЦИЙ</b>\n\n"
        f"🔍 Ищу акции MOEX с падением ≥{DROP_PERCENT}% за {DAYS_BACK} дня\n"
        f"💰 Минимальный объем торгов: {MIN_VOLUME/1e6:.0f} млн ₽\n"
        f"🛑 Автостоп-лосс: {STOP_LOSS_PERCENT}%\n\n"
        "<b>📋 Команды:</b>\n"
        "/scan — запустить сканирование сейчас\n"
        "/status — статус и настройки\n"
        "/help — помощь\n\n"
        "<b>🕐 Автоуведомления:</b>\n"
        "• Ежедневно в <b>17:30 МСК</b>\n\n"
        "<i>Бот автоматически отправляет отчет каждый вечер</i>"
    )
    await update.message.reply_text(text, parse_mode='HTML')

async def scan_cmd(update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /scan — ручное сканирование"""
    msg = await update.message.reply_text(
        f"🔍 <b>Запускаю сканирование...</b>\n"
        f"⏳ Пожалуйста, подождите (анализ может занять 1-2 минуты)",
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
    """Команда /status"""
    # Следующий запуск в 17:30 МСК
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
        f"📅 Следующее автосканирование: <b>сегодня в 17:30 МСК</b>\n"
        f"⏳ Через: {hours} ч. {minutes} мин.\n\n"
        "<b>Текущие настройки:</b>\n"
        f"• Падение: ≥{DROP_PERCENT}%\n"
        f"• Период: {DAYS_BACK} дня\n"
        f"• Мин. объем: {MIN_VOLUME/1e6:.0f} млн ₽\n"
        f"• Стоп-лосс: {STOP_LOSS_PERCENT}%\n"
        f"• Автоуведомления: 17:30 МСК\n\n"
        "<b>Команды:</b> /scan /start /help"
    )
    await update.message.reply_text(text, parse_mode='HTML')

async def help_cmd(update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /help"""
    text = (
        "📚 <b>ПОМОЩЬ</b>\n\n"
        "<b>Как работает бот:</b>\n"
        "1. Каждый день в 17:30 МСК запускается автосканирование\n"
        "2. Бот анализирует все акции MOEX с объемом от 10 млн ₽\n"
        "3. Ищет падение на 10%+ за последние 3 дня\n"
        f"4. Рассчитывает автоматический стоп-лосс: <b>{STOP_LOSS_PERCENT}%</b>\n"
        "5. Отправляет отчет с результатами\n\n"
        "<b>Что в отчете:</b>\n"
        "• Тикер и название акции\n"
        "• Процент падения\n"
        "• Цены на начало и конец периода\n"
        f"• 🛑 Автостоп-лосс ({STOP_LOSS_PERCENT}%)\n"
        "• Риск на акцию в рублях\n"
        "• Объем торгов\n"
        "• Новости (если найдены)\n"
        "• Ссылка на график TradingView\n\n"
        "<b>Команды:</b>\n"
        "/scan — ручной запуск\n"
        "/status — статус бота\n"
        "/start — информация"
    )
    await update.message.reply_text(text, parse_mode='HTML')

# ---------- АВТОМАТИЧЕСКОЕ СКАНИРОВАНИЕ ----------
async def scheduled_scan_1730(context: ContextTypes.DEFAULT_TYPE):
    """Автоматическое сканирование в 17:30 МСК"""
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
                text=f"❌ <b>Ошибка автосканирования!</b>\n\n{escape_html(str(e)[:300])}",
                parse_mode='HTML'
            )
        except:
            pass

# ---------- ЗАПУСК ----------
def main():
    if not TELEGRAM_BOT_TOKEN:
        logger.error("❌ Токен не найден!")
        print("Ошибка: укажите TELEGRAM_BOT_TOKEN в .env файле")
        sys.exit(1)
    
    if TELEGRAM_USER_ID == 0:
        logger.error("❌ ADMIN_CHAT_ID не найден!")
        print("Ошибка: укажите TELEGRAM_USER_ID в .env файле")
        sys.exit(1)
    
    logger.info("=" * 50)
    logger.info("🚀 БОТ ПАДАЮЩИХ АКЦИЙ ЗАПУСКАЕТСЯ")
    logger.info("=" * 50)
    logger.info(f"📊 Падение: ≥{DROP_PERCENT}% за {DAYS_BACK} дн.")
    logger.info(f"💰 Мин. объем: {MIN_VOLUME/1e6:.0f} млн ₽")
    logger.info(f"🛑 Стоп-лосс: {STOP_LOSS_PERCENT}%")
    logger.info(f"🕐 Автосканирование: 17:30 МСК ежедневно")
    logger.info(f"👤 Чат ID: {TELEGRAM_USER_ID}")
    
    # Создаем приложение
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    
    # Добавляем команды
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("scan", scan_cmd))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(CommandHandler("help", help_cmd))
    
    # Настраиваем ежедневный запуск в 17:30 МСК с правильной таймзоной
    job_queue = app.job_queue
    if job_queue:
        # Используем московскую таймзону
        msk_tz = pytz.timezone('Europe/Moscow')
        job_queue.run_daily(
            scheduled_scan_1730,
            time=time(hour=17, minute=30, tzinfo=msk_tz),  # 17:30 МСК
            days=(0, 1, 2, 3, 4, 5, 6)  # Все дни недели
        )
        logger.info("✅ Автосканирование настроено на 17:30 МСК")
    else:
        logger.error("❌ Не удалось настроить JobQueue!")
    
    print("\n" + "=" * 50)
    print("✅ Бот запущен!")
    print("=" * 50)
    print(f"📊 Анализ: падение ≥{DROP_PERCENT}% за {DAYS_BACK} дн.")
    print(f"💰 Мин. объем: {MIN_VOLUME/1e6:.0f} млн ₽")
    print(f"🛑 Стоп-лосс: {STOP_LOSS_PERCENT}%")
    print("🕐 Автоуведомления: каждый день в 17:30 МСК")
    print("📋 Команды: /start, /scan, /status, /help")
    print("Нажмите Ctrl+C для остановки\n")
    
    try:
        app.run_polling(allowed_updates=["message"])
    except KeyboardInterrupt:
        logger.info("👋 Бот остановлен пользователем")
        print("\n👋 Бот остановлен")

if __name__ == '__main__':
    main()
