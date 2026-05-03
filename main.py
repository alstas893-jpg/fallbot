import asyncio
import logging
import os
import sys
import signal
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Tuple

import aiohttp
import pandas as pd
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, ContextTypes, JobQueue
from dotenv import load_dotenv

# Настройка event loop для Windows
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# Загрузка переменных окружения
load_dotenv()

TOKEN = os.getenv("TOKEN")
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "0"))

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)
logger = logging.getLogger(__name__)

# ================= НАСТРОЙКИ =================
MIN_DROP_PERCENT = 10.0
MIN_DAILY_VOLUME_RUB = 30_000_000
MIN_AVG_VOLUME_5D = 100_000_000
LOOKBACK_TRADING_DAYS = 3
VOLUME_DAYS = 5
SCAN_INTERVAL_MINUTES = 5
SCAN_INTERVAL_SECONDS = SCAN_INTERVAL_MINUTES * 60

# ================= ТОРГОВЫЕ СЕССИИ =================
class TradingSession:
    @classmethod
    def get_current_session(cls, dt: Optional[datetime] = None) -> Tuple[Optional[str], bool]:
        if dt is None:
            msk_tz = timezone(timedelta(hours=3))
            dt = datetime.now(msk_tz)
        
        current_time = dt.time()
        weekday = dt.weekday()
        
        if weekday >= 5:
            start = datetime.strptime("10:00", "%H:%M").time()
            end = datetime.strptime("18:50", "%H:%M").time()
            if start <= current_time <= end:
                return "weekend", True
            return None, False
        
        if datetime.strptime("07:00", "%H:%M").time() <= current_time <= datetime.strptime("09:50", "%H:%M").time():
            return "morning", True
        if datetime.strptime("10:00", "%H:%M").time() <= current_time <= datetime.strptime("18:40", "%H:%M").time():
            return "main", True
        if datetime.strptime("19:05", "%H:%M").time() <= current_time <= datetime.strptime("23:50", "%H:%M").time():
            return "evening", True
        
        return None, False
    
    @classmethod
    def get_session_status_text(cls) -> str:
        session_key, is_trading = cls.get_current_session()
        
        if session_key is None:
            return "🔴 Торги закрыты"
        
        session_names = {
            "morning": "🌅 Утренняя сессия",
            "main": "☀️ Основная сессия",
            "evening": "🌙 Вечерняя сессия",
            "weekend": "📅 Сессия выходного дня"
        }
        
        if is_trading:
            return f"🟢 Идут торги ({session_names.get(session_key, session_key)})"
        else:
            return f"⏳ Аукцион ({session_names.get(session_key, session_key)})"


# ================= TRADINGVIEW =================
def get_tradingview_link(ticker: str) -> str:
    return f"https://www.tradingview.com/chart/?symbol=MOEX:{ticker}&theme=dark"

def create_tradingview_keyboard(ticker: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(f"📊 {ticker} на TradingView", url=get_tradingview_link(ticker))
    ]])


# ================= MOEX API (ИСПРАВЛЕННАЯ ВЕРСИЯ) =================
class MoexAPI:
    BASE = 'https://iss.moex.com/iss'
    
    def __init__(self):
        self._session: Optional[aiohttp.ClientSession] = None
        self._tickers_cache = None
        self._cache_time = None
        self._session_lock = asyncio.Lock()
    
    async def _get_session(self) -> aiohttp.ClientSession:
        """Безопасное получение или создание сессии"""
        async with self._session_lock:
            if self._session is None or self._session.closed:
                timeout = aiohttp.ClientTimeout(total=30)
                connector = aiohttp.TCPConnector(force_close=True)  # Принудительно закрываем соединения
                self._session = aiohttp.ClientSession(
                    timeout=timeout,
                    connector=connector
                )
            return self._session
    
    async def close(self):
        """Безопасное закрытие сессии"""
        async with self._session_lock:
            if self._session and not self._session.closed:
                try:
                    await self._session.close()
                    await asyncio.sleep(0.1)  # Даем время на закрытие
                except Exception as e:
                    logger.debug(f"Ошибка при закрытии сессии: {e}")
                finally:
                    self._session = None
    
    async def request(self, url: str) -> Optional[dict]:
        """Выполнение HTTP запроса с правильным управлением сессией"""
        session = None
        try:
            session = await self._get_session()
            async with session.get(url) as r:
                if r.status == 200:
                    return await r.json()
                else:
                    logger.warning(f"HTTP {r.status} для {url}")
        except aiohttp.ClientError as e:
            logger.error(f"Ошибка HTTP запроса: {e}")
            # При ошибке создаем новую сессию при следующем запросе
            await self.close()
        except asyncio.TimeoutError:
            logger.error(f"Таймаут запроса: {url}")
            await self.close()
        except Exception as e:
            logger.error(f"Неожиданная ошибка запроса: {e}")
            await self.close()
        return None
    
    async def get_all_tickers(self) -> List[str]:
        """Получение списка всех акций с фильтрацией по объему"""
        cache_minutes = 10 if SCAN_INTERVAL_MINUTES <= 10 else 30
        if self._tickers_cache and self._cache_time:
            if datetime.now() - self._cache_time < timedelta(minutes=cache_minutes):
                return self._tickers_cache
        
        logger.info("📊 Получение списка всех акций с МосБиржи...")
        
        url = (f"{self.BASE}/engines/stock/markets/shares/boards/TQBR/securities.json"
               f"?iss.meta=off&iss.only=securities&securities.columns=SECID,PREVPRICE")
        
        data = await self.request(url)
        if not data or 'securities' not in data:
            logger.error("Не удалось получить список акций")
            return self._tickers_cache if self._tickers_cache else []
        
        try:
            rows = data['securities']['data']
            cols = data['securities']['columns']
            secid_idx = cols.index('SECID')
            prevprice_idx = cols.index('PREVPRICE')
            
            all_tickers = []
            for row in rows:
                ticker = row[secid_idx]
                prev_price = row[prevprice_idx]
                if prev_price and prev_price > 0:
                    all_tickers.append(ticker)
            
            logger.info(f"📋 Найдено {len(all_tickers)} акций с ценой > 0")
            
            filtered_tickers = []
            
            for i, ticker in enumerate(all_tickers):
                try:
                    df = await self.get_candles(ticker, days=VOLUME_DAYS + 10, interval=24)
                    if df is not None and len(df) >= VOLUME_DAYS:
                        if 'value' in df.columns:
                            recent_values = df['value'].tail(VOLUME_DAYS)
                            avg_volume = recent_values.mean()
                            
                            if avg_volume >= MIN_AVG_VOLUME_5D:
                                filtered_tickers.append(ticker)
                    
                    if i % 100 == 0:
                        logger.info(f"📊 Проверено {i+1}/{len(all_tickers)} акций (отфильтровано: {len(filtered_tickers)})")
                    
                    await asyncio.sleep(0.05)
                    
                except Exception as e:
                    logger.error(f"Ошибка при проверке {ticker}: {e}")
                    continue
            
            logger.info(f"✅ Отфильтровано {len(filtered_tickers)} акций")
            
            if len(filtered_tickers) >= 50:
                self._tickers_cache = filtered_tickers
                self._cache_time = datetime.now()
            
            return filtered_tickers
            
        except Exception as e:
            logger.error(f"Ошибка при парсинге списка тикеров: {e}")
            return self._tickers_cache if self._tickers_cache else []
    
    async def get_candles(self, ticker: str, days: int = 60, interval: int = 24) -> Optional[pd.DataFrame]:
        """Получение дневных свечей"""
        till = datetime.now().strftime('%Y-%m-%d')
        frm = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        
        url = (f"{self.BASE}/engines/stock/markets/shares/boards/TQBR/securities/{ticker}/candles.json"
               f"?from={frm}&till={till}&interval={interval}&iss.meta=off&iss.only=candles")
        
        data = await self.request(url)
        if not data or 'candles' not in data:
            return None
        
        try:
            rows = data['candles']['data']
            cols = data['candles']['columns']
            
            if not rows:
                return None
            
            df = pd.DataFrame(rows, columns=cols)
            df = df.rename(columns={'begin': 'date'})
            
            need = ['date', 'open', 'high', 'low', 'close', 'volume', 'value']
            available = [c for c in need if c in df.columns]
            df = df[available].copy()
            
            df['date'] = pd.to_datetime(df['date'])
            for c in ['open', 'high', 'low', 'close', 'volume', 'value']:
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors='coerce')
            
            df = df.dropna(subset=['close']).sort_values('date')
            return df
        except Exception as e:
            logger.error(f"Ошибка парсинга свечей для {ticker}: {e}")
            return None
    
    async def get_price(self, ticker: str) -> Optional[float]:
        url = (f"{self.BASE}/engines/stock/markets/shares/boards/TQBR/securities/{ticker}.json"
               f"?iss.only=marketdata&iss.meta=off")
        
        data = await self.request(url)
        if not data or not data.get('marketdata', {}).get('data'):
            return None
        
        try:
            cols = data['marketdata']['columns']
            row = data['marketdata']['data'][0]
            
            for name in ['LAST', 'LCURRENTPRICE', 'MARKETPRICE', 'PREVPRICE']:
                if name in cols:
                    v = row[cols.index(name)]
                    if v:
                        return float(v)
        except Exception as e:
            logger.error(f"Ошибка получения цены для {ticker}: {e}")
        
        return None


# ================= СТРАТЕГИЯ ПОИСКА ПАДЕНИЙ =================
class DropScanner:
    """Сканер падающих акций"""
    
    def __init__(self, api: MoexAPI):
        self.api = api
        self.last_scan_time = None
        self.last_drops_count = 0
    
    async def scan_drops(self) -> Tuple[List[dict], List[str], List[str], int]:
        """Сканирует все тикеры с рынка"""
        tickers = await self.api.get_all_tickers()
        
        if not tickers:
            logger.error("❌ Не удалось получить список акций")
            return [], [], [], 0
        
        self.last_scan_time = datetime.now()
        
        logger.info(f"🔍 Сканирование падающих акций (каждые {SCAN_INTERVAL_MINUTES} мин)")
        logger.info(f"📋 Сканируется {len(tickers)} акций")
        
        drops = []
        excluded_liquidity = []
        excluded_other = []
        
        for i, ticker in enumerate(tickers):
            try:
                if i % 100 == 0:
                    logger.info(f"📊 Прогресс: {i+1}/{len(tickers)} (найдено: {len(drops)})")
                
                df = await self.api.get_candles(ticker, days=60)
                
                if df is None or len(df) < LOOKBACK_TRADING_DAYS + 1:
                    continue
                
                trading_days = df.tail(LOOKBACK_TRADING_DAYS + 1).copy()
                
                if len(trading_days) < LOOKBACK_TRADING_DAYS + 1:
                    continue
                
                recent_volumes = trading_days.tail(LOOKBACK_TRADING_DAYS)
                avg_daily_volume = recent_volumes['value'].mean() if 'value' in recent_volumes.columns else 0
                
                if avg_daily_volume < MIN_DAILY_VOLUME_RUB:
                    continue
                
                price_old = trading_days['close'].iloc[0]
                
                current_price = await self.api.get_price(ticker)
                if not current_price:
                    current_price = trading_days['close'].iloc[-1]
                
                change_percent = ((current_price - price_old) / price_old) * 100
                
                if change_percent <= -MIN_DROP_PERCENT:
                    low_price = trading_days['low'].tail(LOOKBACK_TRADING_DAYS).min()
                    high_price = trading_days['high'].tail(LOOKBACK_TRADING_DAYS).max()
                    max_drawdown = ((low_price - price_old) / price_old) * 100
                    last_volume = trading_days['value'].iloc[-1] if 'value' in trading_days.columns else 0
                    
                    daily_changes = []
                    closes = trading_days['close'].tail(LOOKBACK_TRADING_DAYS + 1).values
                    for j in range(1, len(closes)):
                        daily_change = ((closes[j] - closes[j-1]) / closes[j-1]) * 100
                        daily_changes.append(round(daily_change, 2))
                    
                    drops.append({
                        'ticker': ticker,
                        'current_price': round(current_price, 2),
                        'price_3d_ago': round(price_old, 2),
                        'drop_percent': round(change_percent, 2),
                        'max_drawdown': round(max_drawdown, 2),
                        'low_price': round(low_price, 2),
                        'high_price': round(high_price, 2),
                        'avg_daily_volume': avg_daily_volume,
                        'last_volume': last_volume,
                        'daily_changes': daily_changes,
                        'trading_days_count': len(trading_days)
                    })
                    
                    logger.info(f"🔻 {ticker}: {change_percent:+.2f}% | {price_old} → {current_price}")
                
            except Exception as e:
                logger.error(f"Ошибка {ticker}: {e}")
            
            await asyncio.sleep(0.05)
        
        drops.sort(key=lambda x: x['drop_percent'])
        self.last_drops_count = len(drops)
        
        logger.info(f"✅ Сканирование завершено. Падений: {len(drops)}")
        
        return drops, excluded_liquidity, excluded_other, len(tickers)


# ================= БОТ =================
api = MoexAPI()
scanner = DropScanner(api)

def escape_html(text: str) -> str:
    if text is None:
        return ""
    return str(text).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

def format_volume(amount: float) -> str:
    if amount >= 1_000_000_000:
        return f"{amount/1_000_000_000:.2f} млрд"
    elif amount >= 1_000_000:
        return f"{amount/1_000_000:.1f} млн"
    else:
        return f"{amount:,.0f}".replace(",", " ")

def get_drop_emoji(drop_percent: float) -> str:
    if drop_percent <= -25:
        return "🔴💀"
    elif drop_percent <= -20:
        return "🔴🩸"
    elif drop_percent <= -15:
        return "🟠📉"
    else:
        return "🟡⬇️"

async def send_scan_results(context: ContextTypes.DEFAULT_TYPE, chat_id: int = None):
    """Отправка результатов сканирования"""
    if chat_id is None:
        chat_id = ADMIN_CHAT_ID
    
    if chat_id == 0:
        return
    
    try:
        drops, excluded_liquidity, excluded_other, total_checked = await scanner.scan_drops()
        
        session_status = TradingSession.get_session_status_text()
        
        if not drops:
            text = (
                f"📊 <b>Падающих акций не найдено</b>\n\n"
                f"{session_status}\n"
                f"📈 Просканировано: <b>{total_checked}</b>\n"
                f"🔄 Следующее сканирование через {SCAN_INTERVAL_MINUTES} мин.\n\n"
                f"💡 Критерии: падение ≥{MIN_DROP_PERCENT}%, объем ≥{MIN_DAILY_VOLUME_RUB/1e6:.0f} млн ₽"
            )
            await context.bot.send_message(chat_id=chat_id, text=text, parse_mode='HTML')
            return
        
        msk_time = datetime.now() + timedelta(hours=3)
        header = (
            f"🔻 <b>НАЙДЕНО ПАДЕНИЙ: {len(drops)}</b>\n"
            f"{session_status}\n"
            f"📈 Просканировано: <b>{total_checked}</b>\n"
            f"🕐 {msk_time.strftime('%H:%M:%S')} МСК\n"
            f"📊 Падение ≥{MIN_DROP_PERCENT}% за {LOOKBACK_TRADING_DAYS} торг. дня\n\n"
            f"<i>Сортировка по величине падения:</i>"
        )
        
        await context.bot.send_message(chat_id=chat_id, text=header, parse_mode='HTML')
        
        max_show = 5
        for i, d in enumerate(drops[:max_show], 1):
            emoji = get_drop_emoji(d['drop_percent'])
            
            daily_str = ""
            if d.get('daily_changes'):
                daily_parts = []
                for j, change in enumerate(d['daily_changes']):
                    arrow = "🔻" if change < 0 else "🔺" if change > 0 else "➖"
                    daily_parts.append(f"День {j+1}: {arrow} {change:+.2f}%")
                daily_str = "\n".join(daily_parts)
            
            text = (
                f"{emoji} <b>#{i} {d['ticker']}</b>\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"📉 <b>Падение: {d['drop_percent']:+.2f}%</b>\n"
                f"💰 Цена: {d['price_3d_ago']} → <b>{d['current_price']} ₽</b>\n"
                f"📊 Макс. просадка: <b>{d['max_drawdown']:+.2f}%</b>\n"
                f"📏 Диапазон: {d['low_price']} — {d['high_price']} ₽\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"💵 Объем: <b>{format_volume(d['avg_daily_volume'])} ₽</b>\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"📅 Динамика по дням:\n{daily_str}"
            )
            
            try:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=text,
                    parse_mode='HTML',
                    reply_markup=create_tradingview_keyboard(d['ticker'])
                )
            except Exception as e:
                logger.error(f"Ошибка отправки: {e}")
            
            await asyncio.sleep(0.5)
        
        if len(drops) > max_show:
            summary = f"📊 <b>Остальные падения ({len(drops) - max_show}):</b>\n\n"
            for i, d in enumerate(drops[max_show:], max_show + 1):
                summary += f"{i}. {d['ticker']}: <b>{d['drop_percent']:+.2f}%</b> | {d['current_price']} ₽\n"
            
            await context.bot.send_message(chat_id=chat_id, text=summary, parse_mode='HTML')
                
    except Exception as e:
        logger.error(f"Ошибка: {e}", exc_info=True)

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        f"🔻 <b>MOEX Drop Scanner Bot v2.0</b>\n\n"
        f"🔄 Автосканирование каждые {SCAN_INTERVAL_MINUTES} мин.\n"
        f"📉 Падение ≥{MIN_DROP_PERCENT}% за {LOOKBACK_TRADING_DAYS} дн.\n"
        f"💰 Объем ≥{MIN_DAILY_VOLUME_RUB/1e6:.0f} млн ₽\n\n"
        f"/scan - ручное сканирование\n"
        f"/status - статус\n"
        f"/stop_scan - остановить\n"
        f"/start_scan - запустить",
        parse_mode='HTML'
    )

async def scan_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔍 Запуск сканирования...")
    await send_scan_results(context, chat_id=update.effective_chat.id)

async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.job_queue is None:
        await update.message.reply_text("❌ JobQueue не настроен. Установите python-telegram-bot[job-queue]")
        return
    
    jobs = context.job_queue.jobs()
    scan_jobs = [j for j in jobs if 'scan' in j.name.lower()]
    
    text = f"📊 <b>Статус автосканирования</b>\n\n"
    text += f"Активно: {'✅ Да' if scan_jobs else '❌ Нет'}\n"
    
    if scan_jobs and scan_jobs[0].next_t:
        next_run = scan_jobs[0].next_t + timedelta(hours=3)
        text += f"Следующее: {next_run.strftime('%H:%M:%S')} МСК\n"
    
    if scanner.last_scan_time:
        text += f"Последнее: {(scanner.last_scan_time + timedelta(hours=3)).strftime('%H:%M:%S')} МСК\n"
        text += f"Найдено падений: {scanner.last_drops_count}\n"
    
    await update.message.reply_text(text, parse_mode='HTML')

async def stop_scan_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.job_queue is None:
        await update.message.reply_text("❌ JobQueue не настроен")
        return
    
    jobs = context.job_queue.jobs()
    scan_jobs = [j for j in jobs if 'scan' in j.name.lower()]
    
    for job in scan_jobs:
        job.schedule_removal()
    
    await update.message.reply_text("🔴 Автосканирование остановлено")

async def start_scan_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.job_queue is None:
        await update.message.reply_text("❌ JobQueue не настроен. Установите python-telegram-bot[job-queue]")
        return
    
    jobs = context.job_queue.jobs()
    scan_jobs = [j for j in jobs if 'scan' in j.name.lower()]
    
    if scan_jobs:
        await update.message.reply_text("🔄 Автосканирование уже запущено")
        return
    
    context.job_queue.run_repeating(
        auto_scan_job,
        interval=SCAN_INTERVAL_SECONDS,
        first=10,
        name="auto_scan"
    )
    
    await update.message.reply_text(f"🔄 Автосканирование запущено (каждые {SCAN_INTERVAL_MINUTES} мин)")

async def auto_scan_job(context: ContextTypes.DEFAULT_TYPE):
    """Функция для автоматического сканирования"""
    logger.info(f"🔄 Автосканирование (каждые {SCAN_INTERVAL_MINUTES} мин)")
    await send_scan_results(context)

async def cleanup():
    """Очистка ресурсов"""
    logger.info("🧹 Очистка ресурсов...")
    await api.close()
    await asyncio.sleep(0.2)  # Даем время на закрытие соединений

def main():
    if not TOKEN:
        logger.error("❌ Токен не найден!")
        sys.exit(1)
    
    if ADMIN_CHAT_ID == 0:
        logger.error("❌ ADMIN_CHAT_ID не найден!")
        sys.exit(1)
    
    logger.info("=" * 50)
    logger.info("🔻 MOEX DROP SCANNER BOT v2.0")
    logger.info(f"🔄 Автосканирование: каждые {SCAN_INTERVAL_MINUTES} мин")
    logger.info("=" * 50)
    
    app = Application.builder().token(TOKEN).build()
    
    # Проверяем доступность JobQueue
    if app.job_queue is None:
        logger.warning("⚠️ JobQueue не настроен. Автосканирование не будет работать.")
        logger.warning("Установите: pip install 'python-telegram-bot[job-queue]'")
    
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("scan", scan_cmd))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(CommandHandler("stop_scan", stop_scan_cmd))
    app.add_handler(CommandHandler("start_scan", start_scan_cmd))
    
    # Запускаем автосканирование если JobQueue доступен
    if app.job_queue:
        app.job_queue.run_repeating(
            auto_scan_job,
            interval=SCAN_INTERVAL_SECONDS,
            first=10,
            name="auto_scan"
        )
        logger.info(f"🔄 Автосканирование запущено: каждые {SCAN_INTERVAL_MINUTES} минут")
    
    print("\n" + "=" * 50)
    print("✅ Бот запущен!")
    print(f"🔄 Автосканирование каждые {SCAN_INTERVAL_MINUTES} минут")
    print("Нажмите Ctrl+C для остановки\n")
    
    try:
        app.run_polling(allowed_updates=Update.ALL_TYPES)
    except KeyboardInterrupt:
        logger.info("👋 Бот остановлен")
        print("\n👋 Бот остановлен")
    finally:
        # Правильное завершение
        loop = asyncio.new_event_loop()
        loop.run_until_complete(cleanup())
        loop.close()

if __name__ == "__main__":
    main()