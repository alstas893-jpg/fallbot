import asyncio
import logging
import os
import sys
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
MIN_DROP_PERCENT = 10.0  # Минимальное падение за 3 торговых дня: 10%
MIN_DAILY_VOLUME_RUB = 30_000_000  # Минимальный дневной объем торгов: 30 млн ₽
MIN_AVG_VOLUME_5D = 100_000_000  # Минимальный средний объем за 5 дней: 100 млн ₽
LOOKBACK_TRADING_DAYS = 3  # Анализируем падение за последние 3 торговых дня
VOLUME_DAYS = 5  # Количество дней для расчета среднего объема
SCAN_INTERVAL_MINUTES = 5  # Интервал автосканирования в минутах
SCAN_INTERVAL_SECONDS = SCAN_INTERVAL_MINUTES * 60  # Интервал в секундах

# ================= ТОРГОВЫЕ СЕССИИ =================
class TradingSession:
    """Класс для работы с торговыми сессиями МосБиржи"""
    
    @classmethod
    def get_current_session(cls, dt: Optional[datetime] = None) -> Tuple[Optional[str], bool]:
        if dt is None:
            msk_tz = timezone(timedelta(hours=3))
            dt = datetime.now(msk_tz)
        
        current_time = dt.time()
        weekday = dt.weekday()
        
        # Выходные — нет торгов (кроме сессии выходного дня)
        if weekday >= 5:
            # Сессия выходного дня: 10:00-18:50
            start = datetime.strptime("10:00", "%H:%M").time()
            end = datetime.strptime("18:50", "%H:%M").time()
            if start <= current_time <= end:
                return "weekend", True
            return None, False
        
        # Будние дни
        # Утренняя: 07:00-09:50
        if datetime.strptime("07:00", "%H:%M").time() <= current_time <= datetime.strptime("09:50", "%H:%M").time():
            return "morning", True
        # Основная: 10:00-18:40
        if datetime.strptime("10:00", "%H:%M").time() <= current_time <= datetime.strptime("18:40", "%H:%M").time():
            return "main", True
        # Вечерняя: 19:05-23:50
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
    symbol = f"MOEX:{ticker}"
    return f"https://www.tradingview.com/chart/?symbol={symbol}&theme=dark"

def create_tradingview_keyboard(ticker: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(f"📊 {ticker} на TradingView", url=get_tradingview_link(ticker))
    ]])


# ================= MOEX API =================
class MoexAPI:
    BASE = 'https://iss.moex.com/iss'
    
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self._tickers_cache = None
        self._cache_time = None
    
    async def get_session(self) -> aiohttp.ClientSession:
        if not self.session or self.session.closed:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30))
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
        except Exception as e:
            logger.error(f"Ошибка запроса: {e}")
        return None
    
    async def get_all_tickers(self) -> List[str]:
        """
        Получение списка всех акций, торгуемых на МосБирже (режим T+2)
        с фильтрацией по объему торгов
        """
        # Проверяем кеш (обновляем раз в 10 минут при частом сканировании)
        cache_minutes = 10 if SCAN_INTERVAL_MINUTES <= 10 else 30
        if self._tickers_cache and self._cache_time:
            if datetime.now() - self._cache_time < timedelta(minutes=cache_minutes):
                return self._tickers_cache
        
        logger.info("📊 Получение списка всех акций с МосБиржи...")
        
        # Получаем список всех акций
        url = (f"{self.BASE}/engines/stock/markets/shares/boards/TQBR/securities.json"
               f"?iss.meta=off&iss.only=securities&securities.columns=SECID,PREVPRICE")
        
        data = await self.request(url)
        if not data or 'securities' not in data:
            logger.error("Не удалось получить список акций")
            return []
        
        rows = data['securities']['data']
        cols = data['securities']['columns']
        secid_idx = cols.index('SECID')
        prevprice_idx = cols.index('PREVPRICE')
        
        # Фильтруем акции с ненулевой ценой
        all_tickers = []
        for row in rows:
            ticker = row[secid_idx]
            prev_price = row[prevprice_idx]
            if prev_price and prev_price > 0:
                all_tickers.append(ticker)
        
        logger.info(f"📋 Найдено {len(all_tickers)} акций с ценой > 0")
        
        # Получаем объемы для фильтрации
        filtered_tickers = []
        
        logger.info(f"🔍 Фильтрация по объему (мин. {MIN_AVG_VOLUME_5D/1e6:.0f} млн ₽ за {VOLUME_DAYS} дн.)...")
        
        for i, ticker in enumerate(all_tickers):
            try:
                df = await self.get_candles(ticker, days=VOLUME_DAYS + 10, interval=24)
                if df is not None and len(df) >= VOLUME_DAYS:
                    # Считаем средний объем за последние VOLUME_DAYS дней
                    if 'value' in df.columns:
                        recent_values = df['value'].tail(VOLUME_DAYS)
                        avg_volume = recent_values.mean()
                        
                        if avg_volume >= MIN_AVG_VOLUME_5D:
                            filtered_tickers.append(ticker)
                
                if i % 50 == 0:
                    logger.info(f"📊 Проверено {i+1}/{len(all_tickers)} акций (отфильтровано: {len(filtered_tickers)})")
                
                await asyncio.sleep(0.05)  # Уменьшенная задержка для частого сканирования
                
            except Exception as e:
                logger.error(f"Ошибка при проверке {ticker}: {e}")
                continue
        
        logger.info(f"✅ Отфильтровано {len(filtered_tickers)} акций с объемом > {MIN_AVG_VOLUME_5D/1e6:.0f} млн ₽")
        
        # Сохраняем в кеш
        self._tickers_cache = filtered_tickers
        self._cache_time = datetime.now()
        
        return filtered_tickers
    
    async def get_candles(self, ticker: str, days: int = 60, interval: int = 24) -> Optional[pd.DataFrame]:
        """Получение дневных свечей"""
        till = datetime.now().strftime('%Y-%m-%d')
        frm = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        
        url = (f"{self.BASE}/engines/stock/markets/shares/boards/TQBR/securities/{ticker}/candles.json"
               f"?from={frm}&till={till}&interval={interval}&iss.meta=off&iss.only=candles")
        
        data = await self.request(url)
        if not data or 'candles' not in data:
            return None
        
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
    
    async def get_price(self, ticker: str) -> Optional[float]:
        url = (f"{self.BASE}/engines/stock/markets/shares/boards/TQBR/securities/{ticker}.json"
               f"?iss.only=marketdata&iss.meta=off")
        
        data = await self.request(url)
        if not data or not data.get('marketdata', {}).get('data'):
            return None
        
        cols = data['marketdata']['columns']
        row = data['marketdata']['data'][0]
        
        for name in ['LAST', 'LCURRENTPRICE', 'MARKETPRICE', 'PREVPRICE']:
            if name in cols:
                v = row[cols.index(name)]
                if v:
                    return float(v)
        return None


# ================= СТРАТЕГИЯ ПОИСКА ПАДЕНИЙ =================
class DropScanner:
    """Сканер падающих акций"""
    
    def __init__(self, api: MoexAPI):
        self.api = api
        self.last_scan_time = None
        self.last_drops_count = 0
    
    async def scan_drops(self) -> Tuple[List[dict], List[str], List[str], int]:
        """
        Сканирует все тикеры с рынка, возвращает:
        - список упавших с деталями
        - список исключенных по ликвидности
        - список исключенных по другим причинам
        - общее количество проверенных тикеров
        """
        # Получаем динамический список тикеров
        tickers = await self.api.get_all_tickers()
        
        if not tickers:
            logger.error("❌ Не удалось получить список акций")
            return [], [], [], 0
        
        self.last_scan_time = datetime.now()
        
        logger.info("=" * 50)
        logger.info(f"🔍 ЗАПУСК СКАНИРОВАНИЯ ПАДАЮЩИХ АКЦИЙ (каждые {SCAN_INTERVAL_MINUTES} мин)")
        logger.info(f"📊 Параметры: падение ≥{MIN_DROP_PERCENT}% за {LOOKBACK_TRADING_DAYS} торг. дня")
        logger.info(f"💰 Фильтр ликвидности: объем ≥{MIN_DAILY_VOLUME_RUB/1e6:.0f} млн ₽/день")
        logger.info(f"📋 Сканируется {len(tickers)} акций")
        logger.info("=" * 50)
        
        drops = []
        excluded_liquidity = []
        excluded_other = []
        
        for i, ticker in enumerate(tickers):
            try:
                if i % 100 == 0:
                    logger.info(f"📊 Прогресс: {i+1}/{len(tickers)} акций проверено (найдено: {len(drops)})")
                
                # Получаем свечи за последние 60 дней (с запасом)
                df = await self.api.get_candles(ticker, days=60)
                
                if df is None or len(df) < LOOKBACK_TRADING_DAYS + 1:
                    excluded_other.append(f"{ticker} (нет данных)")
                    continue
                
                # Оставляем только последние N торговых дней
                trading_days = df.tail(LOOKBACK_TRADING_DAYS + 1).copy()
                
                if len(trading_days) < LOOKBACK_TRADING_DAYS + 1:
                    excluded_other.append(f"{ticker} (мало данных)")
                    continue
                
                # Проверяем ликвидность за каждый из последних 3 дней
                recent_volumes = trading_days.tail(LOOKBACK_TRADING_DAYS)
                avg_daily_volume = recent_volumes['value'].mean() if 'value' in recent_volumes.columns else 0
                
                if avg_daily_volume < MIN_DAILY_VOLUME_RUB:
                    excluded_liquidity.append(
                        f"{ticker} (объем {avg_daily_volume/1e6:.1f} млн ₽)"
                    )
                    continue
                
                # Расчет падения: цена N дней назад vs текущая
                price_old = trading_days['close'].iloc[0]  # Цена N торговых дней назад
                
                # Текущая цена (пробуем получить онлайн, иначе последнее закрытие)
                current_price = await self.api.get_price(ticker)
                if not current_price:
                    current_price = trading_days['close'].iloc[-1]
                
                # Процент изменения
                change_percent = ((current_price - price_old) / price_old) * 100
                
                # Проверяем условие падения
                if change_percent <= -MIN_DROP_PERCENT:
                    # Дополнительная информация
                    low_price = trading_days['low'].tail(LOOKBACK_TRADING_DAYS).min()
                    high_price = trading_days['high'].tail(LOOKBACK_TRADING_DAYS).max()
                    max_drawdown = ((low_price - price_old) / price_old) * 100
                    
                    # Объем за последний день
                    last_volume = trading_days['value'].iloc[-1] if 'value' in trading_days.columns else 0
                    
                    # Последовательное падение по дням
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
                    
                    logger.info(f"🔻 {ticker}: {change_percent:+.2f}% | "
                               f"Цена: {price_old} → {current_price} | "
                               f"Объем: {avg_daily_volume/1e6:.1f} млн ₽")
                
            except Exception as e:
                logger.error(f"Ошибка {ticker}: {e}")
                excluded_other.append(f"{ticker} (ошибка)")
            
            await asyncio.sleep(0.05)  # Уменьшенная задержка
        
        # Сортировка по величине падения (от большего к меньшему)
        drops.sort(key=lambda x: x['drop_percent'])
        self.last_drops_count = len(drops)
        
        logger.info("=" * 50)
        logger.info(f"СКАНИРОВАНИЕ ЗАВЕРШЕНО. Всего проверено: {len(tickers)}")
        logger.info(f"Найдено падений: {len(drops)}")
        logger.info(f"Исключено по ликвидности: {len(excluded_liquidity)}")
        logger.info(f"Исключено по другим причинам: {len(excluded_other)}")
        logger.info(f"Следующее сканирование через {SCAN_INTERVAL_MINUTES} мин")
        logger.info("=" * 50)
        
        return drops, excluded_liquidity, excluded_other, len(tickers)


# ================= БОТ =================
api = MoexAPI()
scanner = DropScanner(api)

def escape_html(text: str) -> str:
    if text is None:
        return ""
    return str(text).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

def format_volume(amount: float) -> str:
    """Форматирование объема торгов"""
    if amount >= 1_000_000_000:
        return f"{amount/1_000_000_000:.2f} млрд"
    elif amount >= 1_000_000:
        return f"{amount/1_000_000:.1f} млн"
    else:
        return f"{amount:,.0f}".replace(",", " ")

def get_drop_emoji(drop_percent: float) -> str:
    """Эмодзи в зависимости от силы падения"""
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
        logger.error("ADMIN_CHAT_ID не указан")
        return
    
    try:
        drops, excluded_liquidity, excluded_other, total_checked = await scanner.scan_drops()
        
        session_status = TradingSession.get_session_status_text()
        
        if not drops:
            # Отправляем сообщение только если прошло более 4 сканирований
            # или это первое сканирование
            text = (
                f"📊 <b>Падающих акций не найдено</b>\n\n"
                f"{session_status}\n"
                f"📈 Просканировано акций: <b>{total_checked}</b>\n"
                f"🕐 Следующее сканирование через {SCAN_INTERVAL_MINUTES} мин.\n\n"
                f"💡 <i>Критерии: падение ≥{MIN_DROP_PERCENT}%, "
                f"объем ≥{MIN_DAILY_VOLUME_RUB/1e6:.0f} млн ₽/день</i>"
            )
            await context.bot.send_message(chat_id=chat_id, text=text, parse_mode='HTML')
            return
        
        # Заголовок
        msk_time = datetime.now() + timedelta(hours=3)
        header = (
            f"🔻 <b>НАЙДЕНО ПАДЕНИЙ: {len(drops)}</b>\n"
            f"{session_status}\n"
            f"📈 Просканировано акций: <b>{total_checked}</b>\n"
            f"🕐 Время МСК: {msk_time.strftime('%H:%M:%S')}\n"
            f"📊 Падение ≥{MIN_DROP_PERCENT}% за {LOOKBACK_TRADING_DAYS} торг. дня\n\n"
            f"<i>Сортировка по величине падения:</i>"
        )
        
        await context.bot.send_message(chat_id=chat_id, text=header, parse_mode='HTML')
        
        # Отправляем каждый результат (топ-5 для частого сканирования)
        max_show = 5 if SCAN_INTERVAL_MINUTES <= 10 else 10
        for i, d in enumerate(drops[:max_show], 1):
            emoji = get_drop_emoji(d['drop_percent'])
            
            # Дневные изменения
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
                f"💵 <b>Объем торгов:</b>\n"
                f"• Средний за 3 дня: <b>{format_volume(d['avg_daily_volume'])} ₽</b>\n"
                f"• Последний день: <b>{format_volume(d['last_volume'])} ₽</b>\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"📅 <b>Динамика по дням:</b>\n"
                f"{daily_str}"
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
                plain = text.replace('<b>', '').replace('</b>', '').replace('<i>', '').replace('</i>', '')
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=plain,
                    reply_markup=create_tradingview_keyboard(d['ticker'])
                )
            
            await asyncio.sleep(0.5)
        
        # Если результатов больше max_show, показываем сводку
        if len(drops) > max_show:
            summary = f"📊 <b>Остальные падения (еще {len(drops) - max_show}):</b>\n\n"
            for i, d in enumerate(drops[max_show:], max_show + 1):
                summary += f"{i}. {d['ticker']}: <b>{d['drop_percent']:+.2f}%</b> | {d['current_price']} ₽\n"
            
            try:
                await context.bot.send_message(chat_id=chat_id, text=summary, parse_mode='HTML')
            except:
                pass
                
    except Exception as e:
        logger.error(f"Ошибка: {e}", exc_info=True)
        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"❌ Ошибка: {escape_html(str(e)[:200])}"
            )
        except:
            pass

# ================= КОМАНДЫ =================
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "🔻 <b>MOEX Drop Scanner Bot v2.0</b>\n\n"
        "🔍 <b>Что делает бот:</b>\n"
        f"• Сканирует <b>ВСЕ акции МосБиржи</b> с объемом > {MIN_AVG_VOLUME_5D/1e6:.0f} млн ₽\n"
        f"• Находит акции, упавшие на <b>≥{MIN_DROP_PERCENT}%</b> за последние <b>{LOOKBACK_TRADING_DAYS} торговых дня</b>\n"
        f"• Фильтрует по ликвидности: объем <b>≥{MIN_DAILY_VOLUME_RUB/1e6:.0f} млн ₽/день</b>\n"
        "• Показывает динамику падения по дням\n\n"
        f"<b>🔄 Автосканирование:</b> каждые <b>{SCAN_INTERVAL_MINUTES} минут</b>\n\n"
        "<b>📋 Команды:</b>\n"
        "/scan — ручное сканирование\n"
        "/help — справка\n"
        "/stats — статистика рынка\n"
        "/status — статус автосканирования\n"
        "/stop_scan — остановить автосканирование\n"
        "/start_scan — запустить автосканирование"
    )
    await update.message.reply_text(text, parse_mode='HTML')

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "📚 <b>Справка по Drop Scanner v2.0</b>\n\n"
        "<b>🔍 Как работает сканер:</b>\n"
        "1. Бот получает список ВСЕХ акций МосБиржи с объемом >100 млн ₽\n"
        f"2. Сравнивает цену закрытия {LOOKBACK_TRADING_DAYS} торговых дня назад с текущей\n"
        f"3. Отбирает акции с падением ≥{MIN_DROP_PERCENT}%\n"
        f"4. Проверяет ликвидность (средний объем ≥{MIN_DAILY_VOLUME_RUB/1e6:.0f} млн ₽)\n\n"
        f"<b>🔄 Автосканирование:</b> каждые {SCAN_INTERVAL_MINUTES} минут\n\n"
        "<b>📊 Что показывает бот:</b>\n"
        "• Процент падения\n"
        "• Текущая цена и цена 3 дня назад\n"
        "• Максимальная просадка\n"
        "• Диапазон цен (мин/макс)\n"
        "• Средний объем торгов\n"
        "• Динамика по каждому дню\n\n"
        "<b>🎯 Как использовать:</b>\n"
        "• Сильные падения (≥20%) — потенциальный отскок\n"
        "• Умеренные падения (10-15%) — может продолжиться тренд\n"
        "• Смотрите на объем: высокий объем при падении = сильные продажи\n"
        "• Используйте TradingView для анализа графиков\n\n"
        "<b>⚠️ Предупреждение:</b>\n"
        "Бот не дает торговых рекомендаций. Падение может продолжиться. "
        "Всегда проводите собственный анализ."
    )
    await update.message.reply_text(text, parse_mode='HTML')

async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает статистику рынка"""
    msg = await update.message.reply_text("📊 <b>Сбор статистики рынка...</b>", parse_mode='HTML')
    
    try:
        drops, excluded_liq, excluded_other, total_checked = await scanner.scan_drops()
        
        text = (
            "📊 <b>СТАТИСТИКА РЫНКА</b>\n"
            f"{TradingSession.get_session_status_text()}\n\n"
            f"🔍 Проверено акций: <b>{total_checked}</b>\n"
            f"🔻 Найдено падений (≥{MIN_DROP_PERCENT}%): <b>{len(drops)}</b>\n"
            f"🚫 Исключено по ликвидности: <b>{len(excluded_liq)}</b>\n"
            f"⚠️ Нет данных/ошибки: <b>{len(excluded_other)}</b>\n\n"
        )
        
        if drops:
            # Распределение по силе падения
            severe = sum(1 for d in drops if d['drop_percent'] <= -25)
            strong = sum(1 for d in drops if -25 < d['drop_percent'] <= -20)
            moderate = sum(1 for d in drops if -20 < d['drop_percent'] <= -15)
            mild = sum(1 for d in drops if -15 < d['drop_percent'] <= -10)
            
            text += (
                "<b>Распределение падений:</b>\n"
                f"💀 Свыше 25%: <b>{severe}</b>\n"
                f"🩸 20-25%: <b>{strong}</b>\n"
                f"📉 15-20%: <b>{moderate}</b>\n"
                f"⬇️ 10-15%: <b>{mild}</b>\n\n"
            )
            
            # Топ-5 падений
            text += "<b>Топ-5 падений:</b>\n"
            for i, d in enumerate(drops[:5], 1):
                text += f"{i}. {d['ticker']}: <b>{d['drop_percent']:+.2f}%</b>\n"
        
        await msg.edit_text(text, parse_mode='HTML')
        
    except Exception as e:
        logger.error(f"Ошибка: {e}", exc_info=True)
        await msg.edit_text(f"❌ Ошибка: {escape_html(str(e)[:200])}")

async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает статус автосканирования"""
    jobs = context.job_queue.jobs()
    
    text = "📊 <b>СТАТУС СИСТЕМЫ</b>\n\n"
    text += f"{TradingSession.get_session_status_text()}\n\n"
    
    # Информация о заданиях
    scan_jobs = [j for j in jobs if 'scan' in j.name.lower()]
    
    if scan_jobs:
        text += f"🔄 <b>Автосканирование:</b> активно\n"
        text += f"⏱ Интервал: каждые {SCAN_INTERVAL_MINUTES} мин.\n"
        if scan_jobs[0].next_t:
            next_run = scan_jobs[0].next_t + timedelta(hours=3)
            text += f"🕐 Следующее: {next_run.strftime('%H:%M:%S')} МСК\n"
    else:
        text += "🔴 <b>Автосканирование:</b> остановлено\n"
    
    # Информация о последнем сканировании
    if scanner.last_scan_time:
        last_scan_msk = scanner.last_scan_time + timedelta(hours=3)
        text += f"\n📅 Последнее сканирование: {last_scan_msk.strftime('%H:%M:%S')} МСК\n"
        text += f"📊 Найдено падений: {scanner.last_drops_count}\n"
    
    text += f"\n📋 Всего заданий: {len(jobs)}"
    
    await update.message.reply_text(text, parse_mode='HTML')

async def stop_scan_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Останавливает автосканирование"""
    jobs = context.job_queue.jobs()
    scan_jobs = [j for j in jobs if 'scan' in j.name.lower()]
    
    if not scan_jobs:
        await update.message.reply_text("🔴 Автосканирование уже остановлено")
        return
    
    for job in scan_jobs:
        job.schedule_removal()
    
    await update.message.reply_text("🔴 <b>Автосканирование остановлено</b>\n\nИспользуйте /start_scan для запуска", parse_mode='HTML')

async def start_scan_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Запускает автосканирование"""
    jobs = context.job_queue.jobs()
    scan_jobs = [j for j in jobs if 'scan' in j.name.lower()]
    
    if scan_jobs:
        await update.message.reply_text("🔄 Автосканирование уже запущено")
        return
    
    # Запускаем автосканирование
    context.job_queue.run_repeating(
        auto_scan_job,
        interval=SCAN_INTERVAL_SECONDS,
        first=10,
        name="auto_scan"
    )
    
    await update.message.reply_text(
        f"🔄 <b>Автосканирование запущено</b>\n\n"
        f"⏱ Интервал: каждые {SCAN_INTERVAL_MINUTES} мин.",
        parse_mode='HTML'
    )

async def scan_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ручное сканирование"""
    await update.message.reply_text(
        f"🔍 <b>Запуск ручного сканирования...</b>\n"
        f"<i>Интервал: каждые {SCAN_INTERVAL_MINUTES} мин.</i>",
        parse_mode='HTML'
    )
    await send_scan_results(context, chat_id=update.effective_chat.id)

# ================= АВТОСКАНИРОВАНИЕ =================
async def auto_scan_job(context: ContextTypes.DEFAULT_TYPE):
    """Функция для автоматического сканирования"""
    logger.info(f"🔄 Автосканирование (каждые {SCAN_INTERVAL_MINUTES} мин)")
    await send_scan_results(context)

# ================= ЗАПУСК =================
def main():
    if not TOKEN:
        logger.error("❌ Токен не найден!")
        print("Ошибка: укажите TOKEN в .env файле")
        sys.exit(1)
    
    if ADMIN_CHAT_ID == 0:
        logger.error("❌ ADMIN_CHAT_ID не найден!")
        print("Ошибка: укажите ADMIN_CHAT_ID в .env файле")
        sys.exit(1)
    
    logger.info("=" * 50)
    logger.info("🔻 MOEX DROP SCANNER BOT v2.0")
    logger.info("📊 Сканирование ВСЕХ акций МосБиржи")
    logger.info("=" * 50)
    logger.info(f"📉 Мин. падение: {MIN_DROP_PERCENT}% за {LOOKBACK_TRADING_DAYS} торг. дня")
    logger.info(f"💰 Мин. дневной объем: {MIN_DAILY_VOLUME_RUB/1e6:.0f} млн ₽")
    logger.info(f"📊 Мин. средний объем за {VOLUME_DAYS} дн.: {MIN_AVG_VOLUME_5D/1e6:.0f} млн ₽")
    logger.info(f"🔄 Интервал автосканирования: каждые {SCAN_INTERVAL_MINUTES} мин")
    logger.info(f"👤 ADMIN_CHAT_ID: {ADMIN_CHAT_ID}")
    
    app = Application.builder().token(TOKEN).build()
    
    # Добавляем обработчики команд
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("scan", scan_cmd))
    app.add_handler(CommandHandler("stats", stats_cmd))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(CommandHandler("stop_scan", stop_scan_cmd))
    app.add_handler(CommandHandler("start_scan", start_scan_cmd))
    
    # Запускаем автосканирование
    job_queue = app.job_queue
    if job_queue:
        job_queue.run_repeating(
            auto_scan_job,
            interval=SCAN_INTERVAL_SECONDS,
            first=10,  # Первый запуск через 10 секунд
            name="auto_scan"
        )
        logger.info(f"🔄 Автосканирование запущено: каждые {SCAN_INTERVAL_MINUTES} минут")
    
    print("\n" + "=" * 50)
    print("✅ Drop Scanner Bot v2.0 запущен!")
    print("=" * 50)
    print("📊 Сканируются ВСЕ акции МосБиржи")
    print(f"🔄 Автосканирование каждые {SCAN_INTERVAL_MINUTES} минут")
    print(f"🔻 Поиск падений ≥{MIN_DROP_PERCENT}% за {LOOKBACK_TRADING_DAYS} торг. дня")
    print(f"💰 Фильтр ликвидности: ≥{MIN_DAILY_VOLUME_RUB/1e6:.0f} млн ₽/день")
    print("📋 Команды: /start, /help, /scan, /stats, /status, /stop_scan, /start_scan")
    print("Нажмите Ctrl+C для остановки\n")
    
    try:
        app.run_polling(allowed_updates=Update.ALL_TYPES)
    except KeyboardInterrupt:
        logger.info("👋 Бот остановлен")
        print("\n👋 Бот остановлен")
    finally:
        asyncio.run(api.close())

if __name__ == "__main__":
    main()