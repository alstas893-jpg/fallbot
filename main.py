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
LOOKBACK_TRADING_DAYS = 3  # Анализируем падение за последние 3 торговых дня

# ================= СПИСОК НАБЛЮДЕНИЯ (все акции МосБиржи) =================
WATCHLIST = [
    "SBER", "GAZP", "LKOH", "TATN", "ROSN", "NVTK", "GMKN", "PLZL", "VTBR", "MOEX",
    "YDEX", "CHMF", "MAGN", "ALRS", "MTSS", "SNGS", "SNGSP", "TRNFP", "AFKS", "RTKM",
    "MGNT", "RUAL", "PHOR", "NLMK", "BSPB", "CBOM", "AFLT", "IRAO",
    "OZON", "T", "VKCO", "FIXR", "LENT", "X5", "RNFT", "SVCB", "UGLD",
    "POLY", "PIKK", "ETLN", "SGZH", "BELU", "ABIO", "MRKV", "MRKC", "MRKU", "MRKP",
    "HYDR", "FEES", "UPRO", "NKNC", "NKNCP", "KZOS", "KZOSP", "KAZT", "KAZTP",
    "UNKL", "RASP", "MSNG", "OPIN", "LSRG", "GEMC", "SVAV", "KMAZ", "MTLR", "MTLRP",
    "BLNG", "SIBN", "ELFV", "WUSH", "HEAD", "CARM", "RENI", "SOFL", "DIAS", "GTRK",
    "MDMG", "GECO", "CIAN", "VSEH", "ASTA", "MRKK", "MRKY", "MRKS", "RBCM", "SFIN",
    "EUTR", "PRMD", "NKHP", "APTK", "ABRD", "GCHE", "HHRU", "DELI", "POSI", "FLOT",
    "SMLT", "MVID", "KUZB", "TTLK", "DVEC", "TGKA", "TGKB", "TGC1", "OGKB", "MSST",
    "RSTI", "LNZL", "LNZLP", "VRSB", "NAUK", "NSVZ", "PAZA", "KROT", "KRSB", "KRSBP",
    "MGTSP", "OMZZP", "RTSB", "RTSBP", "TASB", "TASBP", "USBN", "YAKG", "YKEN", "YNDX",
]

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
    
    async def get_candles(self, ticker: str, days: int = 60) -> Optional[pd.DataFrame]:
        """Получение дневных свечей"""
        till = datetime.now().strftime('%Y-%m-%d')
        frm = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        
        url = (f"{self.BASE}/engines/stock/markets/shares/boards/TQBR/securities/{ticker}/candles.json"
               f"?from={frm}&till={till}&interval=24&iss.meta=off&iss.only=candles")
        
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
    
    def __init__(self):
        self.api = MoexAPI()
    
    async def scan_drops(self, tickers: List[str]) -> Tuple[List[dict], List[str], List[str]]:
        """
        Сканирует все тикеры, возвращает:
        - список упавших с деталями
        - список исключенных по ликвидности
        - список исключенных по другим причинам
        """
        logger.info("=" * 50)
        logger.info("🔍 ЗАПУСК СКАНИРОВАНИЯ ПАДАЮЩИХ АКЦИЙ")
        logger.info(f"📊 Параметры: падение ≥{MIN_DROP_PERCENT}% за {LOOKBACK_TRADING_DAYS} торг. дня")
        logger.info(f"💰 Фильтр ликвидности: объем ≥{MIN_DAILY_VOLUME_RUB/1e6:.0f} млн ₽/день")
        logger.info("=" * 50)
        
        drops = []
        excluded_liquidity = []
        excluded_other = []
        
        for ticker in tickers:
            try:
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
                    for i in range(1, len(closes)):
                        daily_change = ((closes[i] - closes[i-1]) / closes[i-1]) * 100
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
            
            await asyncio.sleep(0.1)
        
        # Сортировка по величине падения (от большего к меньшему)
        drops.sort(key=lambda x: x['drop_percent'])
        
        logger.info("=" * 50)
        logger.info(f"СКАНИРОВАНИЕ ЗАВЕРШЕНО. Найдено падений: {len(drops)}")
        logger.info(f"Исключено по ликвидности: {len(excluded_liquidity)}")
        logger.info(f"Исключено по другим причинам: {len(excluded_other)}")
        logger.info("=" * 50)
        
        return drops, excluded_liquidity, excluded_other


# ================= БОТ =================
api = MoexAPI()
scanner = DropScanner()

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

async def scan_drops() -> Tuple[List[dict], List[str], List[str]]:
    """Обертка для сканирования"""
    return await scanner.scan_drops(WATCHLIST)

async def send_scan_results(context: ContextTypes.DEFAULT_TYPE, chat_id: int = None):
    """Отправка результатов сканирования"""
    if chat_id is None:
        chat_id = ADMIN_CHAT_ID
    
    if chat_id == 0:
        logger.error("ADMIN_CHAT_ID не указан")
        return
    
    try:
        drops, excluded_liquidity, excluded_other = await scan_drops()
        
        session_status = TradingSession.get_session_status_text()
        
        if not drops:
            text = (
                f"📊 <b>Падающих акций не найдено</b>\n\n"
                f"{session_status}\n\n"
                f"За последние {LOOKBACK_TRADING_DAYS} торговых дня нет акций, "
                f"упавших на {MIN_DROP_PERCENT}% и более.\n\n"
                f"💰 Фильтр ликвидности: объем ≥{MIN_DAILY_VOLUME_RUB/1e6:.0f} млн ₽/день\n\n"
                f"💡 <i>Попробуйте запустить /scan позже</i>"
            )
            await context.bot.send_message(chat_id=chat_id, text=text, parse_mode='HTML')
            return
        
        # Заголовок
        header = (
            f"🔻 <b>НАЙДЕНО ПАДЕНИЙ: {len(drops)}</b>\n"
            f"{session_status}\n"
            f"📊 Падение ≥{MIN_DROP_PERCENT}% за {LOOKBACK_TRADING_DAYS} торг. дня\n"
            f"💰 Объем ≥{MIN_DAILY_VOLUME_RUB/1e6:.0f} млн ₽/день\n\n"
            f"<i>Сортировка по величине падения:</i>"
        )
        
        await context.bot.send_message(chat_id=chat_id, text=header, parse_mode='HTML')
        
        # Отправляем каждый результат
        for i, d in enumerate(drops, 1):
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
                f"{daily_str}\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"<i>Анализ за {d['trading_days_count']} торговых дней</i>"
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
        
        # Сводка исключенных
        if excluded_liquidity:
            excl_text = f"🚫 <b>Исключено по ликвидности ({len(excluded_liquidity)}):</b>\n"
            excl_text += "\n".join([f"• {e}" for e in excluded_liquidity[:10]])
            if len(excluded_liquidity) > 10:
                excl_text += f"\n... и еще {len(excluded_liquidity) - 10}"
            
            try:
                await context.bot.send_message(chat_id=chat_id, text=excl_text, parse_mode='HTML')
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
        "🔻 <b>MOEX Drop Scanner Bot</b>\n\n"
        "🔍 <b>Что делает бот:</b>\n"
        f"• Находит акции, упавшие на <b>≥{MIN_DROP_PERCENT}%</b> за последние <b>{LOOKBACK_TRADING_DAYS} торговых дня</b>\n"
        f"• Фильтрует по ликвидности: объем <b>≥{MIN_DAILY_VOLUME_RUB/1e6:.0f} млн ₽/день</b>\n"
        "• Исключает выходные и праздничные дни (нет ликвидности)\n"
        "• Показывает динамику падения по дням\n\n"
        "<b>🕐 Автоматическое сканирование:</b>\n"
        "• 11:00 МСК\n"
        "• 19:15 МСК\n\n"
        "<b>📋 Команды:</b>\n"
        "/scan — ручное сканирование\n"
        "/help — справка\n"
        "/stats — статистика рынка"
    )
    await update.message.reply_text(text, parse_mode='HTML')

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "📚 <b>Справка по Drop Scanner</b>\n\n"
        "<b>🔍 Как работает сканер:</b>\n"
        f"1. Бот получает дневные свечи для {len(WATCHLIST)} акций\n"
        f"2. Сравнивает цену закрытия {LOOKBACK_TRADING_DAYS} торговых дня назад с текущей\n"
        f"3. Отбирает акции с падением ≥{MIN_DROP_PERCENT}%\n"
        f"4. Проверяет ликвидность (средний объем ≥{MIN_DAILY_VOLUME_RUB/1e6:.0f} млн ₽)\n\n"
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
        drops, excluded_liq, excluded_other = await scan_drops()
        
        total_checked = len(drops) + len(excluded_liq) + len(excluded_other)
        
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

async def scan_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ручное сканирование"""
    session_key, is_trading = TradingSession.get_current_session()
    
    if not is_trading and session_key is not None:
        warning = "⏳ <b>Внимание:</b> Сейчас аукцион. Данные могут быть неполными."
        await update.message.reply_text(warning, parse_mode='HTML')
    elif session_key is None:
        warning = "🔴 <b>Торги закрыты!</b> Данные могут быть неактуальными."
        await update.message.reply_text(warning, parse_mode='HTML')
    
    msg = await update.message.reply_text(
        f"🔍 <b>Сканирование падающих акций...</b>\n"
        f"<i>Анализирую {len(WATCHLIST)} инструментов</i>\n"
        f"<i>Падение ≥{MIN_DROP_PERCENT}% за {LOOKBACK_TRADING_DAYS} торг. дня</i>\n"
        f"<i>Фильтр: объем ≥{MIN_DAILY_VOLUME_RUB/1e6:.0f} млн ₽/день</i>",
        parse_mode='HTML'
    )
    
    try:
        drops, excluded_liquidity, excluded_other = await scan_drops()
        await msg.delete()
        
        session_status = TradingSession.get_session_status_text()
        
        if not drops:
            text = (
                f"📊 <b>Падающих акций не найдено</b>\n\n"
                f"{session_status}\n\n"
                f"За последние {LOOKBACK_TRADING_DAYS} торговых дня нет акций, "
                f"упавших на {MIN_DROP_PERCENT}% и более.\n\n"
                f"💰 Фильтр: объем ≥{MIN_DAILY_VOLUME_RUB/1e6:.0f} млн ₽/день"
            )
            await update.message.reply_text(text, parse_mode='HTML')
            return
        
        header = (
            f"🔻 <b>НАЙДЕНО ПАДЕНИЙ: {len(drops)}</b>\n"
            f"{session_status}\n"
            f"📊 Падение ≥{MIN_DROP_PERCENT}% за {LOOKBACK_TRADING_DAYS} торг. дня\n\n"
            f"<i>Сортировка по величине падения:</i>"
        )
        
        await update.message.reply_text(header, parse_mode='HTML')
        
        for i, d in enumerate(drops, 1):
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
                f"💵 <b>Объем торгов:</b>\n"
                f"• Средний за 3 дня: <b>{format_volume(d['avg_daily_volume'])} ₽</b>\n"
                f"• Последний день: <b>{format_volume(d['last_volume'])} ₽</b>\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"📅 <b>Динамика по дням:</b>\n"
                f"{daily_str}"
            )
            
            try:
                await update.message.reply_text(
                    text, 
                    parse_mode='HTML', 
                    reply_markup=create_tradingview_keyboard(d['ticker'])
                )
            except Exception as e:
                logger.error(f"Ошибка: {e}")
                plain = text.replace('<b>', '').replace('</b>', '').replace('<i>', '').replace('</i>', '')
                await update.message.reply_text(plain, reply_markup=create_tradingview_keyboard(d['ticker']))
            
            await asyncio.sleep(0.5)
        
        # Показываем исключенные
        if excluded_liquidity:
            excl_text = f"🚫 <b>Исключено по ликвидности ({len(excluded_liquidity)}):</b>\n"
            excl_text += "\n".join([f"• {e}" for e in excluded_liquidity[:10]])
            if len(excluded_liquidity) > 10:
                excl_text += f"\n... и еще {len(excluded_liquidity) - 10}"
            await update.message.reply_text(excl_text, parse_mode='HTML')
            
    except Exception as e:
        logger.error(f"Ошибка: {e}", exc_info=True)
        try:
            await msg.edit_text(f"❌ Ошибка: {escape_html(str(e)[:200])}")
        except:
            await update.message.reply_text("❌ Произошла ошибка при сканировании")

# ================= ПЛАНИРОВЩИК =================
async def scheduled_scan_11(context: ContextTypes.DEFAULT_TYPE):
    logger.info("🕐 Запуск сканирования в 11:00 МСК")
    await send_scan_results(context)

async def scheduled_scan_1915(context: ContextTypes.DEFAULT_TYPE):
    logger.info("🕐 Запуск сканирования в 19:15 МСК")
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
    logger.info("🔻 MOEX DROP SCANNER BOT")
    logger.info("=" * 50)
    logger.info(f"📋 Watchlist: {len(WATCHLIST)} инструментов")
    logger.info(f"📉 Мин. падение: {MIN_DROP_PERCENT}% за {LOOKBACK_TRADING_DAYS} торг. дня")
    logger.info(f"💰 Мин. дневной объем: {MIN_DAILY_VOLUME_RUB/1e6:.0f} млн ₽")
    logger.info(f"👤 ADMIN_CHAT_ID: {ADMIN_CHAT_ID}")
    
    app = Application.builder().token(TOKEN).build()
    
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("scan", scan_cmd))
    app.add_handler(CommandHandler("stats", stats_cmd))
    
    job_queue = app.job_queue
    if job_queue:
        job_queue.run_daily(
            scheduled_scan_11,
            time=datetime.strptime("08:00", "%H:%M").time(),
            days=(0, 1, 2, 3, 4)
        )
        
        job_queue.run_daily(
            scheduled_scan_1915,
            time=datetime.strptime("16:15", "%H:%M").time(),
            days=(0, 1, 2, 3, 4)
        )
        
        logger.info("🕐 Автосканирование: 11:00 и 19:15 МСК")
    
    print("\n" + "=" * 50)
    print("✅ Бот запущен!")
    print("=" * 50)
    print(f"🔻 Поиск падений ≥{MIN_DROP_PERCENT}% за {LOOKBACK_TRADING_DAYS} торг. дня")
    print(f"💰 Фильтр ликвидности: ≥{MIN_DAILY_VOLUME_RUB/1e6:.0f} млн ₽/день")
    print("🕐 Автосканирование: 11:00 и 19:15 МСК")
    print("📋 Команды: /start, /help, /scan, /stats")
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