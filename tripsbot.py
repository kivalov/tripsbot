import logging
import sqlite3
import csv
import os
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
import asyncio
import aiohttp
from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import Command, CommandStart
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from dotenv import load_dotenv
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Загрузка переменных окружения
load_dotenv()
BOT_TOKEN = os.getenv('BOT_TOKEN')
ADMINS = [int(admin_id) for admin_id in os.getenv('ADMINS', '').split(',')]

# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot=bot, storage=storage)
router = Router()
dp.include_router(router)

# Геокодер
geolocator = Nominatim(user_agent="tripsbot")

# Мультиязычность
TRANSLATIONS = {
    'ru': {
        'welcome': 'Добро пожаловать! Выберите язык:',
        'registered': 'Вы уже зарегистрированы и у вас есть командировка: {trip}',
        'choose_language': 'Выберите язык:',
        'name_prompt': 'Введите ваше имя:',
        'checkin_prompt': 'Отправьте геопозицию для чек-ина:',
        'status_prompt': 'Выберите статус:',
        'status_ok': 'Всё в порядке',
        'status_help': 'Нужна помощь',
        'checkin_success': 'Чек-ин зарегистрирован: {status}',
        'checkin_error': 'Ошибка при регистрации чек-ина',
        'export_prompt': 'Чек-ины экспортированы в CSV',
        'trip_menu': 'Меню командировки:',
        'trip_frequency': 'Изменить частоту чек-инов',
        'trip_country': 'Изменить страну',
        'frequency_prompt': 'Выберите частоту чек-инов (в часах):',
        'country_prompt': 'Введите новую страну:',
        'frequency_updated': 'Частота чек-инов обновлена: {hours} часов',
        'country_updated': 'Страна обновлена: {country}',
    },
    'en': {
        'welcome': 'Welcome! Choose a language:',
        'registered': 'You are already registered and have a trip: {trip}',
        'choose_language': 'Choose a language:',
        'name_prompt': 'Enter your name:',
        'checkin_prompt': 'Send your location for check-in:',
        'status_prompt': 'Choose status:',
        'status_ok': 'Everything is fine',
        'status_help': 'Need help',
        'checkin_success': 'Check-in registered: {status}',
        'checkin_error': 'Error registering check-in',
        'export_prompt': 'Check-ins exported to CSV',
        'trip_menu': 'Trip menu:',
        'trip_frequency': 'Change check-in frequency',
        'trip_country': 'Change country',
        'frequency_prompt': 'Choose check-in frequency (in hours):',
        'country_prompt': 'Enter new country:',
        'frequency_updated': 'Check-in frequency updated: {hours} hours',
        'country_updated': 'Country updated: {country}',
    },
    'es': {
        'welcome': '¡Bienvenido! Elige un idioma:',
        'registered': 'Ya estás registrado y tienes un viaje: {trip}',
        'choose_language': 'Elige un idioma:',
        'name_prompt': 'Ingresa tu nombre:',
        'checkin_prompt': 'Envía tu ubicación para el check-in:',
        'status_prompt': 'Elige el estado:',
        'status_ok': 'Todo está bien',
        'status_help': 'Necesito ayuda',
        'checkin_success': 'Check-in registrado: {status}',
        'checkin_error': 'Error al registrar el check-in',
        'export_prompt': 'Check-ins exportados a CSV',
        'trip_menu': 'Menú del viaje:',
        'trip_frequency': 'Cambiar la frecuencia de check-ins',
        'trip_country': 'Cambiar el país',
        'frequency_prompt': 'Elige la frecuencia de check-ins (en horas):',
        'country_prompt': 'Ingresa el nuevo país:',
        'frequency_updated': 'Frecuencia de check-ins actualizada: {hours} horas',
        'country_updated': 'País actualizado: {country}',
    }
}

# Инициализация базы данных
def init_db():
    conn = sqlite3.connect('tripsbot.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        name TEXT,
        language TEXT DEFAULT 'ru',
        trip TEXT,
        frequency INTEGER DEFAULT 8,
        country TEXT
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS checkins (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        timestamp TEXT,
        latitude REAL,
        longitude REAL,
        status TEXT,
        country TEXT,
        FOREIGN KEY (user_id) REFERENCES users (user_id)
    )''')
    conn.commit()
    conn.close()

init_db()

# FSM для регистрации
class Registration(StatesGroup):
    Language = State()
    Name = State()

# FSM для чек-ина
class CheckIn(StatesGroup):
    Location = State()
    Status = State()

# FSM для изменения командировки
class TripUpdate(StatesGroup):
    Frequency = State()
    Country = State()

# Получение перевода
def get_translation(user_id: int, key: str, **kwargs) -> str:
    conn = sqlite3.connect('tripsbot.db')
    c = conn.cursor()
    c.execute('SELECT language FROM users WHERE user_id = ?', (user_id,))
    result = c.fetchone()
    lang = result[0] if result else 'ru'
    conn.close()
    translation = TRANSLATIONS.get(lang, TRANSLATIONS['ru']).get(key, key)
    return translation.format(**kwargs)

# Геокодирование
async def get_country_from_coords(latitude: float, longitude: float) -> str:
    try:
        async with aiohttp.ClientSession() as session:
            location = await asyncio.get_event_loop().run_in_executor(
                None, lambda: geolocator.reverse((latitude, longitude), language='en')
            )
        return location.raw['address'].get('country', 'Unknown') if location else 'Unknown'
    except (GeocoderTimedOut, Exception) as e:
        logger.error(f"Geocoding error: {e}")
        return 'Unknown'

# Команда /start
@router.message(CommandStart())
async def start_command(message: Message, state: FSMContext):
    user_id = message.from_user.id
    conn = sqlite3.connect('tripsbot.db')
    c = conn.cursor()
    c.execute('SELECT name, trip FROM users WHERE user_id = ?', (user_id,))
    result = c.fetchone()
    conn.close()

    if result:
        name, trip = result
        await message.answer(
            get_translation(user_id, 'registered', trip=trip),
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=get_translation(user_id, 'choose_language'), callback_data='change_language')]
            ])
        )
    else:
        await message.answer(
            TRANSLATIONS['ru']['welcome'],
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Русский", callback_data='lang_ru')],
                [InlineKeyboardButton(text="English", callback_data='lang_en')],
                [InlineKeyboardButton(text="Español", callback_data='lang_es')]
            ])
        )
        await state.set_state(Registration.Language)

# Выбор языка
@router.callback_query(F.data.startswith('lang_'))
async def process_language(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    lang = callback.data.split('_')[1]
    await state.update_data(language=lang)
    await callback.message.edit_text(get_translation(user_id, 'name_prompt'))
    await state.set_state(Registration.Name)
    await callback.answer()

# Изменение языка
@router.callback_query(F.data == 'change_language')
async def change_language(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    await callback.message.edit_text(
        get_translation(user_id, 'choose_language'),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Русский", callback_data='lang_ru')],
            [InlineKeyboardButton(text="English", callback_data='lang_en')],
            [InlineKeyboardButton(text="Español", callback_data='lang_es')]
        ])
    )
    await state.set_state(Registration.Language)
    await callback.answer()

# Регистрация имени
@router.message(Registration.Name)
async def process_name(message: Message, state: FSMContext):
    user_id = message.from_user.id
    name = message.text
    data = await state.get_data()
    language = data.get('language', 'ru')

    conn = sqlite3.connect('tripsbot.db')
    c = conn.cursor()
    c.execute('INSERT OR REPLACE INTO users (user_id, name, language, trip) VALUES (?, ?, ?, ?)',
              (user_id, name, language, 'Test Trip'))
    conn.commit()
    conn.close()

    await message.answer(get_translation(user_id, 'checkin_prompt'))
    await state.clear()

# Команда /checkin
@router.message(Command('checkin'))
async def checkin_command(message: Message, state: FSMContext):
    user_id = message.from_user.id
    await message.answer(get_translation(user_id, 'checkin_prompt'))
    await state.set_state(CheckIn.Location)

# Обработка геопозиции
@router.message(CheckIn.Location, F.location)
async def process_location(message: Message, state: FSMContext):
    user_id = message.from_user.id
    latitude = message.location.latitude
    longitude = message.location.longitude
    await state.update_data(latitude=latitude, longitude=longitude)
    await message.answer(
        get_translation(user_id, 'status_prompt'),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=get_translation(user_id, 'status_ok'), callback_data='status_ok')],
            [InlineKeyboardButton(text=get_translation(user_id, 'status_help'), callback_data='status_help')]
        ])
    )
    await state.set_state(CheckIn.Status)

# Обработка статуса чек-ина
@router.callback_query(CheckIn.Status, F.data.startswith('status_'))
async def process_status(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    status = callback.data.split('_')[1]
    data = await state.get_data()
    latitude = data.get('latitude')
    longitude = data.get('longitude')
    timestamp = datetime.now().isoformat()
    country = await get_country_from_coords(latitude, longitude)

    try:
        conn = sqlite3.connect('tripsbot.db')
        c = conn.cursor()
        c.execute('INSERT INTO checkins (user_id, timestamp, latitude, longitude, status, country) VALUES (?, ?, ?, ?, ?, ?)',
                  (user_id, timestamp, latitude, longitude, status, country))
        conn.commit()
        conn.close()

        status_text = get_translation(user_id, f'status_{status}')
        await callback.message.edit_text(get_translation(user_id, 'checkin_success', status=status_text))

        # Уведомление админа
        for admin_id in ADMINS:
            await bot.send_message(
                admin_id,
                f"Чек-ин от {user_id}: {status_text} в {country} ({latitude}, {longitude})"
            )
    except Exception as e:
        logger.error(f"Check-in error for user {user_id}: {e}")
        await callback.message.edit_text(get_translation(user_id, 'checkin_error'))
    finally:
        await state.clear()
        await callback.answer()

# Команда /export
@router.message(Command('export'))
async def export_command(message: Message):
    user_id = message.from_user.id
    args = message.text.split()
    period = args[1] if len(args) > 1 else 'all'

    conn = sqlite3.connect('tripsbot.db')
    c = conn.cursor()
    query = 'SELECT user_id, timestamp, latitude, longitude, status, country FROM checkins'
    params = []

    if period != 'all':
        try:
            if period.endswith('w'):
                weeks = int(period[:-1])
                start_date = datetime.now() - timedelta(weeks=weeks)
                query += ' WHERE timestamp >= ?'
                params.append(start_date.isoformat())
            elif period.endswith('d'):
                days = int(period[:-1])
                start_date = datetime.now() - timedelta(days=days)
                query += ' WHERE timestamp >= ?'
                params.append(start_date.isoformat())
        except ValueError:
            await message.answer("Неверный формат периода. Используйте 'all', '1w', '2d' и т.д.")
            conn.close()
            return

    c.execute(query, params)
    checkins = c.fetchall()
    conn.close()

    if not checkins:
        await message.answer(get_translation(user_id, 'export_prompt') + ": нет данных")
        return

    csv_file = 'checkins.csv'
    with open(csv_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['User ID', 'Timestamp', 'Latitude', 'Longitude', 'Status', 'Country'])
        writer.writerows(checkins)

    with open(csv_file, 'rb') as f:
        input_file = BufferedInputFile(f.read(), filename='checkins.csv')
        await message.answer_document(input_file, caption=get_translation(user_id, 'export_prompt'))

    os.remove(csv_file)

# Команда /trip
@router.message(Command('trip'))
async def trip_command(message: Message, state: FSMContext):
    user_id = message.from_user.id
    conn = sqlite3.connect('tripsbot.db')
    c = conn.cursor()
    c.execute('SELECT trip, frequency, country FROM users WHERE user_id = ?', (user_id,))
    result = c.fetchone()
    conn.close()

    if result:
        trip, frequency, country = result
        await message.answer(
            get_translation(user_id, 'trip_menu'),
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=get_translation(user_id, 'trip_frequency'), callback_data='trip_frequency')],
                [InlineKeyboardButton(text=get_translation(user_id, 'trip_country'), callback_data='trip_country')]
            ])
        )
    else:
        await message.answer("Вы не зарегистрированы. Используйте /start.")

# Обработка изменения частоты
@router.callback_query(F.data == 'trip_frequency')
async def process_trip_frequency(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    await callback.message.edit_text(
        get_translation(user_id, 'frequency_prompt'),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="4 часа", callback_data='freq_4')],
            [InlineKeyboardButton(text="8 часов", callback_data='freq_8')],
            [InlineKeyboardButton(text="12 часов", callback_data='freq_12')]
        ])
    )
    await state.set_state(TripUpdate.Frequency)
    await callback.answer()

@router.callback_query(TripUpdate.Frequency, F.data.startswith('freq_'))
async def update_frequency(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    frequency = int(callback.data.split('_')[1])
    conn = sqlite3.connect('tripsbot.db')
    c = conn.cursor()
    c.execute('UPDATE users SET frequency = ? WHERE user_id = ?', (frequency, user_id))
    conn.commit()
    conn.close()
    await callback.message.edit_text(get_translation(user_id, 'frequency_updated', hours=frequency))
    await state.clear()
    await callback.answer()

# Обработка изменения страны
@router.callback_query(F.data == 'trip_country')
async def process_trip_country(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    await callback.message.edit_text(get_translation(user_id, 'country_prompt'))
    await state.set_state(TripUpdate.Country)
    await callback.answer()

@router.message(TripUpdate.Country)
async def update_country(message: Message, state: FSMContext):
    user_id = message.from_user.id
    country = message.text
    conn = sqlite3.connect('tripsbot.db')
    c = conn.cursor()
    c.execute('UPDATE users SET country = ? WHERE user_id = ?', (country, user_id))
    conn.commit()
    conn.close()
    await message.answer(get_translation(user_id, 'country_updated', country=country))
    await state.clear()

# Напоминания
async def send_reminders():
    while True:
        conn = sqlite3.connect('tripsbot.db')
        c = conn.cursor()
        c.execute('SELECT user_id, frequency FROM users')
        users = c.fetchall()
        conn.close()

        now = datetime.now()
        for user_id, frequency in users:
            last_checkin = None
            conn = sqlite3.connect('tripsbot.db')
            c = conn.cursor()
            c.execute('SELECT timestamp FROM checkins WHERE user_id = ? ORDER BY timestamp DESC LIMIT 1', (user_id,))
            result = c.fetchone()
            conn.close()
            if result:
                last_checkin = datetime.fromisoformat(result[0])
            if not last_checkin or (now - last_checkin).total_seconds() >= frequency * 3600:
                await bot.send_message(user_id, get_translation(user_id, 'checkin_prompt'))
        await asyncio.sleep(3600)  # Проверка каждый час

# Запуск бота
async def main():
    logger.info("Бот успешно инициализирован")
    dp.startup.register(lambda: logger.info("Диспетчер успешно инициализирован"))
    asyncio.create_task(send_reminders())
    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()
        logger.info("Сессия бота закрыта")

if __name__ == '__main__':
    asyncio.run(main())
