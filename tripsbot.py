import asyncio
import logging
import sqlite3
import csv
from io import StringIO
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile
from aiogram.filters import Command, CommandStart
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.filters import BaseFilter
from aiogram.types import Message, CallbackQuery, ContentType
import pycountry
from timezonefinder import TimezoneFinder
from pytz import timezone
from geopy.geocoders import Nominatim
from dotenv import load_dotenv
import os
import re
import aiohttp

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    filename='bot.log',
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Загрузка переменных окружения
load_dotenv()
API_TOKEN = os.getenv('API_TOKEN')
ADMIN_ID = os.getenv('ADMIN_ID')

# Проверка переменных окружения
if not API_TOKEN:
    logger.error("API_TOKEN не установлен в .env файле")
    raise ValueError("API_TOKEN должен быть установлен в .env файле")
if not ADMIN_ID:
    logger.error("ADMIN_ID не установлен в .env файле")
    raise ValueError("ADMIN_ID должен быть установлен в .env файле")
try:
    ADMIN_ID = int(ADMIN_ID)
except ValueError:
    logger.error("ADMIN_ID должен быть числом")
    raise ValueError("ADMIN_ID должен быть числом")

# Инициализация бота и диспетчера
try:
    bot = Bot(token=API_TOKEN)
    logger.info("Бот успешно инициализирован")
except Exception as e:
    logger.error(f"Ошибка инициализации бота: {e}")
    raise

storage = MemoryStorage()
try:
    dp = Dispatcher(bot=bot, storage=storage)
    logger.info("Диспетчер успешно инициализирован")
except Exception as e:
    logger.error(f"Ошибка инициализации диспетчера: {e}")
    raise

# Инициализация базы данных
conn = sqlite3.connect('employees.db')
cursor = conn.cursor()
cursor.execute('''
    CREATE TABLE IF NOT EXISTS employees (
        user_id INTEGER PRIMARY KEY,
        name TEXT,
        username TEXT,
        language TEXT DEFAULT 'ru',
        archived INTEGER DEFAULT 0
    )
''')
cursor.execute('''
    CREATE TABLE IF NOT EXISTS trips (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        country TEXT,
        timezone TEXT,
        start_date TEXT,
        end_date TEXT,
        checkin_frequency INTEGER,
        checkin_time TEXT
    )
''')
cursor.execute('''
    CREATE TABLE IF NOT EXISTS checkins (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        latitude REAL,
        longitude REAL,
        status TEXT,
        timestamp TEXT
    )
''')
cursor.execute('CREATE INDEX IF NOT EXISTS idx_trips_user_id ON trips(user_id)')
cursor.execute('CREATE INDEX IF NOT EXISTS idx_checkins_user_id ON checkins(user_id)')
conn.commit()

# Словарь локализации
TRANSLATIONS = {
    'ru': {
        'start_no_employee': 'Начнём регистрацию. Введите ваше имя:',
        'start_registered_active': 'Вы уже зарегистрированы. У вас есть активная командировка. Используйте /trip для просмотра или редактирования.',
        'start_registered_no_trip': 'Вы уже зарегистрированы, но активных командировок нет. Хотите создать новую командировку?',
        'choose_language': 'Выберите язык:',
        'name_empty': 'Имя не может быть пустым. Пожалуйста, введите ваше имя:',
        'country_prompt': 'Введите первую страну пребывания:',
        'country_empty': 'Название страны не может быть пустым. Пожалуйста, введите страну:',
        'start_date_prompt': 'Введите дату начала пребывания (ДД/ММ/ГГГГ):',
        'end_date_prompt': 'Введите дату окончания пребывания (ДД/ММ/ГГГГ):',
        'invalid_date': 'Неверный формат даты. Используйте ДД/ММ/ГГГГ, например, 01/05/2025.',
        'end_before_start': 'Дата окончания не может быть раньше даты начала.',
        'frequency_prompt': 'Выберите частоту чек-инов:',
        'invalid_frequency': 'Неверный выбор частоты.',
        'time_prompt': 'Выберите время чек-ина:',
        'invalid_time': 'Неверный выбор времени.',
        'add_country_prompt': 'Хотите добавить ещё одну страну?',
        'registration_complete': 'Регистрация завершена! Отправляйте геопозицию.',
        'registration_error': 'Произошла ошибка при регистрации. Попробуйте снова.',
        'trip_info': 'Ваша текущая командировка:\nСтрана: {country}\nДаты: {start_date} - {end_date}\nЧастота чек-инов: {freq_text}\nВремя чек-ина: {time_text}\nЧто хотите сделать?',
        'no_active_trip': 'У вас нет активных командировок. Хотите создать новую?',
        'trip_updated': 'Командировка обновлена!',
        'trip_update_error': 'Произошла ошибка при обновлении командировки.',
        'finish_view': 'Просмотр завершён.',
        'new_trip_prompt': 'Введите страну новой командировки:',
        'location_invalid': 'Некорректная геопозиция. Пожалуйста, отправьте снова.',
        'location_received': 'Геопозиция получена. Выберите статус:',
        'checkin_registered': 'Чек-ин зарегистрирован: {status}\nКарта: {maps_url}',
        'checkin_error': 'Произошла ошибка при регистрации чек-ина.',
        'not_registered': 'Сначала зарегистрируйтесь с помощью /start',
        'list_no_employees': 'Нет зарегистрированных сотрудников.',
        'list_error': 'Произошла ошибка при получении списка сотрудников.',
        'status_usage': 'Использование: /status <user_id> или /status @username',
        'status_invalid_id': 'Неверный формат ID. Используйте /status <user_id> или /status @username',
        'status_not_found': 'Сотрудник не найден.',
        'status_response': 'Сотрудник: {name}{username}\nСтатус: {status}\nПоездки: {trips}\nПоследний чек-ин: {time}\nКарта: {maps_url}\nСтатус: {checkin_status}',
        'status_no_checkins': 'Сотрудник: {name}{username}\nСтатус: {status}\nПоездки: {trips}\nЧек-ины отсутствуют.',
        'status_error': 'Произошла ошибка при получении статуса.',
        'export_no_checkins': 'Чек-ины за указанный период или для указанного сотрудника отсутствуют.',
        'export_invalid_format': 'Неверный формат. Используйте /export, /export <число>w, /export @username, /export <user_id>, или их комбинацию (например, /export 2w @username).',
        'export_user_not_found': 'Пользователь {user} не найден.',
        'export_error': 'Произошла ошибка при экспорте чек-инов.',
        'reminder': 'Напоминание: отправьте чек-ин в {time} ({tz})!',
        'missed_checkin': 'Сотрудник {name}{username} не отправил чек-ин!\nОжидалось: {time} ({tz})\nПоследний чек-ин: {last_time}\nПоследняя локация: {location}\nКарта: {maps_url}',
        'archived': 'Сотрудник {name}{username} помечен как архивный (командировки завершены).',
        'freq_1': '1 раз в день',
        'freq_2': '2 раза (утро, вечер)',
        'freq_3': '3 раза (утро, день, вечер)',
        'time_morning': '08:00',
        'time_day': '14:00',
        'time_evening': '20:00',
        'status_ok': 'Всё в порядке',
        'status_health': 'Проблема со здоровьем',
        'status_safety': 'Проблема с безопасностью',
        'edit_country_prompt': 'Введите новую страну пребывания:',
        'edit_freq_prompt': 'Выберите новую частоту чек-инов:',
        'edit_time_prompt': 'Выберите новое время чек-ина:',
    },
    'en': {
        'start_no_employee': 'Let’s start registration. Enter your name:',
        'start_registered_active': 'You are already registered. You have an active trip. Use /trip to view or edit.',
        'start_registered_no_trip': 'You are already registered, but there are no active trips. Want to create a new trip?',
        'choose_language': 'Choose language:',
        'name_empty': 'Name cannot be empty. Please enter your name:',
        'country_prompt': 'Enter the first country of stay:',
        'country_empty': 'Country name cannot be empty. Please enter a country:',
        'start_date_prompt': 'Enter the start date of stay (DD/MM/YYYY):',
        'end_date_prompt': 'Enter the end date of stay (DD/MM/YYYY):',
        'invalid_date': 'Invalid date format. Use DD/MM/YYYY, e.g., 01/05/2025.',
        'end_before_start': 'End date cannot be earlier than start date.',
        'frequency_prompt': 'Select check-in frequency:',
        'invalid_frequency': 'Invalid frequency selection.',
        'time_prompt': 'Select check-in time:',
        'invalid_time': 'Invalid time selection.',
        'add_country_prompt': 'Want to add another country?',
        'registration_complete': 'Registration completed! Send your location.',
        'registration_error': 'An error occurred during registration. Try again.',
        'trip_info': 'Your current trip:\nCountry: {country}\nDates: {start_date} - {end_date}\nCheck-in frequency: {freq_text}\nCheck-in time: {time_text}\nWhat do you want to do?',
        'no_active_trip': 'You have no active trips. Want to create a new one?',
        'trip_updated': 'Trip updated!',
        'trip_update_error': 'An error occurred while updating the trip.',
        'finish_view': 'View completed.',
        'new_trip_prompt': 'Enter the country for the new trip:',
        'location_invalid': 'Invalid location. Please send again.',
        'location_received': 'Location received. Select status:',
        'checkin_registered': 'Check-in registered: {status}\nMap: {maps_url}',
        'checkin_error': 'An error occurred while registering the check-in.',
        'not_registered': 'Register first using /start',
        'list_no_employees': 'No registered employees.',
        'list_error': 'An error occurred while retrieving the employee list.',
        'status_usage': 'Usage: /status <user_id> or /status @username',
        'status_invalid_id': 'Invalid ID format. Use /status <user_id> or /status @username',
        'status_not_found': 'Employee not found.',
        'status_response': 'Employee: {name}{username}\nStatus: {status}\nTrips: {trips}\nLast check-in: {time}\nMap: {maps_url}\nStatus: {checkin_status}',
        'status_no_checkins': 'Employee: {name}{username}\nStatus: {status}\nTrips: {trips}\nNo check-ins.',
        'status_error': 'An error occurred while retrieving status.',
        'export_no_checkins': 'No check-ins for the specified period or employee.',
        'export_invalid_format': 'Invalid format. Use /export, /export <number>w, /export @username, /export <user_id>, or their combination (e.g., /export 2w @username).',
        'export_user_not_found': 'User {user} not found.',
        'export_error': 'An error occurred while exporting check-ins.',
        'reminder': 'Reminder: send a check-in at {time} ({tz})!',
        'missed_checkin': 'Employee {name}{username} missed a check-in!\nExpected: {time} ({tz})\nLast check-in: {last_time}\nLast location: {location}\nMap: {maps_url}',
        'archived': 'Employee {name}{username} marked as archived (trips completed).',
        'freq_1': 'Once a day',
        'freq_2': 'Twice (morning, evening)',
        'freq_3': 'Three times (morning, day, evening)',
        'time_morning': '08:00',
        'time_day': '14:00',
        'time_evening': '20:00',
        'status_ok': 'All Good',
        'status_health': 'Health Issue',
        'status_safety': 'Safety Issue',
        'edit_country_prompt': 'Enter the new country of stay:',
        'edit_freq_prompt': 'Select the new check-in frequency:',
        'edit_time_prompt': 'Select the new check-in time:',
    },
    'es': {
        'start_no_employee': 'Comencemos el registro. Ingresa tu nombre:',
        'start_registered_active': 'Ya estás registrado. Tienes un viaje activo. Usa /trip para ver o editar.',
        'start_registered_no_trip': 'Ya estás registrado, pero no hay viajes activos. ¿Quieres crear un nuevo viaje?',
        'choose_language': 'Elige el idioma:',
        'name_empty': 'El nombre no puede estar vacío. Por favor, ingresa tu nombre:',
        'country_prompt': 'Ingresa el primer país de estancia:',
        'country_empty': 'El nombre del país no puede estar vacío. Por favor, ingresa un país:',
        'start_date_prompt': 'Ingresa la fecha de inicio de la estancia (DD/MM/YYYY):',
        'end_date_prompt': 'Ingresa la fecha de fin de la estancia (DD/MM/YYYY):',
        'invalid_date': 'Formato de fecha inválido. Usa DD/MM/YYYY, ej., 01/05/2025.',
        'end_before_start': 'La fecha de fin no puede ser anterior a la fecha de inicio.',
        'frequency_prompt': 'Selecciona la frecuencia de check-ins:',
        'invalid_frequency': 'Selección de frecuencia inválida.',
        'time_prompt': 'Selecciona la hora del check-in:',
        'invalid_time': 'Selección de hora inválida.',
        'add_country_prompt': '¿Quieres agregar otro país?',
        'registration_complete': '¡Registro completado! Envía tu ubicación.',
        'registration_error': 'Ocurrió un error durante el registro. Intenta de nuevo.',
        'trip_info': 'Tu viaje actual:\nPaís: {country}\nFechas: {start_date} - {end_date}\nFrecuencia de check-ins: {freq_text}\nHora de check-in: {time_text}\n¿Qué quieres hacer?',
        'no_active_trip': 'No tienes viajes activos. ¿Quieres crear uno nuevo?',
        'trip_updated': '¡Viaje actualizado!',
        'trip_update_error': 'Ocurrió un error al actualizar el viaje.',
        'finish_view': 'Visualización completada.',
        'new_trip_prompt': 'Ingresa el país para el nuevo viaje:',
        'location_invalid': 'Ubicación inválida. Por favor, envía de nuevo.',
        'location_received': 'Ubicación recibida. Selecciona el estado:',
        'checkin_registered': 'Check-in registrado: {status}\nMapa: {maps_url}',
        'checkin_error': 'Ocurrió un error al registrar el check-in.',
        'not_registered': 'Regístrate primero usando /start',
        'list_no_employees': 'No hay empleados registrados.',
        'list_error': 'Ocurrió un error al recuperar la lista de empleados.',
        'status_usage': 'Uso: /status <user_id> o /status @username',
        'status_invalid_id': 'Formato de ID inválido. Usa /status <user_id> o /status @username',
        'status_not_found': 'Empleado no encontrado.',
        'status_response': 'Empleado: {name}{username}\nEstado: {status}\nViajes: {trips}\nÚltimo check-in: {time}\nMapa: {maps_url}\nEstado: {checkin_status}',
        'status_no_checkins': 'Empleado: {name}{username}\nEstado: {status}\nViajes: {trips}\nNo hay check-ins.',
        'status_error': 'Ocurrió un error al recuperar el estado.',
        'export_no_checkins': 'No hay check-ins para el período o empleado especificado.',
        'export_invalid_format': 'Formato inválido. Usa /export, /export <número>w, /export @username, /export <user_id>, o su combinación (ej., /export 2w @username).',
        'export_user_not_found': 'Usuario {user} no encontrado.',
        'export_error': 'An error occurred while exporting check-ins.',
        'reminder': 'Recordatorio: envía un check-in a las {time} ({tz})!',
        'missed_checkin': '¡El empleado {name}{username} no envió un check-in!\nEsperado: {time} ({tz})\nÚltimo check-in: {last_time}\nÚltima ubicación: {location}\nMapa: {maps_url}',
        'archived': 'El empleado {name}{username} fue marcado como archivado (viajes completados).',
        'freq_1': 'Una vez al día',
        'freq_2': 'Dos veces (mañana, tarde)',
        'freq_3': 'Tres veces (mañana, mediodía, tarde)',
        'time_morning': '08:00',
        'time_day': '14:00',
        'time_evening': '20:00',
        'status_ok': 'Todo Bien',
        'status_health': 'Problema de Salud',
        'status_safety': 'Problema de Seguridad',
        'edit_country_prompt': 'Ingresa el nuevo país de estancia:',
        'edit_freq_prompt': 'Selecciona la nueva frecuencia de check-ins:',
        'edit_time_prompt': 'Selecciona la nueva hora de check-in:',
    }
}

def get_text(user_id, key, **kwargs):
    """Получение локализованного текста для пользователя."""
    cursor.execute('SELECT language FROM employees WHERE user_id = ?', (user_id,))
    result = cursor.fetchone()
    lang = result[0] if result else 'ru'
    text = TRANSLATIONS.get(lang, TRANSLATIONS['ru']).get(key, key)
    return text.format(**kwargs)

# Клавиатуры
location_button = KeyboardButton(text="Отправить геопозицию", request_location=True)
keyboard = ReplyKeyboardMarkup(
    keyboard=[[location_button]],
    resize_keyboard=True
)

def get_status_keyboard(lang):
    """Создание клавиатуры статусов на основе языка."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=TRANSLATIONS[lang]['status_ok'], callback_data="status_ok")],
            [InlineKeyboardButton(text=TRANSLATIONS[lang]['status_health'], callback_data="status_health")],
            [InlineKeyboardButton(text=TRANSLATIONS[lang]['status_safety'], callback_data="status_safety")]
        ]
    )

frequency_keyboard = InlineKeyboardMarkup(
    inline_keyboard=[
        [InlineKeyboardButton(text="1 раз", callback_data="freq_1")],
        [InlineKeyboardButton(text="2 раза (утро, вечер)", callback_data="freq_2")],
        [InlineKeyboardButton(text="3 раза (утро, день, вечер)", callback_data="freq_3")]
    ]
)

time_keyboard = InlineKeyboardMarkup(
    inline_keyboard=[
        [InlineKeyboardButton(text="Утро (08:00)", callback_data="time_morning")],
        [InlineKeyboardButton(text="День (14:00)", callback_data="time_day")],
        [InlineKeyboardButton(text="Вечер (20:00)", callback_data="time_evening")]
    ]
)

language_keyboard = InlineKeyboardMarkup(
    inline_keyboard=[
        [InlineKeyboardButton(text="Русский", callback_data="lang_ru")],
        [InlineKeyboardButton(text="English", callback_data="lang_en")],
        [InlineKeyboardButton(text="Español", callback_data="lang_es")]
    ]
)

trip_keyboard = InlineKeyboardMarkup(
    inline_keyboard=[
        [InlineKeyboardButton(text="Изменить страну", callback_data="edit_country")],
        [InlineKeyboardButton(text="Изменить даты", callback_data="edit_dates")],
        [InlineKeyboardButton(text="Изменить частоту", callback_data="edit_frequency")],
        [InlineKeyboardButton(text="Изменить время", callback_data="edit_time")],
        [InlineKeyboardButton(text="Завершить", callback_data="finish_view")]
    ]
)

new_trip_keyboard = InlineKeyboardMarkup(
    inline_keyboard=[
        [InlineKeyboardButton(text="Новая командировка", callback_data="new_trip")],
        [InlineKeyboardButton(text="Завершить", callback_data="finish_view")]
    ]
)

# Состояния для регистрации и редактирования
class Registration(StatesGroup):
    Name = State()
    Language = State()
    Country = State()
    StartDate = State()
    EndDate = State()
    Frequency = State()
    CheckinTime = State()
    AddAnotherCountry = State()
    EditCountry = State()
    EditStartDate = State()
    EditEndDate = State()
    EditFrequency = State()
    EditCheckinTime = State()

# Словарь для отслеживания отправленных напоминаний
reminders_sent = {}

def get_timezone_by_country(country_name):
    """Получение часового пояса по названию страны с использованием geopy."""
    try:
        geolocator = Nominatim(user_agent="telegram_bot")
        location = geolocator.geocode(country_name)
        if not location:
            logger.warning(f"Координаты для {country_name} не найдены")
            return 'UTC'
        tf = TimezoneFinder()
        timezone_str = tf.timezone_at(lat=location.latitude, lng=location.longitude)
        if not timezone_str:
            logger.warning(f"Часовой пояс для {country_name} не определён")
            return 'UTC'
        return timezone_str
    except Exception as e:
        logger.error(f"Ошибка определения часового пояса для {country_name}: {e}")
        return 'UTC'

def get_timezone_by_coordinates(latitude, longitude):
    """Получение часового пояса по координатам."""
    try:
        tf = TimezoneFinder()
        timezone_str = tf.timezone_at(lat=latitude, lng=longitude)
        if not timezone_str:
            logger.warning(f"Часовой пояс для координат ({latitude}, {longitude}) не определён")
            return 'UTC'
        return timezone_str
    except Exception as e:
        logger.error(f"Ошибка определения часового пояса для координат ({latitude}, {longitude}): {e}")
        return 'UTC'

def format_time_ago(timestamp, tz):
    """Форматирование времени с последнего чек-ина."""
    try:
        last_checkin = datetime.fromisoformat(timestamp).astimezone(tz)
        now = datetime.now(tz)
        hours_ago = int((now - last_checkin).total_seconds() // 3600)
        return "менее часа назад" if hours_ago == 0 else f"{hours_ago} часов назад"
    except Exception as e:
        logger.error(f"Ошибка форматирования времени: {e}")
        return "неизвестно"

@dp.message(CommandStart())
async def start_command(message: Message, state: FSMContext):
    """Обработка команды /start и начало регистрации или предложение новой командировки."""
    user_id = message.from_user.id
    cursor.execute('SELECT language FROM employees WHERE user_id = ?', (user_id,))
    employee = cursor.fetchone()
    if employee:
        cursor.execute('SELECT id, country, start_date, end_date, checkin_frequency, checkin_time '
                      'FROM trips WHERE user_id = ? AND date("now") BETWEEN start_date AND end_date', (user_id,))
        active_trip = cursor.fetchone()
        lang = employee[0] or 'ru'
        if active_trip:
            await message.reply(get_text(user_id, 'start_registered_active'), reply_markup=keyboard)
        else:
            await message.reply(get_text(user_id, 'start_registered_no_trip'), reply_markup=new_trip_keyboard)
    else:
        username = message.from_user.username or None
        await state.update_data(username=username, trips=[])
        await message.reply(get_text(user_id, 'start_no_employee'))
        await state.set_state(Registration.Name)

@dp.message(Registration.Name)
async def process_name(message: Message, state: FSMContext):
    """Обработка ввода имени."""
    if not message.text.strip():
        await message.reply(get_text(message.from_user.id, 'name_empty'))
        return
    await state.update_data(name=message.text.strip())
    await message.reply(get_text(message.from_user.id, 'choose_language'), reply_markup=language_keyboard)
    await state.set_state(Registration.Language)

@dp.callback_query(lambda c: c.data.startswith('lang_'))
async def process_language(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора языка."""
    lang_map = {'lang_ru': 'ru', 'lang_en': 'en', 'lang_es': 'es'}
    language = lang_map.get(callback.data)
    if not language:
        await callback.message.reply("Неверный выбор языка.")
        return
    await state.update_data(language=language)
    user_id = callback.from_user.id
    user_data = await state.get_data()
    cursor.execute('INSERT OR REPLACE INTO employees (user_id, name, username, language) VALUES (?, ?, ?, ?)', 
                  (user_id, user_data['name'], user_data['username'], language))
    conn.commit()
    await callback.message.reply(get_text(user_id, 'country_prompt'))
    await state.set_state(Registration.Country)

@dp.message(Registration.Country)
async def process_country(message: Message, state: FSMContext):
    """Обработка ввода страны."""
    country = message.text.strip()
    if not country:
        await message.reply(get_text(message.from_user.id, 'country_empty'))
        return
    timezone_str = get_timezone_by_country(country)
    await state.update_data(country=country, timezone=timezone_str)
    await message.reply(get_text(message.from_user.id, 'start_date_prompt'))
    await state.set_state(Registration.StartDate)

@dp.message(Registration.StartDate)
async def process_start_date(message: Message, state: FSMContext):
    """Обработка ввода даты начала."""
    try:
        start_date = datetime.strptime(message.text, '%d/%m/%Y')
        await state.update_data(start_date=start_date.strftime('%Y-%m-%d'))
        await message.reply(get_text(message.from_user.id, 'end_date_prompt'))
        await state.set_state(Registration.EndDate)
    except ValueError:
        await message.reply(get_text(message.from_user.id, 'invalid_date'))

@dp.message(Registration.EndDate)
async def process_end_date(message: Message, state: FSMContext):
    """Обработка ввода даты окончания."""
    try:
        end_date = datetime.strptime(message.text, '%d/%m/%Y')
        user_data = await state.get_data()
        start_date = datetime.strptime(user_data['start_date'], '%Y-%m-%d')
        if end_date < start_date:
            await message.reply(get_text(message.from_user.id, 'end_before_start'))
            return
        await state.update_data(end_date=end_date.strftime('%Y-%m-%d'))
        await message.reply(get_text(message.from_user.id, 'frequency_prompt'), reply_markup=frequency_keyboard)
        await state.set_state(Registration.Frequency)
    except ValueError:
        await message.reply(get_text(message.from_user.id, 'invalid_date'))

@dp.callback_query(lambda c: c.data.startswith('freq_'))
async def process_frequency(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора частоты чек-инов."""
    freq_map = {'freq_1': 1, 'freq_2': 2, 'freq_3': 3}
    frequency = freq_map.get(callback.data)
    if not frequency:
        await callback.message.reply(get_text(callback.from_user.id, 'invalid_frequency'))
        return
    await state.update_data(frequency=frequency)

    if frequency == 1:
        await callback.message.reply(get_text(callback.from_user.id, 'time_prompt'), reply_markup=time_keyboard)
        await state.set_state(Registration.CheckinTime)
    else:
        user_data = await state.get_data()
        user_data['trips'].append({
            'country': user_data['country'],
            'timezone': user_data['timezone'],
            'start_date': user_data['start_date'],
            'end_date': user_data['end_date'],
            'checkin_frequency': frequency,
            'checkin_time': None
        })
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text=get_text(callback.from_user.id, 'add_country_prompt'), callback_data="add_country")],
                [InlineKeyboardButton(text="Завершить", callback_data="finish")]
            ]
        )
        await callback.message.reply(get_text(callback.from_user.id, 'add_country_prompt'), reply_markup=keyboard)
        await state.set_state(Registration.AddAnotherCountry)

@dp.callback_query(lambda c: c.data.startswith('time_'))
async def process_checkin_time(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора времени чек-ина."""
    time_map = {
        'time_morning': 'morning',
        'time_day': 'day',
        'time_evening': 'evening'
    }
    checkin_time = time_map.get(callback.data)
    if not checkin_time:
        await callback.message.reply(get_text(callback.from_user.id, 'invalid_time'))
        return
    user_data = await state.get_data()
    user_data['trips'].append({
        'country': user_data['country'],
        'timezone': user_data['timezone'],
        'start_date': user_data['start_date'],
        'end_date': user_data['end_date'],
        'checkin_frequency': user_data['frequency'],
        'checkin_time': checkin_time
    })
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=get_text(callback.from_user.id, 'add_country_prompt'), callback_data="add_country")],
            [InlineKeyboardButton(text="Завершить", callback_data="finish")]
        ]
    )
    await callback.message.reply(get_text(callback.from_user.id, 'add_country_prompt'), reply_markup=keyboard)
    await state.set_state(Registration.AddAnotherCountry)

@dp.callback_query(lambda c: c.data in ['add_country', 'finish'])
async def process_add_country(callback: CallbackQuery, state: FSMContext):
    """Обработка добавления новой страны или завершения регистрации."""
    if callback.data == "add_country":
        await callback.message.reply(get_text(callback.from_user.id, 'country_prompt'))
        await state.set_state(Registration.Country)
    elif callback.data == "finish":
        user_data = await state.get_data()
        user_id = callback.from_user.id
        try:
            if 'name' in user_data:
                cursor.execute('INSERT INTO employees (user_id, name, username, language) VALUES (?, ?, ?, ?)', 
                              (user_id, user_data['name'], user_data['username'], user_data['language']))
            for trip in user_data['trips']:
                cursor.execute('''
                    INSERT INTO trips (user_id, country, timezone, start_date, end_date, checkin_frequency, checkin_time)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (user_id, trip['country'], trip['timezone'], trip['start_date'], trip['end_date'], 
                      trip['checkin_frequency'], trip['checkin_time']))
            conn.commit()
            await callback.message.reply(get_text(user_id, 'registration_complete'), reply_markup=keyboard)
            logger.info(f"Пользователь {user_id} завершил регистрацию или добавил поездку: {user_data.get('name', 'существующий')}")
            await state.clear()
        except Exception as e:
            logger.error(f"Ошибка сохранения данных пользователя {user_id}: {e}")
            await callback.message.reply(get_text(user_id, 'registration_error'))

@dp.message(Command("trip"))
async def view_trip(message: Message, state: FSMContext):
    """Отображение деталей текущей командировки и предложение вариантов редактирования."""
    user_id = message.from_user.id
    cursor.execute('SELECT language FROM employees WHERE user_id = ?', (user_id,))
    employee = cursor.fetchone()
    if not employee:
        await message.reply(get_text(user_id, 'not_registered'))
        return

    lang = employee[0] or 'ru'
    cursor.execute('SELECT id, country, start_date, end_date, checkin_frequency, checkin_time '
                  'FROM trips WHERE user_id = ? AND date("now") BETWEEN start_date AND end_date', (user_id,))
    active_trip = cursor.fetchone()
    if active_trip:
        trip_id, country, start_date, end_date, frequency, checkin_time = active_trip
        freq_text = TRANSLATIONS[lang].get(f'freq_{frequency}', 'Неизвестно')
        time_text = TRANSLATIONS[lang].get(f'time_{checkin_time}', 'Не указано') if checkin_time else 'Не указано'
        await state.update_data(trip_id=trip_id)
        await message.reply(
            get_text(user_id, 'trip_info', 
                     country=country, start_date=start_date, end_date=end_date, 
                     freq_text=freq_text, time_text=time_text),
            reply_markup=trip_keyboard
        )
    else:
        await message.reply(get_text(user_id, 'no_active_trip'), reply_markup=new_trip_keyboard)

@dp.callback_query(lambda c: c.data in ['edit_country', 'edit_dates', 'edit_frequency', 'edit_time', 'finish_view', 'new_trip'])
async def handle_trip_action(callback: CallbackQuery, state: FSMContext):
    """Обработка действий по редактированию командировки."""
    user_id = callback.from_user.id
    if callback.data == "finish_view":
        await callback.message.reply(get_text(user_id, 'finish_view'), reply_markup=keyboard)
        await state.clear()
    elif callback.data == "edit_country":
        await callback.message.reply(get_text(user_id, 'edit_country_prompt'))
        await state.set_state(Registration.EditCountry)
    elif callback.data == "edit_dates":
        await callback.message.reply(get_text(user_id, 'start_date_prompt'))
        await state.set_state(Registration.EditStartDate)
    elif callback.data == "edit_frequency":
        await callback.message.reply(get_text(user_id, 'edit_freq_prompt'), reply_markup=frequency_keyboard)
        await state.set_state(Registration.EditFrequency)
    elif callback.data == "edit_time":
        user_data = await state.get_data()
        trip_id = user_data.get('trip_id')
        cursor.execute('SELECT checkin_frequency FROM trips WHERE id = ?', (trip_id,))
        frequency = cursor.fetchone()[0]
        if frequency != 1:
            await callback.message.reply("Время чек-ина можно установить только для одного чек-ина в день.")
            return
        await callback.message.reply(get_text(user_id, 'edit_time_prompt'), reply_markup=time_keyboard)
        await state.set_state(Registration.EditCheckinTime)
    elif callback.data == "new_trip":
        await state.update_data(trips=[])
        await callback.message.reply(get_text(user_id, 'new_trip_prompt'))
        await state.set_state(Registration.Country)

@dp.message(Registration.EditCountry)
async def process_edit_country(message: Message, state: FSMContext):
    """Обработка ввода новой страны для редактирования командировки."""
    country = message.text.strip()
    if not country:
        await message.reply(get_text(message.from_user.id, 'country_empty'))
        return
    timezone_str = get_timezone_by_country(country)
    user_data = await state.get_data()
    trip_id = user_data.get('trip_id')
    cursor.execute('UPDATE trips SET country = ?, timezone = ? WHERE id = ?', 
                  (country, timezone_str, trip_id))
    conn.commit()
    await message.reply(get_text(message.from_user.id, 'trip_updated'), reply_markup=keyboard)
    logger.info(f"Пользователь {message.from_user.id} обновил страну командировки ID {trip_id} на {country}")
    await state.clear()

@dp.message(Registration.EditStartDate)
async def process_edit_start_date(message: Message, state: FSMContext):
    """Обработка ввода новой даты начала для редактирования командировки."""
    try:
        start_date = datetime.strptime(message.text, '%d/%m/%Y')
        await state.update_data(start_date=start_date.strftime('%Y-%m-%d'))
        await message.reply(get_text(message.from_user.id, 'end_date_prompt'))
        await state.set_state(Registration.EditEndDate)
    except ValueError:
        await message.reply(get_text(message.from_user.id, 'invalid_date'))

@dp.message(Registration.EditEndDate)
async def process_edit_end_date(message: Message, state: FSMContext):
    """Обработка ввода новой даты окончания и обновление командировки."""
    try:
        end_date = datetime.strptime(message.text, '%d/%m/%Y')
        user_data = await state.get_data()
        start_date = datetime.strptime(user_data['start_date'], '%Y-%m-%d')
        if end_date < start_date:
            await message.reply(get_text(message.from_user.id, 'end_before_start'))
            return
        trip_id = user_data.get('trip_id')
        cursor.execute('UPDATE trips SET start_date = ?, end_date = ? WHERE id = ?', 
                      (user_data['start_date'], end_date.strftime('%Y-%m-%d'), trip_id))
        conn.commit()
        await message.reply(get_text(message.from_user.id, 'trip_updated'), reply_markup=keyboard)
        logger.info(f"Пользователь {message.from_user.id} обновил даты командировки ID {trip_id}")
        await state.clear()
    except ValueError:
        await message.reply(get_text(message.from_user.id, 'invalid_date'))
    except Exception as e:
        logger.error(f"Ошибка обновления командировки: {e}")
        await message.reply(get_text(message.from_user.id, 'trip_update_error'))

@dp.callback_query(lambda c: c.data.startswith('freq_'), Registration.EditFrequency)
async def process_edit_frequency(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора новой частоты чек-инов для редактирования командировки."""
    freq_map = {'freq_1': 1, 'freq_2': 2, 'freq_3': 3}
    frequency = freq_map.get(callback.data)
    if not frequency:
        await callback.message.reply(get_text(callback.from_user.id, 'invalid_frequency'))
        return
    user_data = await state.get_data()
    trip_id = user_data.get('trip_id')
    cursor.execute('UPDATE trips SET checkin_frequency = ?, checkin_time = ? WHERE id = ?', 
                  (frequency, None if frequency != 1 else user_data.get('checkin_time'), trip_id))
    conn.commit()
    if frequency == 1:
        await callback.message.reply(get_text(callback.from_user.id, 'edit_time_prompt'), reply_markup=time_keyboard)
        await state.set_state(Registration.EditCheckinTime)
    else:
        await callback.message.reply(get_text(callback.from_user.id, 'trip_updated'), reply_markup=keyboard)
        logger.info(f"Пользователь {callback.from_user.id} обновил частоту командировки ID {trip_id} на {frequency}")
        await state.clear()

@dp.callback_query(lambda c: c.data.startswith('time_'), Registration.EditCheckinTime)
async def process_edit_checkin_time(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора нового времени чек-ина для редактирования командировки."""
    time_map = {
        'time_morning': 'morning',
        'time_day': 'day',
        'time_evening': 'evening'
    }
    checkin_time = time_map.get(callback.data)
    if not checkin_time:
        await callback.message.reply(get_text(callback.from_user.id, 'invalid_time'))
        return
    user_data = await state.get_data()
    trip_id = user_data.get('trip_id')
    cursor.execute('UPDATE trips SET checkin_time = ? WHERE id = ?', (checkin_time, trip_id))
    conn.commit()
    await callback.message.reply(get_text(callback.from_user.id, 'trip_updated'), reply_markup=keyboard)
    logger.info(f"Пользователь {callback.from_user.id} обновил время чек-ина командировки ID {trip_id} на {checkin_time}")
    await state.clear()

class LocationFilter(BaseFilter):
    async def __call__(self, message: Message) -> bool:
        return message.content_type == ContentType.LOCATION

@dp.message(LocationFilter())
async def handle_location(message: Message, state: FSMContext):
    """Обработка отправки геопозиции."""
    user_id = message.from_user.id
    cursor.execute('SELECT language FROM employees WHERE user_id = ?', (user_id,))
    employee = cursor.fetchone()
    if not employee:
        await message.reply(get_text(user_id, 'not_registered'))
        return

    location = message.location
    if not (-90 <= location.latitude <= 90 and -180 <= location.longitude <= 180):
        await message.reply(get_text(user_id, 'location_invalid'))
        return

    timezone_str = get_timezone_by_coordinates(location.latitude, location.longitude)
    current_date = datetime.now().strftime('%Y-%m-%d')
    cursor.execute('''
        UPDATE trips
        SET timezone = ?
        WHERE user_id = ? AND ? BETWEEN start_date AND end_date
    ''', (timezone_str, user_id, current_date))
    conn.commit()

    await state.update_data(latitude=location.latitude, longitude=location.longitude)
    lang = employee[0] or 'ru'
    await message.reply(get_text(user_id, 'location_received'), reply_markup=get_status_keyboard(lang))
    logger.info(f"Геопозиция получена от {user_id}: ({location.latitude}, {location.longitude}), часовой пояс: {timezone_str}")

@dp.callback_query(lambda c: c.data.startswith('status_'))
async def handle_status(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора статуса чек-ина."""
    user_id = callback.from_user.id
    cursor.execute('SELECT name, username, language FROM employees WHERE user_id = ?', (user_id,))
    employee = cursor.fetchone()
    if not employee:
        await callback.message.reply(get_text(user_id, 'not_registered'))
        return

    name, username, lang = employee
    status_map = {
        'status_ok': TRANSLATIONS[lang]['status_ok'],
        'status_health': TRANSLATIONS[lang]['status_health'],
        'status_safety': TRANSLATIONS[lang]['status_safety']
    }
    status = status_map.get(callback.data, "Неизвестно")
    state_data = await state.get_data()
    latitude = state_data.get('latitude')
    longitude = state_data.get('longitude')
    timestamp = datetime.now().isoformat()

    try:
        cursor.execute('''
            INSERT INTO checkins (user_id, latitude, longitude, status, timestamp)
            VALUES (?, ?, ?, ?, ?)
        ''', (user_id, latitude, longitude, status, timestamp))
        conn.commit()
        maps_url = f"https://www.google.com/maps?q={latitude},{longitude}"
        await callback.message.reply(get_text(user_id, 'checkin_registered', user_id=user_id, status=status, maps_url=maps_url))
        logger.info(f"Чек-ин зарегистрирован для {user_id}: {status}")

        # Уведомление админа при проблемах со здоровьем или безопасностью
        if callback.data in ['status_health', 'status_safety']:
            checkin_time = datetime.fromisoformat(timestamp).strftime('%d-%m-%Y %H:%M')
            await bot.send_message(
                ADMIN_ID,
                f"Сотрудник {name}{f' @{username}' if username else ''} сообщил о \"{status}\"!\n"
                f"Время: {checkin_time}\n"
                f"Локация: Координаты: {latitude}, {longitude}\n"
                f"Карта: {maps_url}"
            )
            logger.info(f"Админ уведомлён о {status} для пользователя {user_id}")
    except Exception as e:
        logger.error(f"Ошибка сохранения чек-ина для {user_id}: {e}")
        await callback.message.reply(get_text(user_id, 'checkin_error'))

@dp.message(Command("list"))
async def list_employees(message: Message):
    """Список всех сотрудников (только для админа)."""
    if message.from_user.id != ADMIN_ID:
        return
    try:
        cursor.execute('SELECT user_id, name, username, archived FROM employees')
        employees = cursor.fetchall()
        if not employees:
            await message.reply(get_text(message.from_user.id, 'list_no_employees'))
            return

        response = "Список сотрудников:\n"
        for emp in employees:
            cursor.execute('SELECT country, start_date, end_date FROM trips WHERE user_id = ?', (emp[0],))
            trips = cursor.fetchall()
            trip_info = ", ".join([f"{t[0]} ({t[1]} - {t[2]})" for t in trips])
            status = "Архивный" if emp[3] else "Активный"
            response += f"ID: {emp[0]}, Имя: {emp[1]}{f' @{emp[2]}' if emp[2] else ''}, Статус: {status}, Поездки: {trip_info}\n"
        await message.reply(response)
    except Exception as e:
        logger.error(f"Ошибка получения списка сотрудников: {e}")
        await message.reply(get_text(message.from_user.id, 'list_error'))

@dp.message(Command("status"))
async def employee_status(message: Message):
    """Отображение статуса сотрудника по ID или @username (только для админа)."""
    if message.from_user.id != ADMIN_ID:
        return
    try:
        input_str = message.text.split()[1] if len(message.text.split()) > 1 else None
        if not input_str:
            await message.reply(get_text(message.from_user.id, 'status_usage'))
            return

        employee = None
        if input_str.startswith('@'):
            username = input_str[1:]
            cursor.execute('SELECT user_id, name, username, archived FROM employees WHERE username = ?', (username,))
            employee = cursor.fetchone()
        else:
            try:
                user_id = int(input_str)
                cursor.execute('SELECT user_id, name, username, archived FROM employees WHERE user_id = ?', (user_id,))
                employee = cursor.fetchone()
            except ValueError:
                await message.reply(get_text(message.from_user.id, 'status_invalid_id'))
                return

        if not employee:
            await message.reply(get_text(message.from_user.id, 'status_not_found'))
            return

        cursor.execute('SELECT country, start_date, end_date FROM trips WHERE user_id = ?', (employee[0],))
        trips = cursor.fetchall()
        trip_info = ", ".join([f"{t[0]} ({t[1]} - {t[2]})" for t in trips])

        cursor.execute('SELECT latitude, longitude, status, timestamp FROM checkins WHERE user_id = ? ORDER BY timestamp DESC LIMIT 1', (employee[0],))
        checkin = cursor.fetchone()
        if checkin:
            checkin_time = datetime.fromisoformat(checkin[3]).strftime('%H:%M')
            maps_url = f"https://www.google.com/maps?q={checkin[0]},{checkin[1]}"
            await message.reply(
                get_text(message.from_user.id, 'status_response',
                         name=employee[1],
                         username=f' @{employee[2]}' if employee[2] else '',
                         status='Архивный' if employee[3] else 'Активный',
                         trips=trip_info,
                         time=checkin_time,
                         maps_url=maps_url,
                         checkin_status=checkin[2])
            )
        else:
            await message.reply(
                get_text(message.from_user.id, 'status_no_checkins',
                         name=employee[1],
                         username=f' @{employee[2]}' if employee[2] else '',
                         status='Архивный' if employee[3] else 'Активный',
                         trips=trip_info)
            )
    except IndexError:
        await message.reply(get_text(message.from_user.id, 'status_usage'))
    except Exception as e:
        logger.error(f"Ошибка получения статуса сотрудника: {e}")
        await message.reply(get_text(message.from_user.id, 'status_error'))

@dp.message(Command("export"))
async def export_checkins(message: Message):
    """Экспорт чек-инов в CSV (только для админа)."""
    if message.from_user.id != ADMIN_ID:
        return
    try:
        args = message.text.split()
        weeks = None
        employee_id = None

        for arg in args[1:]:
            if re.match(r'^(\d+)w$', arg):
                weeks = int(re.match(r'^(\d+)w$', arg).group(1))
            elif arg.startswith('@'):
                username = arg[1:]
                cursor.execute('SELECT user_id FROM employees WHERE username = ?', (username,))
                result = cursor.fetchone()
                if result:
                    employee_id = result[0]
                else:
                    await message.reply(get_text(message.from_user.id, 'export_user_not_found', user=arg))
                    return
            else:
                try:
                    employee_id = int(arg)
                    cursor.execute('SELECT user_id FROM employees WHERE user_id = ?', (employee_id,))
                    if not cursor.fetchone():
                        await message.reply(get_text(message.from_user.id, 'export_user_not_found', user=employee_id))
                        return
                except ValueError:
                    await message.reply(get_text(message.from_user.id, 'export_invalid_format'))
                    return

        query = ('SELECT c.user_id, e.name, e.username, c.latitude, c.longitude, c.status, c.timestamp '
                 'FROM checkins c JOIN employees e ON c.user_id = e.user_id')
        conditions = []
        params = []
        if weeks is not None:
            start_date = (datetime.now() - timedelta(weeks=weeks)).strftime('%Y-%m-%d')
            conditions.append('date(c.timestamp) >= ?')
            params.append(start_date)
        if employee_id is not None:
            conditions.append('c.user_id = ?')
            params.append(employee_id)
        if conditions:
            query += ' WHERE ' + ' AND '.join(conditions)

        cursor.execute(query, params)
        checkins = cursor.fetchall()

        if not checkins:
            await message.reply(get_text(message.from_user.id, 'export_no_checkins'))
            return

        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(['User ID', 'Name', 'Username', 'Latitude', 'Longitude', 'Status', 'Timestamp', 'Country', 'Maps URL'])

        for checkin in checkins:
            user_id, name, username, latitude, longitude, status, timestamp = checkin
            formatted_timestamp = datetime.fromisoformat(timestamp).strftime('%d-%m-%Y %H:%M')
            checkin_date = datetime.fromisoformat(timestamp).strftime('%Y-%m-%d')
            cursor.execute('SELECT country FROM trips WHERE user_id = ? AND ? BETWEEN start_date AND end_date', 
                          (user_id, checkin_date))
            trip = cursor.fetchone()
            country = trip[0] if trip else 'Неизвестно'
            maps_url = f"https://www.google.com/maps?q={latitude},{longitude}"
            writer.writerow([user_id, name, username, latitude, longitude, status, formatted_timestamp, country, maps_url])

        output.seek(0)
        csv_data = output.getvalue().encode('utf-8')
        await message.reply_document(BufferedInputFile(csv_data, filename='checkins.csv'), caption="Экспорт чек-инов")
        logger.info(f"Чек-ины экспортированы в CSV {'за последние ' + str(weeks) + ' недель' if weeks else ''} "
                     f"{'для сотрудника ' + str(employee_id) if employee_id else ''}")
    except Exception as e:
        logger.error(f"Ошибка экспорта чек-инов: {e}")
        await message.reply(get_text(message.from_user.id, 'export_error'))

async def send_reminder(user_id, tz, checkin_time):
    """Отправка напоминания о чек-ине."""
    reminder_time = checkin_time - timedelta(minutes=30)
    reminder_key = f"{user_id}_{checkin_time.date()}_{checkin_time.hour}"
    if datetime.now(tz) >= reminder_time and reminder_key not in reminders_sent:
        try:
            await bot.send_message(
                user_id,
                get_text(user_id, 'reminder', time=checkin_time.strftime('%H:%M'), tz=tz.zone),
                reply_markup=keyboard
            )
            reminders_sent[reminder_key] = True
            logger.info(f"Напоминание отправлено пользователю {user_id} для {checkin_time} ({tz.zone})")
        except Exception as e:
            logger.error(f"Ошибка отправки напоминания пользователю {user_id}: {e}")

async def check_employees():
    """Проверка сотрудников и отправка напоминаний или уведомлений о пропущенных чек-инах."""
    while True:
        try:
            cursor.execute('SELECT user_id, name, username, archived, language FROM employees WHERE archived = 0')
            employees = cursor.fetchall()
            for emp in employees:
                user_id, name, username, _, lang = emp
                cursor.execute('SELECT id, country, timezone, start_date, end_date, checkin_frequency, checkin_time '
                              'FROM trips WHERE user_id = ?', (user_id,))
                trips = cursor.fetchall()

                current_time = datetime.now()
                current_trip = None
                for trip in trips:
                    start_date = datetime.strptime(trip[3], '%Y-%m-%d')
                    end_date = datetime.strptime(trip[4], '%Y-%m-%d')
                    if start_date.date() <= current_time.date() <= end_date.date():
                        current_trip = trip
                        break

                if not current_trip:
                    cursor.execute('UPDATE employees SET archived = 1 WHERE user_id = ?', (user_id,))
                    conn.commit()
                    await bot.send_message(ADMIN_ID, get_text(user_id, 'archived', name=name, username=f' @{username}' if username else ''))
                    logger.info(f"Сотрудник {user_id} помечен как архивный")
                    continue

                tz = timezone(current_trip[2])
                current_time_tz = datetime.now(tz)
                freq = current_trip[5]
                checkin_time = current_trip[6]

                if freq == 1 and checkin_time:
                    time_map = {
                        'morning': (8, 0),
                        'day': (14, 0),
                        'evening': (20, 0)
                    }
                    checkin_times = [time_map[checkin_time]]
                else:
                    checkin_times = {
                        1: [(8, 0)],
                        2: [(8, 0), (20, 0)],
                        3: [(8, 0), (14, 0), (20, 0)]
                    }[freq]

                for checkin_hour, checkin_minute in checkin_times:
                    expected_time = current_time_tz.replace(hour=checkin_hour, minute=checkin_minute, second=0, microsecond=0)
                    if expected_time.date() != current_time_tz.date():
                        continue

                    await send_reminder(user_id, tz, expected_time)

                    window_start = (expected_time - timedelta(minutes=90)).isoformat()
                    window_end = (expected_time + timedelta(minutes=20)).isoformat()
                    cursor.execute('SELECT timestamp FROM checkins WHERE user_id = ? AND timestamp BETWEEN ? AND ?',
                                  (user_id, window_start, window_end))
                    checkin_in_window = cursor.fetchone()

                    if checkin_in_window:
                        continue

                    cursor.execute('SELECT latitude, longitude, timestamp FROM checkins WHERE user_id = ? ORDER BY timestamp DESC LIMIT 1', (user_id,))
                    last_checkin = cursor.fetchone()
                    last_location = "Неизвестно"
                    maps_url = ""
                    if last_checkin:
                        latitude, longitude, last_timestamp = last_checkin
                        last_location = f"Координаты: {latitude}, {longitude}"
                        maps_url = f"https://www.google.com/maps?q={latitude},{longitude}"
                        last_checkin_time = datetime.fromisoformat(last_timestamp).astimezone(tz)
                    else:
                        last_checkin_time = datetime.strptime(current_trip[3], '%Y-%m-%d').astimezone(tz)

                    await bot.send_message(
                        ADMIN_ID,
                        get_text(user_id, 'missed_checkin',
                                 name=name,
                                 username=f' @{username}' if username else '',
                                 time=expected_time.strftime('%H:%M'),
                                 tz=tz.zone,
                                 last_time=last_checkin_time.strftime('%Y-%m-%d %H:%M') if last_checkin else 'Никогда',
                                 location=last_location,
                                 maps_url=maps_url if maps_url else 'Нет')
                    )
                    logger.warning(f"Пропущен чек-ин для {user_id} в {expected_time} ({tz.zone})")
        except Exception as e:
            logger.error(f"Ошибка в check_employees: {e}")
        await asyncio.sleep(1800)

async def main():
    """Основная функция для запуска б exaggerationота."""
    # Очистка существующих процессов бота
    try:
        bot_info = await bot.get_me()
        logger.info(f"Проверка единственного экземпляра для бота @{bot_info.username}")
    except Exception as e:
        logger.error(f"Ошибка проверки экземпляра бота: {e}")

    async with aiohttp.ClientSession() as session:
        try:
            await bot.delete_webhook()
            logger.info("Вебхук удалён")
            asyncio.create_task(check_employees())
            await dp.start_polling(bot)
            logger.info("Polling успешно запущен")
        except Exception as e:
            logger.error(f"Ошибка запуска бота: {e}")
            raise
        finally:
            await bot.session.close()
            logger.info("Сессия бота закрыта")

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем")
    except Exception as e:
        logger.error(f"Фатальная ошибка: {e}")
