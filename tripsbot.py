import asyncio
import logging
import sqlite3
import csv
from io import StringIO
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton, Message, CallbackQuery, BufferedInputFile, ContentType
from aiogram.filters import Command, CommandStart, BaseFilter
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
import pycountry
from timezonefinder import TimezoneFinder
from pytz import timezone
from geopy.geocoders import Nominatim
from dotenv import load_dotenv
import os
import re

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    filename='bot.log',
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Загрузка переменных окружения
load_dotenv()
API_TOKEN = os.getenv('API_TOKEN')
ADMIN_ID = int(os.getenv('ADMIN_ID') or 0)
if not API_TOKEN or not ADMIN_ID:
    logging.error("API_TOKEN или ADMIN_ID не заданы!")
    raise ValueError("Необходимо задать API_TOKEN и ADMIN_ID в .env файле")

# Инициализация бота и диспетчера
bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
try:
    dp = Dispatcher(storage=storage)
except Exception as e:
    logging.error(f"Ошибка при инициализации Dispatcher: {e}")
    raise

# Инициализация базы данных
conn = sqlite3.connect('employees.db')
cursor = conn.cursor()
cursor.execute('''
    CREATE TABLE IF NOT EXISTS employees (
        user_id INTEGER PRIMARY KEY,
        name TEXT,
        username TEXT,
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
# Создание индексов для оптимизации запросов
cursor.execute('CREATE INDEX IF NOT EXISTS idx_trips_user_id ON trips(user_id)')
cursor.execute('CREATE INDEX IF NOT EXISTS idx_checkins_user_id ON checkins(user_id)')
conn.commit()

# Клавиатуры
location_button = KeyboardButton(text="Отправить геопозицию", request_location=True)
keyboard = ReplyKeyboardMarkup(
    keyboard=[[location_button]],
    resize_keyboard=True
)

status_keyboard = InlineKeyboardMarkup(
    inline_keyboard=[
        [InlineKeyboardButton(text="Всё в порядке", callback_data="status_ok")],
        [InlineKeyboardButton(text="Нужна помощь", callback_data="status_help")]
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

trip_keyboard = InlineKeyboardMarkup(
    inline_keyboard=[
        [InlineKeyboardButton(text="Изменить сроки", callback_data="edit_trip")],
        [InlineKeyboardButton(text="Завершить просмотр", callback_data="finish_view")]
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
    Country = State()
    StartDate = State()
    EndDate = State()
    Frequency = State()
    CheckinTime = State()
    AddAnotherCountry = State()
    EditStartDate = State()
    EditEndDate = State()

# Словарь для отслеживания отправленных напоминаний
reminders_sent = {}

def get_timezone_by_country(country_name):
    """Получает часовой пояс по названию страны с использованием geopy."""
    try:
        geolocator = Nominatim(user_agent="telegram_bot")
        location = geolocator.geocode(country_name)
        if not location:
            logging.warning(f"Не найдены координаты для {country_name}")
            return 'UTC'
        tf = TimezoneFinder()
        timezone_str = tf.timezone_at(lat=location.latitude, lng=location.longitude)
        if not timezone_str:
            logging.warning(f"Не удалось определить часовой пояс для {country_name}")
            return 'UTC'
        return timezone_str
    except Exception as e:
        logging.error(f"Ошибка при определении часового пояса для {country_name}: {e}")
        return 'UTC'

def get_timezone_by_coordinates(latitude, longitude):
    """Получает часовой пояс по координатам."""
    try:
        tf = TimezoneFinder()
        timezone_str = tf.timezone_at(lat=latitude, lng=longitude)
        if not timezone_str:
            logging.warning(f"Не удалось определить часовой пояс для координат ({latitude}, {longitude})")
            return 'UTC'
        return timezone_str
    except Exception as e:
        logging.error(f"Ошибка при определении часового пояса для координат ({latitude}, {longitude}): {e}")
        return 'UTC'

def format_time_ago(timestamp, tz):
    """Форматирует время последнего чек-ина."""
    try:
        last_checkin = datetime.fromisoformat(timestamp).astimezone(tz)
        now = datetime.now(tz)
        hours_ago = int((now - last_checkin).total_seconds() // 3600)
        return "менее часа назад" if hours_ago == 0 else f"{hours_ago} часов назад"
    except Exception as e:
        logging.error(f"Ошибка при форматировании времени: {e}")
        return "неизвестно"

@dp.message(CommandStart())
async def start_command(message: Message, state: FSMContext):
    """Обрабатывает команду /start и инициирует регистрацию или предлагает новую командировку."""
    user_id = message.from_user.id
    cursor.execute('SELECT * FROM employees WHERE user_id = ?', (user_id,))
    employee = cursor.fetchone()
    if employee:
        cursor.execute('SELECT id, country, start_date, end_date, checkin_frequency, checkin_time '
                      'FROM trips WHERE user_id = ? AND date("now") BETWEEN start_date AND end_date', (user_id,))
        active_trip = cursor.fetchone()
        if active_trip:
            await message.reply("Вы уже зарегистрированы. У вас есть активная командировка. "
                              "Используйте /trip для просмотра или редактирования.", reply_markup=keyboard)
        else:
            await message.reply("Вы уже зарегистрированы, но активных командировок нет. "
                              "Хотите создать новую командировку?", reply_markup=new_trip_keyboard)
    else:
        username = message.from_user.username or None
        await state.update_data(username=username, trips=[])
        await message.reply("Начнём регистрацию. Введите ваше имя:")
        await state.set_state(Registration.Name)
        logging.info(f"Пользователь {user_id} начал регистрацию")

@dp.message(Registration.Name)
async def process_name(message: Message, state: FSMContext):
    """Обрабатывает ввод имени."""
    if not message.text.strip():
        await message.reply("Имя не может быть пустым. Пожалуйста, введите ваше имя:")
        return
    await state.update_data(name=message.text.strip())
    await message.reply("Введите первую страну пребывания:")
    await state.set_state(Registration.Country)

@dp.message(Registration.Country)
async def process_country(message: Message, state: FSMContext):
    """Обрабатывает ввод страны."""
    country = message.text.strip()
    if not country:
        await message.reply("Название страны не может быть пустым. Пожалуйста, введите страну:")
        return
    timezone_str = get_timezone_by_country(country)
    await state.update_data(country=country, timezone=timezone_str)
    await message.reply("Введите дату начала пребывания (ДД/ММ/ГГГГ):")
    await state.set_state(Registration.StartDate)

@dp.message(Registration.StartDate)
async def process_start_date(message: Message, state: FSMContext):
    """Обрабатывает ввод даты начала."""
    try:
        start_date = datetime.strptime(message.text, '%d/%m/%Y')
        await state.update_data(start_date=start_date.strftime('%Y-%m-%d'))
        await message.reply("Введите дату окончания пребывания (ДД/ММ/ГГГГ):")
        await state.set_state(Registration.EndDate)
    except ValueError:
        await message.reply("Неверный формат даты. Используйте ДД/ММ/ГГГГ, например, 01/05/2025.")

@dp.message(Registration.EndDate)
async def process_end_date(message: Message, state: FSMContext):
    """Обрабатывает ввод даты окончания."""
    try:
        end_date = datetime.strptime(message.text, '%d/%m/%Y')
        user_data = await state.get_data()
        start_date = datetime.strptime(user_data['start_date'], '%Y-%m-%d')
        if end_date < start_date:
            await message.reply("Дата окончания не может быть раньше даты начала.")
            return
        await state.update_data(end_date=end_date.strftime('%Y-%m-%d'))
        await message.reply("Выберите частоту чек-инов:", reply_markup=frequency_keyboard)
        await state.set_state(Registration.Frequency)
    except ValueError:
        await message.reply("Неверный формат даты. Используйте ДД/ММ/ГГГГ, например, 01/05/2025.")

@dp.callback_query(lambda c: c.data.startswith('freq_'))
async def process_frequency(callback: CallbackQuery, state: FSMContext):
    """Обрабатывает выбор частоты чек-инов."""
    freq_map = {'freq_1': 1, 'freq_2': 2, 'freq_3': 3}
    frequency = freq_map.get(callback.data)
    if not frequency:
        await callback.message.reply("Неверный выбор частоты.")
        return
    await state.update_data(frequency=frequency)

    if frequency == 1:
        await callback.message.reply("Выберите время чек-ина:", reply_markup=time_keyboard)
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
                [InlineKeyboardButton(text="Добавить ещё страну", callback_data="add_country")],
                [InlineKeyboardButton(text="Завершить", callback_data="finish")]
            ]
        )
        await callback.message.reply("Хотите добавить ещё одну страну?", reply_markup=keyboard)
        await state.set_state(Registration.AddAnotherCountry)

@dp.callback_query(lambda c: c.data.startswith('time_'))
async def process_checkin_time(callback: CallbackQuery, state: FSMContext):
    """Обрабатывает выбор времени чек-ина."""
    time_map = {
        'time_morning': 'morning',
        'time_day': 'day',
        'time_evening': 'evening'
    }
    checkin_time = time_map.get(callback.data)
    if not checkin_time:
        await callback.message.reply("Неверный выбор времени.")
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
            [InlineKeyboardButton(text="Добавить ещё страну", callback_data="add_country")],
            [InlineKeyboardButton(text="Завершить", callback_data="finish")]
        ]
    )
    await callback.message.reply("Хотите добавить ещё одну страну?", reply_markup=keyboard)
    await state.set_state(Registration.AddAnotherCountry)

@dp.callback_query(lambda c: c.data in ['add_country', 'finish'])
async def process_add_country(callback: CallbackQuery, state: FSMContext):
    """Обрабатывает выбор добавления страны или завершения регистрации."""
    if callback.data == "add_country":
        await callback.message.reply("Введите следующую страну пребывания:")
        await state.set_state(Registration.Country)
    elif callback.data == "finish":
        user_data = await state.get_data()
        user_id = callback.from_user.id
        try:
            # Если это регистрация нового сотрудника
            if 'name' in user_data:
                cursor.execute('INSERT INTO employees (user_id, name, username) VALUES (?, ?, ?)', 
                              (user_id, user_data['name'], user_data['username']))
            for trip in user_data['trips']:
                cursor.execute('''
                    INSERT INTO trips (user_id, country, timezone, start_date, end_date, checkin_frequency, checkin_time)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (user_id, trip['country'], trip['timezone'], trip['start_date'], trip['end_date'], 
                      trip['checkin_frequency'], trip['checkin_time']))
            conn.commit()
            await callback.message.reply("Регистрация завершена! Отправляйте геопозицию.", reply_markup=keyboard)
            logging.info(f"Пользователь {user_id} завершил регистрацию или добавил командировку: {user_data.get('name', 'существующий')}")
            await state.clear()
        except Exception as e:
            logging.error(f"Ошибка при сохранении данных пользователя {user_id}: {e}")
            await callback.message.reply("Произошла ошибка при регистрации. Попробуйте снова.")

@dp.message(Command("trip"))
async def view_trip(message: Message, state: FSMContext):
    """Показывает текущую командировку сотрудника и предлагает редактировать сроки."""
    user_id = message.from_user.id
    cursor.execute('SELECT * FROM employees WHERE user_id = ?', (user_id,))
    employee = cursor.fetchone()
    if not employee:
        await message.reply("Сначала зарегистрируйтесь с помощью /start")
        return

    cursor.execute('SELECT id, country, start_date, end_date, checkin_frequency, checkin_time '
                  'FROM trips WHERE user_id = ? AND date("now") BETWEEN start_date AND end_date', (user_id,))
    active_trip = cursor.fetchone()
    if active_trip:
        trip_id, country, start_date, end_date, frequency, checkin_time = active_trip
        freq_text = {1: "1 раз в день", 2: "2 раза (утро, вечер)", 3: "3 раза (утро, день, вечер)"}.get(frequency, "Неизвестно")
        time_text = {"morning": "08:00", "day": "14:00", "evening": "20:00"}.get(checkin_time, "Не указано")
        await state.update_data(trip_id=trip_id)
        await message.reply(
            f"Ваша текущая командировка:\n"
            f"Страна: {country}\n"
            f"Даты: {start_date} - {end_date}\n"
            f"Частота чек-инов: {freq_text}\n"
            f"Время чек-ина: {time_text}\n"
            f"Что хотите сделать?",
            reply_markup=trip_keyboard
        )
    else:
        await message.reply("У вас нет активных командировок. Хотите создать новую?", reply_markup=new_trip_keyboard)

@dp.callback_query(lambda c: c.data in ['edit_trip', 'finish_view', 'new_trip'])
async def handle_trip_action(callback: CallbackQuery, state: FSMContext):
    """Обрабатывает действия с командировкой."""
    if callback.data == "finish_view":
        await callback.message.reply("Просмотр завершён.", reply_markup=keyboard)
        await state.clear()
    elif callback.data == "edit_trip":
        await callback.message.reply("Введите новую дату начала пребывания (ДД/ММ/ГГГГ):")
        await state.set_state(Registration.EditStartDate)
    elif callback.data == "new_trip":
        await state.update_data(trips=[])
        await callback.message.reply("Введите страну новой командировки:")
        await state.set_state(Registration.Country)

@dp.message(Registration.EditStartDate)
async def process_edit_start_date(message: Message, state: FSMContext):
    """Обрабатывает ввод новой даты начала для редактирования командировки."""
    try:
        start_date = datetime.strptime(message.text, '%d/%m/%Y')
        await state.update_data(start_date=start_date.strftime('%Y-%m-%d'))
        await message.reply("Введите новую дату окончания пребывания (ДД/ММ/ГГГГ):")
        await state.set_state(Registration.EditEndDate)
    except ValueError:
        await message.reply("Неверный формат даты. Используйте ДД/ММ/ГГГГ, например, 01/05/2025.")

@dp.message(Registration.EditEndDate)
async def process_edit_end_date(message: Message, state: FSMContext):
    """Обрабатывает ввод новой даты окончания и обновляет командировку."""
    try:
        end_date = datetime.strptime(message.text, '%d/%m/%Y')
        user_data = await state.get_data()
        start_date = datetime.strptime(user_data['start_date'], '%Y-%m-%d')
        if end_date < start_date:
            await message.reply("Дата окончания не может быть раньше даты начала.")
            return
        trip_id = user_data.get('trip_id')
        cursor.execute('UPDATE trips SET start_date = ?, end_date = ? WHERE id = ?', 
                      (user_data['start_date'], end_date.strftime('%Y-%m-%d'), trip_id))
        conn.commit()
        await message.reply("Сроки командировки обновлены!", reply_markup=keyboard)
        logging.info(f"Пользователь {message.from_user.id} обновил командировку ID {trip_id}")
        await state.clear()
    except ValueError:
        await message.reply("Неверный формат даты. Используйте ДД/ММ/ГГГГ, например, 01/05/2025.")
    except Exception as e:
        logging.error(f"Ошибка при обновлении командировки: {e}")
        await message.reply("Произошла ошибка при обновлении командировки.")

class LocationFilter(BaseFilter):
    async def __call__(self, message: Message) -> bool:
        return message.content_type == ContentType.LOCATION

@dp.message(LocationFilter())
async def handle_location(message: Message, state: FSMContext):
    """Обрабатывает отправку геопозиции."""
    user_id = message.from_user.id
    cursor.execute('SELECT * FROM employees WHERE user_id = ?', (user_id,))
    employee = cursor.fetchone()
    if not employee:
        await message.reply("Сначала зарегистрируйтесь с помощью /start")
        return

    location = message.location
    if not (-90 <= location.latitude <= 90 and -180 <= location.longitude <= 180):
        await message.reply("Некорректная геопозиция. Пожалуйста, отправьте снова.")
        return

    # Определяем часовой пояс на основе координат
    timezone_str = get_timezone_by_coordinates(location.latitude, location.longitude)
    
    # Обновляем часовой пояс в таблице trips для текущей поездки
    current_date = datetime.now().strftime('%Y-%m-%d')
    cursor.execute('''
        UPDATE trips
        SET timezone = ?
        WHERE user_id = ? AND ? BETWEEN start_date AND end_date
    ''', (timezone_str, user_id, current_date))
    conn.commit()

    await state.update_data(latitude=location.latitude, longitude=location.longitude)
    await message.reply("Геопозиция получена. Выберите статус:", reply_markup=status_keyboard)
    logging.info(f"Геопозиция получена от {user_id}: ({location.latitude}, {location.longitude}), часовой пояс: {timezone_str}")

@dp.callback_query(lambda c: c.data.startswith('status_'))
async def handle_status(callback: CallbackQuery, state: FSMContext):
    """Обрабатывает выбор статуса чек-ина."""
    user_id = callback.from_user.id
    cursor.execute('SELECT * FROM employees WHERE user_id = ?', (user_id,))
    employee = cursor.fetchone()
    if not employee:
        await callback.message.reply("Сначала зарегистрируйтесь с помощью /start")
        return

    status = "Всё в порядке" if callback.data == "status_ok" else "Нужна помощь"
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
        await callback.message.reply(f"Чек-ин зарегистрирован: {status}\nКарта: {maps_url}")
        logging.info(f"Чек-ин зарегистрирован для {user_id}: {status}")
    except Exception as e:
        logging.error(f"Ошибка при сохранении чек-ина для {user_id}: {e}")
        await callback.message.reply("Произошла ошибка при регистрации чек-ина.")

@dp.message(Command("list"))
async def list_employees(message: Message):
    """Выводит список всех сотрудников (для админа)."""
    if message.from_user.id != ADMIN_ID:
        return
    try:
        cursor.execute('SELECT user_id, name, username, archived FROM employees')
        employees = cursor.fetchall()
        if not employees:
            await message.reply("Нет зарегистрированных сотрудников.")
            return

        response = "Список сотрудников:\n"
        for emp in employees:
            cursor.execute('SELECT country, start_date, end_date FROM trips WHERE user_id = ?', (emp[0],))
            trips = cursor.fetchall()
            trip_info = ", ".join([f"{t[0]} ({t[1]} - {t[2]})" for t in trips])
            status = "Архив" if emp[3] else "Активен"
            response += f"ID: {emp[0]}, Имя: {emp[1]}{f' @{emp[2]}' if emp[2] else ''}, Статус: {status}, Поездки: {trip_info}\n"
        await message.reply(response)
    except Exception as e:
        logging.error(f"Ошибка при получении списка сотрудников: {e}")
        await message.reply("Произошла ошибка при получении списка сотрудников.")

@dp.message(Command("status"))
async def employee_status(message: Message):
    """Выводит статус сотрудника по ID или @username (для админа)."""
    if message.from_user.id != ADMIN_ID:
        return
    try:
        input_str = message.text.split()[1] if len(message.text.split()) > 1 else None
        if not input_str:
            await message.reply("Использование: /status <user_id> или /status @username")
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
                await message.reply("Неверный формат ID. Используйте /status <user_id> или /status @username")
                return

        if not employee:
            await message.reply("Сотрудник не найден.")
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
                f"Сотрудник: {employee[1]}{f' @{employee[2]}' if employee[2] else ''}\n"
                f"Статус: {'Архив' if employee[3] else 'Активен'}\n"
                f"Поездки: {trip_info}\n"
                f"Последний чек-ин: {checkin_time}\n"
                f"Карта: {maps_url}\n"
                f"Статус: {checkin[2]}"
            )
        else:
            await message.reply(
                f"Сотрудник: {employee[1]}{f' @{employee[2]}' if employee[2] else ''}\n"
                f"Статус: {'Архив' if employee[3] else 'Активен'}\n"
                f"Поездки: {trip_info}\n"
                f"Чек-ины отсутствуют."
            )
    except IndexError:
        await message.reply("Использование: /status <user_id> или /status @username")
    except Exception as e:
        logging.error(f"Ошибка при получении статуса сотрудника: {e}")
        await message.reply("Произошла ошибка при получении статуса.")

@dp.message(Command("export"))
async def export_checkins(message: Message):
    """Экспортирует чек-ины в CSV (для админа)."""
    if message.from_user.id != ADMIN_ID:
        return
    try:
        args = message.text.split()
        weeks = None
        employee_id = None

        # Парсим аргументы
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
                    await message.reply(f"Пользователь {arg} не найден.")
                    return
            else:
                try:
                    employee_id = int(arg)
                    cursor.execute('SELECT user_id FROM employees WHERE user_id = ?', (employee_id,))
                    if not cursor.fetchone():
                        await message.reply(f"Пользователь с ID {employee_id} не найден.")
                        return
                except ValueError:
                    await message.reply(
                        "Неверный формат. Используйте /export, /export <число>w, /export @username, "
                        "/export <user_id>, или их комбинацию (например, /export 2w @username)."
                    )
                    return

        # Формируем SQL-запрос
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
            await message.reply("Чек-ины за указанный период или для указанного сотрудника отсутствуют.")
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
        logging.info(f"Чек-ины экспортированы в CSV {'за последние ' + str(weeks) + ' недель' if weeks else ''} "
                     f"{'для сотрудника ' + str(employee_id) if employee_id else ''}")
    except Exception as e:
        logging.error(f"Ошибка при экспорте чек-инов: {e}")
        await message.reply("Произошла ошибка при экспорте чек-инов.")

async def send_reminder(user_id, tz, checkin_time):
    """Отправляет напоминание о необходимости чек-ина."""
    reminder_time = checkin_time - timedelta(minutes=30)
    reminder_key = f"{user_id}_{checkin_time.date()}_{checkin_time.hour}"
    if datetime.now(tz) >= reminder_time and reminder_key not in reminders_sent:
        try:
            await bot.send_message(
                user_id,
                f"Напоминание: отправьте чек-ин в {checkin_time.strftime('%H:%M')} ({tz.zone})!",
                reply_markup=keyboard
            )
            reminders_sent[reminder_key] = True
            logging.info(f"Напоминание отправлено пользователю {user_id} для {checkin_time} ({tz.zone})")
        except Exception as e:
            logging.error(f"Ошибка при отправке напоминания пользователю {user_id}: {e}")

async def check_employees():
    """Проверяет сотрудников и отправляет напоминания или уведомления админу."""
    while True:
        try:
            cursor.execute('SELECT user_id, name, username, archived FROM employees WHERE archived = 0')
            employees = cursor.fetchall()
            for emp in employees:
                user_id, name, username, _ = emp
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
                    await bot.send_message(ADMIN_ID, f"Сотрудник {name}{f' @{username}' if username else ''} помечен как архивный (командировки завершены).")
                    logging.info(f"Сотрудник {user_id} помечен как архивный")
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

                    # Проверяем чек-ины в окне: -90 минут до +20 минут от ожидаемого времени
                    window_start = (expected_time - timedelta(minutes=90)).isoformat()
                    window_end = (expected_time + timedelta(minutes=20)).isoformat()
                    cursor.execute('SELECT timestamp FROM checkins WHERE user_id = ? AND timestamp BETWEEN ? AND ?',
                                  (user_id, window_start, window_end))
                    checkin_in_window = cursor.fetchone()

                    if checkin_in_window:
                        continue  # Чек-ин найден в окне, пропускаем уведомление

                    # Если чек-ин пропущен
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
                        f"Сотрудник {name}{f' @{username}' if username else ''} не отправил чек-ин!\n"
                        f"Ожидалось: {expected_time.strftime('%H:%M')} ({tz.zone})\n"
                        f"Последний чек-ин: {(last_checkin_time.strftime('%Y-%m-%d %H:%M') if last_checkin else 'Никогда')}\n"
                        f"Последняя локация: {last_location}\n"
                        f"Карта: {maps_url if maps_url else 'Отсутствует'}"
                    )
                    logging.warning(f"Пропущен чек-ин для {user_id} в {expected_time} ({tz.zone})")
        except Exception as e:
            logging.error(f"Ошибка в check_employees: {e}")
        await asyncio.sleep(1800)

async def main():
    """Основная функция запуска бота."""
    try:
        await bot.delete_webhook()
        asyncio.create_task(check_employees())
        await dp.start_polling(bot, skip_updates=True)
    except Exception as e:
        logging.error(f"Ошибка при запуске бота: {e}")
        raise

if __name__ == '__main__':
    asyncio.run(main())
