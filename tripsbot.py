import asyncio
import logging
import sqlite3
import csv
from io import StringIO
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command, CommandStart
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
import pycountry
from timezonefinder import TimezoneFinder
from pytz import timezone
from geopy.geocoders import Nominatim
from dotenv import load_dotenv
import os

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
keyboard = ReplyKeyboardMarkup(resize_keyboard=True).add(location_button)

status_keyboard = InlineKeyboardMarkup()
status_keyboard.add(InlineKeyboardButton(text="Всё в порядке", callback_data="status_ok"))
status_keyboard.add(InlineKeyboardButton(text="Нужна помощь", callback_data="status_help"))

frequency_keyboard = InlineKeyboardMarkup()
frequency_keyboard.add(InlineKeyboardButton(text="1 раз", callback_data="freq_1"))
frequency_keyboard.add(InlineKeyboardButton(text="2 раза (утро, вечер)", callback_data="freq_2"))
frequency_keyboard.add(InlineKeyboardButton(text="3 раза (утро, день, вечер)", callback_data="freq_3"))

time_keyboard = InlineKeyboardMarkup()
time_keyboard.add(InlineKeyboardButton(text="Утро (8:00)", callback_data="time_morning"))
time_keyboard.add(InlineKeyboardButton(text="День (14:00)", callback_data="time_day"))
time_keyboard.add(InlineKeyboardButton(text="Вечер (20:00)", callback_data="time_evening"))

# Состояния для регистрации
class Registration(StatesGroup):
    Name = State()
    Country = State()
    StartDate = State()
    EndDate = State()
    Frequency = State()
    CheckinTime = State()
    AddAnotherCountry = State()

# Константы
MONTHS = {
    1: 'Янв', 2: 'Фев', 3: 'Мар', 4: 'Апр', 5: 'Май', 6: 'Июн',
    7: 'Июл', 8: 'Авг', 9: 'Сен', 10: 'Окт', 11: 'Ноя', 12: 'Дек'
}

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
async def start_command(message: types.Message, state: FSMContext):
    """Обрабатывает команду /start и инициирует регистрацию."""
    user_id = message.from_user.id
    cursor.execute('SELECT * FROM employees WHERE user_id = ?', (user_id,))
    if cursor.fetchone():
        await message.reply("Вы уже зарегистрированы. Отправьте геопозицию.", reply_markup=keyboard)
    else:
        username = message.from_user.username or None
        await state.update_data(username=username, trips=[])
        await message.reply("Начнём регистрацию. Введите ваше имя:")
        await Registration.Name.set()
        logging.info(f"Пользователь {user_id} начал регистрацию")

@dp.message(Registration.Name)
async def process_name(message: types.Message, state: FSMContext):
    """Обрабатывает ввод имени."""
    if not message.text.strip():
        await message.reply("Имя не может быть пустым. Пожалуйста, введите ваше имя:")
        return
    await state.update_data(name=message.text.strip())
    await message.reply("Введите первую страну пребывания:")
    await Registration.Country.set()

@dp.message(Registration.Country)
async def process_country(message: types.Message, state: FSMContext):
    """Обрабатывает ввод страны."""
    country = message.text.strip()
    if not country:
        await message.reply("Название страны не может быть пустым. Пожалуйста, введите страну:")
        return
    timezone_str = get_timezone_by_country(country)
    await state.update_data(country=country, timezone=timezone_str)
    await message.reply("Введите дату начала пребывания (ГГГГ-ММ-ДД):")
    await Registration.StartDate.set()

@dp.message(Registration.StartDate)
async def process_start_date(message: types.Message, state: FSMContext):
    """Обрабатывает ввод даты начала."""
    try:
        start_date = datetime.strptime(message.text, '%Y-%m-%d')
        await state.update_data(start_date=message.text)
        await message.reply("Введите дату окончания пребывания (ГГГГ-ММ-ДД):")
        await Registration.EndDate.set()
    except ValueError:
        await message.reply("Неверный формат даты. Используйте ГГГГ-ММ-ДД.")

@dp.message(Registration.EndDate)
async def process_end_date(message: types.Message, state: FSMContext):
    """Обрабатывает ввод даты окончания."""
    try:
        end_date = datetime.strptime(message.text, '%Y-%m-%d')
        user_data = await state.get_data()
        start_date = datetime.strptime(user_data['start_date'], '%Y-%m-%d')
        if end_date < start_date:
            await message.reply("Дата окончания не может быть раньше даты начала.")
            return
        await state.update_data(end_date=message.text)
        await message.reply("Выберите частоту чек-инов:", reply_markup=frequency_keyboard)
        await Registration.Frequency.set()
    except ValueError:
        await message.reply("Неверный формат даты. Используйте ГГГГ-ММ-ДД.")

@dp.callback_query(lambda c: c.data.startswith('freq_'))
async def process_frequency(callback: types.CallbackQuery, state: FSMContext):
    """Обрабатывает выбор частоты чек-инов."""
    freq_map = {'freq_1': 1, 'freq_2': 2, 'freq_3': 3}
    frequency = freq_map.get(callback.data)
    if not frequency:
        await callback.message.reply("Неверный выбор частоты.")
        return
    await state.update_data(frequency=frequency)

    if frequency == 1:
        await callback.message.reply("Выберите время чек-ина:", reply_markup=time_keyboard)
        await Registration.CheckinTime.set()
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
        keyboard = InlineKeyboardMarkup()
        keyboard.add(InlineKeyboardButton(text="Добавить ещё страну", callback_data="add_country"))
        keyboard.add(InlineKeyboardButton(text="Завершить", callback_data="finish"))
        await callback.message.reply("Хотите добавить ещё одну страну?", reply_markup=keyboard)
        await Registration.AddAnotherCountry.set()

@dp.callback_query(lambda c: c.data.startswith('time_'))
async def process_checkin_time(callback: types.CallbackQuery, state: FSMContext):
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
    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton(text="Добавить ещё страну", callback_data="add_country"))
    keyboard.add(InlineKeyboardButton(text="Завершить", callback_data="finish"))
    await callback.message.reply("Хотите добавить ещё одну страну?", reply_markup=keyboard)
    await Registration.AddAnotherCountry.set()

@dp.callback_query(lambda c: c.data in ['add_country', 'finish'])
async def process_add_country(callback: types.CallbackQuery, state: FSMContext):
    """Обрабатывает выбор добавления страны или завершения регистрации."""
    if callback.data == "add_country":
        await callback.message.reply("Введите следующую страну пребывания:")
        await Registration.Country.set()
    elif callback.data == "finish":
        user_data = await state.get_data()
        user_id = callback.from_user.id
        try:
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
            logging.info(f"Пользователь {user_id} завершил регистрацию: {user_data['name']}")
            await state.finish()
        except Exception as e:
            logging.error(f"Ошибка при сохранении данных пользователя {user_id}: {e}")
            await callback.message.reply("Произошла ошибка при регистрации. Попробуйте снова.")

@dp.message(content_types=['location'])
async def handle_location(message: types.Message, state: FSMContext):
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

    await state.update_data(latitude=location.latitude, longitude=location.longitude)
    await message.reply("Геопозиция получена. Выберите статус:", reply_markup=status_keyboard)
    logging.info(f"Геопозиция получена от {user_id}: ({location.latitude}, {location.longitude})")

@dp.callback_query(lambda c: c.data.startswith('status_'))
async def handle_status(callback: types.CallbackQuery, state: FSMContext):
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
async def list_employees(message: types.Message):
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
async def employee_status(message: types.Message):
    """Выводит статус сотрудника по ID (для админа)."""
    if message.from_user.id != ADMIN_ID:
        return
    try:
        user_id = int(message.text.split()[1])
        cursor.execute('SELECT name, username, archived FROM employees WHERE user_id = ?', (user_id,))
        employee = cursor.fetchone()
        if not employee:
            await message.reply("Сотрудник не найден.")
            return

        cursor.execute('SELECT country, start_date, end_date FROM trips WHERE user_id = ?', (user_id,))
        trips = cursor.fetchall()
        trip_info = ", ".join([f"{t[0]} ({t[1]} - {t[2]})" for t in trips])

        cursor.execute('SELECT latitude, longitude, status, timestamp FROM checkins WHERE user_id = ? ORDER BY timestamp DESC LIMIT 1', (user_id,))
        checkin = cursor.fetchone()
        if checkin:
            maps_url = f"https://www.google.com/maps?q={checkin[0]},{checkin[1]}"
            await message.reply(
                f"Сотрудник: {employee[0]}{f' @{employee[1]}' if employee[1] else ''}\n"
                f"Статус: {'Архив' if employee[2] else 'Активен'}\n"
                f"Поездки: {trip_info}\n"
                f"Последний чек-ин: {checkin[3]}\n"
                f"Геопозиция: {checkin[0]}, {checkin[1]}\n"
                f"Статус: {checkin[2]}\n"
                f"Карта: {maps_url}"
            )
        else:
            await message.reply(
                f"Сотрудник: {employee[0]}{f' @{employee[1]}' if employee[1] else ''}\n"
                f"Статус: {'Архив' if employee[2] else 'Активен'}\n"
                f"Поездки: {trip_info}\n"
                f"Чек-ины отсутствуют."
            )
    except IndexError:
        await message.reply("Использование: /status <user_id>")
    except Exception as e:
        logging.error(f"Ошибка при получении статуса сотрудника: {e}")
        await message.reply("Произошла ошибка при получении статуса.")

@dp.message(Command("export"))
async def export_checkins(message: types.Message):
    """Экспортирует чек-ины в CSV (для админа)."""
    if message.from_user.id != ADMIN_ID:
        return
    try:
        cursor.execute('SELECT c.user_id, e.name, e.username, c.latitude, c.longitude, c.status, c.timestamp '
                      'FROM checkins c JOIN employees e ON c.user_id = e.user_id')
        checkins = cursor.fetchall()

        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(['User ID', 'Name', 'Username', 'Latitude', 'Longitude', 'Status', 'Timestamp'])
        for checkin in checkins:
            writer.writerow(checkin)

        output.seek(0)
        await message.reply_document(types.InputFile(output, filename='checkins.csv'), caption="Экспорт чек-инов")
        logging.info("Чек-ины экспортированы в CSV")
    except Exception as e:
        logging.error(f"Ошибка при экспорте чек-инов: {e}")
        await message.reply("Произошла ошибка при экспорте чек-инов.")

@dp.message(Command("map"))
async def show_map(message: types.Message):
    """Показывает карту с позициями активных сотрудников (для админа)."""
    if message.from_user.id != ADMIN_ID:
        return
    try:
        cursor.execute('SELECT user_id, name, username FROM employees WHERE archived = 0')
        employees = cursor.fetchall()
        if not employees:
            await message.reply("Нет активных сотрудников.")
            return

        markers = []
        for emp in employees:
            user_id, name, username = emp
            cursor.execute('SELECT country, start_date, end_date, timezone FROM trips WHERE user_id = ? AND ? BETWEEN start_date AND end_date', 
                          (user_id, datetime.now().strftime('%Y-%m-%d')))
            trip = cursor.fetchone()
            if trip:
                cursor.execute('SELECT latitude, longitude, timestamp FROM checkins WHERE user_id = ? ORDER BY timestamp DESC LIMIT 1', (user_id,))
                checkin = cursor.fetchone()
                if checkin:
                    start_date = datetime.strptime(trip[1], '%Y-%m-%d')
                    end_date = datetime.strptime(trip[2], '%Y-%m-%d')
                    start_str = f"{start_date.day:02d} {MONTHS[start_date.month]}"
                    end_str = f"{end_date.day:02d} {MONTHS[end_date.month]}"
                    
                    tz = timezone(trip[3])
                    time_ago = format_time_ago(checkin[2], tz)
                    
                    label = f"{name}{f' @{username}' if username else ''}, {start_str} - {end_str}, последний чек-ин: {time_ago}"
                    label = label.replace(" ", "+")
                    markers.append(f"{checkin[0]},{checkin[1]},{label}")

        if not markers:
            await message.reply("Нет актуальных геопозиций для активных сотрудников.")
            return

        base_url = "https://www.google.com/maps/search/?api=1&query="
        map_url = base_url + ",".join(markers)
        await message.reply(f"Карта с позициями сотрудников:\n{map_url}")
    except Exception as e:
        logging.error(f"Ошибка при формировании карты: {e}")
        await message.reply("Произошла ошибка при формировании карты.")

async def send_reminder(user_id, tz, checkin_time):
    """Отправляет напоминание о необходимости чек-ина."""
    reminder_time = checkin_time - timedelta(minutes=30)
    reminder_key = f"{user_id}_{checkin_time.date()}_{checkin_time.hour}"
    if datetime.now(tz) >= reminder_time and reminder_key not in reminders_sent:
        try:
            await bot.send_message(
                user_id,
                f"Напоминание: отправьте чек-ин в {checkin_time.strftime('%H:%M')}!",
                reply_markup=keyboard
            )
            reminders_sent[reminder_key] = True
            logging.info(f"Напоминание отправлено пользователю {user_id} для {checkin_time}")
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

                cursor.execute('SELECT timestamp FROM checkins WHERE user_id = ? ORDER BY timestamp DESC LIMIT 1', (user_id,))
                last_checkin = cursor.fetchone()
                last_checkin_time = datetime.fromisoformat(last_checkin[0]).astimezone(tz) if last_checkin else datetime.strptime(current_trip[3], '%Y-%m-%d').astimezone(tz)

                for checkin_hour, checkin_minute in checkin_times:
                    expected_time = current_time_tz.replace(hour=checkin_hour, minute=checkin_minute, second=0, microsecond=0)
                    if expected_time.date() == current_time_tz.date():
                        await send_reminder(user_id, tz, expected_time)
                        if expected_time > last_checkin_time and (current_time_tz - expected_time).total_seconds() > 3600:
                            maps_url = ""
                            cursor.execute('SELECT latitude, longitude FROM checkins WHERE user_id = ? ORDER BY timestamp DESC LIMIT 1', (user_id,))
                            last_location = cursor.fetchone()
                            if last_location:
                                maps_url = f"https://www.google.com/maps?q={last_location[0]},{last_location[1]}"
                            await bot.send_message(
                                ADMIN_ID,
                                f"Сотрудник {name}{f' @{username}' if username else ''} не отправил чек-ин!\n"
                                f"Ожидалось: {expected_time}\n"
                                f"Последний чек-ин: {last_checkin_time if last_checkin else 'Никогда'}\n"
                                f"Последняя локация: {maps_url or 'Неизвестно'}"
                            )
                            logging.warning(f"Пропущен чек-ин для {user_id} в {expected_time}")
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
