import asyncio
import logging
import sqlite3
import csv
from io import StringIO
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, executor, types
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.dispatcher.filters import Command
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
import pycountry
from timezonefinder import TimezoneFinder
from pytz import timezone

# Настройка логирования
logging.basicConfig(level=logging.INFO, filename='bot.log')

# Инициализация бота
API_TOKEN = 'ТВОЙ_ТОКЕН _ОТ_BOTFATHER' # Замените на токен от @BotFather
ADMIN_ID = ТВОЙ_CHAT_ID # Замените на ваш Telegram chat_id
bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

# Инициализация базы данных SQLite
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
conn.commit()

# Клавиатура для геопозиции
location_button = KeyboardButton("Отправить геопозицию", request_location=True)
keyboard = ReplyKeyboardMarkup(resize_keyboard=True).add(location_button)

# Инлайн-кнопки для статуса
status_keyboard = InlineKeyboardMarkup()
status_keyboard.add(InlineKeyboardButton("Всё в порядке", callback_data="status_ok"))
status_keyboard.add(InlineKeyboardButton("Нужна помощь", callback_data="status_help"))

# Клавиатура для выбора частоты чек-инов
frequency_keyboard = InlineKeyboardMarkup()
frequency_keyboard.add(InlineKeyboardButton("1 раз (утро)", callback_data="freq_1"))
frequency_keyboard.add(InlineKeyboardButton("2 раза (утро, вечер)", callback_data="freq_2"))
frequency_keyboard.add(InlineKeyboardButton("3 раза (утро, день, вечер)", callback_data="freq_3"))

# Клавиатура для выбора времени чек-ина
time_keyboard = InlineKeyboardMarkup()
time_keyboard.add(InlineKeyboardButton("Утро (8:00)", callback_data="time_morning"))
time_keyboard.add(InlineKeyboardButton("День (14:00)", callback_data="time_day"))
time_keyboard.add(InlineKeyboardButton("Вечер (20:00)", callback_data="time_evening"))

# FSM для регистрации
class Registration(StatesGroup):
 Name = State()
 Country = State()
 StartDate = State()
 EndDate = State()
 Frequency = State()
 CheckinTime = State()
 AddAnotherCountry = State()

# Локализация месяцев для русского формата
MONTHS = {
 1: 'Янв', 2: 'Фев', 3: 'Мар', 4: 'Апр', 5: 'Май', 6: 'Июн',
 7: 'Июл', 8: 'Авг', 9: 'Сен', 10: 'Окт', 11: 'Ноя', 12: 'Дек'
}

# Функция для получения часового пояса по стране
def get_timezone_by_country(country_name):
 try:
 country = pycountry.countries.search_fuzzy(country_name)[0]
 tf = TimezoneFinder()
 latitude, longitude = {
 'Russia': (55.7558, 37.6173),
 'United States': (38.8951, -77.0364),
 # Добавьте координаты для других стран по необходимости
 }.get(country_name, (0, 0))
 timezone_str = tf.timezone_at(lat=latitude, lng=longitude)
 return timezone_str or 'UTC'
 except:
 return 'UTC'

# Форматирование времени "X часов назад"
def format_time_ago(timestamp, tz):
 last_checkin = datetime.fromisoformat(timestamp).astimezone(tz)
 now = datetime.now(tz)
 hours_ago = int((now - last_checkin).total_seconds() // 3600)
 return "менее часа назад" if hours_ago == 0 else f"{hours_ago} часов назад"

# Обработчик команды /start
@dp.message_handler(commands=['start'])
async def start_command(message: types.Message):
 user_id = message.from_user.id
 cursor.execute('SELECT * FROM employees WHERE user_id = ?', (user_id,))
 if cursor.fetchone():
 await message .reply("Вы уже зарегистрированы. Отправьте геопозицию.", reply_markup=keyboard)
 else:
 username = message.from_user.username or None
 await message.bot.get_state(user_id).update_data(username=username, trips=[])
 await message.reply("Начнём регистрацию. Введите ваше имя:")
 await Registration.Name.set()

# Регистрация: имя
@dp.message_handler(state=Registration.Name)
async def process_name(message: types.Message, state: FSMContext):
 await state.update_data(name=message.text)
 await message.reply("Введите первую страну пребывания:")
 await Registration.Country.set()

# Регистрация: страна
@dp.message_handler(state=Registration.Country)
async def process_country(message: types.Message, state: FSMContext):
 country = message.text
 timezone_str = get_timezone_by_country(country)
 await state.update_data(country=country, timezone=timezone_str)
 await message.reply("Введите дату начала пребывания (ГГГГ-ММ-ДД):")
 await Registration.StartDate.set()

# Регистрация: дата начала
@dp.message_handler(state=Registration.StartDate)
async def process_start_date(message: types.Message, state: FSMContext):
 try:
 datetime.strptime(message.text, '%Y-%m-%d')
 await state.update_data(start_date=message.text)
 await message.reply("Введите дату окончания пребывания (ГГГГ-ММ-ДД):")
 await Registration.EndDate.set()
 except ValueError:
 await message.reply("Неверный формат даты. Используйте ГГГГ-ММ-ДД.")

# Регистрация: дата окончания
@dp.message_handler(state=Registration.EndDate)
async def process_end_date(message: types.Message, state: FSMContext):
 try:
 datetime.strptime(message.text, '%Y-%m-%d')
 await state.update_data(end_date=message.text)
 await message.reply("Выберите частоту чек-инов:", reply_markup=frequency_keyboard)
 await Registration.Frequency.set()
 except ValueError:
 await message.reply("Неверный формат даты. Используйте ГГГГ-ММ-ДД.")

# Регистрация: частота чек-инов
@dp.callback_query_handler(state=Registration.Frequency)
async def process_frequency(callback: types.CallbackQuery, state: FSMContext):
 freq_map = {'freq_1': 1, 'freq_2': 2, 'freq_3': 3}
 frequency = freq_map[callback.data]
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
 keyboard.add(InlineKeyboardButton("Добавить ещё страну", callback_data="add_country"))
 keyboard.add(InlineKeyboardButton("Завершить", callback_data="finish"))
 await callback.message.reply("Хотите добавить ещё одну страну?", reply_markup=keyboard)
 await Registration.AddAnotherCountry.set()

# Регистрация: выбор времени чек-ина
@dp.callback_query_handler(state=Registration.CheckinTime)
async def process_checkin_time(callback: types.CallbackQuery, state: FSMContext):
 time_map = {
 'time_morning': 'morning',
 'time_day': 'day',
 'time_evening': 'evening'
 }
 checkin_time = time_map[callback.data]
 user_data = await state.get_data()
 user_data['trips'].append({
 'country': user_data['country'],
 'timezone': user_data['timezone'],
 zastrava start_date': user_data['start_date'],
 'end_date': user_data['end_date'],
 'checkin_frequency': user_data['frequency'],
 'checkin_time': checkin_time
 })
 keyboard = InlineKeyboardMarkup()
 keyboard.add(InlineKeyboardButton("Добавить ещё страну", callback_data="add_country"))
 keyboard.add(InlineKeyboardButton("Завершить", callback_data="finish"))
 await callback.message.reply("Хотите добавить ещё одну страну?", reply_markup=keyboard)
 await Registration.AddAnotherCountry.set()

# Регистрация: добавить ещё страну или завершить
@dp.callback_query_handler(state=Registration.AddAnotherCountry)
async def process_add_country(callback: types.CallbackQuery, state: FSMContext):
 if callback.data == "add_country":
 await callback.message.reply("Введите следующую страну пребывания:")
 await Registration.Country.set()
 else:
 user_data = await state.get_data()
 user_id = callback.from_user.id
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
 await state.finish()

# Обработчик геопозиции
@dp.message_handler(content_types=['location'])
async def handle_location(message: types.Message):
 user_id = message.from_user.id
 cursor.execute('SELECT * FROM employees WHERE user_id = ?', (user_id,))
 employee = cursor.fetchone()
 if not employee:
 await message.reply("Сначала зарегистрируйтесь с помощью /start")
 return

 location = message.location
 await message.reply("Геопозиция получена. Выберите статус:", reply_markup=status_keyboard)
 await message.bot.get_state(user_id).update_data(latitude=location.latitude, longitude=location.longitude)

# Обработчик статуса
@dp.callback_query_handler(lambda c: c.data.startswith('status_'))
async def handle_status(callback: types.CallbackQuery):
 user_id = callback.from_user.id
 cursor.execute('SELECT * FROM employees WHERE user_id = ?', (user_id,))
 employee = cursor.fetchone()
 if not employee:
 await callback.message.reply("Сначала зарегистрируйтесь с помощью /start")
 return

 status = "Всё в порядке" if callback.data == "status_ok" else "Нужна помощь"
 state_data = await callback.message.bot.get_state(user_id).get_data()
 latitude = state_data.get('latitude')
 longitude = state_data.get('longitude')
 timestamp = datetime.now().isoformat()

 cursor.execute('''
 INSERT INTO checkins (user_id, latitude, longitude, status, timestamp)
 VALUES (?, ?, ?, ?, ?)
 ''', (user_id, latitude, longitude, status, timestamp)) 
 conn.commit()

 maps_url = f"https://www.google.com/maps?q={latitude},{longitude}"
 await callback.message.reply(f"Чек-ин зарегистрирован: {status}\nКарта: {maps_url}")

# Админ-команда: список сотрудников
@dp.message_handler(commands=['list'], user_id=ADMIN_ID)
async def list_employees(message: types.Message):
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

# Админ-команда: статус сотрудника
@dp.message_handler(commands=['status'], user_id=ADMIN_ID)
async def employee_status(message: types.Message):
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
 except:
 await message.reply("Использование: /status <user_id>")

# Админ-команда: экспорт чек-инов
@dp.message_handler(commands=['export'], user_id=ADMIN_ID)
async def export_checkins(message: types.Message):
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

# Админ-команда: карта всех сотрудников
@dp.message_handler(commands=['map'], user_id=ADMIN_ID)
async def show_map(message: types.Message):
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
 # Форматируем даты в ДД МММ
 start_date = datetime.strptime(trip[1], '%Y-%m-%d')
 end_date = datetime.strptime(trip[2], '%Y-%m-%d')
 start_str = f"{start_date.day:02d} {MONTHS[start_date.month]}"
 end_str = f"{end_date.day:02d} {MONTHS[end_date.month]}"
 
 # Время последнего чек-ина
 tz = timezone(trip[3])
 time_ago = format_time_ago(checkin[2], tz)
 
 # Формируем метку
 label = f"{name}{f' @{username}' if username else ''}, {start_str} - {end_str}, последний чек-ин: {time_ago}"
 label = label.replace(" ", "+")
 markers.append(f"{checkin[0]},{checkin[1]},{label}")

 if not markers:
 await message.reply("Нет актуальных геопозиций для активных сотрудников.")
 return

 base_url = "https://www.google.com/maps/dir/"
 map_url = base_url + "/".join(markers)
 await message.reply(f"Карта с позициями сотрудников:\n{map_url}")

# Отправка напоминаний
async def send_reminder(user_id, tz, checkin_time):
 reminder_time = checkin_time - timedelta(minutes=30)
 if datetime.now(tz) >= reminder_time:
 await bot.send_message(user_id, f"Напоминание: отправьте чек-ин в {checkin_time.strftime('%H:%M')}!", reply_markup=keyboard)

# Проверка чек-инов и архивация
async def check_employees():
 while True:
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
 await bot.send_message(ADMIN_ID, f"Сотрудник {name}{f' @{username}' if username else ''} помечен как архивный (командировки завершрованы).")
 continue

 tz = timezone(current_trip[2])
 current_time_tz = datetime.now(tz)
 freq = current_trip[5]
 checkin_time = current_trip[6]

 # Определяем времена чек-инов
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
 # Отправка напоминания
 await send_reminder(user_id, tz, expected_time)
 # Проверка пропущенного чек-ина
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

 await asyncio.sleep(1800) # Проверяем каждые 30 минут

# Запуск проверки
async def on_startup(_):
 asyncio.create_task(check_employees())

if __name__ == '__main__':
 executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
