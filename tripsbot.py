from datetime import datetime
import locale

# Локализация месяцев для русского формата
MONTHS = {
    1: 'Янв', 2: 'Фев', 3: 'Мар', 4: 'Апр', 5: 'Май', 6: 'Июн',
    7: 'Июл', 8: 'Авг', 9: 'Сен', 10: 'Окт', 11: 'Ноя', 12: 'Дек'
}

# Форматирование времени "X часов назад"
def format_time_ago(timestamp, tz):
    last_checkin = datetime.fromisoformat(timestamp).astimezone(tz)
    now = datetime.now(tz)
    hours_ago = int((now - last_checkin).total_seconds() // 3600)
    return "менее часа назад" if hours_ago == 0 else f"{hours_ago} часов назад"

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
                label = label.replace(" ", "+")  # Для URL
                markers.append(f"{checkin[0]},{checkin[1]},{label}")

    if not markers:
        await message.reply("Нет актуальных геопозиций для активных сотрудников.")
        return

    # Формируем URL для Google Maps
    base_url = "https://www.google.com/maps/dir/"
    map_url = base_url + "/".join(markers)
    await message.reply(f"Карта с позициями сотрудников:\n{map_url}")
