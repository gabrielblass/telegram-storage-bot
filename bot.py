import telebot
import sqlite3

# CONFIGURACION
TOKEN = "8635566110:AAEi0fY9H9S_0CGbxLxUajvNmteT10awzEc"
CHANNEL_ID = -1003628952931

bot = telebot.TeleBot(TOKEN)

# BASE DE DATOS
conn = sqlite3.connect("database.db", check_same_thread=False)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS files(
    file_unique_id TEXT PRIMARY KEY,
    file_id TEXT,
    type TEXT,
    name TEXT,
    size INTEGER,
    message_id INTEGER
)
""")
conn.commit()


# VERIFICAR DUPLICADOS
def exists(file_unique_id):
    cursor.execute(
        "SELECT file_unique_id FROM files WHERE file_unique_id=?",
        (file_unique_id,)
    )
    return cursor.fetchone()


# GUARDAR EN BASE DE DATOS
def save(data):
    cursor.execute(
        "INSERT INTO files VALUES (?,?,?,?,?,?)",
        data
    )
    conn.commit()


# IMAGENES
@bot.message_handler(content_types=['photo'])
def photo(message):
    file = message.photo[-1]

    if exists(file.file_unique_id):
        bot.reply_to(message, "⚠️ Imagen duplicada")
        return

    msg = bot.forward_message(
        CHANNEL_ID,
        message.chat.id,
        message.message_id
    )

    save((
        file.file_unique_id,
        file.file_id,
        "photo",
        "photo",
        file.file_size,
        msg.message_id
    ))

    bot.reply_to(message, "✅ Imagen guardada")


# VIDEOS
@bot.message_handler(content_types=['video'])
def video(message):
    file = message.video

    if exists(file.file_unique_id):
        bot.reply_to(message, "⚠️ Video duplicado")
        return

    msg = bot.forward_message(
        CHANNEL_ID,
        message.chat.id,
        message.message_id
    )

    # Manejo seguro del nombre del archivo por si Telegram no lo proporciona
    file_name = getattr(file, 'file_name', 'video_sin_nombre.mp4')
    if file_name is None:
        file_name = 'video_sin_nombre.mp4'

    save((
        file.file_unique_id,
        file.file_id,
        "video",
        file_name,
        file.file_size,
        msg.message_id
    ))

    bot.reply_to(message, "✅ Video guardado")


print("BOT ACTIVO - Escuchando mensajes...")
bot.infinity_polling()
