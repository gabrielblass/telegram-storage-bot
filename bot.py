import telebot
import psycopg2
import os
from flask import Flask
from threading import Thread

# 1. CREDENCIALES DESDE VARIABLES DE ENTORNO (Seguridad)
TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHANNEL_ID = int(os.environ.get("CHANNEL_ID", -1003628952931))
DB_URL = os.environ.get("DATABASE_URL")

bot = telebot.TeleBot(TOKEN)

# 2. CONFIGURACIÓN DE POSTGRESQL (Base de datos persistente)
def get_db_connection():
    return psycopg2.connect(DB_URL)

def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS files(
        file_unique_id TEXT PRIMARY KEY,
        file_id TEXT,
        type TEXT,
        name TEXT,
        size BIGINT,
        message_id INTEGER
    )
    """)
    conn.commit()
    cursor.close()
    conn.close()

init_db()

# VERIFICAR DUPLICADOS (Nota: en Postgres se usa %s en lugar de ?)
def exists(file_unique_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT file_unique_id FROM files WHERE file_unique_id=%s",
        (file_unique_id,)
    )
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    return result

# GUARDAR EN BASE DE DATOS
def save(data):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO files VALUES (%s,%s,%s,%s,%s,%s)",
        data
    )
    conn.commit()
    cursor.close()
    conn.close()

# IMAGENES
@bot.message_handler(content_types=['photo'])
def photo(message):
    file = message.photo[-1]
    if exists(file.file_unique_id):
        bot.reply_to(message, "⚠️ Imagen duplicada")
        return

    msg = bot.forward_message(CHANNEL_ID, message.chat.id, message.message_id)
    save((file.file_unique_id, file.file_id, "photo", "photo", file.file_size, msg.message_id))
    bot.reply_to(message, "✅ Imagen guardada")

# VIDEOS
@bot.message_handler(content_types=['video'])
def video(message):
    file = message.video
    if exists(file.file_unique_id):
        bot.reply_to(message, "⚠️ Video duplicado")
        return

    msg = bot.forward_message(CHANNEL_ID, message.chat.id, message.message_id)
    file_name = getattr(file, 'file_name', 'video_sin_nombre.mp4')
    if file_name is None:
        file_name = 'video_sin_nombre.mp4'

    save((file.file_unique_id, file.file_id, "video", file_name, file.file_size, msg.message_id))
    bot.reply_to(message, "✅ Video guardado")

# 3. SERVIDOR WEB FANTASMA PARA RENDER
app = Flask(__name__)

@app.route('/')
def index():
    return "Bot activo y funcionando 🚀"

def run_web():
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)

if __name__ == "__main__":
    # Inicia el servidor web en un hilo secundario
    Thread(target=run_web).start()
    print("BOT ACTIVO - Escuchando mensajes...")
    # Inicia el bot en el hilo principal
    bot.infinity_polling()
