import telebot
import os
import time
import logging
import psycopg2
import requests
from psycopg2 import pool
from flask import Flask
from threading import Thread
from queue import Queue
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

# ==============================
# 1. CONFIGURACIÓN
# ==============================
TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHANNEL_ID = int(os.environ.get("CHANNEL_ID", -1003628952931))
DB_URL = os.environ.get("DATABASE_URL")
RENDER_URL = "https://telegram-storage-bot-y9pu.onrender.com"

# Bot con múltiples hilos para recibir mensajes sin pausa
bot = telebot.TeleBot(TOKEN, threaded=True, num_threads=120)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ==============================
# 2. POOL DE BASE DE DATOS
# ==============================
try:
    db_pool = psycopg2.pool.SimpleConnectionPool(1, 50, DB_URL)
    conn = db_pool.getconn()
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS storage (file_unique_id TEXT PRIMARY KEY)")
    conn.commit()
    db_pool.putconn(conn)
    logging.info("✅ Base de datos conectada y lista")
except Exception as e:
    logging.error(f"❌ Error crítico en DB: {e}")

# ==============================
# 3. GESTIÓN DE COLA Y RESUMEN
# ==============================
file_queue = Queue()
batch_data = {}

def check_and_save(file_unique_id):
    conn = None
    try:
        conn = db_pool.getconn()
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM storage WHERE file_unique_id = %s", (file_unique_id,))
        if cur.fetchone():
            return "duplicado"
        cur.execute("INSERT INTO storage (file_unique_id) VALUES (%s)", (file_unique_id,))
        conn.commit()
        return "nuevo"
    except: return "error"
    finally:
        if conn: db_pool.putconn(conn)

# ==============================
# 4. TRABAJADOR ASÍNCRONO
# ==============================
def worker():
    while True:
        message = file_queue.get()
        try:
            process_message(message)
        except Exception as e:
            logging.error(f"Error en worker: {e}")
        file_queue.task_done()

Thread(target=worker, daemon=True).start()

def process_message(message):
    # Obtener el objeto de media
    media = None
    if message.content_type == 'photo': media = message.photo[-1]
    elif message.content_type == 'video': media = message.video
    elif message.content_type == 'document': media = message.document
    else: media = getattr(message, message.content_type)

    if not media: return

    user_id = message.chat.id
    if user_id not in batch_data:
        batch_data[user_id] = {"ok": 0, "dup": 0, "fail": 0, "last_link": None}

    # Verificar duplicado
    result = check_and_save(media.file_unique_id)
    if result == "duplicado":
        batch_data[user_id]["dup"] += 1
        return

    try:
        # Reenvío directo (Soporta hasta 2GB)
        sent = bot.forward_message(
            chat_id=CHANNEL_ID,
            from_chat_id=message.chat.id,
            message_id=message.message_id
        )
        
        # Generar link del canal
        msg_id = sent.message_id
        channel_internal = str(CHANNEL_ID).replace("-100", "")
        batch_data[user_id]["last_link"] = f"https://t.me/c/{channel_internal}/{msg_id}"
        batch_data[user_id]["ok"] += 1

        # Borrar el mensaje original para limpiar el chat
        bot.delete_message(message.chat.id, message.message_id)

    except Exception as e:
        logging.error(f"Error reenviando: {e}")
        batch_data[user_id]["fail"] += 1

# ==============================
# 5. MANEJADORES DE COMANDOS Y MEDIA
# ==============================
@bot.message_handler(commands=['resumen'])
def send_resumen(message):
    user_id = message.chat.id
    if user_id not in batch_data:
        bot.reply_to(message, "No hay actividad reciente.")
        return

    data = batch_data[user_id]
    text = f"📦 *Resultado del envío*\n\n✅ Guardados: `{data['ok']}`\n⚠️ Duplicados: `{data['dup']}`\n❌ Errores: `{data['fail']}`"
    
    markup = InlineKeyboardMarkup()
    if data["last_link"]:
        markup.add(InlineKeyboardButton("📂 Ver último archivo", url=data["last_link"]))
    
    bot.send_message(user_id, text, reply_markup=markup, parse_mode="Markdown")

@bot.message_handler(commands=['reset'])
def reset_db(message):
    conn = None
    try:
        conn = db_pool.getconn()
        cur = conn.cursor()
        cur.execute("DELETE FROM storage")
        conn.commit()
        db_pool.putconn(conn)
        batch_data[message.chat.id] = {"ok": 0, "dup": 0, "fail": 0, "last_link": None}
        bot.reply_to(message, "♻️ Memoria reseteada. Puedes enviar todo de nuevo.")
    except:
        bot.reply_to(message, "❌ Error al resetear.")

@bot.message_handler(content_types=['photo', 'video', 'document', 'audio', 'voice', 'video_note'])
def handle_incoming(message):
    file_queue.put(message)

# ==============================
# 6. SERVIDOR Y MANTENIMIENTO
# ==============================
app = Flask(__name__)
@app.route('/')
def home(): return "Bot High-Performance Activo 🚀"

def run_web():
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)

def wake_up():
    while True:
        try: requests.get(RENDER_URL)
        except: pass
        time.sleep(600)

if __name__ == "__main__":
    Thread(target=run_web, daemon=True).start()
    Thread(target=wake_up, daemon=True).start()
    logging.info("🚀 Iniciando Infinity Polling")
    bot.infinity_polling(timeout=90, long_polling_timeout=30)
