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
except Exception as e:
    logging.error(f"❌ Error DB: {e}")

# ==============================
# 3. GESTIÓN DE COLA Y ESTADÍSTICAS
# ==============================
file_queue = Queue()
batch_data = {}

def get_stats(user_id):
    if user_id not in batch_data:
        batch_data[user_id] = {"ok": 0, "dup": 0, "fail": 0, "last_link": None}
    return batch_data[user_id]

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
        if conn and not conn.closed: db_pool.putconn(conn)

# ==============================
# 4. TRABAJADOR (WORKER)
# ==============================
def worker():
    while True:
        message = file_queue.get()
        try:
            process_message(message)
        except Exception as e:
            logging.error(f"Error procesando: {e}")
        file_queue.task_done()

Thread(target=worker, daemon=True).start()

def process_message(message):
    user_id = message.chat.id
    stats = get_stats(user_id)
    
    media = None
    if message.content_type == 'photo': media = message.photo[-1]
    elif message.content_type == 'video': media = message.video
    elif message.content_type == 'document': media = message.document
    else: media = getattr(message, message.content_type)

    if not media: return

    # 1. Verificar Duplicado
    result = check_and_save(media.file_unique_id)
    
    if result == "duplicado":
        stats["dup"] += 1
        try:
            bot.delete_message(user_id, message.message_id) # Borrar duplicado al instante
        except: pass
        return

    # 2. Reenvío de Archivo Nuevo
    try:
        sent = bot.forward_message(CHANNEL_ID, user_id, message.message_id)
        
        # Link para el resumen
        channel_id_str = str(CHANNEL_ID).replace("-100", "")
        stats["last_link"] = f"https://t.me/c/{channel_id_str}/{sent.message_id}"
        stats["ok"] += 1
        
        # Borrar el original para mantener limpio
        bot.delete_message(user_id, message.message_id)

    except Exception as e:
        logging.error(f"Fallo reenvío: {e}")
        stats["fail"] += 1

# ==============================
# 5. COMANDOS
# ==============================
@bot.message_handler(commands=['resumen'])
def send_resumen(message):
    stats = get_stats(message.chat.id)
    text = (f"📊 *Estado de la Carga*\n\n"
            f"✅ *Nuevos:* `{stats['ok']}`\n"
            f"⚠️ *Ya existían (Borrados):* `{stats['dup']}`\n"
            f"❌ *Errores:* `{stats['fail']}`\n\n"
            f"_Los duplicados fueron eliminados automáticamente para limpiar el chat._")
    
    markup = InlineKeyboardMarkup()
    if stats["last_link"]:
        markup.add(InlineKeyboardButton("📂 Ver último en canal", url=stats["last_link"]))
    
    bot.send_message(message.chat.id, text, reply_markup=markup, parse_mode="Markdown")

@bot.message_handler(commands=['reset'])
def reset_all(message):
    conn = None
    try:
        conn = db_pool.getconn()
        cur = conn.cursor()
        cur.execute("DELETE FROM storage")
        conn.commit()
        batch_data[message.chat.id] = {"ok": 0, "dup": 0, "fail": 0, "last_link": None}
        bot.reply_to(message, "♻️ Memoria limpiada. Todo listo para reenviar desde cero.")
    finally:
        if conn: db_pool.putconn(conn)

@bot.message_handler(content_types=['photo', 'video', 'document', 'audio', 'voice', 'video_note'])
def handle_docs(message):
    file_queue.put(message)

# ==============================
# 6. MANTENIMIENTO
# ==============================
app = Flask(__name__)
@app.route('/')
def home(): return "Bot Operativo 🚀"

def wake_up():
    while True:
        try: requests.get(RENDER_URL)
        except: pass
        time.sleep(600)

if __name__ == "__main__":
    Thread(target=lambda: app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080))), daemon=True).start()
    Thread(target=wake_up, daemon=True).start()
    bot.infinity_polling(timeout=90)
