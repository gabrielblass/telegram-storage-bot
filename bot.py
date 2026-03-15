import telebot
import os
import time
import logging
import psycopg2
import requests
from psycopg2 import pool
from flask import Flask
from threading import Thread, Timer
from queue import Queue
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

# ==============================
# 1. CONFIGURACIÓN
# ==============================
TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHANNEL_ID = int(os.environ.get("CHANNEL_ID", -1003628952931))
DB_URL = os.environ.get("DATABASE_URL")
RENDER_URL = "https://telegram-storage-bot-y9pu.onrender.com"

bot = telebot.TeleBot(TOKEN, threaded=True, num_threads=150)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ==============================
# 2. POOL DE BASE DE DATOS
# ==============================
try:
    db_pool = psycopg2.pool.SimpleConnectionPool(1, 60, DB_URL)
    conn = db_pool.getconn()
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS storage (file_unique_id TEXT PRIMARY KEY)")
    conn.commit()
    db_pool.putconn(conn)
except Exception as e:
    logging.error(f"❌ Error DB: {e}")

# ==============================
# 3. VARIABLES DE CONTROL
# ==============================
file_queue = Queue()
batch_data = {}
timers = {} # Para manejar el informe automático

def get_stats(user_id):
    if user_id not in batch_data:
        batch_data[user_id] = {"ok": 0, "dup": 0, "fail": 0, "last_link": None, "active": False}
    return batch_data[user_id]

# ==============================
# 4. FUNCIÓN DEL INFORME FINAL
# ==============================
def send_final_report(user_id):
    stats = batch_data.get(user_id)
    if not stats or (stats["ok"] == 0 and stats["dup"] == 0 and stats["fail"] == 0):
        return

    text = (f"🏁 *INFORME FINAL DE CARGA*\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"✅ *Nuevos guardados:* `{stats['ok']}`\n"
            f"⚠️ *Duplicados eliminados:* `{stats['dup']}`\n"
            f"❌ *Errores detectados:* `{stats['fail']}`\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"✨ _El chat ha sido limpiado por completo._")
    
    markup = InlineKeyboardMarkup()
    if stats["last_link"]:
        markup.add(InlineKeyboardButton("📂 Ir al último video", url=stats["last_link"]))
    
    bot.send_message(user_id, text, reply_markup=markup, parse_mode="Markdown")
    
    # Reiniciar estadísticas para el siguiente lote después del informe
    batch_data[user_id] = {"ok": 0, "dup": 0, "fail": 0, "last_link": None, "active": False}

# ==============================
# 5. TRABAJADOR Y PROCESAMIENTO
# ==============================
def worker():
    while True:
        message = file_queue.get()
        try:
            process_message(message)
        except Exception as e:
            logging.error(f"Error: {e}")
        file_queue.task_done()

Thread(target=worker, daemon=True).start()

def process_message(message):
    user_id = message.chat.id
    stats = get_stats(user_id)
    stats["active"] = True

    # Reiniciar el temporizador cada vez que llega un mensaje
    if user_id in timers:
        timers[user_id].cancel()
    
    # El bot esperará 20 segundos de silencio para enviar el informe
    timers[user_id] = Timer(20.0, send_final_report, [user_id])
    timers[user_id].start()

    media = None
    if message.content_type == 'photo': media = message.photo[-1]
    elif message.content_type == 'video': media = message.video
    elif message.content_type == 'document': media = message.document
    else: media = getattr(message, message.content_type)

    if not media: return

    # Verificar Duplicado
    conn = None
    try:
        conn = db_pool.getconn()
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM storage WHERE file_unique_id = %s", (media.file_unique_id,))
        exists = cur.fetchone()
        
        if exists:
            stats["dup"] += 1
            bot.delete_message(user_id, message.message_id)
        else:
            # Reenvío Directo
            sent = bot.forward_message(CHANNEL_ID, user_id, message.message_id)
            cur.execute("INSERT INTO storage (file_unique_id) VALUES (%s)", (media.file_unique_id,))
            conn.commit()
            
            stats["ok"] += 1
            channel_id_str = str(CHANNEL_ID).replace("-100", "")
            stats["last_link"] = f"https://t.me/c/{channel_id_str}/{sent.message_id}"
            
            bot.delete_message(user_id, message.message_id)
            
    except Exception as e:
        logging.error(f"Fallo: {e}")
        stats["fail"] += 1
    finally:
        if conn: db_pool.putconn(conn)

# ==============================
# 6. COMANDOS Y WEB
# ==============================
@bot.message_handler(commands=['reset'])
def reset_all(message):
    conn = db_pool.getconn()
    cur = conn.cursor()
    cur.execute("DELETE FROM storage")
    conn.commit()
    db_pool.putconn(conn)
    bot.reply_to(message, "♻️ Base de datos vaciada. Listo para nueva carga.")

@bot.message_handler(content_types=['photo', 'video', 'document', 'audio', 'voice', 'video_note'])
def handle_docs(message):
    file_queue.put(message)

app = Flask(__name__)
@app.route('/')
def home(): return "Bot con Informe Final Activo 🚀"

if __name__ == "__main__":
    Thread(target=lambda: app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080))), daemon=True).start()
    bot.infinity_polling(timeout=90)
