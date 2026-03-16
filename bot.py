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

# ==============================
# 1. CONFIGURACIÓN
# ==============================
TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHANNEL_ID = int(os.environ.get("CHANNEL_ID", -1003628952931))
DB_URL = os.environ.get("DATABASE_URL")
RENDER_URL = "https://telegram-storage-bot-y9pu.onrender.com"

# Reducimos hilos para evitar bloqueos de Telegram (Calidad sobre velocidad)
bot = telebot.TeleBot(TOKEN, threaded=True, num_threads=20) 
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ==============================
# 2. POOL DE BASE DE DATOS
# ==============================
try:
    db_pool = psycopg2.pool.SimpleConnectionPool(1, 20, DB_URL)
except Exception as e:
    logging.error(f"❌ Error DB: {e}")

# ==============================
# 3. CONTROL DE DATOS
# ==============================
file_queue = Queue()
batch_data = {}
timers = {}

def get_stats(user_id):
    if user_id not in batch_data:
        batch_data[user_id] = {"ok": 0, "dup": 0, "fail": 0}
    return batch_data[user_id]

def send_final_report(user_id):
    stats = batch_data.get(user_id)
    if not stats or (stats["ok"] == 0 and stats["dup"] == 0 and stats["fail"] == 0): return
    
    text = (f"🏁 *INFORME DE CARGA GARANTIZADA*\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"✅ *Guardados con éxito:* `{stats['ok']}`\n"
            f"⚠️ *Duplicados omitidos:* `{stats['dup']}`\n"
            f"❌ *Errores definitivos:* `{stats['fail']}`\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"👤 _Autoría eliminada y chat limpio._")
    
    bot.send_message(user_id, text, parse_mode="Markdown")
    batch_data[user_id] = {"ok": 0, "dup": 0, "fail": 0}

# ==============================
# 4. PROCESADOR CON REINTENTOS (MÁS SEGURO)
# ==============================
def worker():
    while True:
        message = file_queue.get()
        try:
            process_safe(message)
        except Exception as e:
            logging.error(f"Error crítico en worker: {e}")
        file_queue.task_done()

Thread(target=worker, daemon=True).start()

def process_safe(message):
    user_id = message.chat.id
    stats = get_stats(user_id)

    # Temporizador para el informe (30 seg de silencio para estar seguros)
    if user_id in timers: timers[user_id].cancel()
    timers[user_id] = Timer(30.0, send_final_report, [user_id])
    timers[user_id].start()

    media = None
    if message.content_type == 'photo': media = message.photo[-1]
    elif message.content_type == 'video': media = message.video
    elif message.content_type == 'document': media = message.document
    else: media = getattr(message, message.content_type)

    if not media: return

    conn = None
    try:
        conn = db_pool.getconn()
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM storage WHERE file_unique_id = %s", (media.file_unique_id,))
        
        if cur.fetchone():
            stats["dup"] += 1
            bot.delete_message(user_id, message.message_id)
        else:
            # BUCLE DE REINTENTO EN CASO DE ERROR
            success = False
            attempts = 0
            while not success and attempts < 3:
                try:
                    # Usamos copy_message para que llegue limpio (sin autor)
                    bot.copy_message(CHANNEL_ID, user_id, message.message_id)
                    
                    cur.execute("INSERT INTO storage (file_unique_id) VALUES (%s)", (media.file_unique_id,))
                    conn.commit()
                    
                    stats["ok"] += 1
                    bot.delete_message(user_id, message.message_id)
                    success = True
                except telebot.apihelper.ApiTelegramException as e:
                    if e.error_code == 429: # Flood control (demasiado rápido)
                        wait_time = e.result_json['parameters']['retry_after']
                        logging.warning(f"Esperando {wait_time}s por límite de Telegram...")
                        time.sleep(wait_time + 1)
                        attempts += 1
                    else:
                        raise e # Otro error (ej. archivo muy grande)

    except Exception as e:
        logging.error(f"Fallo final en archivo: {e}")
        stats["fail"] += 1
    finally:
        if conn: db_pool.putconn(conn)

# ==============================
# 5. INICIO
# ==============================
@bot.message_handler(content_types=['photo', 'video', 'document', 'audio', 'voice', 'video_note'])
def handle_all(message):
    file_queue.put(message)

app = Flask(__name__)
@app.route('/')
def home(): return "Bot Safe-Storage Activo 🛡️"

if __name__ == "__main__":
    Thread(target=lambda: app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080))), daemon=True).start()
    bot.infinity_polling(timeout=90)
