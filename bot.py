import telebot
import os
import time
import logging
import psycopg2
import requests
from psycopg2 import pool
from flask import Flask
from threading import Thread, Lock
from queue import Queue

# ==============================
# CONFIG
# ==============================
TOKEN = os.getenv("TELEGRAM_TOKEN")
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "-1003628952931"))
DB_URL = os.getenv("DATABASE_URL")
MY_URL = os.getenv("RENDER_EXTERNAL_URL")

bot = telebot.TeleBot(TOKEN, threaded=True, num_threads=10)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ==============================
# DB POOL
# ==============================
db_pool = psycopg2.pool.SimpleConnectionPool(1, 20, DB_URL)
db_lock = Lock()

def save_file(file_id):
    if not file_id:
        return "fail"

    conn = None
    try:
        conn = db_pool.getconn()
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO storage (file_unique_id) VALUES (%s) ON CONFLICT DO NOTHING",
                (file_id,)
            )
            conn.commit()
            return "ok"
    except:
        if conn:
            conn.rollback()
        return "fail"
    finally:
        if conn:
            db_pool.putconn(conn)

# ==============================
# COLA (LO IMPORTANTE 🔥)
# ==============================
task_queue = Queue(maxsize=1000)

batch_data = {}
lock = Lock()

def worker():
    while True:
        message = task_queue.get()

        try:
            cid = message.chat.id

            media = (
                message.photo[-1] if message.content_type == 'photo'
                else getattr(message, message.content_type, None)
            )

            if not media:
                continue

            # 1️⃣ enviar al canal
            sent = bot.copy_message(
                CHANNEL_ID,
                cid,
                message.message_id
            )

            # 2️⃣ reenviar desde canal → usuario (sale "reenviado de")
            bot.forward_message(
                cid,
                CHANNEL_ID,
                sent.message_id
            )

            # 3️⃣ guardar en DB
            status = save_file(getattr(media, "file_unique_id", None))

            with lock:
                batch_data.setdefault(cid, {"ok":0,"dup":0,"fail":0})
                if status == "ok":
                    batch_data[cid]["ok"] += 1
                else:
                    batch_data[cid]["dup"] += 1

            # 4️⃣ borrar original
            bot.delete_message(cid, message.message_id)

        except Exception as e:
            logging.error(f"Worker error: {e}")
            with lock:
                batch_data.setdefault(cid, {"ok":0,"dup":0,"fail":0})
                batch_data[cid]["fail"] += 1

        finally:
            task_queue.task_done()

# ==============================
# REPORTES
# ==============================
def send_report(chat_id):
    with lock:
        stats = batch_data.get(chat_id)
        if not stats:
            return

        text = (
            "╭───〔 REPORTE 〕───╮\n"
            f"│ Guardados : {stats['ok']}\n"
            f"│ Duplicados: {stats['dup']}\n"
            f"│ Fallidos  : {stats['fail']}\n"
            "╰──────────────────╯"
        )

        try:
            bot.send_message(chat_id, text)
        except:
            pass

        batch_data[chat_id] = {"ok":0,"dup":0,"fail":0}

# ==============================
# HANDLER
# ==============================
@bot.message_handler(content_types=[
    'photo','video','document','audio','voice','video_note'
])
def handle(message):
    try:
        task_queue.put_nowait(message)

        # lanzar reporte en segundo plano
        Thread(target=lambda: (time.sleep(20), send_report(message.chat.id))).start()

    except:
        bot.send_message(message.chat.id, "⚠️ Cola llena, intenta de nuevo")

# ==============================
# KEEP ALIVE
# ==============================
app = Flask(__name__)

@app.route('/')
def home():
    return "OK"

def keep_alive():
    while True:
        time.sleep(180)
        if MY_URL:
            try:
                requests.get(MY_URL, timeout=10)
            except:
                pass

# ==============================
# INICIO
# ==============================
if __name__ == "__main__":

    # workers (clave 🔥)
    for _ in range(5):
        Thread(target=worker, daemon=True).start()

    Thread(target=lambda: app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080))), daemon=True).start()
    Thread(target=keep_alive, daemon=True).start()

    bot.infinity_polling(timeout=60, long_polling_timeout=25)
