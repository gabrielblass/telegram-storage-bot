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

logging.basicConfig(level=logging.INFO)

# ==============================
# DB
# ==============================
db_pool = psycopg2.pool.SimpleConnectionPool(1, 10, DB_URL)

def process_db(file_id):
    if not file_id:
        return "err"

    conn = None
    try:
        conn = db_pool.getconn()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO storage (file_unique_id)
                VALUES (%s)
                ON CONFLICT (file_unique_id) DO NOTHING
                RETURNING file_unique_id
            """, (file_id,))

            res = cur.fetchone()
            conn.commit()
            return "ok" if res else "dup"

    except Exception as e:
        if conn:
            conn.rollback()
        logging.error(f"DB error: {e}")
        return "err"

    finally:
        if conn:
            db_pool.putconn(conn)

# ==============================
# SAFE COPY
# ==============================
def safe_copy(chat_id, message_id, caption):
    for _ in range(10):
        try:
            return bot.copy_message(CHANNEL_ID, chat_id, message_id, caption=caption)

        except telebot.apihelper.ApiTelegramException as e:
            if e.error_code == 429:
                wait = e.result_json.get("parameters", {}).get("retry_after", 5)
                time.sleep(wait + 2)
            else:
                time.sleep(2)

        except Exception:
            time.sleep(2)

    return None

# ==============================
# COLA Y CONTROL
# ==============================
queue = Queue(maxsize=1000)

batch_data = {}
last_activity = {}
processing_count = 0
lock = Lock()

INACTIVITY = 8

# ==============================
# WORKER
# ==============================
def worker():
    global processing_count

    while True:
        message = queue.get()

        with lock:
            processing_count += 1

        try:
            cid = message.chat.id

            media = (
                message.photo[-1] if message.content_type == 'photo'
                else getattr(message, message.content_type, None)
            )

            if not media:
                continue

            res = safe_copy(cid, message.message_id, message.caption)

            if not res:
                raise Exception("Copy falló")

            status = process_db(getattr(media, "file_unique_id", None))

            with lock:
                batch_data[cid][status if status in ["ok","dup"] else "fail"] += 1

            try:
                bot.delete_message(cid, message.message_id)
            except:
                pass

            time.sleep(0.1)

        except Exception as e:
            logging.error(f"Worker error: {e}")
            with lock:
                batch_data[cid]["fail"] += 1

        finally:
            with lock:
                processing_count -= 1
            queue.task_done()

# ==============================
# MONITOR (CORREGIDO 🔥)
# ==============================
def monitor():
    while True:
        time.sleep(2)
        now = time.time()

        with lock:
            for cid in list(last_activity.keys()):
                inactive = now - last_activity[cid] > INACTIVITY

                if inactive and queue.empty() and processing_count == 0:
                    send_report(cid)
                    del last_activity[cid]

# ==============================
# REPORTE BONITO
# ==============================
def send_report(cid):
    stats = batch_data.get(cid)
    if not stats:
        return

    total = sum(stats.values())

    text = (
        "╭━━━ 📊 *RESULTADO FINAL* ━━━╮\n"
        f"┃ 📁 Total: *{total}*\n"
        "┃━━━━━━━━━━━━━━━━━━━━━━\n"
        f"┃ ✅ Guardados : *{stats['ok']}*\n"
        f"┃ ♻️ Duplicados: *{stats['dup']}*\n"
        f"┃ ❌ Fallidos  : *{stats['fail']}*\n"
        "╰━━━━━━━━━━━━━━━━━━━━━━╯"
    )

    try:
        bot.send_message(cid, text, parse_mode="Markdown")
    except:
        pass

    batch_data[cid] = {"ok": 0, "dup": 0, "fail": 0}

# ==============================
# HANDLER
# ==============================
@bot.message_handler(content_types=[
    'photo','video','document','audio','voice','video_note'
])
def handle(message):
    cid = message.chat.id

    with lock:
        batch_data.setdefault(cid, {"ok": 0, "dup": 0, "fail": 0})
        last_activity[cid] = time.time()

    try:
        queue.put(message, timeout=5)
    except:
        bot.send_message(cid, "⚠️ Cola llena, espera...")

# ==============================
# SERVER
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
                requests.get(MY_URL)
            except:
                pass

# ==============================
# RUN
# ==============================
if __name__ == "__main__":
    for _ in range(5):
        Thread(target=worker, daemon=True).start()

    Thread(target=monitor, daemon=True).start()
    Thread(target=keep_alive, daemon=True).start()
    Thread(target=lambda: app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080))), daemon=True).start()

    bot.infinity_polling()
