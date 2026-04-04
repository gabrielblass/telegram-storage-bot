import telebot
import os
import time
import logging
import psycopg2
import requests
from psycopg2 import pool
from flask import Flask
from threading import Thread, Lock

# ==============================
# CONFIG
# ==============================
TOKEN = os.getenv("TELEGRAM_TOKEN")
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "-1003628952931"))
DB_URL = os.getenv("DATABASE_URL")
MY_URL = os.getenv("RENDER_EXTERNAL_URL")

bot = telebot.TeleBot(TOKEN, threaded=True, num_threads=40)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ==============================
# DB POOL (AUMENTADO)
# ==============================
db_pool = None
pool_lock = Lock()

def init_pool():
    global db_pool
    with pool_lock:
        if db_pool:
            return True
        try:
            db_pool = psycopg2.pool.SimpleConnectionPool(1, 50, DB_URL)
            logging.info("DB Pool iniciado")
            return True
        except Exception as e:
            logging.error(f"Error creando pool: {e}")
            return False

def get_conn():
    try:
        return db_pool.getconn()
    except:
        logging.warning("Reiniciando pool...")
        init_pool()
        return db_pool.getconn()

init_pool()

# ==============================
# DB (RÁPIDO)
# ==============================
def process_db(file_id):
    if not file_id:
        return "err"

    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO storage (file_unique_id)
                VALUES (%s)
                ON CONFLICT (file_unique_id) DO NOTHING
                RETURNING file_unique_id
            """, (file_id,))

            result = cur.fetchone()
            conn.commit()

            return "ok" if result else "dup"

    except Exception as e:
        if conn:
            conn.rollback()
        logging.error(f"DB error: {e}")
        return "err"

    finally:
        if conn:
            db_pool.putconn(conn)

# ==============================
# SAFE COPY (ANTI FLOOD)
# ==============================
def safe_copy(chat_id, message_id, caption):
    for attempt in range(5):
        try:
            res = bot.copy_message(
                CHANNEL_ID,
                chat_id,
                message_id,
                caption=caption
            )
            time.sleep(0.05)  # anti flood
            return res

        except telebot.apihelper.ApiTelegramException as e:
            if e.error_code == 429:
                wait = e.result_json.get("parameters", {}).get("retry_after", 5)
                time.sleep(wait + 1)
            else:
                time.sleep(2)

    raise Exception("Falló copy")

# ==============================
# SISTEMA AUTOMÁTICO
# ==============================
batch_data = {}
last_activity = {}
lock = Lock()

INACTIVITY_LIMIT = 8

def monitor_activity():
    while True:
        time.sleep(2)
        now = time.time()

        with lock:
            for cid in list(last_activity.keys()):
                if now - last_activity[cid] > INACTIVITY_LIMIT:
                    send_final_report(cid)
                    del last_activity[cid]

def send_final_report(chat_id):
    stats = batch_data.get(chat_id)
    if not stats:
        return

    total = stats['ok'] + stats['dup'] + stats['fail']

    text = (
        "╭━━━ 📊 *RESULTADO DEL PROCESO* ━━━╮\n"
        f"┃ 📁 Total procesados: *{total}*\n"
        "┃━━━━━━━━━━━━━━━━━━━━━━\n"
        f"┃ ✅ Guardados : *{stats['ok']}*\n"
        f"┃ ♻️ Duplicados: *{stats['dup']}*\n"
        f"┃ ❌ Fallidos  : *{stats['fail']}*\n"
        "╰━━━━━━━━━━━━━━━━━━━━━━╯"
    )

    try:
        bot.send_message(chat_id, text, parse_mode="Markdown")
    except:
        pass

    batch_data[chat_id] = {"ok": 0, "dup": 0, "fail": 0}

# ==============================
# PROCESAMIENTO EN PARALELO 🚀
# ==============================
def process_message(message):
    cid = message.chat.id

    media = (
        message.photo[-1] if message.content_type == 'photo'
        else getattr(message, message.content_type, None)
    )

    if not media:
        return

    try:
        safe_copy(cid, message.message_id, message.caption)

        status = process_db(getattr(media, "file_unique_id", None))

        with lock:
            batch_data[cid][status if status in ["ok","dup"] else "fail"] += 1

        if status in ["ok", "dup"]:
            bot.delete_message(cid, message.message_id)

    except Exception as e:
        logging.error(f"Error: {e}")
        with lock:
            batch_data[cid]["fail"] += 1

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

    Thread(target=process_message, args=(message,)).start()

# ==============================
# SERVER
# ==============================
app = Flask(__name__)

@app.route('/')
def home():
    return "BOT ACTIVO"

def keep_alive():
    while True:
        time.sleep(180)
        if MY_URL:
            try:
                requests.get(MY_URL, timeout=10)
            except:
                pass

# ==============================
# RUN
# ==============================
if __name__ == "__main__":
    Thread(target=lambda: app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080))), daemon=True).start()
    Thread(target=keep_alive, daemon=True).start()
    Thread(target=monitor_activity, daemon=True).start()

    bot.infinity_polling(timeout=60, long_polling_timeout=25)
