import telebot
import os
import time
import logging
import psycopg2
import requests
from psycopg2 import pool
from flask import Flask
from threading import Thread, Timer, Lock

# ==============================
# CONFIG
# ==============================
TOKEN = os.getenv("TELEGRAM_TOKEN")
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "-1003628952931"))
DB_URL = os.getenv("DATABASE_URL")
MY_URL = os.getenv("RENDER_EXTERNAL_URL")

bot = telebot.TeleBot(TOKEN, threaded=True, num_threads=20)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ==============================
# DB POOL
# ==============================
db_pool = None
pool_lock = Lock()

def init_pool():
    global db_pool
    with pool_lock:
        if db_pool:
            return True
        try:
            db_pool = psycopg2.pool.SimpleConnectionPool(1, 20, DB_URL)
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
# DB (OPTIMIZADO 🚀)
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

            if result:
                return "ok"
            else:
                return "dup"

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
            return bot.copy_message(
                CHANNEL_ID,
                chat_id,
                message_id,
                caption=caption
            )

        except telebot.apihelper.ApiTelegramException as e:
            if e.error_code == 429:
                wait = e.result_json.get("parameters", {}).get("retry_after", 5)
                logging.warning(f"Flood → esperando {wait}s")
                time.sleep(wait + 1)
            else:
                logging.warning(f"Error intento {attempt+1}: {e}")
                time.sleep(2)

    raise Exception("Falló copy tras 5 intentos")

# ==============================
# STATS
# ==============================
batch_data = {}
timers = {}
lock = Lock()

def send_final_report(chat_id):
    with lock:
        stats = batch_data.get(chat_id)
        if not stats:
            return

        text = (
            f"🏁 *REPORTE FINAL*\n"
            f"━━━━━━━━━━━━━━\n"
            f"✅ {stats['ok']}\n"
            f"⚠️ {stats['dup']}\n"
            f"❌ {stats['fail']}"
        )

        try:
            bot.send_message(chat_id, text, parse_mode="Markdown")
        except:
            pass

        batch_data[chat_id] = {"ok": 0, "dup": 0, "fail": 0}

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

    if cid in timers:
        timers[cid].cancel()

    timers[cid] = Timer(30, send_final_report, [cid])
    timers[cid].start()

    media = (
        message.photo[-1] if message.content_type == 'photo'
        else getattr(message, message.content_type, None)
    )

    if not media:
        return

    try:
        # 1 copiar (rápido)
        safe_copy(cid, message.message_id, message.caption)

        # 2 verificar DB (optimizado)
        status = process_db(getattr(media, "file_unique_id", None))

        with lock:
            batch_data[cid][status if status in ["ok","dup"] else "fail"] += 1

        # 3 borrar si ya se procesó
        if status in ["ok", "dup"]:
            bot.delete_message(cid, message.message_id)

    except Exception as e:
        logging.error(f"Error total: {e}")
        with lock:
            batch_data[cid]["fail"] += 1

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
    Thread(
        target=lambda: app.run(
            host="0.0.0.0",
            port=int(os.getenv("PORT", 8080))
        ),
        daemon=True
    ).start()

    Thread(target=keep_alive, daemon=True).start()

    bot.infinity_polling(timeout=60, long_polling_timeout=25)
