import telebot
import os
import time
import logging
import psycopg2
from psycopg2 import pool
from flask import Flask
from threading import Thread, Timer, Lock
from queue import Queue

# ==============================
# 1. CONFIGURACIÓN
# ==============================
TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHANNEL_ID = int(os.environ.get("CHANNEL_ID", -1003628952931))
DB_URL = os.environ.get("DATABASE_URL")

bot = telebot.TeleBot(TOKEN, threaded=True, num_threads=10)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ==============================
# 2. POOL DE BASE DE DATOS
# ==============================
db_pool = None
try:
    db_pool = psycopg2.pool.SimpleConnectionPool(1, 10, DB_URL)
    logging.info("✅ Pool de DB inicializado")
except Exception as e:
    logging.error(f"❌ Error DB: {e}")

# ==============================
# 3. CONTROL DE DATOS
# ==============================
file_queue = Queue()
batch_data = {}
timers = {}
data_lock = Lock()

def get_stats(user_id):
    with data_lock:
        if user_id not in batch_data:
            batch_data[user_id] = {"ok": 0, "dup": 0, "fail": 0}
        return batch_data[user_id]

def send_final_report(user_id):
    with data_lock:
        stats = batch_data.get(user_id)

        if not stats or sum(stats.values()) == 0:
            return

        text = (
            f"🏁 *INFORME DE CARGA*\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"✅ *Guardados:* `{stats['ok']}`\n"
            f"⚠️ *Duplicados:* `{stats['dup']}`\n"
            f"❌ *Errores:* `{stats['fail']}`"
        )

        try:
            bot.send_message(user_id, text, parse_mode="Markdown")
        except Exception as e:
            logging.error(f"Error enviando reporte: {e}")

        batch_data[user_id] = {"ok": 0, "dup": 0, "fail": 0}
        timers.pop(user_id, None)

# ==============================
# 4. WORKERS (MEJOR ESCALADO)
# ==============================
def worker():
    while True:
        message = file_queue.get()
        try:
            process_safe(message)
        except Exception as e:
            logging.error(f"Error crítico en worker: {e}")
        finally:
            file_queue.task_done()

# Lanzamos varios workers (balanceado)
for _ in range(3):
    Thread(target=worker, daemon=True).start()

# ==============================
# 5. PROCESAMIENTO
# ==============================
def process_safe(message):
    user_id = message.chat.id
    stats = get_stats(user_id)

    # Reset timer
    if user_id in timers:
        timers[user_id].cancel()

    timers[user_id] = Timer(20.0, send_final_report, [user_id])
    timers[user_id].start()

    # Detectar media
    media = None
    if message.content_type == 'photo':
        media = message.photo[-1]
    else:
        media = getattr(message, message.content_type, None)

    if not media or not hasattr(media, "file_unique_id"):
        return

    conn = None

    try:
        conn = db_pool.getconn()
        cur = conn.cursor()

        cur.execute(
            "SELECT 1 FROM storage WHERE file_unique_id = %s",
            (media.file_unique_id,)
        )

        if cur.fetchone():
            stats["dup"] += 1
            safe_delete(user_id, message.message_id)
            return

        attempts = 0
        while attempts < 3:
            try:
                bot.copy_message(CHANNEL_ID, user_id, message.message_id)

                cur.execute(
                    "INSERT INTO storage (file_unique_id) VALUES (%s)",
                    (media.file_unique_id,)
                )
                conn.commit()

                stats["ok"] += 1
                safe_delete(user_id, message.message_id)
                return

            except telebot.apihelper.ApiTelegramException as e:
                if e.error_code == 429:
                    wait = e.result_json.get("parameters", {}).get("retry_after", 5)
                    logging.warning(f"⏳ Flood control: esperando {wait}s")
                    time.sleep(wait + 1)
                    attempts += 1
                else:
                    raise e

    except Exception as e:
        logging.error(f"❌ Error procesando archivo: {e}")
        stats["fail"] += 1

    finally:
        if conn:
            db_pool.putconn(conn)

# ==============================
# 6. UTILIDADES
# ==============================
def safe_delete(chat_id, msg_id):
    try:
        bot.delete_message(chat_id, msg_id)
    except:
        pass

# ==============================
# 7. HANDLER
# ==============================
@bot.message_handler(content_types=[
    'photo', 'video', 'document', 'audio',
    'voice', 'video_note', 'animation'
])
def handle_all(message):
    file_queue.put(message)

# ==============================
# 8. FLASK KEEP ALIVE
# ==============================
app = Flask(__name__)

@app.route('/')
def home():
    return "Bot activo 🛡️"

# ==============================
# 9. MAIN
# ==============================
if __name__ == "__main__":
    Thread(
        target=lambda: app.run(
            host="0.0.0.0",
            port=int(os.environ.get("PORT", 8080))
        ),
        daemon=True
    ).start()

    bot.infinity_polling(timeout=60, long_polling_timeout=60)
