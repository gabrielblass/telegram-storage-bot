import telebot
import os
import time
import logging
import psycopg2
import requests
from psycopg2 import pool, errors
from flask import Flask
from threading import Thread, Timer, Lock
from queue import Queue, Empty

# ==============================
# 1. CONFIGURACIÓN
# ==============================
TOKEN = os.getenv("TELEGRAM_TOKEN")
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "-1003628952931"))
DB_URL = os.getenv("DATABASE_URL")
MY_URL = os.getenv("RENDER_EXTERNAL_URL") 
ADMIN_ID = 1243433271 

bot = telebot.TeleBot(TOKEN, threaded=True, num_threads=30)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ==============================
# 2. POOL DB SEGURO
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
            return True
        except Exception as e:
            logging.error(f"Error DB pool: {e}")
            return False

init_pool()

# ==============================
# 3. ALERTAS
# ==============================
def alert_admin(msg):
    try:
        bot.send_message(ADMIN_ID, msg, parse_mode="Markdown")
    except:
        pass

# ==============================
# 4. DB (ANTI DUPLICADOS)
# ==============================
def process_db(file_id):
    conn = None
    try:
        if not db_pool:
            if not init_pool():
                return "err"

        conn = db_pool.getconn()
        cur = conn.cursor()

        try:
            cur.execute("INSERT INTO storage (file_unique_id) VALUES (%s)", (file_id,))
            conn.commit()
            return "ok"
        except errors.UniqueViolation:
            conn.rollback()
            return "dup"
        finally:
            cur.close()

    except Exception as e:
        if "unique" not in str(e).lower():
            alert_admin(f"🚨 DB ERROR\n`{str(e)[:60]}`")
        if conn:
            try: conn.rollback()
            except: pass
        return "err"

    finally:
        if conn and db_pool:
            try: db_pool.putconn(conn)
            except: pass

# ==============================
# 5. COLA Y WORKERS
# ==============================
task_queue = Queue(maxsize=1000)
stats_lock = Lock()

batch_data = {}
timers = {}
failed_tasks = Queue()

def safe_forward(cid, from_chat, msg_id, retries=3):
    for i in range(retries):
        try:
            return bot.forward_message(cid, from_chat, msg_id)
        except Exception as e:
            if "RemoteDisconnected" in str(e) or "Connection aborted" in str(e):
                time.sleep(2 * (i + 1))
                continue
            else:
                raise e
    return None

def worker():
    while True:
        try:
            message = task_queue.get(timeout=5)
        except Empty:
            continue

        cid = message.chat.id

        try:
            media = message.photo[-1] if message.content_type == 'photo' else getattr(message, message.content_type, None)
            if not media:
                continue

            result = safe_forward(CHANNEL_ID, cid, message.message_id)

            if not result:
                failed_tasks.put(message)
                with stats_lock:
                    batch_data[cid]["fail"] += 1
                continue

            status = process_db(media.file_unique_id)

            with stats_lock:
                if status == "ok":
                    batch_data[cid]["ok"] += 1
                elif status == "dup":
                    batch_data[cid]["dup"] += 1
                else:
                    batch_data[cid]["fail"] += 1

            bot.delete_message(cid, message.message_id)

        except Exception as e:
            logging.error(f"Worker error: {e}")
            failed_tasks.put(message)

        finally:
            task_queue.task_done()

def retry_failed_worker():
    while True:
        try:
            msg = failed_tasks.get(timeout=10)
        except Empty:
            continue

        time.sleep(5)  # espera antes de reintentar

        try:
            task_queue.put(msg)
        except:
            pass

# ==============================
# 6. REPORTES
# ==============================
def send_final_report(chat_id):
    with stats_lock:
        stats = batch_data.get(chat_id)
        if not stats or all(v == 0 for v in stats.values()):
            return

        text = (f"🏁 *INFORME DE CARGA FINALIZADA*\n"
                f"━━━━━━━━━━━━━━━━━━━\n"
                f"✅ *Guardados:* `{stats['ok']}`\n"
                f"⚠️ *Duplicados:* `{stats['dup']}`\n"
                f"❌ *Fallidos:* `{stats['fail']}`\n"
                f"━━━━━━━━━━━━━━━━━━━\n"
                f"👤 _Chat limpio y autoría eliminada._")

        try:
            bot.send_message(chat_id, text, parse_mode="Markdown")
        except:
            pass

        batch_data[chat_id] = {"ok": 0, "dup": 0, "fail": 0}

# ==============================
# 7. HANDLER
# ==============================
@bot.message_handler(content_types=['photo', 'video', 'document', 'audio', 'voice', 'video_note'])
def handle_docs(message):
    cid = message.chat.id

    with stats_lock:
        if cid not in batch_data:
            batch_data[cid] = {"ok": 0, "dup": 0, "fail": 0}

    if cid in timers:
        timers[cid].cancel()

    timers[cid] = Timer(25.0, send_final_report, [cid])
    timers[cid].start()

    try:
        task_queue.put_nowait(message)
    except:
        with stats_lock:
            batch_data[cid]["fail"] += 1

# ==============================
# 8. KEEP ALIVE
# ==============================
app = Flask(__name__)

@app.route('/')
def home():
    return "OK"

def health_monitor():
    while True:
        time.sleep(120)
        try:
            if MY_URL:
                requests.get(MY_URL, timeout=10)
            if db_pool:
                conn = db_pool.getconn()
                cur = conn.cursor()
                cur.execute("SELECT 1")
                cur.close()
                db_pool.putconn(conn)
        except:
            pass

# ==============================
# 9. INICIO
# ==============================
if __name__ == "__main__":
    alert_admin("🚀 BOT PRO ACTIVO")

    for _ in range(10):  # workers
        Thread(target=worker, daemon=True).start()

    Thread(target=retry_failed_worker, daemon=True).start()
    Thread(target=lambda: app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080))), daemon=True).start()
    Thread(target=health_monitor, daemon=True).start()

    bot.infinity_polling(timeout=60, long_polling_timeout=25)
