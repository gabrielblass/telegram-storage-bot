import telebot
import os
import time
import logging
import psycopg2
import requests
from psycopg2 import pool, errors
from flask import Flask
from threading import Thread, Timer, Lock

# ==============================
# 1. CONFIGURACIÓN
# ==============================
TOKEN = os.getenv("TELEGRAM_TOKEN")
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "-1003628952931"))
DB_URL = os.getenv("DATABASE_URL")
MY_URL = os.getenv("RENDER_EXTERNAL_URL") 
ADMIN_ID = 1243433271 

bot = telebot.TeleBot(TOKEN, threaded=True, num_threads=100)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Pool de conexiones
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
            logging.error(f"Error creando pool: {e}")
            return False

init_pool()

# ==============================
# 2. SISTEMA DE ALERTAS
# ==============================
def alert_admin(message):
    try:
        bot.send_message(ADMIN_ID, message, parse_mode="Markdown")
    except Exception as e:
        logging.error(f"Error alert_admin: {e}")

# ==============================
# 3. BASE DE DATOS (ANTI-DUPLICADOS)
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
        err_msg = str(e)

        if "unique" not in err_msg.lower():
            logging.error(f"Fallo DB Real: {e}")
            alert_admin(f"🚨 *FALLO DB REAL*\n`{err_msg[:60]}`")

        if conn:
            try:
                conn.rollback()
            except:
                pass

        return "err"

    finally:
        if conn and db_pool:
            try:
                db_pool.putconn(conn)
            except:
                pass

# ==============================
# 4. MANEJADOR Y REPORTES
# ==============================
batch_data = {}
timers = {}
stats_lock = Lock()

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
        except Exception as e:
            logging.error(f"Error enviando reporte: {e}")

        batch_data[chat_id] = {"ok": 0, "dup": 0, "fail": 0}

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

    media = message.photo[-1] if message.content_type == 'photo' else getattr(message, message.content_type, None)
    if not media:
        return

    caption = message.caption if message.caption else None

    try:
        # 🔥 ENVÍO SIN "REENVIADO DE"
        if message.content_type == 'photo':
            bot.send_photo(CHANNEL_ID, media.file_id, caption=caption)

        elif message.content_type == 'video':
            bot.send_video(CHANNEL_ID, media.file_id, caption=caption)

        elif message.content_type == 'document':
            bot.send_document(CHANNEL_ID, media.file_id, caption=caption)

        elif message.content_type == 'audio':
            bot.send_audio(CHANNEL_ID, media.file_id, caption=caption)

        elif message.content_type == 'voice':
            bot.send_voice(CHANNEL_ID, media.file_id)

        elif message.content_type == 'video_note':
            bot.send_video_note(CHANNEL_ID, media.file_id)

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
        logging.error(f"Fallo envío: {e}")

        with stats_lock:
            batch_data[cid]["fail"] += 1

        alert_admin(f"⚠️ *FALLO DE ENVÍO*\n`{str(e)[:60]}`")

# ==============================
# 5. KEEP ALIVE
# ==============================
app = Flask(__name__)

@app.route('/')
def home():
    return "🛡️ MONITOR ACTIVO"

def health_monitor():
    while True:
        time.sleep(120)
        if MY_URL:
            try:
                requests.get(MY_URL, timeout=10)

                if db_pool:
                    conn = db_pool.getconn()
                    cur = conn.cursor()
                    cur.execute("SELECT 1")
                    cur.close()
                    db_pool.putconn(conn)

            except Exception as e:
                logging.warning(f"Health check fallo: {e}")

# ==============================
# 6. INICIO
# ==============================
if __name__ == "__main__":
    alert_admin("🚀 *BOT REINICIADO*\nSin 'reenviado de' activo.")

    Thread(target=lambda: app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080))), daemon=True).start()
    Thread(target=health_monitor, daemon=True).start()

    bot.infinity_polling(timeout=60, long_polling_timeout=25)
