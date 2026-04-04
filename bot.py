import telebot
import os
import logging
import psycopg2
import time
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
RENDER_URL = os.getenv("RENDER_EXTERNAL_URL") 

if not TOKEN or not DB_URL:
    raise ValueError("Faltan variables de entorno")

bot = telebot.TeleBot(TOKEN, threaded=True, num_threads=50)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Caché temporal para duplicados (Escudo extra)
duplicate_cache = set()
cache_lock = Lock()

# Pool de conexiones
def create_pool():
    try:
        return psycopg2.pool.SimpleConnectionPool(1, 30, DB_URL)
    except Exception as e:
        logging.error(f"❌ Error Pool: {e}")
        return None

db_pool = create_pool()

# ==============================
# 2. MEJORAS DE DUPLICADOS Y DB
# ==============================
def is_duplicate_strictly(file_id):
    """Verificación doble: Caché + Base de Datos"""
    with cache_lock:
        if file_id in duplicate_cache:
            return True
    
    conn = None
    try:
        if not db_pool: return False
        conn = db_pool.getconn()
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM storage WHERE file_unique_id = %s", (file_id,))
        exists = cur.fetchone() is not None
        cur.close()
        if exists:
            with cache_lock: duplicate_cache.add(file_id)
        return exists
    except:
        return False
    finally:
        if conn: db_pool.putconn(conn)

def save_id_strictly(file_id):
    conn = None
    try:
        conn = db_pool.getconn()
        cur = conn.cursor()
        cur.execute("INSERT INTO storage (file_unique_id) VALUES (%s)", (file_id,))
        conn.commit()
        cur.close()
        with cache_lock:
            duplicate_cache.add(file_id)
            if len(duplicate_cache) > 1000: duplicate_cache.pop() # Mantener memoria limpia
        return True
    except errors.UniqueViolation:
        if conn: conn.rollback()
        return "dup"
    except:
        if conn: conn.rollback()
        return False
    finally:
        if conn: db_pool.putconn(conn)

# ==============================
# 3. LÓGICA ANTI-ERROR (REINTENTOS)
# ==============================
def copy_with_retry(chat_id, message_id, attempts=3):
    """Intenta copiar el mensaje y si hay error de velocidad, espera y reintenta"""
    for i in range(attempts):
        try:
            return bot.copy_message(CHANNEL_ID, chat_id, message_id)
        except telebot.apihelper.ApiTelegramException as e:
            if e.error_code == 429: # Flood limit
                wait = e.result_json['parameters']['retry_after']
                time.sleep(wait + 1)
                continue
            raise e
    return None

# ==============================
# 4. MANEJADOR Y REPORTES
# ==============================
batch_data = {}
timers = {}

def send_report(chat_id):
    s = batch_data.get(chat_id, {"ok": 0, "dup": 0, "fail": 0})
    if any(v > 0 for v in s.values()):
        bot.send_message(chat_id, f"🏁 *CARGA FINALIZADA*\n\n✅ Guardados: `{s['ok']}`\n⚠️ Duplicados: `{s['dup']}`\n❌ Fallidos: `{s['fail']}`", parse_mode="Markdown")
    batch_data[chat_id] = {"ok": 0, "dup": 0, "fail": 0}

@bot.message_handler(content_types=['photo', 'video', 'document', 'audio', 'voice', 'video_note'])
def handle_all(message):
    cid = message.chat.id
    if cid not in batch_data: batch_data[cid] = {"ok": 0, "dup": 0, "fail": 0}
    
    if cid in timers: timers[cid].cancel()
    timers[cid] = Timer(25.0, send_report, [cid])
    timers[cid].start()

    media = message.photo[-1] if message.content_type == 'photo' else getattr(message, message.content_type, None)
    if not media: return

    # Verificar Duplicado
    if is_duplicate_strictly(media.file_unique_id):
        batch_data[cid]["dup"] += 1
        try: bot.delete_message(cid, message.message_id)
        except: pass
        return

    # Proceso de Envío
    try:
        if copy_with_retry(cid, message.message_id):
            res = save_id_strictly(media.file_unique_id)
            if res == "dup": batch_data[cid]["dup"] += 1
            else: batch_data[cid]["ok"] += 1
            bot.delete_message(cid, message.message_id)
    except Exception as e:
        logging.error(f"Error crítico: {e}")
        batch_data[cid]["fail"] += 1

# ==============================
# 5. MANTENIMIENTO ACTIVO
# ==============================
app = Flask(__name__)
@app.route('/')
def home(): return "Bot Online y Protegido 🛡️"

def keep_alive():
    """Mantiene Render y Neon despiertos"""
    while True:
        if RENDER_URL:
            try: requests.get(RENDER_URL, timeout=10)
            except: pass
        # Tocar la DB cada 4 minutos para que Neon no se duerma
        try:
            conn = db_pool.getconn()
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.close()
            db_pool.putconn(conn)
        except: pass
        time.sleep(240)

if __name__ == "__main__":
    Thread(target=lambda: app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080))), daemon=True).start()
    Thread(target=keep_alive, daemon=True).start()
    bot.infinity_polling(timeout=90)
