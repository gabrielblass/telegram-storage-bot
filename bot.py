import telebot
import os
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
RENDER_URL = os.getenv("RENDER_EXTERNAL_URL") # Para el auto-despertador

if not TOKEN or not DB_URL:
    raise ValueError("Faltan variables de entorno (TOKEN o DATABASE_URL)")

bot = telebot.TeleBot(TOKEN, threaded=True, num_threads=50)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Pool de conexiones con manejo de reconexión
def create_pool():
    try:
        return psycopg2.pool.SimpleConnectionPool(1, 25, DB_URL)
    except Exception as e:
        logging.error(f"❌ Error al iniciar pool DB: {e}")
        return None

db_pool = create_pool()

# ==============================
# 2. VARIABLES DE CONTROL
# ==============================
batch_data = {}
timers = {}
stats_lock = Lock()

def get_stats(chat_id):
    with stats_lock:
        if chat_id not in batch_data:
            batch_data[chat_id] = {"ok": 0, "dup": 0, "fail": 0}
        return batch_data[chat_id]

# ==============================
# 3. BASE DE DATOS (REFORZADA)
# ==============================
def get_db_connection():
    """Obtiene una conexión y la reinicia si el pool falló"""
    global db_pool
    try:
        if not db_pool:
            db_pool = create_pool()
        return db_pool.getconn()
    except:
        return None

def check_only(file_id):
    conn = get_db_connection()
    if not conn: return False
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM storage WHERE file_unique_id = %s", (file_id,))
        exists = cur.fetchone() is not None
        cur.close()
        return exists
    except Exception as e:
        logging.error(f"Error verificando DB: {e}")
        return False
    finally:
        if conn: db_pool.putconn(conn)

def save_id(file_id):
    conn = get_db_connection()
    if not conn: return False
    try:
        cur = conn.cursor()
        cur.execute("INSERT INTO storage (file_unique_id) VALUES (%s)", (file_id,))
        conn.commit()
        cur.close()
        return True
    except errors.UniqueViolation:
        conn.rollback()
        return False
    except Exception as e:
        logging.error(f"Error guardando en DB: {e}")
        conn.rollback()
        return False
    finally:
        if conn: db_pool.putconn(conn)

# ==============================
# 4. INFORME FINAL
# ==============================
def send_final_report(chat_id):
    stats = batch_data.get(chat_id)
    if not stats or all(v == 0 for v in stats.values()):
        return

    text = (
        f"🏁 *INFORME DE CARGA FINALIZADA*\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"✅ *Guardados:* `{stats['ok']}`\n"
        f"⚠️ *Duplicados:* `{stats['dup']}`\n"
        f"❌ *Fallidos:* `{stats['fail']}`\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"👤 _Autoría eliminada y chat limpio._"
    )

    try:
        bot.send_message(chat_id, text, parse_mode="Markdown")
    except Exception as e:
        logging.error(f"Error enviando reporte: {e}")

    with stats_lock:
        batch_data[chat_id] = {"ok": 0, "dup": 0, "fail": 0}
        timers.pop(chat_id, None)

# ==============================
# 5. MANEJADOR PRINCIPAL
# ==============================
@bot.message_handler(content_types=['photo', 'video', 'document', 'audio', 'voice', 'video_note'])
def handle_docs(message):
    chat_id = message.chat.id
    stats = get_stats(chat_id)

    if chat_id in timers:
        timers[chat_id].cancel()
    
    timer = Timer(25.0, send_final_report, [chat_id])
    timers[chat_id] = timer
    timer.start()

    media = message.photo[-1] if message.content_type == 'photo' else getattr(message, message.content_type, None)

    if not media: return

    # 1. VERIFICAR DUPLICADO
    if check_only(media.file_unique_id):
        stats["dup"] += 1
        try: bot.delete_message(chat_id, message.message_id)
        except: pass
        return

    # 2. COPIAR AL CANAL
    try:
        bot.copy_message(CHANNEL_ID, chat_id, message.message_id)
        
        # 3. GUARDAR EN DB (Solo si se copió con éxito)
        if save_id(media.file_unique_id):
            stats["ok"] += 1
        else:
            stats["fail"] += 1 # Error de registro pero el archivo pasó
            
        try: bot.delete_message(chat_id, message.message_id)
        except: pass

    except Exception as e:
        logging.error(f"Fallo al mover archivo: {e}")
        stats["fail"] += 1

# ==============================
# 6. MANTENIMIENTO (ANTI-SUSPENSIÓN)
# ==============================
app = Flask(__name__)

@app.route('/')
def home():
    return "Bot Safe-Storage Online 🛡️"

def wake_up():
    """Evita que Render y Neon se duerman"""
    if RENDER_URL:
        while True:
            try:
                requests.get(RENDER_URL)
                logging.info("Ping de mantenimiento enviado")
            except: pass
            time.sleep(600)

# ==============================
# 7. ARRANQUE
# ==============================
if __name__ == "__main__":
    Thread(target=lambda: app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080))), daemon=True).start()
    Thread(target=wake_up, daemon=True).start()
    
    logging.info("🚀 Bot iniciado correctamente...")
    bot.infinity_polling(timeout=90, long_polling_timeout=30)
