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
def init_pool():
    global db_pool
    try:
        db_pool = psycopg2.pool.SimpleConnectionPool(1, 50, DB_URL)
        return True
    except Exception as e:
        return str(e)

init_pool()

# ==============================
# 2. SISTEMA DE ALERTAS
# ==============================
def alert_admin(message):
    try:
        bot.send_message(ADMIN_ID, message, parse_mode="Markdown")
    except:
        pass

# ==============================
# 3. BASE DE DATOS OPTIMIZADA (ANTI-DUPLICADOS)
# ==============================
def process_db(file_id):
    """
    Retorna: 'ok' si se guardó, 'dup' si ya existía, 'err' si falló la conexión
    """
    conn = None
    try:
        if not db_pool: init_pool()
        conn = db_pool.getconn()
        cur = conn.cursor()
        
        # Intentar insertar directamente
        try:
            cur.execute("INSERT INTO storage (file_unique_id) VALUES (%s)", (file_id,))
            conn.commit()
            cur.close()
            return "ok"
        except errors.UniqueViolation:
            conn.rollback()
            cur.close()
            return "dup" # Es un duplicado normal, no es error
            
    except Exception as e:
        err_msg = str(e)
        # Solo alertar si NO es un problema de duplicados
        if "unique" not in err_msg.lower():
            logging.error(f"Fallo DB Real: {e}")
            alert_admin(f"🚨 *FALLO DB REAL*\nLa base de datos no responde.\n`{err_msg[:60]}`")
        if conn: conn.rollback()
        return "err"
    finally:
        if conn: db_pool.putconn(conn)

# ==============================
# 4. MANEJADOR Y REPORTES
# ==============================
batch_data = {}
timers = {}
stats_lock = Lock()

def send_final_report(chat_id):
    stats = batch_data.get(chat_id)
    if not stats or all(v == 0 for v in stats.values()): return

    text = (f"🏁 *INFORME DE CARGA FINALIZADA*\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"✅ *Guardados:* `{stats['ok']}`\n"
            f"⚠️ *Duplicados:* `{stats['dup']}`\n"
            f"❌ *Fallidos:* `{stats['fail']}`\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"👤 _Chat limpio y autoría eliminada._")
    
    try: bot.send_message(chat_id, text, parse_mode="Markdown")
    except: pass
    
    with stats_lock:
        batch_data[chat_id] = {"ok": 0, "dup": 0, "fail": 0}

@bot.message_handler(content_types=['photo', 'video', 'document', 'audio', 'voice', 'video_note'])
def handle_docs(message):
    cid = message.chat.id
    
    with stats_lock:
        if cid not in batch_data:
            batch_data[cid] = {"ok": 0, "dup": 0, "fail": 0}

    # Temporizador de reporte
    if cid in timers: timers[cid].cancel()
    timers[cid] = Timer(25.0, send_final_report, [cid])
    timers[cid].start()

    # Detectar media
    media = message.photo[-1] if message.content_type == 'photo' else getattr(message, message.content_type, None)
    if not media: return

    # 1. Intentar reenvío primero (Para no perder el video)
    try:
        bot.forward_message(CHANNEL_ID, cid, message.message_id)
        
        # 2. Verificar duplicado y registrar
        status = process_db(media.file_unique_id)
        
        if status == "ok":
            batch_data[cid]["ok"] += 1
        elif status == "dup":
            batch_data[cid]["dup"] += 1
        else:
            batch_data[cid]["fail"] += 1

        # 3. Limpiar chat
        bot.delete_message(cid, message.message_id)

    except Exception as e:
        logging.error(f"Fallo Reenvío: {e}")
        batch_data[cid]["fail"] += 1
        alert_admin(f"⚠️ *FALLO DE REENVÍO*\nError: `{str(e)[:60]}`")

# ==============================
# 5. AUTO-DESPERTADOR (KEEP-ALIVE)
# ==============================
app = Flask(__name__)
@app.route('/')
def home(): return "🛡️ MONITOR DE ALMACENAMIENTO ACTIVO"

def health_monitor():
    while True:
        time.sleep(120) # Cada 2 minutos (más rápido para Neon)
        if MY_URL:
            try:
                requests.get(MY_URL, timeout=10)
                # Tocar la DB para que Neon no se duerma
                conn = db_pool.getconn()
                cur = conn.cursor()
                cur.execute("SELECT 1")
                cur.close()
                db_pool.putconn(conn)
            except: pass

if __name__ == "__main__":
    alert_admin("🚀 *BOT REINICIADO*\nLógica anti-duplicados corregida. Listo para recibir videos.")
    Thread(target=lambda: app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080))), daemon=True).start()
    Thread(target=health_monitor, daemon=True).start()
    bot.infinity_polling(timeout=90, long_polling_timeout=30)
