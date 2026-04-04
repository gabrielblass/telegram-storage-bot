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
# 1. CONFIGURACIÓN (Rellena en Render)
# ==============================
TOKEN = os.getenv("TELEGRAM_TOKEN")
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "-1003628952931"))
DB_URL = os.getenv("DATABASE_URL")
MY_URL = os.getenv("RENDER_EXTERNAL_URL") 

# Bot con alta capacidad de hilos
bot = telebot.TeleBot(TOKEN, threaded=True, num_threads=100)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Pool de conexiones (Mantiene a Neon "despierto")
try:
    db_pool = psycopg2.pool.SimpleConnectionPool(1, 30, DB_URL)
    logging.info("✅ Pool de conexiones DB activo")
except Exception as e:
    logging.error(f"❌ Error DB: {e}")

# ==============================
# 2. VARIABLES DE CONTROL
# ==============================
batch_data = {}
timers = {}
stats_lock = Lock()
duplicate_cache = set() # Caché rápida en RAM

def get_stats(chat_id):
    with stats_lock:
        if chat_id not in batch_data:
            batch_data[chat_id] = {"ok": 0, "dup": 0, "fail": 0}
        return batch_data[chat_id]

# ==============================
# 3. BASE DE DATOS (SUPER RESISTENTE)
# ==============================
def is_duplicate_and_save(file_id):
    """Verifica en RAM y luego en DB para máxima velocidad"""
    if file_id in duplicate_cache:
        return True
    
    conn = None
    try:
        conn = db_pool.getconn()
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM storage WHERE file_unique_id = %s", (file_id,))
        exists = cur.fetchone() is not None
        
        if exists:
            duplicate_cache.add(file_id)
            cur.close()
            return True
        
        # Si no existe, lo guardamos
        cur.execute("INSERT INTO storage (file_unique_id) VALUES (%s)", (file_id,))
        conn.commit()
        duplicate_cache.add(file_id)
        cur.close()
        return False
    except errors.UniqueViolation:
        if conn: conn.rollback()
        return True
    except Exception as e:
        logging.error(f"Error DB: {e}")
        return False
    finally:
        if conn: db_pool.putconn(conn)

# ==============================
# 4. INFORME FINAL AUTOMÁTICO
# ==============================
def send_final_report(chat_id):
    stats = batch_data.get(chat_id)
    if not stats or all(v == 0 for v in stats.values()): return

    text = (f"🏁 *INFORME DE CARGA FINALIZADA*\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"✅ *Guardados:* `{stats['ok']}`\n"
            f"⚠️ *Duplicados:* `{stats['dup']}`\n"
            f"❌ *Fallidos:* `{stats['fail']}`\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"🚀 _Procesado sin límite de peso._")
    
    try: bot.send_message(chat_id, text, parse_mode="Markdown")
    except: pass
    
    with stats_lock:
        batch_data[chat_id] = {"ok": 0, "dup": 0, "fail": 0}

# ==============================
# 5. MANEJADOR DE ALTO RENDIMIENTO
# ==============================
@bot.message_handler(content_types=['photo', 'video', 'document', 'audio', 'voice', 'video_note'])
def handle_docs(message):
    cid = message.chat.id
    stats = get_stats(cid)

    # Temporizador para el informe (25 seg de silencio)
    if cid in timers: timers[cid].cancel()
    timers[cid] = Timer(25.0, send_final_report, [cid])
    timers[cid].start()

    # Detectar el archivo
    media = message.photo[-1] if message.content_type == 'photo' else getattr(message, message.content_type, None)
    if not media: return

    # 1. ¿Es duplicado? (Caché + DB)
    if is_duplicate_and_save(media.file_unique_id):
        stats["dup"] += 1
        try: bot.delete_message(cid, message.message_id)
        except: pass
        return

    # 2. Reenvío (Forward) - SOPORTA MÁS DE 2GB
    try:
        # Usamos forward para mover archivos de cualquier tamaño al instante
        bot.forward_message(CHANNEL_ID, cid, message.message_id)
        stats["ok"] += 1
        bot.delete_message(cid, message.message_id)
    except Exception as e:
        logging.error(f"Fallo envío: {e}")
        stats["fail"] += 1

# ==============================
# 6. MANTENIMIENTO (SISTEMA ANTI-SUEÑO)
# ==============================
app = Flask(__name__)
@app.route('/')
def home(): return "SISTEMA 24/7 ACTIVO 🛡️"

def wake_up():
    """Mantiene Render y Neon despiertos cada 5 minutos"""
    while True:
        time.sleep(300)
        if MY_URL:
            try: 
                requests.get(MY_URL, timeout=10)
                # Tocar la DB para que Neon no se duerma
                conn = db_pool.getconn()
                cur = conn.cursor()
                cur.execute("SELECT 1")
                cur.close()
                db_pool.putconn(conn)
                logging.info("♻️ Pulso de vida enviado")
            except: pass

if __name__ == "__main__":
    Thread(target=lambda: app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080))), daemon=True).start()
    Thread(target=wake_up, daemon=True).start()
    logging.info("🚀 Bot iniciado correctamente...")
    bot.infinity_polling(timeout=90, long_polling_timeout=30)
