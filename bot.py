import telebot
import os
import logging
import psycopg2
from psycopg2 import pool, errors
from flask import Flask
from threading import Thread, Timer, Lock

# ==============================
# 1. CONFIGURACIÓN
# ==============================
TOKEN = os.getenv("TELEGRAM_TOKEN")
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "-1003628952931"))
DB_URL = os.getenv("DATABASE_URL")

if not TOKEN or not DB_URL:
    raise ValueError("Faltan variables de entorno (TOKEN o DATABASE_URL)")

# Ajuste de hilos para estabilidad en Render
bot = telebot.TeleBot(TOKEN, threaded=True, num_threads=40)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Pool de conexiones optimizado
try:
    db_pool = psycopg2.pool.SimpleConnectionPool(1, 20, DB_URL)
    logging.info("✅ Pool de conexiones DB iniciado")
except Exception as e:
    logging.error(f"❌ Error al iniciar pool DB: {e}")

# ==============================
# 2. VARIABLES DE CONTROL
# ==============================
batch_data = {}
timers = {}
stats_lock = Lock() # Para evitar errores al escribir estadísticas simultáneas

def get_stats(chat_id):
    with stats_lock:
        if chat_id not in batch_data:
            batch_data[chat_id] = {"ok": 0, "dup": 0, "fail": 0}
        return batch_data[chat_id]

# ==============================
# 3. BASE DE DATOS (LÓGICA INVERSA)
# ==============================
def check_only(file_id):
    """Solo verifica si existe sin insertar todavía"""
    conn = None
    try:
        conn = db_pool.getconn()
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
    """Guarda el ID después de confirmar el envío exitoso"""
    conn = None
    try:
        conn = db_pool.getconn()
        cur = conn.cursor()
        cur.execute("INSERT INTO storage (file_unique_id) VALUES (%s)", (file_id,))
        conn.commit()
        cur.close()
        return True
    except errors.UniqueViolation:
        if conn: conn.rollback()
        return False
    except Exception as e:
        logging.error(f"Error guardando en DB: {e}")
        if conn: conn.rollback()
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

    # Reiniciar temporizador (30 segundos para dar margen a videos grandes)
    if chat_id in timers:
        timers[chat_id].cancel()
    
    timer = Timer(30.0, send_final_report, [chat_id])
    timers[chat_id] = timer
    timer.start()

    # Detectar el objeto media
    media = None
    if message.content_type == 'photo':
        media = message.photo[-1]
    else:
        media = getattr(message, message.content_type, None)

    if not media:
        return

    # 1. VERIFICAR DUPLICADO (Sin insertar aún)
    if check_only(media.file_unique_id):
        stats["dup"] += 1
        try:
            bot.delete_message(chat_id, message.message_id)
        except: pass
        return

    # 2. INTENTAR COPIAR AL CANAL
    try:
        # copy_message quita la autoría original
        bot.copy_message(CHANNEL_ID, chat_id, message.message_id)
        
        # 3. SOLO SI SE COPIÓ, GUARDAMOS EN DB
        if save_id(media.file_unique_id):
            stats["ok"] += 1
        else:
            # Si falló la DB pero se envió, lo contamos como error de registro pero no duplicado
            stats["fail"] += 1
            
        try:
            bot.delete_message(chat_id, message.message_id)
        except: pass

    except Exception as e:
        logging.error(f"Fallo al mover archivo: {e}")
        stats["fail"] += 1

# ==============================
# 6. FLASK KEEP-ALIVE
# ==============================
app = Flask(__name__)

@app.route('/')
def home():
    return "Bot Safe-Storage Online 🛡️"

def run_flask():
    port = int(os.getenv("PORT", 8080))
    app.run(host="0.0.0.0", port=port)

# ==============================
# 7. ARRANQUE
# ==============================
if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    logging.info("🚀 Bot iniciado correctamente...")
    
    # infinity_polling es más resistente a caídas de red
    bot.infinity_polling(timeout=90, long_polling_timeout=30)
