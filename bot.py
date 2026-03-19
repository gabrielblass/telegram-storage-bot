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

bot = telebot.TeleBot(TOKEN, threaded=True, num_threads=30)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Pool de conexiones (MUCHO más rápido)
db_pool = psycopg2.pool.SimpleConnectionPool(1, 10, DB_URL)

db_lock = Lock()

# ==============================
# 2. VARIABLES DE CONTROL
# ==============================
batch_data = {}
timers = {}

def get_stats(chat_id):
    if chat_id not in batch_data:
        batch_data[chat_id] = {"ok": 0, "dup": 0, "fail": 0}
    return batch_data[chat_id]

# ==============================
# 3. BASE DE DATOS (OPTIMIZADA)
# ==============================
def is_duplicate_and_save(file_id):
    conn = None
    try:
        conn = db_pool.getconn()
        cur = conn.cursor()

        # 🔥 IMPORTANTE: usar UNIQUE en la DB
        cur.execute(
            "INSERT INTO storage (file_unique_id) VALUES (%s)",
            (file_id,)
        )
        conn.commit()
        cur.close()
        return False

    except errors.UniqueViolation:
        conn.rollback()
        return True

    except Exception as e:
        logging.error(f"DB Error: {e}")
        if conn:
            conn.rollback()
        return False

    finally:
        if conn:
            db_pool.putconn(conn)

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
        f"✅ *Nuevos:* `{stats['ok']}`\n"
        f"⚠️ *Duplicados:* `{stats['dup']}`\n"
        f"❌ *Errores:* `{stats['fail']}`\n"
        f"━━━━━━━━━━━━━━━━━━━"
    )

    try:
        bot.send_message(chat_id, text, parse_mode="Markdown")
    except Exception as e:
        logging.error(f"Error enviando reporte: {e}")

    batch_data[chat_id] = {"ok": 0, "dup": 0, "fail": 0}
    timers.pop(chat_id, None)

# ==============================
# 5. MANEJADOR PRINCIPAL
# ==============================
@bot.message_handler(content_types=['photo', 'video', 'document', 'audio', 'voice', 'video_note'])
def handle_docs(message):
    chat_id = message.chat.id
    stats = get_stats(chat_id)

    # Reiniciar temporizador
    if chat_id in timers:
        timers[chat_id].cancel()

    timer = Timer(20.0, send_final_report, [chat_id])
    timers[chat_id] = timer
    timer.start()

    # Obtener media
    media = None
    if message.content_type == 'photo':
        media = message.photo[-1]
    else:
        media = getattr(message, message.content_type, None)

    if not media:
        return

    # Verificar duplicado (sin lock pesado)
    if is_duplicate_and_save(media.file_unique_id):
        stats["dup"] += 1
        try:
            bot.delete_message(chat_id, message.message_id)
        except Exception as e:
            logging.warning(f"No se pudo borrar duplicado: {e}")
        return

    # Copiar al canal
    try:
        bot.copy_message(CHANNEL_ID, chat_id, message.message_id)
        stats["ok"] += 1

        try:
            bot.delete_message(chat_id, message.message_id)
        except Exception as e:
            logging.warning(f"No se pudo borrar mensaje: {e}")

    except Exception as e:
        logging.error(f"Error enviando: {e}")
        stats["fail"] += 1

# ==============================
# 6. FLASK KEEP-ALIVE
# ==============================
app = Flask(__name__)

@app.route('/')
def home():
    return "Bot Anti-Duplicados Activo 🛡️"

# ==============================
# 7. ARRANQUE
# ==============================
def run_flask():
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))

if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    logging.info("🚀 Bot iniciado correctamente...")
    bot.infinity_polling(timeout=60, long_polling_timeout=60)
