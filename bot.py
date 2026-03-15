import telebot
import os
import time
import logging
import psycopg2
import requests
from psycopg2 import pool
from flask import Flask
from threading import Thread, Timer
from queue import Queue

# ==============================
# 1. CONFIGURACIÓN
# ==============================
TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHANNEL_ID = int(os.environ.get("CHANNEL_ID", -1003628952931))
DB_URL = os.environ.get("DATABASE_URL")
RENDER_URL = "https://telegram-storage-bot-y9pu.onrender.com"

# Bot con alta capacidad de respuesta
bot = telebot.TeleBot(TOKEN, threaded=True, num_threads=150)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ==============================
# 2. POOL DE BASE DE DATOS
# ==============================
try:
    db_pool = psycopg2.pool.SimpleConnectionPool(1, 60, DB_URL)
    conn = db_pool.getconn()
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS storage (file_unique_id TEXT PRIMARY KEY)")
    conn.commit()
    db_pool.putconn(conn)
except Exception as e:
    logging.error(f"❌ Error DB: {e}")

# ==============================
# 3. VARIABLES DE CONTROL
# ==============================
file_queue = Queue()
batch_data = {}
timers = {}

def get_stats(user_id):
    if user_id not in batch_data:
        batch_data[user_id] = {"ok": 0, "dup": 0, "fail": 0}
    return batch_data[user_id]

# ==============================
# 4. FUNCIÓN DEL INFORME FINAL (AUTOMÁTICO)
# ==============================
def send_final_report(user_id):
    stats = batch_data.get(user_id)
    if not stats or (stats["ok"] == 0 and stats["dup"] == 0 and stats["fail"] == 0):
        return

    text = (f"🏁 *INFORME DE CARGA FINALIZADA*\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"✅ *Nuevos almacenados:* `{stats['ok']}`\n"
            f"⚠️ *Duplicados omitidos:* `{stats['dup']}`\n"
            f"❌ *Errores en proceso:* `{stats['fail']}`\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"👤 _Autoría eliminada y chat limpio._")
    
    bot.send_message(user_id, text, parse_mode="Markdown")
    
    # Reiniciar estadísticas para el próximo envío
    batch_data[user_id] = {"ok": 0, "dup": 0, "fail": 0}

# ==============================
# 5. TRABAJADOR DE COLA (WORKER)
# ==============================
def worker():
    while True:
        message = file_queue.get()
        try:
            process_message(message)
        except Exception as e:
            logging.error(f"Error: {e}")
        file_queue.task_done()

Thread(target=worker, daemon=True).start()

def process_message(message):
    user_id = message.chat.id
    stats = get_stats(user_id)

    # Temporizador: envía informe tras 20 segundos de inactividad
    if user_id in timers:
        timers[user_id].cancel()
    
    timers[user_id] = Timer(20.0, send_final_report, [user_id])
    timers[user_id].start()

    # Identificar media
    media = None
    if message.content_type == 'photo': media = message.photo[-1]
    elif message.content_type == 'video': media = message.video
    elif message.content_type == 'document': media = message.document
    else: media = getattr(message, message.content_type)

    if not media: return

    conn = None
    try:
        conn = db_pool.getconn()
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM storage WHERE file_unique_id = %s", (media.file_unique_id,))
        
        if cur.fetchone():
            stats["dup"] += 1
        else:
            # COPY_MESSAGE: Envía el archivo sin la etiqueta de "Reenviado"
            bot.copy_message(
                chat_id=CHANNEL_ID,
                from_chat_id=user_id,
                message_id=message.message_id
            )
            cur.execute("INSERT INTO storage (file_unique_id) VALUES (%s)", (media.file_unique_id,))
            conn.commit()
            stats["ok"] += 1
            
        # Borrado inmediato del mensaje original para limpiar el chat
        bot.delete_message(user_id, message.message_id)
            
    except Exception as e:
        logging.error(f"Fallo al procesar: {e}")
        stats["fail"] += 1
    finally:
        if conn: db_pool.putconn(conn)

# ==============================
# 6. COMANDOS Y SERVIDOR
# ==============================
@bot.message_handler(commands=['reset'])
def reset_all(message):
    conn = db_pool.getconn()
    cur = conn.cursor()
    cur.execute("DELETE FROM storage")
    conn.commit()
    db_pool.putconn(conn)
    bot.reply_to(message, "♻️ Base de datos reiniciada. Todo listo para empezar de cero.")

@bot.message_handler(content_types=['photo', 'video', 'document', 'audio', 'voice', 'video_note'])
def handle_docs(message):
    file_queue.put(message)

app = Flask(__name__)
@app.route('/')
def home(): return "Bot de Almacenamiento Limpio Activo 🚀"

if __name__ == "__main__":
    Thread(target=lambda: app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080))), daemon=True).start()
    bot.infinity_polling(timeout=90)
