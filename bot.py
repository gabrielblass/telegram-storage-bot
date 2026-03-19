import telebot
import os
import time
import logging
import psycopg2
from flask import Flask
from threading import Thread, Timer, Lock

# ==============================
# 1. CONFIGURACIÓN
# ==============================
TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHANNEL_ID = int(os.environ.get("CHANNEL_ID", -1003628952931))
DB_URL = os.environ.get("DATABASE_URL")

bot = telebot.TeleBot(TOKEN, threaded=True, num_threads=50)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Bloqueo para evitar que dos hilos procesen el mismo archivo a la vez
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
# 3. BASE DE DATOS (PROTEGIDA)
# ==============================
def is_duplicate_and_save(file_id):
    """Verifica y guarda en una sola operación protegida"""
    with db_lock:
        conn = None
        try:
            conn = psycopg2.connect(DB_URL)
            cur = conn.cursor()
            
            # Verificación estricta
            cur.execute("SELECT 1 FROM storage WHERE file_unique_id = %s", (file_id,))
            exists = cur.fetchone()
            
            if exists:
                cur.close()
                conn.close()
                return True
            
            # Si no existe, lo insertamos de inmediato
            cur.execute("INSERT INTO storage (file_unique_id) VALUES (%s)", (file_id,))
            conn.commit()
            cur.close()
            conn.close()
            return False
        except Exception as e:
            logging.error(f"Error crítico DB: {e}")
            if conn: conn.close()
            return False

# ==============================
# 4. INFORME FINAL
# ==============================
def send_final_report(chat_id):
    stats = batch_data.get(chat_id)
    if not stats or (stats["ok"] == 0 and stats["dup"] == 0 and stats["fail"] == 0):
        return

    text = (f"🏁 *INFORME DE CARGA FINALIZADA*\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"✅ *Nuevos guardados:* `{stats['ok']}`\n"
            f"⚠️ *Duplicados evitados:* `{stats['dup']}`\n"
            f"❌ *Errores:* `{stats['fail']}`\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"👤 _Chat limpio y autoría eliminada._")
    
    try:
        bot.send_message(chat_id, text, parse_mode="Markdown")
    except: pass
    
    batch_data[chat_id] = {"ok": 0, "dup": 0, "fail": 0}

# ==============================
# 5. MANEJADOR PRINCIPAL
# ==============================
@bot.message_handler(content_types=['photo', 'video', 'document', 'audio', 'voice', 'video_note'])
def handle_docs(message):
    chat_id = message.chat.id
    stats = get_stats(chat_id)

    # Temporizador de informe (20 segundos)
    if chat_id in timers:
        timers[chat_id].cancel()
    timers[chat_id] = Timer(20.0, send_final_report, [chat_id])
    timers[chat_id].start()

    # Obtener objeto media
    media = None
    if message.content_type == 'photo': media = message.photo[-1]
    elif message.content_type == 'video': media = message.video
    elif message.content_type == 'document': media = message.document
    else: media = getattr(message, message.content_type)

    if not media: return

    # 1. Verificación de duplicado con BLOQUEO
    if is_duplicate_and_save(media.file_unique_id):
        stats["dup"] += 1
        try:
            bot.delete_message(chat_id, message.message_id)
        except: pass
        return

    # 2. Copia al canal (sin autor)
    try:
        bot.copy_message(CHANNEL_ID, chat_id, message.message_id)
        stats["ok"] += 1
        # Borrar mensaje original
        bot.delete_message(chat_id, message.message_id)
    except Exception as e:
        logging.error(f"Fallo al enviar: {e}")
        stats["fail"] += 1

# ==============================
# 6. ARRANQUE
# ==============================
app = Flask(__name__)
@app.route('/')
def home(): return "Bot Anti-Duplicados Activo 🛡️"

if __name__ == "__main__":
    Thread(target=lambda: app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080))), daemon=True).start()
    logging.info("🚀 Bot iniciado...")
    bot.infinity_polling(timeout=90)
        daemon=True
    ).start()

    bot.infinity_polling(timeout=60, long_polling_timeout=60)
