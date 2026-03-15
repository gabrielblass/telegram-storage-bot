import telebot
import os
import time
import logging
import psycopg2
import requests
from flask import Flask
from threading import Thread

# -----------------------------
# 1. CONFIGURACIÓN
# -----------------------------
TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHANNEL_ID = int(os.environ.get("CHANNEL_ID", -1003628952931))
DB_URL = os.environ.get("DATABASE_URL")
RENDER_URL = "https://telegram-storage-bot-y9pu.onrender.com"

bot = telebot.TeleBot(TOKEN)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# -----------------------------
# 2. FUNCIONES DE BASE DE DATOS
# -----------------------------
def get_db_connection():
    return psycopg2.connect(DB_URL)

def init_db():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS storage (
                    file_unique_id TEXT PRIMARY KEY,
                    file_type TEXT)''')
    conn.commit()
    cur.close()
    conn.close()

def is_duplicate(file_unique_id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM storage WHERE file_unique_id = %s", (file_unique_id,))
    exists = cur.fetchone()
    cur.close()
    conn.close()
    return exists is not None

def save_file(file_unique_id, file_type):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("INSERT INTO storage (file_unique_id, file_type) VALUES (%s, %s) ON CONFLICT DO NOTHING", 
                (file_unique_id, file_type))
    conn.commit()
    cur.close()
    conn.close()

def get_count():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM storage")
    count = cur.fetchone()[0]
    cur.close()
    conn.close()
    return count

# -----------------------------
# 3. LIMPIEZA Y COMANDOS
# -----------------------------
def delete_later(chat_id, status_id, original_id, delay=4):
    time.sleep(delay)
    try:
        bot.delete_message(chat_id, status_id)
        bot.delete_message(chat_id, original_id)
    except: pass

@bot.message_handler(commands=['reset'])
def reset_storage(message):
    """Borra todos los registros para empezar de cero"""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM storage")
    conn.commit()
    cur.close()
    conn.close()
    msg = bot.reply_to(message, "🗑️ Base de datos reiniciada. ¡Todo de cero!")
    time.sleep(5)
    bot.delete_message(message.chat.id, msg.message_id)

# -----------------------------
# 4. MANEJO DE MEDIA (50+ ARCHIVOS)
# -----------------------------
@bot.message_handler(content_types=['photo', 'video'])
def media_handler(message):
    file_obj = message.photo[-1] if message.content_type == 'photo' else message.video
    f_type = "foto" if message.content_type == 'photo' else "video"
    
    unique_id = file_obj.file_unique_id
    repetido = is_duplicate(unique_id)
    
    # Aviso de procesamiento
    status_text = f"⏳ Procesando {f_type}..."
    if repetido: status_text = f"⚠️ {f_type.capitalize()} repetido, guardando igual..."
    
    status_msg = bot.reply_to(message, status_text)

    try:
        bot.copy_message(chat_id=CHANNEL_ID, from_chat_id=message.chat.id, message_id=message.message_id)
        save_file(unique_id, f_type)
        total = get_count()
        
        bot.edit_message_text(f"✅ {f_type.capitalize()} #{total} guardado", 
                             message.chat.id, status_msg.message_id)
    except Exception as e:
        logging.error(f"Error: {e}")
        bot.edit_message_text("❌ Error al guardar", message.chat.id, status_msg.message_id)

    Thread(target=delete_later, args=(message.chat.id, status_msg.message_id, message.message_id)).start()

# -----------------------------
# 5. WEB Y MANTENIMIENTO
# -----------------------------
app = Flask(__name__)
@app.route('/')
def index(): return "Bot Activo 🚀"

def wake_up():
    while True:
        try: requests.get(RENDER_URL)
        except: pass
        time.sleep(600)

if __name__ == "__main__":
    init_db() # Crea la tabla si no existe
    Thread(target=lambda: app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080))), daemon=True).start()
    Thread(target=wake_up, daemon=True).start()
    logging.info("Bot iniciado con contador y base de datos")
    bot.infinity_polling(timeout=60, long_polling_timeout=20)
