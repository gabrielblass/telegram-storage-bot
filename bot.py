import telebot
import os
import time
import logging
import psycopg2
from psycopg2 import pool
from flask import Flask
from threading import Thread
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

# ==============================
# CONFIGURACIÓN
# ==============================

TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHANNEL_ID = int(os.environ.get("CHANNEL_ID", -1003628952931))
DB_URL = os.environ.get("DATABASE_URL")

if not TOKEN:
    raise ValueError("TOKEN no configurado")

bot = telebot.TeleBot(
    TOKEN,
    threaded=True,
    num_threads=80
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ==============================
# POOL POSTGRESQL
# ==============================

db_pool = psycopg2.pool.SimpleConnectionPool(
    1,
    20,
    DB_URL
)

# ==============================
# BASE DE DATOS
# ==============================

def check_and_save(file_unique_id, file_type):

    conn = None

    try:
        conn = db_pool.getconn()
        cur = conn.cursor()

        cur.execute(
            "SELECT 1 FROM storage WHERE file_unique_id=%s",
            (file_unique_id,)
        )

        if cur.fetchone():
            cur.close()
            return "duplicado"

        cur.execute(
            "INSERT INTO storage (file_unique_id,file_type) VALUES (%s,%s)",
            (file_unique_id,file_type)
        )

        conn.commit()
        cur.close()

        return "nuevo"

    except Exception as e:

        logging.error(f"DB Error {e}")
        return "error"

    finally:

        if conn:
            db_pool.putconn(conn)

# ==============================
# LIMPIAR CHAT
# ==============================

def cleanup_success(chat_id,status_id,original_id):

    time.sleep(6)

    try:
        bot.delete_message(chat_id,status_id)
    except:
        pass

    try:
        bot.delete_message(chat_id,original_id)
    except:
        pass

# ==============================
# PROCESAR MENSAJE
# ==============================

def process_message(message):

    media_map = {
        "photo": message.photo[-1] if message.photo else None,
        "video": message.video,
        "document": message.document,
        "audio": message.audio,
        "voice": message.voice,
        "video_note": message.video_note
    }

    media = media_map.get(message.content_type)

    if not media:
        return

    # verificar duplicado
    res = check_and_save(media.file_unique_id,message.content_type)

    if res == "duplicado":

        status_dup = bot.reply_to(
            message,
            "⚠️ Este archivo ya existe en el almacén"
        )

        Thread(
            target=cleanup_success,
            args=(message.chat.id,status_dup.message_id,message.message_id),
            daemon=True
        ).start()

        return

    status_msg = bot.reply_to(message,"⚡ Guardando archivo...")

    try:

        sent = bot.copy_message(
            chat_id=CHANNEL_ID,
            from_chat_id=message.chat.id,
            message_id=message.message_id
        )

        msg_id = sent.message_id

        channel_internal_id = str(CHANNEL_ID)[4:]
        link = f"https://t.me/c/{channel_internal_id}/{msg_id}"

        markup = InlineKeyboardMarkup()

        markup.add(
            InlineKeyboardButton(
                "📂 Ver archivo",
                url=link
            )
        )

        bot.edit_message_text(
            "✅ Archivo guardado",
            message.chat.id,
            status_msg.message_id,
            reply_markup=markup
        )

        Thread(
            target=cleanup_success,
            args=(message.chat.id,status_msg.message_id,message.message_id),
            daemon=True
        ).start()

    except Exception as e:

        logging.error(f"Telegram error {e}")

        bot.edit_message_text(
            f"❌ Error: {e}",
            message.chat.id,
            status_msg.message_id
        )

# ==============================
# HANDLER
# ==============================

@bot.message_handler(content_types=[
'photo','video','document','audio','voice','video_note'
])
def secure_handler(message):

    try:
        process_message(message)

    except Exception as e:

        logging.error(f"Handler crash {e}")

# ==============================
# SERVIDOR RENDER
# ==============================

app = Flask(__name__)

@app.route("/")
def index():
    return "Bot activo 🚀"

# ==============================
# INICIAR BOT
# ==============================

if __name__ == "__main__":

    Thread(
        target=lambda: app.run(
            host="0.0.0.0",
            port=int(os.environ.get("PORT",8080))
        ),
        daemon=True
    ).start()

    logging.info("Bot listo")

    bot.infinity_polling(
        timeout=60,
        long_polling_timeout=60,
        allowed_updates=["message"]
    )
