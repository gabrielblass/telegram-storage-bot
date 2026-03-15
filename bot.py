import telebot
import os
import time
import logging
import psycopg2
from psycopg2 import pool
from flask import Flask
from threading import Thread

# ==============================
# 1. CONFIGURACIÓN
# ==============================

TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHANNEL_ID = int(os.environ.get("CHANNEL_ID", -1003628952931))
DB_URL = os.environ.get("DATABASE_URL")

if not TOKEN:
    raise ValueError("❌ TELEGRAM_TOKEN no configurado")

if not DB_URL:
    raise ValueError("❌ DATABASE_URL no configurado")

# Bot optimizado para ráfagas
bot = telebot.TeleBot(
    TOKEN,
    threaded=True,
    num_threads=120
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ==============================
# 2. POOL DE CONEXIONES DB
# ==============================

try:
    db_pool = psycopg2.pool.SimpleConnectionPool(
        1,
        40,
        DB_URL
    )
    logging.info("✅ Pool PostgreSQL iniciado")
except Exception as e:
    logging.error(f"❌ Error creando pool DB: {e}")
    raise

# ==============================
# 3. VERIFICAR DUPLICADOS
# ==============================

def check_and_save(file_unique_id):

    conn = None
    cur = None

    try:

        conn = db_pool.getconn()
        cur = conn.cursor()

        cur.execute(
            "SELECT 1 FROM storage WHERE file_unique_id=%s",
            (file_unique_id,)
        )

        if cur.fetchone():
            return "duplicado"

        cur.execute(
            "INSERT INTO storage (file_unique_id) VALUES (%s)",
            (file_unique_id,)
        )

        conn.commit()
        return "nuevo"

    except Exception as e:

        logging.error(f"DB Error: {e}")
        return "error"

    finally:

        if cur:
            cur.close()

        if conn:
            db_pool.putconn(conn)

# ==============================
# 4. LIMPIEZA ASÍNCRONA
# ==============================

def fast_cleanup(chat_id, status_id, original_id):

    time.sleep(3)

    try:
        bot.delete_message(chat_id, status_id)
    except:
        pass

    try:
        bot.delete_message(chat_id, original_id)
    except:
        pass

# ==============================
# 5. DETECTAR MEDIA
# ==============================

def get_media(message):

    if message.content_type == "photo":
        return message.photo[-1]

    media_map = {
        "video": message.video,
        "document": message.document,
        "audio": message.audio,
        "voice": message.voice,
        "video_note": message.video_note
    }

    return media_map.get(message.content_type)

# ==============================
# 6. HANDLER PRINCIPAL
# ==============================

@bot.message_handler(content_types=[
    'photo',
    'video',
    'document',
    'audio',
    'video_note',
    'voice'
])
def forward_handler(message):

    try:

        media = get_media(message)

        if not media:
            return

        file_id = getattr(media, "file_unique_id", None)

        if not file_id:
            return

        # verificar duplicado
        result = check_and_save(file_id)

        if result == "duplicado":

            temp = bot.reply_to(
                message,
                "⚠️ Este archivo ya existe en el almacén."
            )

            Thread(
                target=fast_cleanup,
                args=(message.chat.id, temp.message_id, message.message_id),
                daemon=True
            ).start()

            return

        # mensaje de estado
        status_msg = bot.reply_to(
            message,
            "🚀 Reenviando al canal..."
        )

        # reenviar sin descargar archivo
        bot.forward_message(
            chat_id=CHANNEL_ID,
            from_chat_id=message.chat.id,
            message_id=message.message_id
        )

        bot.edit_message_text(
            "✅ Reenviado con éxito",
            message.chat.id,
            status_msg.message_id
        )

        Thread(
            target=fast_cleanup,
            args=(message.chat.id, status_msg.message_id, message.message_id),
            daemon=True
        ).start()

    except telebot.apihelper.ApiTelegramException as e:

        logging.error(f"Telegram API Error: {e}")

    except Exception as e:

        logging.error(f"Handler Error: {e}")

# ==============================
# 7. SERVIDOR RENDER
# ==============================

app = Flask(__name__)

@app.route('/')
def index():
    return "Bot de almacenamiento activo 🚀"

# ==============================
# 8. INICIAR BOT
# ==============================

if __name__ == "__main__":

    Thread(
        target=lambda: app.run(
            host="0.0.0.0",
            port=int(os.environ.get("PORT", 8080))
        ),
        daemon=True
    ).start()

    logging.info("🚀 Bot iniciado")

    bot.infinity_polling(
        timeout=60,
        long_polling_timeout=60,
        allowed_updates=["message"]
    )
