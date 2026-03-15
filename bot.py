import telebot
import os
import time
import logging
from flask import Flask
from threading import Thread

# -----------------------------
# 1. CONFIGURACIÓN
# -----------------------------
TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHANNEL_ID = int(os.environ.get("CHANNEL_ID", -1003628952931))

bot = telebot.TeleBot(TOKEN)

# Logging (muy importante en producción)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# -----------------------------
# 2. SISTEMA ANTI SPAM
# -----------------------------
last_message = {}

def anti_spam(user_id):
    now = time.time()

    if user_id in last_message:
        if now - last_message[user_id] < 1:
            return True

    last_message[user_id] = now
    return False


# -----------------------------
# 3. BORRAR MENSAJES SIN BLOQUEAR
# -----------------------------
def delete_later(chat_id, msg_id, delay=3):
    time.sleep(delay)
    try:
        bot.delete_message(chat_id, msg_id)
    except:
        pass


# -----------------------------
# 4. MANEJO DE MEDIA
# -----------------------------
@bot.message_handler(content_types=['photo', 'video'])
def media_handler(message):

    if anti_spam(message.from_user.id):
        return

    tipo = "imagen" if message.content_type == "photo" else "video"

    status_msg = bot.reply_to(message, f"⏳ Procesando {tipo}...")

    try:

        bot.copy_message(
            chat_id=CHANNEL_ID,
            from_chat_id=message.chat.id,
            message_id=message.message_id
        )

        bot.edit_message_text(
            f"✅ {tipo.capitalize()} almacenado",
            message.chat.id,
            status_msg.message_id
        )

    except Exception as e:

        logging.error(f"Error guardando {tipo}: {e}")

        bot.edit_message_text(
            "❌ Error al guardar",
            message.chat.id,
            status_msg.message_id
        )

    Thread(
        target=delete_later,
        args=(message.chat.id, status_msg.message_id)
    ).start()


# -----------------------------
# 5. SERVIDOR WEB (RENDER)
# -----------------------------
app = Flask(__name__)

@app.route('/')
def index():
    return "Bot activo 🚀"

def run_web():
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)


# -----------------------------
# 6. INICIO DEL BOT
# -----------------------------
if __name__ == "__main__":

    Thread(target=run_web).start()

    logging.info("Bot iniciado correctamente")

    bot.infinity_polling(
        timeout=30,
        long_polling_timeout=10
    )
