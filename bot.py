import telebot
import os
import time
import logging
import requests  # Librería para el sistema anti-suspensión
from flask import Flask
from threading import Thread

# -----------------------------
# 1. CONFIGURACIÓN
# -----------------------------
TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHANNEL_ID = int(os.environ.get("CHANNEL_ID", -1003628952931))
# Tu enlace de Render para que el bot se visite a sí mismo
RENDER_URL = "https://telegram-storage-bot-y9pu.onrender.com"

bot = telebot.TeleBot(TOKEN)

# Logging profesional para Render
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
        if now - last_message[user_id] < 1.5:
            return True
    last_message[user_id] = now
    return False

# -----------------------------
# 3. LIMPIEZA AUTOMÁTICA (HILOS)
# -----------------------------
def delete_later(chat_id, status_id, original_id, delay=4):
    """Borra el aviso del bot y el archivo original del usuario"""
    time.sleep(delay)
    try:
        bot.delete_message(chat_id, status_id)    # Borra "✅ Almacenado"
        bot.delete_message(chat_id, original_id)  # Borra el video/foto que tú enviaste
    except Exception as e:
        logging.warning(f"No se pudo limpiar el chat: {e}")

# -----------------------------
# 4. MANEJO DE MEDIA
# -----------------------------
@bot.message_handler(content_types=['photo', 'video'])
def media_handler(message):
    # Filtro Anti-Spam
    if anti_spam(message.from_user.id):
        return

    tipo = "imagen" if message.content_type == "photo" else "video"
    status_msg = bot.reply_to(message, f"⏳ Procesando {tipo}...")

    try:
        # COPIAR AL CANAL (Elimina el 'reenviado de...')
        bot.copy_message(
            chat_id=CHANNEL_ID,
            from_chat_id=message.chat.id,
            message_id=message.message_id
        )

        bot.edit_message_text(
            f"✅ {tipo.capitalize()} guardado correctamente",
            message.chat.id,
            status_msg.message_id
        )

    except Exception as e:
        logging.error(f"Error en {tipo}: {e}")
        bot.edit_message_text(
            "❌ Error al guardar en el canal",
            message.chat.id,
            status_msg.message_id
        )

    # Lanzar hilo de limpieza para dejar el chat vacío
    Thread(
        target=delete_later,
        args=(message.chat.id, status_msg.message_id, message.message_id)
    ).start()

# -----------------------------
# 5. SERVIDOR WEB Y ANTI-SUSPENSIÓN
# -----------------------------
app = Flask(__name__)

@app.route('/')
def index():
    return "Bot de Almacenamiento Online 🚀"

def wake_up():
    """Mantiene el bot despierto haciendo peticiones cada 10 min"""
    while True:
        try:
            # Auto-visita
            response = requests.get(RENDER_URL)
            logging.info(f"Auto-despertador: Estado {response.status_code}")
        except Exception as e:
            logging.warning(f"Auto-despertador: No se pudo contactar al servidor: {e}")
        time.sleep(600) # 10 minutos

def run_web():
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)

# -----------------------------
# 6. INICIO
# -----------------------------
if __name__ == "__main__":
    # Iniciar Servidor Flask en segundo plano
    Thread(target=run_web, daemon=True).start()
    
    # Iniciar el sistema que evita que el bot se duerma
    Thread(target=wake_up, daemon=True).start()

    logging.info("Bot iniciado con éxito y sistema anti-suspensión")
    
    # Infinity polling optimizado
    bot.infinity_polling(timeout=60, long_polling_timeout=20)
