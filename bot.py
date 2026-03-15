import telebot
import os
import time
import logging
import psycopg2
from psycopg2 import pool
from flask import Flask
from threading import Thread
from queue import Queue
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

# ==============================
# CONFIGURACIÓN
# ==============================

TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHANNEL_ID = int(os.environ.get("CHANNEL_ID"))
DB_URL = os.environ.get("DATABASE_URL")

bot = telebot.TeleBot(TOKEN, threaded=True, num_threads=120)

logging.basicConfig(level=logging.INFO)

# ==============================
# POOL DB
# ==============================

db_pool = psycopg2.pool.SimpleConnectionPool(1,40,DB_URL)

# ==============================
# COLA DE PROCESAMIENTO
# ==============================

file_queue = Queue()

# ==============================
# CONTROL DE LOTES
# ==============================

batch_data = {}

# ==============================
# DB DUPLICADOS
# ==============================

def check_and_save(file_id):

    conn=None
    cur=None

    try:

        conn=db_pool.getconn()
        cur=conn.cursor()

        cur.execute(
            "SELECT 1 FROM storage WHERE file_unique_id=%s",
            (file_id,)
        )

        if cur.fetchone():
            return "duplicado"

        cur.execute(
            "INSERT INTO storage(file_unique_id) VALUES(%s)",
            (file_id,)
        )

        conn.commit()
        return "nuevo"

    except Exception as e:

        logging.error(e)
        return "error"

    finally:

        if cur: cur.close()
        if conn: db_pool.putconn(conn)

# ==============================
# DETECTAR MEDIA
# ==============================

def get_media(message):

    if message.content_type=="photo":
        return message.photo[-1]

    media_map={
        "video":message.video,
        "document":message.document,
        "audio":message.audio,
        "voice":message.voice,
        "video_note":message.video_note
    }

    return media_map.get(message.content_type)

# ==============================
# TRABAJADOR DE COLA
# ==============================

def worker():

    while True:

        message=file_queue.get()

        try:

            process_message(message)

        except Exception as e:

            logging.error(e)

        file_queue.task_done()

# iniciar worker
Thread(target=worker,daemon=True).start()

# ==============================
# PROCESAR MENSAJE
# ==============================

def process_message(message):

    media=get_media(message)

    if not media:
        return

    user_id=message.chat.id

    if user_id not in batch_data:

        batch_data[user_id]={
            "ok":0,
            "dup":0,
            "fail":[],
            "last_link":None
        }

    file_id=media.file_unique_id

    result=check_and_save(file_id)

    if result=="duplicado":

        batch_data[user_id]["dup"]+=1
        batch_data[user_id]["fail"].append("duplicado")
        return

    try:

        sent=bot.forward_message(
            chat_id=CHANNEL_ID,
            from_chat_id=message.chat.id,
            message_id=message.message_id
        )

        msg_id=sent.message_id

        channel_internal=str(CHANNEL_ID)[4:]

        link=f"https://t.me/c/{channel_internal}/{msg_id}"

        batch_data[user_id]["last_link"]=link
        batch_data[user_id]["ok"]+=1

    except:

        batch_data[user_id]["fail"].append("error")

# ==============================
# HANDLER
# ==============================

@bot.message_handler(content_types=[
'photo','video','document','audio','voice','video_note'
])
def handle(message):

    file_queue.put(message)

# ==============================
# COMANDO RESUMEN
# ==============================

@bot.message_handler(commands=["resumen"])
def resumen(message):

    user_id=message.chat.id

    if user_id not in batch_data:

        bot.reply_to(message,"No hay archivos procesados.")
        return

    data=batch_data[user_id]

    text=f"""
📦 Resultado del envío

✅ Correctos: {data['ok']}
⚠️ Duplicados: {data['dup']}
❌ Fallidos: {len(data['fail'])}
"""

    markup=None

    if data["last_link"]:

        markup=InlineKeyboardMarkup()
        markup.add(
            InlineKeyboardButton(
                "📂 Ver último archivo",
                url=data["last_link"]
            )
        )

    bot.send_message(
        message.chat.id,
        text,
        reply_markup=markup
    )

# ==============================
# SERVIDOR
# ==============================

app=Flask(__name__)

@app.route("/")
def home():
    return "Bot activo"

# ==============================
# INICIAR BOT
# ==============================

if __name__=="__main__":

    Thread(
        target=lambda: app.run(
            host="0.0.0.0",
            port=int(os.environ.get("PORT",8080))
        ),
        daemon=True
    ).start()

    bot.infinity_polling()
