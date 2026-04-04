import telebot
import os
import time
import logging
import psycopg2
import requests
from psycopg2 import pool, errors
from flask import Flask
from threading import Thread, Timer, Lock
from queue import Queue, Empty
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

# ==============================
# CONFIG
# ==============================
TOKEN = os.getenv("TELEGRAM_TOKEN")
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "-1003628952931"))
DB_URL = os.getenv("DATABASE_URL")
MY_URL = os.getenv("RENDER_EXTERNAL_URL") 
ADMIN_ID = 1243433271 

bot = telebot.TeleBot(TOKEN, threaded=True, num_threads=30)

logging.basicConfig(level=logging.INFO)

# ==============================
# DB
# ==============================
db_pool = None
lock = Lock()

def init_pool():
    global db_pool
    if db_pool: return
    db_pool = psycopg2.pool.SimpleConnectionPool(1, 50, DB_URL)

init_pool()

def save_file(file_id, msg_id):
    conn = db_pool.getconn()
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO storage (file_unique_id, message_id) VALUES (%s,%s)",
            (file_id, msg_id)
        )
        conn.commit()
        return "ok"
    except errors.UniqueViolation:
        conn.rollback()
        return "dup"
    finally:
        cur.close()
        db_pool.putconn(conn)

def get_file(file_id):
    conn = db_pool.getconn()
    cur = conn.cursor()
    cur.execute("SELECT message_id FROM storage WHERE file_unique_id=%s", (file_id,))
    row = cur.fetchone()
    cur.close()
    db_pool.putconn(conn)
    return row

# ==============================
# COLA
# ==============================
queue = Queue()
failed = Queue()
stats = {}
timers = {}
lock_stats = Lock()

def build_link(msg_id):
    cid = str(CHANNEL_ID).replace("-100", "")
    return f"https://t.me/c/{cid}/{msg_id}"

def worker():
    while True:
        try:
            msg = queue.get(timeout=5)
        except Empty:
            continue

        cid = msg.chat.id
        media = msg.photo[-1] if msg.content_type == 'photo' else getattr(msg, msg.content_type, None)

        try:
            sent = bot.forward_message(CHANNEL_ID, cid, msg.message_id)

            status = save_file(media.file_unique_id, sent.message_id)

            with lock_stats:
                if status == "ok":
                    stats[cid]["ok"] += 1

                    link = build_link(sent.message_id)

                    kb = InlineKeyboardMarkup()
                    kb.add(InlineKeyboardButton("📎 Ver archivo", url=link))

                    bot.send_message(cid, "✅ Guardado", reply_markup=kb)

                elif status == "dup":
                    stats[cid]["dup"] += 1

                else:
                    stats[cid]["fail"] += 1

            bot.delete_message(cid, msg.message_id)

        except Exception as e:
            failed.put(msg)
            with lock_stats:
                stats[cid]["fail"] += 1

        queue.task_done()

def retry():
    while True:
        try:
            m = failed.get(timeout=10)
        except:
            continue
        time.sleep(5)
        queue.put(m)

# ==============================
# REPORTES
# ==============================
def report(cid):
    s = stats.get(cid)
    if not s: return

    txt = f"""
🏁 INFORME
OK: {s['ok']}
DUP: {s['dup']}
FAIL: {s['fail']}
"""
    bot.send_message(cid, txt)

    stats[cid] = {"ok":0,"dup":0,"fail":0}

# ==============================
# HANDLER
# ==============================
@bot.message_handler(content_types=['photo','video','document','audio','voice','video_note'])
def handler(m):
    cid = m.chat.id

    with lock_stats:
        if cid not in stats:
            stats[cid] = {"ok":0,"dup":0,"fail":0}

    if cid in timers:
        timers[cid].cancel()

    timers[cid] = Timer(25, report, [cid])
    timers[cid].start()

    queue.put(m)

# ==============================
# COMANDOS
# ==============================
@bot.message_handler(commands=['buscar'])
def buscar(m):
    try:
        file_id = m.text.split(" ")[1]
        res = get_file(file_id)

        if not res:
            bot.reply_to(m, "No encontrado")
            return

        link = build_link(res[0])

        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("Abrir", url=link))

        bot.reply_to(m, "Encontrado", reply_markup=kb)

    except:
        bot.reply_to(m, "Uso: /buscar file_id")

@bot.message_handler(commands=['stats'])
def stats_cmd(m):
    s = stats.get(m.chat.id, {})
    bot.reply_to(m, str(s))

# ==============================
# KEEP ALIVE
# ==============================
app = Flask(__name__)

@app.route('/')
def home():
    return "OK"

def ping():
    while True:
        time.sleep(120)
        try:
            if MY_URL:
                requests.get(MY_URL)
        except:
            pass

# ==============================
# START
# ==============================
if __name__ == "__main__":

    for _ in range(10):
        Thread(target=worker, daemon=True).start()

    Thread(target=retry, daemon=True).start()
    Thread(target=ping, daemon=True).start()
    Thread(target=lambda: app.run(host="0.0.0.0", port=int(os.getenv("PORT",8080))), daemon=True).start()

    bot.infinity_polling()
