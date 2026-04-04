import telebot
import os
import time
import logging
import psycopg2
import requests
import socket
from psycopg2 import pool, errors
from flask import Flask
from threading import Thread, Timer, Lock

# ==============================
# 1. CONFIGURACIÓN
# ==============================
TOKEN = os.getenv("TELEGRAM_TOKEN")
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "-1003628952931"))
DB_URL = os.getenv("DATABASE_URL")
MY_URL = os.getenv("RENDER_EXTERNAL_URL") 
ADMIN_ID = 1243433271 # Asegúrate de que este sea tu ID real

bot = telebot.TeleBot(TOKEN, threaded=True, num_threads=100)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Variables de salud del sistema
START_TIME = time.time()
db_pool = None

def init_pool():
    global db_pool
    try:
        db_pool = psycopg2.pool.SimpleConnectionPool(1, 40, DB_URL)
        return True
    except Exception as e:
        return str(e)

init_pool()

# ==============================
# 2. SISTEMA DE ALERTAS AVANZADO
# ==============================
def alert_admin(message, parse="Markdown"):
    """Notificación inmediata al administrador"""
    try:
        bot.send_message(ADMIN_ID, message, parse_mode=parse)
    except Exception as e:
        logging.error(f"Error enviando alerta: {e}")

# ==============================
# 3. MONITOREO DE CONEXIÓN (EL "PRE-AVISO")
# ==============================
def check_network():
    """Verifica si el bot tiene salida a internet antes de que Render lo mate"""
    try:
        socket.create_connection(("8.8.8.8", 53), timeout=3)
        return True
    except OSError:
        return False

# ==============================
# 4. BASE DE DATOS CON PROTECCIÓN
# ==============================
def is_duplicate_and_save(file_id):
    conn = None
    try:
        if not db_pool: init_pool()
        conn = db_pool.getconn()
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM storage WHERE file_unique_id = %s", (file_id,))
        exists = cur.fetchone() is not None
        
        if not exists:
            cur.execute("INSERT INTO storage (file_unique_id) VALUES (%s)", (file_id,))
            conn.commit()
            
        cur.close()
        return exists
    except Exception as e:
        alert_admin(f"🚨 *FALLO DB (NEON)*\nLa base de datos se ha desconectado.\nDetalle: `{str(e)[:50]}`")
        return False
    finally:
        if conn: db_pool.putconn(conn)

# ==============================
# 5. MANEJADOR PRINCIPAL (SIN LÍMITES)
# ==============================
@bot.message_handler(content_types=['photo', 'video', 'document', 'audio', 'voice', 'video_note'])
def handle_docs(message):
    cid = message.chat.id
    
    # Extraer ID de archivo según tipo
    media = message.photo[-1] if message.content_type == 'photo' else getattr(message, message.content_type, None)
    if not media: return

    # Verificación de duplicado
    if is_duplicate_and_save(media.file_unique_id):
        try: bot.delete_message(cid, message.message_id)
        except: pass
        return

    # Reenvío de alta velocidad
    try:
        bot.forward_message(CHANNEL_ID, cid, message.message_id)
        bot.delete_message(cid, message.message_id)
    except Exception as e:
        alert_admin(f"⚠️ *ERROR DE REENVÍO*\nNo se pudo mover el archivo.\nCausa: `{str(e)[:100]}`")

# ==============================
# 6. EL "DESPERTADOR" Y VIGILANTE
# ==============================
app = Flask(__name__)
@app.route('/')
def home(): return "🛡️ SISTEMA DE MONITOREO ACTIVO"

def health_monitor():
    """Revisa la conexión cada 60 segundos y avisa si Render está fallando"""
    while True:
        # Si la red interna de Render falla
        if not check_network():
            logging.warning("Pérdida de red detectada...")
            # Aquí no podemos enviar mensaje porque no hay internet, pero el log queda guardado
        
        # Ping al propio bot para evitar que Render lo duerma
        if MY_URL:
            try:
                r = requests.get(MY_URL, timeout=5)
                if r.status_code != 200:
                    alert_admin("🟡 *ESTADO:* Render está respondiendo con código " + str(r.status_code))
            except:
                # Si esto falla, es que el bot está a punto de desconectarse
                alert_admin("🔴 *ALERTA:* Conexión inestable. El bot podría caerse en segundos.")
        
        time.sleep(60) # Revisión cada minuto para máxima seguridad

if __name__ == "__main__":
    alert_admin("🚀 *BOT CONECTADO*\nMonitoreo de red y DB iniciado.")
    
    # Iniciar Flask y Monitor en hilos separados
    Thread(target=lambda: app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080))), daemon=True).start()
    Thread(target=health_monitor, daemon=True).start()
    
    # Polling infinito
    bot.infinity_polling(timeout=90, long_polling_timeout=20)
