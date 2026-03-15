# Modifica esta parte en tu código:
def delete_later(chat_id, status_id, original_id, delay=3):
    time.sleep(delay)
    try:
        bot.delete_message(chat_id, status_id) # Borra el "✅ Almacenado"
        bot.delete_message(chat_id, original_id) # Borra el Video/Foto que enviaste
    except:
        pass

# Y en el media_handler lo llamas así:
Thread(
    target=delete_later, 
    args=(message.chat.id, status_msg.message_id, message.message_id)
).start()
