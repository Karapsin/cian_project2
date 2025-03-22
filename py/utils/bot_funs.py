from py.constants.constants import BOT_TOKEN, CHAT_ID
import requests

def send_telegram_message(message):
    response = requests.post(f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage', 
                             data={'chat_id': CHAT_ID, 
                                   'text': message
                                  }
               )
    response.raise_for_status()