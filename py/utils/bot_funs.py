from py.constants.constants import BOT_TOKEN, CHAT_ID, CIAN_ALERTS_CHAT_ID
import requests

def send_telegram_message(message, chat_type = 'alerts'):
    response = requests.post(f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage', 
                             data={'chat_id': CIAN_ALERTS_CHAT_ID if chat_type == 'alerts' else CHAT_ID, 
                                   'text': message
                                  }
               )
    response.raise_for_status()
