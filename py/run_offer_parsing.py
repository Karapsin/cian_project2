from offer_parsing import parse_offers
from SleepyScraper import SleepyScraper

try:
    parse_offers(SleepyScraper(mean_sleep = 2.8))
except Exception as e:
    send_telegram_message("An error occured during offer page parisng:")
    send_telegram_message(f"{e}\n{traceback.format_exc()}")
    raise
