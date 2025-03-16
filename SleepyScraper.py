
import cloudscraper
class SleepyScraper:
    def __init__(self, mean_sleep=30, var_sleep=5, save_to = None):
        self.scraper = cloudscraper.create_scraper()
        self.mean_sleep = mean_sleep
        self.var_sleep = var_sleep
        self.save_to = save_to

    def get(self, url, **kwargs):
        return self.scraper.get(url, **kwargs)