import cloudscraper

class SleepyScraper:
    def __init__(self, 
                 mean_sleep=30, 
                 var_sleep=5, 
                 save_to=None, 
                 proxy=None,
                 timeout=30
        ):                      
        self.scraper     = cloudscraper.create_scraper()
        self.mean_sleep  = mean_sleep
        self.var_sleep   = var_sleep
        self.save_to     = save_to
        self.proxy       = proxy
        self.timeout     = timeout                 
        self.safe_proxy  = None  # initialized just in case

    def get(self, url, **kwargs):
        kwargs.setdefault("timeout", self.timeout)   
        if self.proxy:
            self.safe_proxy = self.proxy.split("@", 1)[1]
            proxies = {
                "http":  f"http://{self.proxy}",
                "https": f"http://{self.proxy}",
            }
            return self.scraper.get(url, proxies=proxies, **kwargs)

        return self.scraper.get(url, **kwargs)

    def close(self):
        self.scraper.close()
