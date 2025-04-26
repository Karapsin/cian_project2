import os
import asyncio
from datetime import datetime
import random
from py.utils.utils import random_sleep

def get_proxy_list():
    return os.environ['PROXIES'].split(',')


COOLDOWN_MEAN = 10
COOLDOWN_VAR = 2

proxies = get_proxy_list()
proxy_lock = asyncio.Lock()
proxy_last_used = {proxy: datetime.min for proxy in proxies}
available_proxies = set(proxies)

async def get_unique_proxy():
    while True:
        async with proxy_lock:
            if available_proxies:
                least_recently_used_proxy = min(
                    available_proxies, 
                    key=lambda p: proxy_last_used[p]
                )

                time_since_used = datetime.now() - proxy_last_used[least_recently_used_proxy]

                random_min_timeout = max(random.normalvariate(COOLDOWN_MEAN, COOLDOWN_VAR), 0)

                if time_since_used.total_seconds() >= random_min_timeout:
                    available_proxies.remove(least_recently_used_proxy)
                    return least_recently_used_proxy

                await random_sleep(random_min_timeout - time_since_used.total_seconds(), 2)


        await random_sleep(1, 2)

async def release_proxy(proxy):
    async with proxy_lock:
        available_proxies.add(proxy)
        proxy_last_used[proxy] = datetime.now()
