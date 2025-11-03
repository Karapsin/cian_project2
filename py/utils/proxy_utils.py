import os
import asyncio
from datetime import datetime, timedelta
import random
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from py.utils.random_sleep import original_random_sleep

random_sleep = original_random_sleep

TEST_URL = "https://www.cian.ru/"
CURL_FLAGS = ["-I", "--max-time", "10", "-s"]  # -I: headers only, -s: silent

COOLDOWN_MEAN = 10
COOLDOWN_VAR = 2


def _probe(proxy: str) -> bool:
    """
    Return True if the proxy returns 2xx or 3xx HTTP headers for TEST_URL.
    """
    try:
        result = subprocess.run(
            ["curl", "-x", proxy, *CURL_FLAGS, TEST_URL],
            text=True, capture_output=True, check=True
        )
        # Check status code in the headers
        for line in result.stdout.splitlines():
            if line.startswith("HTTP/"):
                code = line.split()[1]
                return code.startswith(("2", "3"))
        return False
    except subprocess.SubprocessError:
        print(f"{proxy} does not respond")
        return False                          # timeout / non-zero exit

def get_proxy_list():
    raw = os.getenv("PROXIES", "")
    proxies = [p.strip() for p in raw.split(",") if p.strip()]
    if not proxies:
        return []

    working = []
    with ThreadPoolExecutor(max_workers=10) as pool:
        future_to_proxy = {pool.submit(_probe, p): p for p in proxies}
        for fut in as_completed(future_to_proxy):
            proxy = future_to_proxy[fut]
            if fut.result():
                working.append(proxy)

    print(f"{len(working)} proxies are active ")

    return working


proxies             = get_proxy_list()                
proxy_lock          = asyncio.Lock()
proxy_last_used     = {p: datetime.min for p in proxies}
available_proxies   = set(proxies)
last_proxy_refresh  = datetime.now()  

async def get_unique_proxy():
    global proxies, available_proxies, proxy_last_used, last_proxy_refresh

    while True:
        async with proxy_lock:

            # ---------- auto-refresh every 1 h --------------------------------
            if datetime.now() - last_proxy_refresh >= timedelta(hours=1):
                new_proxies        = get_proxy_list()
                last_proxy_refresh = datetime.now()

                # rebuild bookkeeping structures
                available_proxies  = set(new_proxies)
                proxy_last_used    = {p: proxy_last_used.get(p, datetime.min)
                                      for p in new_proxies}
                proxies = new_proxies
            # ------------------------------------------------------------------

            if available_proxies:
                least_recent = min(available_proxies,
                                   key=lambda p: proxy_last_used[p])

                time_since = (datetime.now() - proxy_last_used[least_recent]
                              ).total_seconds()
                min_wait   = max(random.normalvariate(COOLDOWN_MEAN,
                                                      COOLDOWN_VAR), 0)

                if time_since >= min_wait:
                    available_proxies.remove(least_recent)
                    return least_recent

                await random_sleep(min_wait - time_since, 2)

        await random_sleep(1, 2)

async def release_proxy(proxy):
    async with proxy_lock:
        available_proxies.add(proxy)
        proxy_last_used[proxy] = datetime.now()
