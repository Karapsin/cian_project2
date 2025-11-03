import random
import time
import asyncio
import datetime



def original_get_current_datetime(output='text'):
    now = datetime.datetime.now()
    return now.strftime('%Y-%m-%d %H:%M') if output == 'text' else now

def original_time_print(string):
    return print((original_get_current_datetime()+' '+string))

async def original_random_sleep(mean, var, prefix = None, async_sleep = True):
    sleep_duration = random.normalvariate(mean, var)
    sleep_duration = max(max(random.normalvariate(0.5, 1), 0), sleep_duration)

    str_print = f"sleeping for {sleep_duration:.2f} seconds"
    if prefix is not None:
        str_print = f"{prefix} {str_print}"

    original_time_print(str_print)

    if async_sleep:
        await asyncio.sleep(sleep_duration)
    else:
        time.sleep(sleep_duration)