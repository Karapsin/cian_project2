from dotenv import load_dotenv
import os 

load_dotenv()

OFFER_JSON_TEMPLATE = "window._cianConfig['frontend-offer-card'] =" 
SEARCH_PAGE_JSON_TEMPLATE = "window._cianConfig['frontend-serp'] ="
NEW_SEARCH_PAGE_JSON_TEMPLATE = "window._cianConfig['search-frontend'] ="

CIAN_SEARCH_URLS = {"long_rent": "https://www.cian.ru/cat.php?deal_type=rent&engine_version=2&offer_type=flat&type=4",
                    "short_rent": "https://www.cian.ru/cat.php?deal_type=rent&engine_version=2&offer_type=flat&type=2",
                    "sale_secondary": "https://www.cian.ru/cat.php?deal_type=sale&engine_version=2&object_type%5B0%5D=1&offer_type=flat&region=1",
                    "sale_primary": "https://www.cian.ru/cat.php?deal_type=sale&engine_version=2&hand_over=1&object_type%5B0%5D=2&offer_type=flat&with_newobject=1"
                   }
BOT_TOKEN = os.getenv('CIAN_REPORTER_TOKEN')
CHAT_ID = os.getenv('CIAN_CHAT_ID')
CIAN_ALERTS_CHAT_ID = os.getenv('CIAN_ALERTS_ID')
YADISK_TOKEN = os.getenv('YANDEX_DISK_TOKEN')


DISTRICTS_LISTS = ['district%5B0%5D=13', 'district%5B100%5D=113', 'district%5B101%5D=114', 
                   'district%5B102%5D=115', 'district%5B103%5D=116', 'district%5B104%5D=117', 
                   'district%5B105%5D=118', 'district%5B106%5D=119', 'district%5B107%5D=120', 
                   'district%5B108%5D=121', 'district%5B109%5D=122', 'district%5B10%5D=23', 
                   'district%5B110%5D=123', 'district%5B111%5D=124', 'district%5B112%5D=125', 
                   'district%5B113%5D=126', 'district%5B114%5D=127', 'district%5B115%5D=128', 
                   'district%5B116%5D=129', 'district%5B117%5D=130', 'district%5B118%5D=131', 
                   'district%5B119%5D=132', 'district%5B11%5D=24', 'district%5B120%5D=348', 
                   'district%5B121%5D=349', 'district%5B122%5D=350', 'district%5B12%5D=25', 
                   'district%5B13%5D=26', 'district%5B14%5D=27', 'district%5B15%5D=28', 
                   'district%5B16%5D=29', 'district%5B17%5D=30', 'district%5B18%5D=31', 
                   'district%5B19%5D=32', 'district%5B1%5D=14', 'district%5B20%5D=33', 
                   'district%5B21%5D=34', 'district%5B22%5D=35', 'district%5B23%5D=36', 
                   'district%5B24%5D=37', 'district%5B25%5D=38', 'district%5B26%5D=39', 
                   'district%5B27%5D=40', 'district%5B28%5D=41', 'district%5B29%5D=42', 
                   'district%5B2%5D=15', 'district%5B30%5D=43', 'district%5B31%5D=44', 
                   'district%5B32%5D=45', 'district%5B33%5D=46', 'district%5B34%5D=47', 
                   'district%5B35%5D=48', 'district%5B36%5D=49', 'district%5B37%5D=50', 
                   'district%5B38%5D=51', 'district%5B39%5D=52', 'district%5B3%5D=16', 
                   'district%5B40%5D=53', 'district%5B41%5D=54', 'district%5B42%5D=55', 
                   'district%5B43%5D=56', 'district%5B44%5D=57', 'district%5B45%5D=58', 
                   'district%5B46%5D=59', 'district%5B47%5D=60', 'district%5B48%5D=61', 
                   'district%5B49%5D=62', 'district%5B4%5D=17', 'district%5B50%5D=63', 
                   'district%5B51%5D=64', 'district%5B52%5D=65', 'district%5B53%5D=66', 
                   'district%5B54%5D=67', 'district%5B55%5D=68', 'district%5B56%5D=69', 
                   'district%5B57%5D=70', 'district%5B58%5D=71', 'district%5B59%5D=72', 
                   'district%5B5%5D=18', 'district%5B60%5D=73', 'district%5B61%5D=74', 
                   'district%5B62%5D=75', 'district%5B63%5D=76', 'district%5B64%5D=77', 
                   'district%5B65%5D=78', 'district%5B66%5D=79', 'district%5B67%5D=80', 
                   'district%5B68%5D=81', 'district%5B69%5D=82', 'district%5B6%5D=19', 
                   'district%5B70%5D=83', 'district%5B71%5D=84', 'district%5B72%5D=85', 
                   'district%5B73%5D=86', 'district%5B74%5D=87', 'district%5B75%5D=88', 
                   'district%5B76%5D=89', 'district%5B77%5D=90', 'district%5B78%5D=91', 
                   'district%5B79%5D=92', 'district%5B7%5D=20', 'district%5B80%5D=93', 
                   'district%5B81%5D=94', 'district%5B82%5D=95', 'district%5B83%5D=96', 
                   'district%5B84%5D=97', 'district%5B85%5D=98', 'district%5B86%5D=99', 
                   'district%5B87%5D=100', 'district%5B88%5D=101', 'district%5B89%5D=102', 
                   'district%5B8%5D=21', 'district%5B90%5D=103', 'district%5B91%5D=104', 
                   'district%5B92%5D=105', 'district%5B93%5D=106', 'district%5B94%5D=107', 
                   'district%5B95%5D=108', 'district%5B96%5D=109', 'district%5B97%5D=110', 
                   'district%5B98%5D=111', 'district%5B99%5D=112', 'district%5B9%5D=22'
                   ]