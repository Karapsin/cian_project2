This repo contains scripts to parse data from the famous cian.ru. Below is the brief description of the repo structure.

# **py folder**
- **constants**: contains some usefuls constants as well as some long lists/dicts/sets which are used by other scripts.
- **utils**: contains functions/classes based on external libraries.
    - **SleepyScraper.py**: just a cloudscraper with some extra parameters related to the random sleep time. TO DO: add proxy support
    - **bot_funs.py**: functions to send messages via telegram bot
    - **csv_utils.py**: functions to manage so called "split csv", more on that later
    - **utils.py**: functions used mostly for convinient parsing
    - **data_cleaning_utils.py**: functions to clean and filter parsed data
      
