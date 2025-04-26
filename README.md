This repo contains scripts to parse data from the famous cian.ru. Below is the brief description of the repo structure.

# **py folder**

This section describes python scripts. The main idea is that we have 5 layers of abstraction:
1) constants (layer 0).
2) functions/classes based on external libraries  (layer 1).
3) functions which are loading and parsing state json (layer 2) (based on layer 1).
4) functions to manage layer 2, i.e. which are telling which URL to parse and where to store it (layer 3) (based on layer 2).
5) scripts which start parsing, report about errors and define state (layer 4).

Below is the information about each folder and how it is related to every layer.

- **constants**: contains some useful constants as well as some long lists/dicts/sets which are used by other scripts.
  
- **utils**: contains functions/classes based on external libraries.
    - **SleepyScraper.py**: just a cloudscraper with some extra parameters related to the random sleep time. TO DO: add proxy support.
    - **bot_funs.py**: functions to send messages via telegram bot.
    - **csv_utils.py**: functions to manage so called "splitted csv", more on that later.
    - **utils.py**: functions used mostly for convenient parsing.
    - **data_cleaning_utils.py**: functions to clean and filter parsed data.
      
- **parsing_funs**: functions to do parsing.
    - **search_parsing_logic.py**: functions to load and parse state json from the search pages.
    - **search_parsing.py**: functions which are organising search parsing by specifying which URL to parse and where to store the results.
    - **offer_parsing_logic.py**: state json parsing logic (offer pages).
    - **offer_parsing.py**: offer parsing organisation, which URL to parse, some errors handling.
 
- **run_search_parsing.py**: responsible for starting search page parsing and reporting about it.
- **run_offer_parsing.py**: responsible for sampling of URL for offer parsing and reporting about it.
          
# **data_load folder**

This folder contains parsed data and stores the state of the unfinished parsing. Some folders contain so called "splitted csv", i.e. single csv divided into multiple chunks to fit into the git repo. Functions from **py.utils.csv_utils.py** allow to manage it quite effectively by creating splitted csv from a single one, appending data or removing all chunks.

- **csv_search_parsed**: splitted csv with the parsed data from the search pages. Later it is cleaned and reallocated to **csv_search_clean**.
- **csv_search_clean**: splitted csv with the clean parsed data from the search pages. 
- **csv_offer_pages_parsed**: splitted csv with the parsed offers data. TO DO: write a cleaning procedure for it once we have enough data.
  
- **long_rent_ads**, **short_rent_ads**, **sale_primary_ads**, **sale_secondary_ads**: contains photos, raw html and state json for each parsed offer
  
- **search_parsing_progress.csv**: contains districts and deal types for which information is parsed already at this search parsing iteration.
- **offers_to_parse**: contains URL of offers to parse in this iteration of the offers parsing procedure.
- **offers_parsed.csv**: contains URL of offers which are already parsed in this iteration of the offers parsing procedure


