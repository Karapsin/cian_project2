This repo contains scripts to parse data from the famous cian.ru. Below is the brief description of the repo structure.

# **py folder**

The main idea is that we have 4 layers of abstraction:
1) functions/classes bases on external librarires
2) functions which are loading and parsing state json (based on layer 1)
3) functions to manage layer 2, i.e. which are telling which URL to parse and where to store it (based on layer 2)
4) scripts which start parsing, report about errors and define state

- **constants**: contains some usefuls constants as well as some long lists/dicts/sets which are used by other scripts.
  
- **utils**: contains functions/classes based on external libraries.
    - **SleepyScraper.py**: just a cloudscraper with some extra parameters related to the random sleep time. TO DO: add proxy support
    - **bot_funs.py**: functions to send messages via telegram bot
    - **csv_utils.py**: functions to manage so called "split csv", more on that later
    - **utils.py**: functions used mostly for convinient parsing
    - **data_cleaning_utils.py**: functions to clean and filter parsed data
      
- **parsing_funs**: functions to do parsing
    - **search_parsing_logic.py**: functions to load and parse state json from the search pages
    - **search_parsing.py**: functions which are organising search parsing by specifying which URL to parse and where to store the results
    - **offer_parsing_logic.py**: state json parsing logic (offer pages)
    - **offer_parsing.py**: offer parsing organisation, which URL to parse, some errors handling
 
- **run_search_parsing.py**: responsible for starting search page parsing and reporting about it.
- **run_offer_parsing.py**: responsible for sampling of URL for offer parsing and reporting about it
          
