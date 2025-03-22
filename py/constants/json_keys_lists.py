# to get an info from search and offer pages, we are using some very nested jsons.
# processing involves checking A LOT OF keys, and when all of them written in the script,
# readability falls drammatically
# the following objects are helping to avoid that

######################################################################################################
# search page
######################################################################################################

# needed to have consistent names in both data frames
search_offer_cols_map = {"fullUrl": "url",
                         "rentByPartsDescription": "isRentByParts",
                         "isDuplicatedDescription": "isDuplicate",
                         "userInput": "parsed_address",
                         "user":"author_info",
                         "saleType": "sale_terms",
                         "price": "priceTotal"
                        }
# offer
search_keys1 = ['demolishedInMoscowProgramm','fullUrl','isAuction','creationDate',
                'hasFurniture','cadastralNumber','rentByPartsDescription','totalArea',
                'balconiesCount','isApartments','floorNumber','title','roomsCount','kp',
                'decoration','isByHomeowner','rosreestrCheck','livingArea','kitchenArea',
                'loggiasCount','bedroomsCount','description','isDuplicatedDescription',
                'user'
               ]

# offer['geo']
search_keys2 = ['userInput', 'coordinates', 'railways',
                'undergrounds', 'jk'
               ]


# offer['building']
search_keys3 = ['parking', 'type', 'passengerLiftsCount',
                'cargoLiftsCount', 'materialType', 'floorsCount',
                'classType', 'buildYear', 'deadline'
               ]


# offer['bargainTerms']
search_keys4 = ['mortgageAllowed', 'saleType', 'priceType', 'vatType',
                'price', 'utilitiesTerms', 'agentBonus', 'deposit',
                'agentFee', 'bargainAllowed', 'currency'
               ]

search_keys_meta_list = [search_keys1, search_keys2, search_keys3, search_keys4]

######################################################################################################
# offer page 
######################################################################################################
# ad_data['newbuilding']
keys_list1 = ['isFromDeveloper', 
              'isFromBuilder', 
              'isFromSeller', 
              'isFromLeadFactory'
             ]  

# ad_data['building']
keys_list2 = ['floorsCount', 'buildYear', 'parking',
              'ceilingHeight', 'cranageTypes','houseMaterialType'
             ]    

# offer_json['offerData']['bti']['houseData']
keys_list3 = ['entrances', 'flatCount', 'isEmergency',
              'houseHeatSupplyType', 'houseOverhaulFundType',
              'houseGasSupplyType', 'houseMaterialType', 
              'houseOverlapType', 'seriesName', 'lifts'
             ]

# ad_data
keys_list4 = [
                'creationDate', 'editDate', 'externalOfferUrl', 'isIllegalConstruction',
                'isCianPartner', 'isDuplicate', 'isUniqueCheckDate', 'isUniqueForCian',
                'isUnique', 'moderationInfo', 'userTrustLevel', 'userTrust', 
                'demolishedInMoscowProgramm', 'isRentByParts', 'priceTotal',
                'floorNumber', 'roomsCount', 'repairType', 'windowsViewType',
                'totalArea', 'kitchenArea', 'isPenthouse', 'isApartments', 
                'flatType', 'isInHiddenBase', 'isObjectHidden', 'isClosedVisibility',
                'videos', 'title','description'
             ]

keys_meta_list = [keys_list1, keys_list2, keys_list3, keys_list4]