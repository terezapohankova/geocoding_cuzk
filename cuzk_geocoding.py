import os
import json
import csv
import logging
import requests
import time
from pprint import pprint

LOG_FILE = 'cuzk_log.log'
ADDRESSES_CSV = 'input.csv'


## https://ags.cuzk.cz/arcgis/rest/services/RUIAN/Vyhledavaci_sluzba_nad_daty_RUIAN/MapServer/exts/GeocodeSOE/find CUZK API
LINK_FIND = 'https://ags.cuzk.cz/arcgis/rest/services/RUIAN/Vyhledavaci_sluzba_nad_daty_RUIAN/MapServer/exts/GeocodeSOE/find'
STATIC_PARAMS = '&bbox=&location=&distance=&outSR=4326&outFields=&maxLocations=&magicKey=&f=pjson'

# Log file to record any errors or important events.
logging.basicConfig(filename=LOG_FILE, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info("script run")

# CSV file containing input addresses to process.
with open(ADDRESSES_CSV, encoding='utf-8') as csv_file:
    reader = csv.reader(csv_file)
    link_lines = []
    for row in reader:
        # id, address can contain [[1,"address 1 city xxx xx"],[2, "address 2 city 2, xxx xx"]]
        row = ','.join(row).replace('  ', '+').replace(' ', '+').split(',', 1)
        link_lines.append(row)

# Base URL of the Geocode API service.
for id_row, address in link_lines:
    link = f"{LINK_FIND}?text={address}{STATIC_PARAMS}"
    pprint(link)

    # send a task to the server based on the link above
    try:
        time.sleep(1)  # to avoid overwhelming the server
        response = requests.get(link)
        response.raise_for_status()
        logging.info('connected to: ' + link)
    except Exception as e:
        logging.critical(f'Error connecting to: {link}, {e}')
        continue
    
    #get a JSON response
    response_json = response.json()
    
    # Check if the response contains any locations
    if "locations" in response_json and len(response_json["locations"]) > 0:
        # Handle multiple entries of location
        # write them in multivalue.csv -> write each entry
        if len(response_json["locations"]) > 1:
            with open("multivalue.csv", "a", newline="", encoding='utf-8') as f:
                writer = csv.writer(f, delimiter=';', quoting=csv.QUOTE_MINIMAL)
                if not os.path.exists("multivalue.csv") or os.stat("multivalue.csv").st_size == 0:
                    writer.writerow(['ID', 'x', 'y', 'city', \
                                     'country', 'addr_type', 'loc_name', 'match_addr', 'types'])
                for location in response_json["locations"]:
                    attributes = location["feature"]["attributes"]
                    geom = response_json["locations"][0]["feature"]["geometry"]
                    writer.writerow([id_row,
                                     geom["x"],
                                     geom["y"],
                                     attributes.get("City", ''),
                                     attributes.get("Country", ''),
                                     attributes.get("Addr_type", ''),
                                     attributes.get("Loc_name", ''),
                                     attributes.get("Match_addr", ''),
                                     attributes.get("Type", '')
                                     ])
                    ### aatribute score was left out -> every multivalue has score 100
            logging.info(f"Multiple values found for ID {id_row}: {address}")
            continue

        # Handle single entry
        if len(response_json["locations"]) == 1:
            attributes = response_json["locations"][0]["feature"]["attributes"]
            geom = response_json["locations"][0]["feature"]["geometry"]
            with open("output.csv", "a", newline="", encoding='utf-8') as f:
                writer = csv.writer(f, delimiter=';', quoting=csv.QUOTE_MINIMAL)
                if not os.path.exists("output.csv") or os.stat("output.csv").st_size == 0:
                    writer.writerow(['ID', 'x', 'y', 'city', \
                                     'country', 'addr_type', 'loc_name', 'match_addr', 'types'])
                writer.writerow([id_row,
                                 geom["x"],
                                 geom["y"],
                                 attributes.get("City", ''),
                                 attributes.get("Country", ''),
                                 attributes.get("Addr_type", ''),
                                 attributes.get("Loc_name", ''),
                                 attributes.get("Match_addr", ''),
                                 attributes.get("Type", ''),
                                 ])
            logging.info(f"Single value found for ID {id_row}: {address}")
            continue
    else:
        # If no locations are found, write to not_found.csv
        with open("not_found.csv", "a", newline="", encoding='utf-8') as f:
            writer = csv.writer(f, delimiter=',', quoting=csv.QUOTE_MINIMAL, escapechar='\\')
            if not os.path.exists("not_found.csv") or os.stat("not_found.csv").st_size == 0:
                writer.writerow(['ID', 'Address', 'Status'])
            writer.writerow([id_row, address.replace('+', ' '), 'not found'])

        logging.info(f"Not found for ID {id_row}: {address}")