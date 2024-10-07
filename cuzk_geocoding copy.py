import os
import json
import csv
import logging
import requests
import time
from pprint import pprint

## https://ags.cuzk.cz/arcgis/rest/services/RUIAN/Vyhledavaci_sluzba_nad_daty_RUIAN/MapServer/exts/GeocodeSOE/find



LOG_FILE = 'cuzk_log.log'
ADDRESSES_CSV = 'input.csv'

LINK_FIND = 'https://ags.cuzk.cz/arcgis/rest/services/RUIAN/Vyhledavaci_sluzba_nad_daty_RUIAN/MapServer/exts/GeocodeSOE/find'
STATIC_PARAMS = '&bbox=&location=&distance=&outSR=4326&outFields=&maxLocations=&magicKey=&f=pjson'

logging.basicConfig(filename=LOG_FILE, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info("script run")

with open(ADDRESSES_CSV, encoding='utf-8') as csv_file:
    reader = csv.reader(csv_file)
    link_lines = []
    for row in reader:
        # id, address can contain [[1,"address 1 city xxx xx"],[2, "address 2 city 2, xxx xx"]]
        row = ','.join(row).replace('  ', '+').replace(' ', '+').split(',', 1)
        link_lines.append(row)

for id_row, address in link_lines:
    link = f"{LINK_FIND}?text={address}{STATIC_PARAMS}"
    pprint(link)

    try:
        time.sleep(1)  # to avoid overwhelming the server
        response = requests.get(link)
        response.raise_for_status()
        logging.info('connected to: ' + link)
    except Exception as e:
        logging.critical(f'Error connecting to: {link}, {e}')
        continue

    response_json = response.json()

    # Handle not found (response_json == [])
    if len(response_json) == 0:
        with open("not_found.csv", "a", newline="", encoding='utf-8') as f:
            writer = csv.writer(f, delimiter=',', quoting=csv.QUOTE_NONE)
            if not os.path.exists("not_found.csv") or os.stat("not_found.csv").st_size == 0:
                writer.writerow(['ID', 'Address', 'Status'])
            writer.writerow([id_row, address.replace('+', ' '), 'not found'])
        logging.info(f"Not found for ID {id_row}: {address}")
        pprint({id_row})
        continue

    # Handle multiple entries
    if len(response_json["locations"]) > 1:
        with open("multivalue.csv", "a", newline="", encoding='utf-8') as f:
            writer = csv.writer(f, delimiter=';', quoting=csv.QUOTE_MINIMAL)
            if not os.path.exists("multivalue.csv") or os.stat("multivalue.csv").st_size == 0:
                writer.writerow(['ID', 'xmax', 'xmin', 'ymax', 'ymin', 'city', \
                                 'country', 'addr_type', 'loc_name', 'match_addr', 'types', 'score'])
            for location in response_json["locations"]:
                attributes = location["feature"]["attributes"]
                writer.writerow([id_row,
                                 attributes["Xmax"], attributes["Xmin"],
                                 attributes["Ymax"], attributes["Ymin"],
                                 attributes.get("City", ''),
                                 attributes.get("Country", ''),
                                 attributes.get("Addr_type", ''),
                                 attributes.get("Loc_name", ''),
                                 attributes.get("Match_addr", ''),
                                 attributes.get("Type", ''),
                                 attributes.get("Score", '')])
        logging.info(f"Multiple values found for ID {id_row}: {address}")
        continue

    # Handle single entry (len(response_json["locations"]) == 1)
    if len(response_json["locations"]) == 1:
        attributes = response_json["locations"][0]["feature"]["attributes"]
        with open("output.csv", "a", newline="", encoding='utf-8') as f:
            writer = csv.writer(f, delimiter=';', quoting=csv.QUOTE_MINIMAL)
            if not os.path.exists("output.csv") or os.stat("output.csv").st_size == 0:
                writer.writerow(['ID', 'xmax', 'xmin', 'ymax', 'ymin', 'city', \
                                 'country', 'addr_type', 'loc_name', 'match_addr', 'types', 'score'])
            writer.writerow([id_row,
                             attributes["Xmax"], attributes["Xmin"],
                             attributes["Ymax"], attributes["Ymin"],
                             attributes.get("City", ''),
                             attributes.get("Country", ''),
                             attributes.get("Addr_type", ''),
                             attributes.get("Loc_name", ''),
                             attributes.get("Match_addr", ''),
                             attributes.get("Type", ''),
                             attributes.get("Score", '')])
        logging.info(f"Single value found for ID {id_row}: {address}")

