#-*- coding: UTF-8 -*-

"""here_geocoding.py: input file input.csv with ID, search will by geocode by Here API."""

__author__      = "Tomáš POHAHNKA"
__copyright__   = "Copyright 2020, Czech Republic"
__email__ = "pohankatomas@email.cz"
import json
import csv
import logging
import urllib.request
import time
import sys

original_json="""{"Response": {
    "MetaInfo": {
        "Timestamp": "2019-12-24T16:11:10.532+0000"
    },
    "View": [
        {
        "_type": "SearchResultsViewType",
        "ViewId": 0,
        "Result": [
        {
            "Relevance": 1,
            "MatchLevel": "houseNumber",
            "MatchQuality": {
                "City": 1,
                "Street": [
                    1
                ],
                "HouseNumber": 1
            },
            "MatchType": "pointAddress",
            "Location": {
                "LocationId": "NT_nMS7m.lnkcAJGnQB2RC9nC_yEDM0A",
                "LocationType": "point",
                "DisplayPosition": {
                    "Latitude": 49.6102,
                    "Longitude": 15.58147
                },
                "NavigationPosition": [
                    {
                    "Latitude": 49.6098,
                    "Longitude": 15.58228
                    }
                ],
                "MapView": {
                    "TopLeft": {
                        "Latitude": 49.6113242,
                        "Longitude": 15.5797352
                    },
                    "BottomRight": {
                        "Latitude": 49.6090758,
                        "Longitude": 15.5832048
                    }
                },
                "Address": {
                    "Label": "U Trojice 2104, 580 01 Havlíčkův Brod, Česká Republika",
                    "Country": "CZE",
                    "State": "Vysočina",
                    "County": "Havlíčkův Brod",
                    "City": "Havlíčkův Brod",
                    "District": "Havlíčkův Brod",
                    "Street": "U Trojice",
                    "HouseNumber": "2104",
                    "PostalCode": "580 01",
                    "AdditionalData": [
                        {
                            "value": "Česká Republika",
                            "key": "CountryName"
                        },
                        {
                            "value": "Vysočina",
                            "key": "StateName"
                        },
                        {
                            "value": "Havlíčkův Brod",
                            "key": "CountyName"
                        }
                    ]
                }
            }
        }
    ]
    }
    ]
    }
    }"""

API_KEY = 'Y-CxAXWge8aXPvwUbhAGCzaBSXT105qphtblp3WiYdk'

LINK = 'https://geocoder.ls.hereapi.com/6.2/geocode.json?'

LOG_FILE = 'log.log'
ADDRESSES_CSV = 'input.csv'
logging.basicConfig(filename=LOG_FILE, level = logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

logging.info("script run")

with open(ADDRESSES_CSV, encoding='utf-8') as csv_file:
    reader = csv.reader(csv_file)
    link_lines = []
    for row in reader:
        # id, adresse can contain , [[1,"address 1, city xxx xx"],[2, "address 2, city 2, xxx xx"]]
        row = ', '.join(row).replace(' ', '+').split(',',1)
        link_lines.append(row)

all_values = []
bad_values = []

multi_values = []
for id_row, address  in link_lines:
    link = LINK + 'apiKey={}&searchtext={}'.format(API_KEY, urllib.parse.quote_plus(address))
    log_link = LINK + 'searchtext={}&apiKey='.format(urllib.parse.quote_plus(address),)
    print("geocoding row: ", id_row)
    try:
        try:
            time.sleep(0.1)
            req=urllib.request.Request(link, None, {'User-Agent': 'Mozilla/5.0 (X11; Linux i686)'})
            result = urllib.request.urlopen(req)
            logging.info('connected to: ' + log_link)
        except urllib.error.URLError as e:
            logging.critical('chyba: ' + e)
            break
    except:
        logging.critical('NOT connected to: ' + log_link)
        continue
    
    json_data = json.load(result)
    #print(json_data)
    
    multivalue = False
    json_values = json_data['Response']['View']

    if len(json_values) == 0:
        a = [x for x in address.replace('+', ' ').split(',')]
        bad_values.append([id_row] + a)
        all_values.append([id_row, '', '', '', '', '', \
            '', '', '', '', '', '', '', '', '', '', 'not found'])
        continue
    
    if len(json_values[0]['Result']) > 1:
        multivalue = True
    first_occurence = False
    for detail in json_values[0]['Result']:
        #detail = json_values[0]['Result'][0]
        relevance = detail['Relevance']
        matchlevel = detail['MatchLevel']
        latitude = detail['Location']['DisplayPosition']['Latitude']
        longitude = detail['Location']['DisplayPosition']['Longitude']
        lat_nav = detail['Location']['NavigationPosition'][0]['Latitude']
        lon_nav = detail['Location']['NavigationPosition'][0]['Longitude']
        address = detail['Location']['Address']
        
        
        try:
            label = address['Label']
        except:
            label = ""
        
        try:
            country = address['Country']
        except:
            country = ""

        try:
            state = address['State']
        except:
            state = ""
                
        try:
            county = address['County']
        except:
            county = ""
        
        
        try:
            city = address['City']
        except:
            city = ""
        
        try:
            district = address['District']
        except:
            district = ""

        try:
            street = address['Street']
        except:
            street = ""

        try:
            houseNum = address['HouseNumber']
        except:
            houseNum = ""

        try:
            postal = address['PostalCode']
        except:
            postal = ""    
        

        if not multivalue:
            
            all_values.append([id_row,latitude, longitude, lat_nav, lon_nav, label, \
            country, state, county, city, district, street, houseNum, postal, relevance, matchlevel, 'OK'])
        else:
            multi_values.append([id_row,latitude, longitude, lat_nav, lon_nav, label, \
            country, state, county, city, district, street, houseNum, postal, relevance, matchlevel])
            
            if not first_occurence:
                all_values.append([id_row,latitude, longitude, lat_nav, lon_nav, label, \
                country, state, county, city, district, street, houseNum, postal, relevance, matchlevel, 'multivalue'])
                first_occurence = True

if multi_values:
    with open("multivalue.csv", "w",newline="", encoding='utf-8') as f:
        writer = csv.writer(f, delimiter = ';', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(['ID', 'latitude', 'longitude', 'lat_nav', 'lon_nav', 'label', \
            'country', 'state', 'county', 'city', 'district', 'street', 'houseNum', 'postal', 'relevance', 'matchlevel'])
        writer.writerows(multi_values)
if all_values:
    with open("output.csv", "w",newline="", encoding='utf-8') as f:
        writer = csv.writer(f, delimiter = ';', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(['ID', 'latitude', 'longitude', 'lat_nav', 'lon_nav', 'label', \
                'country', 'state', 'county', 'city', 'district', 'street', 'houseNum', 'postal', 'relevance', 'matchlevel', 'status'])
        writer.writerows(all_values)
if bad_values:
    with open("not found.csv", "w",newline="", encoding='utf-8') as f:
        writer = csv.writer(f, delimiter = ',', quoting=csv.QUOTE_NONE)
        writer.writerow(['ID', 'address'])
        writer.writerows(bad_values)
logging.info('successful finish')