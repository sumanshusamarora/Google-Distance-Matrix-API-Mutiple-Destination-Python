# -*- coding: utf-8 -*-
"""
Created on Thu Oct 10 10:04:30 2019

@author: suman
"""

import pandas as pd
import numpy as np
from googlegeocoder import GoogleGeocoder
import googlemaps
import os
import requests

folder_path = r'F:\Upwork\Geo-cordiantes mapping and distance calculation'
excel_file_name = "Watewater UPS Sites- WRE List.xlsx"
API_KEY = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXX'



data = pd.read_excel(os.path.join(folder_path, excel_file_name))
data["longitude"] = 0.000000000000000000000000000
data["lattitude"] = 0.000000000000000000000000000
data["Cordinates"] = None

i=0
for i in range(0, len(data)):
    geocoder = GoogleGeocoder(API_KEY)
    try:
        #search = geocoder.get(data["Location"][i])#+ " " + data["City"][i]+ " " + data["State"][i])
        search = geocoder.get(data["Address"][i] + " " + data["City"][i]+ " " + data["State"][i])
        cord = search[0].geometry.location
        data.loc[i, "Cordinates"] = cord
        cord_list = str(cord).split(',')
        lattitude = cord_list[0].replace('(', '')
        lattitude = lattitude.replace(')', '')
        lattitude = lattitude.replace(' ', '')
        
        longitude = cord_list[1].replace(')', '')
        longitude = longitude.replace("'", '')
        longitude = longitude.replace(' ', '')
        
        data.at[i, "longitude"] = longitude
        data.at[i, "lattitude"] = lattitude
    except:
        continue

data.to_csv(os.path.join(folder_path, excel_file_name.split(".")[0] + " - upwork_updated.csv"))
#data = pd.read_csv(os.path.join(r"F:\Upwork\Geo-cordiantes mapping and distance calculation", excel_file_name.split(".")[0] + " - upwork_updated.csv"))

data2 = pd.read_excel(os.path.join(folder_path, excel_file_name), sheet_name='Branch Locations')
data2["longitude"] = 0.000000000000000000000000000
data2["lattitude"] = 0.000000000000000000000000000
data2["Cordinates"] = None
for i in range(len(data2)):
    geocoder = GoogleGeocoder(API_KEY)
    try:
        search = geocoder.get(data2["Address"][i])#+ " " + data2["City"][i]+ " " + data2["State"][i])
        cord = search[0].geometry.location
        data2.loc[i, "Cordinates"] = cord
        cord_list = str(cord).split(',')
        lattitude = cord_list[0].replace('(', '')
        lattitude = lattitude.replace(')', '')
        lattitude = lattitude.replace(' ', '')
        
        longitude = cord_list[1].replace(')', '')
        longitude = longitude.replace("'", '')
        longitude = longitude.replace(' ', '')
        
        data2.at[i, "longitude"] = longitude
        data2.at[i, "lattitude"] = lattitude
    except:
        continue
data2.to_csv(os.path.join(folder_path, "data2_temp.csv"))
#data2 = pd.read_csv(os.path.join(folder_path, "data2_temp.csv"))


gmaps = googlemaps.Client(key=API_KEY)

data["Nearest Branch"] = None
data["Distance"] = None
data["Drive Duration"] = None


destination_cords_list = []
for i in range(0, len(data2["Branch"]), 25):
    destination_cords = ""
    min_25_len_rem = min(25, len(data2["Branch"][i:]))
    for k in range(min(25, min_25_len_rem)):
        try:
            destination_cords += str(data2['lattitude'][i+k]) + ', ' + str(data2['longitude'][i+k])
            if k < min(25, min_25_len_rem)-1:
                destination_cords += ' | '
        except:
            break
    destination_cords_list.append(destination_cords)


#data = data.iloc[:5,:]
#outer_i = 0
#inner_i = 0
#destination = destination_cords_list[0]
for outer_i in range(0, len(data)):
    distance_result = []
    duration_result = []
    outer_lat = str(data["lattitude"][outer_i])
    outer_lon = str(data["longitude"][outer_i])
    try:
        for destination in destination_cords_list:
            response = requests.get('https://maps.googleapis.com/maps/api/distancematrix/json', params={"origins":outer_lat+', '+outer_lon, "destinations":destination, "key":API_KEY})
            dist_matrix = response.json()
            for dist_i in range(len(dist_matrix["rows"][0]["elements"])):
                distance_result.append(dist_matrix["rows"][0]["elements"][dist_i]["distance"]["value"])
                duration_result.append(dist_matrix["rows"][0]["elements"][dist_i]["duration"]["value"])
                    
        min_distance = min(distance_result)
        min_distance_index = [i for i, val in enumerate(distance_result) if val == min_distance][0]
        duration = duration_result[min_distance_index]
        data.loc[outer_i,"Nearest Branch"] = data2["Branch"][min_distance_index]
        data.loc[outer_i,"Distance"] = (min_distance/1000)*0.621371
        data.loc[outer_i,"Drive Duration"] = duration/3600
    except:
        pass


data.to_csv(os.path.join(folder_path, "Temp File.csv"))
 
