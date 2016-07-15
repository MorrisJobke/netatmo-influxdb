#!/usr/bin/env python3

import requests
from influxdb import InfluxDBClient
from datetime import datetime, timezone

CLIENT_ID = ''
CLIENT_SECRET = ''
NETATMO_USERNAME = ''
NETATMO_PASSWORD = ''

_ALLOWED_TYPES = ('Temperature', 'CO2', 'Humidity', 'Pressure', 'Noise', 'Rain', 'WindStrength', 'WindAngle', 'GustStrenght', 'GustAngle')


def getAccessToken():
    '''
    returns:
     { access_token, expires_in, refresh_token }
    '''
    payload = {
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'grant_type': 'password',
        'username': NETATMO_USERNAME,
        'password': NETATMO_PASSWORD,
        'scope': 'read_station'
    }

    r = requests.post('https://api.netatmo.com/oauth2/token', data=payload)

    return r.json()

def refreshToken(refreshToken):
    '''
    returns:
     { access_token, expires_in, refresh_token }
    '''
    payload = {
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'grant_type': 'refresh_token',
        'refresh_token': refresh_token
    }

    r = requests.post('https://api.netatmo.com/oauth2/token', data=payload)

    return r.json()

def getStationInfo(access_token):
    payload = {
        'access_token': access_token,
    }

    r = requests.get('https://api.netatmo.com/api/getstationsdata', params=payload)

    return r.json()

def getMeasure(access_token, device_id, module_id, measurement_type, date_begin):
    if measurement_type not in _ALLOWED_TYPES:
        print('not allowed type "%s"'%measurement_type)
        return

    payload = {
        'access_token': access_token,
        'device_id': device_id,
        'module_id': module_id,
        'scale': 'max',
        'optimize': 'false',
        'type': measurement_type,
        'date_begin': date_begin
    }

    r = requests.get('https://api.netatmo.com/api/getmeasure', params=payload)

    return r.json()

def printStation(station):
    if 'station_name' in station:
        name = station['station_name'] + ' - ' + station['module_name']
        indent = ''
        last_seen = station['last_status_store']
    else:
        name = station['module_name']
        indent = '\t'
        last_seen = station['last_seen']

    last_seen = datetime.fromtimestamp(last_seen)
    print('%sName: %s ID: %s Last seen: %s\n%sData types: %s '%(
        indent, name, station['_id'], last_seen.isoformat(), indent, station['data_type']
    ))

def getInfluxDBClient():
    client = InfluxDBClient()
    if {'name': 'netatmo'} not in client.get_list_database():
        client.create_database('netatmo')

    return client

def iterateStations(access_token):
    client = getInfluxDBClient()
    station_info = getStationInfo(access_token)
    if 'body' not in station_info:
        raise Exception(station_info)
    stations = station_info['body']['devices']

    for station in stations:
        printStation(station)
        for measurement_type in station['data_type']:
            fetchMeasurements(access_token, station['_id'], "", measurement_type, station['station_name'], station['module_name'], client)

        print('Modules:')
        for substation in station['modules']:
            printStation(substation)
            for measurement_type in substation['data_type']:
                fetchMeasurements(access_token, station['_id'], substation['_id'], measurement_type, station['station_name'], substation['module_name'], client)

def fetchMeasurements(access_token, device_id, module_id, measurement_type, station_name, module_name, client):
    get_latest_timestamp_query = "SELECT value FROM %s WHERE station='%s' AND module='%s' ORDER BY time DESC LIMIT 1"%(measurement_type, station_name, module_name)
    result = client.query(get_latest_timestamp_query, database='netatmo')

    time = 0
    points = result.get_points()
    for point in points:
        time = int(datetime.strptime(point['time'], '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc).timestamp())
        break

    time += 1
    measurements = getMeasure(access_token, device_id, module_id, measurement_type, time)
    if 'body' not in measurements:
        raise Exception(measurements)
    measurements = measurements['body']

    min_time = 0
    max_time = 0
    data = []
    for time in measurements:
        timestamp = int(time)
        data.append({
            "measurement": measurement_type,
            "tags": {
                "station": station_name,
                "module": module_name
            },
            "time": timestamp,
            "fields": {
                "value": float(measurements[time][0])
            }
        })
        if min_time == 0 or min_time > timestamp:
            min_time = timestamp
        if max_time == 0 or max_time < timestamp:
            max_time = timestamp

    if client.write_points(
        data,
        time_precision='s',
        database='netatmo'
    ):
        print('%i points written - %s, %s, %s - start %s - end %s'%(len(data), station_name, module_name, measurement_type, datetime.fromtimestamp(min_time).isoformat(), datetime.fromtimestamp(max_time).isoformat()))
        if len(data) == 102244:
            fetchMeasurements(access_token, device_id, module_id, measurement_type, station_name, module_name, client)
    else:
        print('write failed')

token_info = getAccessToken()
# token_info = refreshToken(token_info['refresh_token'])

iterateStations(token_info['access_token'])
