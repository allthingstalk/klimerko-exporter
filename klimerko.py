#!/usr/bin/env python3

import csv
import furl
import requests
import json
import datetime
import tqdm
import os
import dateutil.parser
from itertools import groupby

DAYS = os.environ.get('DAYS', None)
USER = os.environ.get('USER', None)
PASSWORD = os.environ.get('PASSWORD', None)
GROUND = os.environ.get('GROUND', None)

URL = 'https://api.allthingstalk.io'

def get_token(username, password):
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    data = 'grant_type=password&username=%s&password=%s&client_id=web' % (username, password)
    r = requests.post('%s/login' % URL, headers=headers, data=data)
    return r.json()['access_token']

def get_devices(token):
    return requests.get('%s/ground/%s/devices?includeAssets=true' % (URL, GROUND),
                        headers={'Authorization': 'Bearer ' + token}).json()

def get_aggregated(token, device, asset, progress=None, days=None):
    to_date = datetime.datetime.utcnow()
    if days is None:
        from_date = dateutil.parser.parse(device['createdOn'][:-1])
    else:
        from_date = to_date - datetime.timedelta(days=float(days))
    link = '%s/asset/%s/activity?resolution=hour&from=%s&to=%s' % (
        URL, asset['id'], from_date.isoformat(), to_date.isoformat()
    )

    frm = furl.furl(link).args['from']
    if progress:
        progress.set_description(desc=asset['id'] + ', 50k: ' + frm)
    r = requests.get(link,
                     headers={'Authorization': 'Bearer ' + token}).json()

    if not r:
        return
    for data in r.get('data', []):
        if data['data'] is None:
            continue
        yield [data['at'], data['data']['avg']]

def get_history(token, device, progress=None, days=None):
    skip = ['wifi-signal' 'height', 'location', 'interval', 'air-quality', 'watchdog']
    results = []
    for asset in device['assets']:
        if asset['name'] in skip:
            continue
        if asset['title'] in ['Watchdog']:
            continue
        for agg in get_aggregated(token, device, asset, progress=progress, days=days):
            results.append([agg[0], asset['name'], agg[1]])

    return [{
        'device_id': device['id'],
        'timestamp': ts, **{name: val for _, name, val in data}}
            for ts, data in groupby(sorted(results, key=lambda x: x[0]), lambda x: x[0])]

def get_loc(d):
    assets =  [a for a in d['assets'] if a.get('title', '') == 'Location']
    if not assets or not assets[0]['state']:
        return ''
    v = assets[0]['state']['value']
    return '%s,%s' % (v['latitude'], v['longitude'])

token = get_token(USER, PASSWORD)
devices = get_devices(token)['items']

history = []
progress = tqdm.tqdm(devices)


with open('export.csv', 'w') as csvfile:
    fieldnames = ['timestamp', 'device_id', 'device_title', 'location', 'pm10',
                  'humidity', 'pm1', 'pm2-5', 'pressure', 'temperature', 'uv-index']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for d in progress:
        for row in get_history(token, d, progress=progress, days=DAYS):
            row = {k: v for k, v in row.items() if k in fieldnames}
            writer.writerow({'device_title': d['title'], 'location': get_loc(d), **row})
