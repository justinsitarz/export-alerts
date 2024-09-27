#! /usr/bin/python

import csv
import requests
import json
import pandas as pd
import urllib.parse
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

## Fill in the 4 variables below ##

api_key = ''
csv_file = './alerts.csv' # set this to your desired path or leave the same to generate csv in same directory
alert_url = list_alert_url = "https://api.opsgenie.com/v2/alerts/" # set as https://api.eu.opsgenie.com/v2/users/ if account is in the EU region
logging.basicConfig(filename='script.log', level=logging.DEBUG)


def get_alert_ids():
    print('Retrieving alerts.', end='', flush=True)
    res = requests.get(url = list_alert_url, headers = api_headers)
    alerts = json.loads(res.text) 
    alert_ids = []
    for alert in alerts['data']:
        alert_ids.append(alert['id'])

    while alerts.get('paging') and 'next' in alerts['paging']:
        print('.', end='', flush=True)
        next_url = str(alerts['paging']['next'])
        res = requests.get(url = next_url, headers = api_headers)
        alerts = json.loads(res.text)
        if alerts.get('data'):    
            for alert in alerts['data']:
                alert_ids.append(alert['id'])
    return alert_ids

def runner():
    print('Starting runner...')
    threads = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        while alert_ids:
            for alert_id in alert_ids:
                threads.append(executor.submit(get_alert_data, alert_id))
            for task in as_completed(threads):
                logging.debug('Task dict: {}'.format(str(task.__dict__)))

def get_alert_data(alert_id):
    alert = {}
    res = requests.get(url = alert_url + alert_id, headers = api_headers)
    if res.status_code != 200:
        logging.error('Status: ' + str(res.status_code) + ' - - ' + 'Retrying alert: {}'.format(res.url)[35::])
        return
    alert_ids.remove(alert_id)
    alert_data = json.loads(res.text)['data']
    print('Alert: {} (tinyId {}) added'.format(alert_id, alert_data['tinyId']))

    alert['Message'] = alert_data.get('message')
    alert['Description'] = alert_data.get('description')
    alert['Status'] = alert_data.get('status')
    alert['Priority'] = alert_data.get('priority')
    alert['Tiny Id'] = alert_data.get('tinyId')
    alert['Alias'] = alert_data.get('alias')
    alert['Acknowledged'] = alert_data.get('acknowleged')
    alert['Tags'] = ', '.join(alert_data.get('tags'))
    alert['Snoozed'] = alert_data.get('snoozed')
    alert['Count'] = alert_data.get('count')
    alert['Last Occurred At'] = alert_data.get('lastOccurredAt')
    alert['Created At'] = alert_data.get('createdAt')
    alert['Updated At'] = alert_data.get('updatedAt')
    alert['Source'] = alert_data.get('source')
    alert['Owner'] = alert_data.get('owner')
    # alert['Teams'] = ', '.join(alert_data.get('teams'))
    # alert['Responders'] = ', '.join(alert_data.get('responders'))
    alert['Integration Id'] = alert_data.get('integration', {}).get('id')
    alert['Integration Name'] = alert_data.get('integration', {}).get('name')
    alert['Integration Type'] = alert_data.get('integration', {}).get('type')
    alert['Actions'] = ', '.join(alert_data.get('actions'))
    alert['Owner Team Id'] = alert_data.get('ownerTeamId')
    alert['Entity'] = alert_data.get('entity')
    alert['Seen'] = alert_data.get('seen')

    if alert_data['details']:
        for key, value in alert_data['details'].items():
            alert[key] = value
    
    alert_dict[alert_id] = alert


def generate_csv(alerts):
    df = pd.DataFrame(alerts)
    df_transposed = df.transpose()
    df_transposed.to_csv(csv_file, sep=',', encoding='utf-8')

def main():
    global api_key
    global alert_url
    global list_alert_url
    global api_headers
    global csv_file
    global alert_dict  
    global alert_ids  
    global search_query

    if api_key == '':
        api_key = input("API key: ")
   
    api_headers = {'Content-Type': 'application/json','Authorization':'GenieKey ' + api_key}
    alert_dict = {}
    list_alert_url += '?limit=100'

    search_query = input("Enter your search query (press enter to skip): ")
    start_date = input("Enter the start date *inclusive* in DD-MM-YYYY (ie. 21-04-1984) format. Press enter to skip: ")
    end_date = input("Enter the end date *exclusive* in DD-MM-YYYY (ie. 21-04-1984) format. Press enter to skip: ")
    query_string = '&query='

    if start_date and end_date:
        query_string += 'createdAt>={}+AND+createdAt<={}'.format(start_date, end_date)

    if search_query:
        formatted_query = urllib.parse.quote_plus(search_query)
        query_string += '+AND+{}'.format(formatted_query)

    
    list_alert_url += query_string
    logging.debug('List alert url: {}'.format(list_alert_url))
    print(list_alert_url)



    alert_ids = get_alert_ids()
    runner()
    generate_csv(alert_dict)


if __name__ == '__main__':
    main()
