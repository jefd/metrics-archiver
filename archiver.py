import requests, sys
import json
from datetime import datetime
from tk import *
import sqlite3


DB = 'metrics.db'

REPOS = [
        {'owner': 'ufs-community', 'name': 'ufs-weather-model', 'token': TOKEN },
        {'owner': 'ufs-community', 'name': 'ufs-srweather-app', 'token': TOKEN },
]

# map of metric name to github api path
METRICS = {'views': '/traffic/views',
           'clones': '/traffic/clones',
}

def get_latest(con, table_name):
    try:
        cursor = con.cursor()
        sql = f'''select timestamp from "{table_name}" order by timestamp desc limit 1;''' 
        cursor.execute(sql)
        latest_timestamp = cursor.fetchone()
        cursor.close()
        return latest_timestamp[0]
    except:
        return None


def prune_list(lst, latest):
    if not latest:
        return lst
    return [obj for obj in lst if obj['timestamp'] > latest]


def insert(con, table_name, count, uniques, timestamp):
    cursor = con.cursor()
    insertQuery = f"""INSERT INTO "{table_name}"
        VALUES (?, ?, ?);"""

    cursor.execute(insertQuery, (count, uniques, timestamp))
    cursor.close()


def insert_metric(con, table_name, dct):
    # {'timestamp': '2022-08-19T00:00:00Z', 'count': 105, 'uniques': 19}
    count = dct['count']
    uniques = dct['uniques']
    timestamp = dct['timestamp']

    insert_query = f"""INSERT INTO "{table_name}"
        VALUES (?, ?, ?);"""

    cursor = con.cursor()
    cursor.execute(insert_query, (count, uniques, timestamp))
    cursor.close()

    
def insert_metrics(con, table_name, lst):
    insert_query = f"""INSERT INTO "{table_name}"
        VALUES (?, ?, ?);"""

    cursor = con.cursor()
    cursor.execute('begin')
    try:
        for dct in lst:
            count = dct['count']
            uniques = dct['uniques']
            timestamp = dct['timestamp']
            cursor.execute(insert_query, (count, uniques, timestamp))
        cursor.execute('commit')
    except:
        cursor.execute('rollback')
    finally:
        cursor.close()

    #cursor.close()

    

def create_metric_table(con, repo, metric):
    owner = repo['owner']
    repo_name = repo['name']
    #table_name = f'{owner}/{repo_name}/{metric}'
    table_name = get_table_name(repo, metric)

    createTable = f'''CREATE TABLE IF NOT EXISTS "{table_name}" (
        count integer,
        uniques integer,
        timestamp text);'''

    #con = sqlite3.connect(DB)
    cursor = con.cursor()
    cursor.execute(createTable)
    cursor.close()
    #con.commit()


def str2dt(timestamp):
    #return datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d')
    
    #return datetime.strptime(timestamp[0:10], '%Y-%m-%d').date()
    #return datetime.strptime(timestamp, '%Y-%m-%d').date()
    return datetime.strptime(timestamp,"%Y-%m-%dT%H:%M:%SZ")

def dt2str(dt):
    #fmt = '%Y-%m-%d'
    #fmt = '%Y-%m-%dT%H:%M:%SZ'
    fmt = '%Y-%m-%d %H:%M:%S'
    return dt.strftime(fmt)


def get_url(repo, metric):
    owner = repo['owner']
    repo_name = repo['name']
    path = METRICS[metric]
    url = f'https://api.github.com/repos/{owner}/{repo_name}{path}';
    return url

def get_table_name(repo, metric):
    owner = repo['owner']
    repo_name = repo['name']
    return f'{owner}/{repo_name}/{metric}'


def get_headers(repo):
    token = repo['token']
    headers = {'Accept': 'application/vnd.github.v3+json', 
               'User-Agent': 'epic',
               'Authorization': f'token {token}'
    }
    return headers


def get_views(repo):
    url = get_url(repo, 'views')
    headers = get_headers(repo)
    r = requests.get(url, headers=headers)
    if r.status_code == 200:
        return json.loads(r.content)

def get_clones(repo):
    url = get_url(repo, 'clones')
    headers = get_headers(repo)
    r = requests.get(url, headers=headers)
    if r.status_code == 200:
        return json.loads(r.content)


def get_metrics(repo, metric):
    url = get_url(repo, metric)
    headers = get_headers(repo)
    r = requests.get(url, headers=headers)
    if r.status_code == 200:
        return json.loads(r.content)[metric]

def main():
    '''
    for repo in REPOS.keys():
        for metric in METRICS.keys():
            print('#############################################')
            print(get_table_name(repo, metric))
            print('#############################################')
    #ts = '2022-08-11T00:00:00Z'
    '''

    con = sqlite3.connect(DB, isolation_level=None)

    for repo in REPOS:
        for metric in METRICS.keys():
            table = get_table_name(repo, metric)
            create_metric_table(con, repo, metric)
            insert_metric(con, table, {'timestamp': '2022-08-19T00:00:00Z', 'count': 105, 'uniques': 19})
            insert_metric(con, table, {'timestamp': '2022-08-20T00:00:00Z', 'count': 105, 'uniques': 19})
            insert_metric(con, table, {'timestamp': '2022-08-21T00:00:00Z', 'count': 105, 'uniques': 19})
            insert_metric(con, table, {'timestamp': '2022-08-22T00:00:00Z', 'count': 105, 'uniques': 19})

            lst = get_metrics(repo, metric)
            latest = get_latest(con, table)
            pruned_list = prune_list(lst, latest)
            insert_metrics(con, table, pruned_list)

            print('****************************** list ****************************')
            print(lst)
            print('****************************************************************')

            print('****************************** latest ****************************')
            print(latest)
            print('****************************************************************')

            print('****************************** new list ****************************')
            print(pruned_list)
            print('****************************************************************')
            return 

    con.close() 
        
    #create_table('ufs-community', 'ufs-weather-model', 'views')

    '''
    for repo in REPOS:
        for metric in METRICS.keys():
            # create the table if it does not already exist
            create_metric_table(con, repo, metric)
            m = get_metrics(repo, metric)
            print(m)
    '''



if __name__ == '__main__':
    main()





