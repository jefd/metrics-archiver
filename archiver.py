import requests, sys
import json
from datetime import datetime
from tk import *
import sqlite3
import os


DB_PATH = os.path.join(os.path.dirname(__file__), 'metrics.db')

REPOS = [
        {'owner': 'ufs-community', 'name': 'ufs-weather-model', 'title': 'Weather Model', 'token': TOKEN },
        {'owner': 'ufs-community', 'name': 'ufs-srweather-app', 'title': 'Short Range Weather App', 'token': TOKEN },
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
    insert_query = f"""insert into "{table_name}" values (?, ?, ?);"""

    cursor.execute(insert_query, (count, uniques, timestamp))
    cursor.close()


def insert_metric(con, table_name, dct):
    # {'timestamp': '2022-08-19T00:00:00Z', 'count': 105, 'uniques': 19}
    count = dct['count']
    uniques = dct['uniques']
    timestamp = dct['timestamp']

    insert_query = f"""insert into "{table_name}"
        values (?, ?, ?);"""

    cursor = con.cursor()
    cursor.execute(insert_query, (count, uniques, timestamp))
    cursor.close()

    
def insert_metrics(con, table_name, lst):
    print(f'inserting metrics into {table_name}')
    insert_query = f"""insert into "{table_name}"
        values (?, ?, ?);"""

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
    print(f'creating table {table_name}')

    create_table = f'''create table if not exists "{table_name}" (
        count integer,
        uniques integer,
        timestamp text);'''

    #con = sqlite3.connect(DB)
    cursor = con.cursor()
    cursor.execute(create_table)
    cursor.close()
    #con.commit()


def create_repo_table(con):
    create_table = f'''create table if not exists repos (
        owner text,
        name text,
        title text,
        minDate text);'''
    cursor = con.cursor()
    cursor.execute(create_table)
    cursor.close()



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
    print(f'getting metrics from {url}')
    r = requests.get(url, headers=headers)
    if r.status_code == 200:
        return json.loads(r.content)[metric]

def update_repo_table(con, repo, minDate):
    if row_exists(con, repo):
        return
    
    cursor = con.cursor()
    owner = repo['owner']
    name = repo['name']
    title = repo['title']

    insert_query = f"""insert into repos values (?, ?, ?, ?);"""
    cursor.execute(insert_query, (owner, name, title, minDate))
    cursor.close()

def row_exists(con, repo):
    cursor = con.cursor()
    owner = repo['owner']
    name = repo['name']
    sql = f'''select minDate from repos where owner="{owner}" and name="{name}";'''
    cursor.execute(sql)
    result = cursor.fetchone()
    cursor.close()
    return result



def main():
    con = sqlite3.connect(DB_PATH, isolation_level=None)
    create_repo_table(con)

    for repo in REPOS:
        for metric in METRICS.keys():
            table_name = get_table_name(repo, metric)
            create_metric_table(con, repo, metric)

            lst = get_metrics(repo, metric)
            if not lst: continue
           
            # remove last element because that is being continually
            # updated and we don't want to store it prematurely.
            lst = lst[:-1] 

            latest = get_latest(con, table_name)
            pruned_list = prune_list(lst, latest)
            if not pruned_list: continue
            insert_metrics(con, table_name, pruned_list)

            # After inserting the metrics data, we want to insert the minDate into the repos
            # table if it doesn't already exist. 

            minDate = pruned_list[0]['timestamp']
            update_repo_table(con, repo, minDate)

    con.close() 


if __name__ == '__main__':
    main()





