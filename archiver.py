import requests, sys
import json
from datetime import datetime
from tk import *
import sqlite3
import os, sys


DB_PATH = os.path.join(os.path.dirname(__file__), 'metrics.db')

REPOS = [
        {'owner': 'ufs-community', 'name': 'ufs-weather-model', 'token': TOKEN },
        {'owner': 'ufs-community', 'name': 'ufs-srweather-app', 'token': TOKEN },
]

# map of metric name to github api path
METRICS = {'views': '/traffic/views',
           'clones': '/traffic/clones',
           'frequency': '/stats/code_frequency',
           'commits': '/commits?per_page=100&page=1',
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


def insert(con, table_name, timestamp, count, uniques):
    cursor = con.cursor()
    insert_query = f"""insert into "{table_name}" values (?, ?, ?);"""

    cursor.execute(insert_query, (timestamp, count, uniques))
    cursor.close()


def insert_metric(con, table_name, dct):
    # {'timestamp': '2022-08-19T00:00:00Z', 'count': 105, 'uniques': 19}
    timestamp = dct['timestamp']
    count = dct['count']
    uniques = dct['uniques']

    insert_query = f"""insert into "{table_name}"
        values (?, ?, ?);"""

    cursor = con.cursor()
    cursor.execute(insert_query, (timestamp, count, uniques))
    cursor.close()

    
def insert_metrics(con, table_name, lst):
    print(f'inserting metrics into {table_name}')
    insert_query = f"""insert into "{table_name}"
        values (?, ?, ?);"""

    cursor = con.cursor()
    cursor.execute('begin')
    try:
        for dct in lst:
            timestamp = dct['timestamp']
            count = dct['count']
            uniques = dct['uniques']
            cursor.execute(insert_query, (timestamp, count, uniques))
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
        timestamp text not null,
        count integer not null,
        uniques integer not null);'''

    #con = sqlite3.connect(DB)
    cursor = con.cursor()
    cursor.execute(create_table)
    cursor.close()
    #con.commit()


def create_repo_table(con):
    create_table = f'''create table if not exists repos (
        owner text not null,
        name text not null,
        metric text not null,
        minDate text not null);'''
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

def to_date(timestamp):
    return datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%dT%H:%M:%SZ')

def get_metrics(repo, metric):
    url = get_url(repo, metric)
    headers = get_headers(repo)
    print(f'getting metrics from {url}')
    r = requests.get(url, headers=headers)
    if r.status_code == 200:
        return json.loads(r.content)[metric]


def get_min_date_freq(repo, metric):
    url = get_url(repo, metric)
    headers = get_headers(repo)
    print(f'getting minDate from {url}')
    r = requests.get(url, headers=headers)
    if r.status_code == 200:
        lst = json.loads(r.content)
        return to_date(lst[0][0])


def get_min_date_commits(repo, metric):
    def split_strip(s, ch):
        return [a.strip() for a in s.split(ch)]

    def get_links(headers):
        rel_list = ['first', 'last', 'next', 'prev']

        m = {}

        link = headers.get('Link')
        if not link: return m

        for rel in rel_list:
            l = split_strip(link, ',')
            l2 = [ split_strip(item, ';') for item in l]

            for url, relative in l2:
                if rel in relative:
                    m[rel] = url[1:-1]

        return m

    url = get_url(repo, metric)
    headers = get_headers(repo)

    commit_list = []
    while url:
        print(f'getting commits from {url}')
        r = requests.get(url, headers=headers)
        lst = json.loads(r.content)
        #print(lst); sys.exit()

        for l in lst:
            date = l['commit']['author']['date']
            #print(date)
            commit_list.append(date)

        links = get_links(r.headers)

        url = links.get('next')

    return sorted(commit_list)[0]



def update_repo_table(con, repo, metric, lst=None):
    funs = {'commits': get_min_date_commits, 'frequency': get_min_date_freq}

    if lst:
        minDate = lst[0]['timestamp']
    else:
        minDate = funs[metric](repo, metric)

    if not minDate:
        return

    cursor = con.cursor()
    owner = repo['owner']
    name = repo['name']
    metric = metric

    insert_query = f"""insert into repos values (?, ?, ?, ?);"""
    cursor.execute(insert_query, (owner, name, metric, minDate))
    cursor.close()

def row_exists(con, repo, metric):
    cursor = con.cursor()
    owner = repo['owner']
    name = repo['name']
    sql = f'''select minDate from repos where owner="{owner}" and name="{name}" and metric="{metric}";'''
    cursor.execute(sql)
    result = cursor.fetchone()
    cursor.close()
    return result



def main():
    con = sqlite3.connect(DB_PATH, isolation_level=None)
    create_repo_table(con)

    for repo in REPOS:
        for metric in METRICS.keys():
            row_ex = row_exists(con, repo, metric)

            if metric == 'views' or metric == 'clones':
                table_name = get_table_name(repo, metric)
                create_metric_table(con, repo, metric)

                lst = get_metrics(repo, metric)
                print(lst);sys.exit()
                if not lst: continue
           
                # remove last element because that is being continually
                # updated and we don't want to store it prematurely.
                lst = lst[:-1] 

                latest = get_latest(con, table_name)
                pruned_list = prune_list(lst, latest)
                if not pruned_list: continue
                insert_metrics(con, table_name, pruned_list)

                # Get the minDate to insert into the repos
                # table if it doesn't already exist. 

                # if the relevant row does not exist in the repos table
                # then update the table with the row.
                if not row_ex:
                    update_repo_table(con, repo, metric, pruned_list)

            elif (metric == 'frequency' or metric == 'commits') and not row_ex:
                update_repo_table(con, repo, metric)

    con.close() 


if __name__ == '__main__':
    main()





