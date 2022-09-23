import requests, sys, time
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
           'forks': '/forks?per_page=100&page=1',
}


# multiple attempt get
def mget(url, headers, n=10):
    tries = 0
    sleep = 5
    while tries < n:
        print(f'request: {url}')
        tries += 1
        r = requests.get(url, headers=headers)
        if r.status_code == 200:
            print(f'received response! Tries = {tries}')
            return r 

        print(f'status code = {r.status_code}')
        print(f'Going to sleep for {sleep} seconds...')
        time.sleep(sleep) # No real hurry. Be nice to the server.

    return r


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


def create_repo_table(con):
    create_table = f'''create table if not exists repos (
        owner text not null,
        name text not null,
        metric text not null,
        minDate text not null);'''
    cursor = con.cursor()
    cursor.execute(create_table)
    cursor.close()


def create_metric_table(con, table_name):
    create_table = f'''create table if not exists "{table_name}" (
        timestamp text not null,
        count integer not null,
        uniques integer not null);'''

    cursor = con.cursor()
    cursor.execute(create_table)
    cursor.close()


def create_freq_table(con, table_name):
    create_table = f'''create table if not exists "{table_name}" (
        timestamp text not null,
        additions integer not null,
        deletions integer not null);'''

    cursor = con.cursor()
    cursor.execute(create_table)
    cursor.close()

def create_commit_table(con, table_name):
    create_table = f'''create table if not exists "{table_name}" (
        timestamp text not null,
        commits integer not null);'''

    cursor = con.cursor()
    cursor.execute(create_table)
    cursor.close()

def create_fork_table(con, table_name):
    create_table = f'''create table if not exists "{table_name}" (
        fork_count integer not null);'''

    cursor = con.cursor()
    cursor.execute(create_table)
    cursor.close()


def insert_metrics(con, table_name, lst):
    if not lst:
        return
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


def insert_commits(con, table_name, lst):
    if not lst:
        return
    print(f'inserting metrics into {table_name}')
    insert_query = f"""insert into "{table_name}"
        values (?, ?);"""

    cursor = con.cursor()
    cursor.execute('begin')
    try:
        for dct in lst:
            timestamp = dct['timestamp']
            commits = dct['commits']
            cursor.execute(insert_query, (timestamp, commits))
        cursor.execute('commit')
    except:
        cursor.execute('rollback')
    finally:
        cursor.close()

def insert_frequency(con, table_name, lst):
    if not lst:
        return

    print(f'inserting metrics into {table_name}')
    insert_query = f"""insert into "{table_name}"
        values (?, ?, ?);"""

    cursor = con.cursor()
    cursor.execute('begin')
    try:
        for dct in lst:
            timestamp = dct['timestamp']
            additions = dct['additions']
            deletions = dct['deletions']
            cursor.execute(insert_query, (timestamp, additions, deletions))
        cursor.execute('commit')
    except:
        cursor.execute('rollback')
    finally:
        cursor.close()


def insert_or_update_forks(con, table_name, fork_count):
    insert_sql = f'''insert or ignore into "{table_name}" (fork_count) values (?);''' 
    update_sql = f'''update "{table_name}" set fork_count = ?;''' 
    sql = insert_sql

    cursor = con.cursor()
    cursor.execute(f'select count(*) from "{table_name}";')
    exists = cursor.fetchone()[0]
    if exists:
        sql = update_sql

    cursor.execute(sql, (fork_count,))
    cursor.close()



'''
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
'''


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
    #r = requests.get(url, headers=headers)
    r = mget(url, headers)
    if r.status_code == 200:
        return json.loads(r.content)

def get_clones(repo):
    url = get_url(repo, 'clones')
    headers = get_headers(repo)
    # r = requests.get(url, headers=headers)
    r = mget(url, headers)
    if r.status_code == 200:
        return json.loads(r.content)

def to_date(timestamp):
    return datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%dT%H:%M:%SZ')

def get_metrics(repo, metric):
    url = get_url(repo, metric)
    headers = get_headers(repo)
    print(f'getting metrics from {url}')
    #r = requests.get(url, headers=headers)
    r = mget(url, headers)
    if r.status_code == 200:
        return json.loads(r.content)[metric]

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

def get_fork_count(repo):
    url = get_url(repo, 'forks')
    headers = get_headers(repo)

    total = 0
    while url:
        #r = requests.get(url, headers=headers)
        r = mget(url, headers)
        if r.status_code == 200:
            lst = json.loads(r.content)

            for l in lst:
                count = l['forks_count']
                if count == 0:
                    total += 1
                else:
                    total += (count + 1)


            links = get_links(r.headers)

            url = links.get('next')

    return total


def get_commits(repo, metric):
    url = get_url(repo, metric)
    headers = get_headers(repo)

    commit_dct = {}

    while url:
        #print(url)
        #r = requests.get(url, headers=headers)
        r = mget(url, headers)
        if r.status_code == 200:
            lst = json.loads(r.content)
            #print(lst); sys.exit()

            for l in lst:
                date = l['commit']['author']['date'][0:10] + 'T00:00:00Z'
                if date in commit_dct:
                    commit_dct[date] += 1
                else:
                    commit_dct[date] = 1

            links = get_links(r.headers)

            url = links.get('next')

    # sort dct by key
    sorted_dct = {key:commit_dct[key] for key in sorted(commit_dct.keys())}
    return [{'timestamp': k, 'commits': v} for k,v in sorted_dct.items()]
    #return commit_dct
    #return sorted_dct

def get_frequency(repo, metric):
    url = get_url(repo, metric)
    headers = get_headers(repo)

    r = mget(url, headers, n=20)
    if r.status_code == 200:
        lst = json.loads(r.content)
        return [{'timestamp': to_date(l[0]), 'additions': l[1], 'deletions': l[2]} for l in lst]


def update_repo_table(con, repo, metric):
    cursor = con.cursor()

    owner = repo['owner']
    name = repo['name']
    metric = metric
    table_name = get_table_name(repo, metric)

    select_query = f"""select timestamp from "{table_name}" order by timestamp limit 1;"""
    cursor.execute(select_query)
    minDate = cursor.fetchone()[0]

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
                create_metric_table(con, table_name)

                lst = get_metrics(repo, metric)
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
                    update_repo_table(con, repo, metric)

            elif metric == 'commits':
                table_name = get_table_name(repo, metric)
                create_commit_table(con, table_name)

                lst = get_commits(repo, metric)
                if not lst: continue

                lst = lst[:-1] 

                latest = get_latest(con, table_name)
                pruned_list = prune_list(lst, latest)
                if not pruned_list: continue
                insert_commits(con, table_name, pruned_list)

                if not row_ex:
                    update_repo_table(con, repo, metric)


            elif metric == 'frequency':
                table_name = get_table_name(repo, metric)
                create_freq_table(con, table_name)
                lst = get_frequency(repo, metric)
                if not lst: continue

                lst = lst[:-1] 

                latest = get_latest(con, table_name)
                pruned_list = prune_list(lst, latest)
                if not pruned_list: continue
                insert_frequency(con, table_name, pruned_list)

                if not row_ex:
                    update_repo_table(con, repo, metric)


            elif metric == 'forks':
                table_name = get_table_name(repo, metric)
                create_fork_table(con, table_name)
                fork_count = get_fork_count(repo)
                insert_or_update_forks(con, table_name, fork_count)

    con.close() 
    print('Finished!')


if __name__ == '__main__':
    main()





