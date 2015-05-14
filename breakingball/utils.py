import datetime as dt
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import requests
import re


def try_int(x):
    try:
        out = int(x)
    except (TypeError, ValueError):
        out = None
    return out


def try_float(x):
    try:
        out = float(x)
    except (TypeError, ValueError):
        out = None
    return out


def fetch_game_listings(date):
    date_url = date_to_url(date)
    request = requests.get(date_url)
    try:
        request.raise_for_status()
    except requests.HTTPError:
        print('No game data on {}'.format(date.strftime('%Y-%m-%d')))
    soup = BeautifulSoup(request.content)
    links = soup.find_all('a', href=re.compile('gid_'))
    gids = [l.text.strip().strip('/') for l in links]
    return gids

def gid_to_date(game_id):
    year = game_id[4:8]
    month = game_id[9:11]
    day = game_id[12:14]
    game_date = dt.date(int(year), int(month), int(day))
    return game_date


def gid_to_url(game_id):
    game_date = gid_to_date(game_id)
    date_url = date_to_url(game_date)
    game_url = urljoin(date_url, game_id) + '/'
    return game_url


def date_to_url(game_date):
    base_url = "http://gd2.mlb.com/components/game/mlb/"
    date_pattern = "year_{0:04}/month_{1:02}/day_{2:02}/".format(
        game_date.year, game_date.month, game_date.day)
    date_url = urljoin(base_url, date_pattern)
    return date_url


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + dt.timedelta(n)


def dict_to_db(d, table, engine):
    columns = d.keys()
    sql_template = 'INSERT INTO {} ({}) VALUES ({});'
    sql = sql_template.format(table,
                              ', '.join(columns),
                              ', '.join('%(' + key + ')s' for key in columns))
    return sql
