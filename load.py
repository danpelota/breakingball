from models import Game
from functools import partial
from bs4 import BeautifulSoup
import datetime as dt
import requests
import re
import os
import logging
from multiprocessing import Pool
from utils import gid_to_date, gid_to_url


LOG_FILENAME = 'log/download.log'
logging.basicConfig(filename=LOG_FILENAME, level=logging.DEBUG,
                    format='%(asctime)s %(message)s')


def download_days_games(date, pool, skip_if_exists=True):
    ''' Download game data from specified date. '''

    game_ids = fetch_game_listings(date)
    pool.map(partial(download_game_xml, skip_if_exists=skip_if_exists),
             game_ids)


def fetch_game_listings(date):
    base_url = "http://gd2.mlb.com/components/game/mlb/"
    date_pattern = "year_{0:04}/month_{1:02}/day_{2:02}/".format(
        date.year, date.month, date.day)
    date_url = base_url + date_pattern
    request = requests.get(date_url)
    try:
        request.raise_for_status()
    except requests.HTTPError:
        logging.warning("No game data on{}".format(date.strftime("%Y-%m-%d")))
    soup = BeautifulSoup(request.content)
    links = soup.find_all("a", href=re.compile("gid_"))
    gids = [l.text.strip().strip("/") for l in links]
    return gids


def download_game_xml(game_id, skip_if_exists=True):
    game_url = gid_to_url(game_id)
    s = requests.session()

    download_file(game_url + 'linescore.xml',
                  'xml/{}/linescore.xml'.format(game_id),
                  session=s,
                  skip_if_exists=skip_if_exists)

    download_file(game_url + 'boxscore.xml',
                  'xml/{}/boxscore.xml'.format(game_id),
                  session=s,
                  skip_if_exists=skip_if_exists)

    # List available batters
    batters = requests.get(game_url + 'batters')
    if batters.ok:
        batter_soup = BeautifulSoup(batters.content)
        batter_urls = batter_soup.find_all('a', href=re.compile(r'.xml$'))
        for batter_url in batter_urls:
            download_file(game_url + 'batters/' + batter_url.get('href'),
                          'xml/{}/batters/{}'.format(game_id, batter_url.get('href')),
                          session=s,
                          skip_if_exists=skip_if_exists)

    # List available innings
    innings = requests.get(game_url + 'inning')
    if innings.ok:
        inning_soup = BeautifulSoup(innings.content)
        inning_urls = inning_soup.find_all('a', href=re.compile(r'[0-9]\.xml$'))
        for inning_url in inning_urls:
            download_file(game_url + 'inning/' + inning_url.get('href'),
                          'xml/{}/inning/{}'.format(game_id, inning_url.get('href')),
                          session=s,
                          skip_if_exists=skip_if_exists)
    s.close()


def download_file(url, local_path, session, skip_if_exists=True):
    if skip_if_exists & os.path.exists(local_path):
        logging.info('{} exists. Skipping'.format(local_path))
        return

    logging.info('Saving {} to {}'.format(url, local_path))
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    r = session.get(url)
    if not r.ok:
        logging.warning("* {} doesn't exist: {}".format(url, r.reason))
        return
    with open(local_path, 'wb') as outfile:
        outfile.write(r.content)


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + dt.timedelta(n)


if __name__ == "__main__":
    pool = Pool(4)
    for game_date in daterange(dt.date(2008, 1, 1), dt.date(2015, 5, 1)):
        download_days_games(game_date, pool=pool, skip_if_exists=True)
