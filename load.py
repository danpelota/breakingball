#!/usr/bin/env python

from gameloader import load_game
import sys
import argparse
import datetime as dt
from utils import daterange, fetch_game_listings


def valid_date(x):
    try:
        return dt.datetime.strptime(x, '%Y-%m-%d')
    except ValueError:
        msg = 'Not a valid date: "{0}"'.format(x)
        raise argparse.ArgumentTypeError(msg)

parser = argparse.ArgumentParser()
parser.add_argument('--start-date', help='First game date to extract',
                    type=valid_date, required=True)
parser.add_argument('--end-date', help='Last game date to extract',
                    type=valid_date, required=False)
parser.add_argument('--refresh',
                    help='Reload the game data, even if the score is final',
                    required=False, action='store_true')
args = parser.parse_args()
print(args)

if args.end_date is None:
    args.end_date = args.start_date

for d in daterange(args.start_date, args.end_date):
    print('Getting listings for {}'.format(d.strftime('%Y-%m-%d')))
    game_ids = fetch_game_listings(d)
    for gid in game_ids:
        load_game.delay(gid, skip_if_final=not args.refresh)
