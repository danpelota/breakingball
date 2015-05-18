from gameloader import load_game
import sys
import datetime as dt
from utils import daterange, fetch_game_listings

start_text = sys.argv[1]
if len(sys.argv) == 3:
    end_text = sys.argv[2]
else:
    end_text = start_text

start_date = dt.datetime.strptime(start_text, '%Y-%m-%d')
end_date = dt.datetime.strptime(end_text, '%Y-%m-%d')

for d in daterange(start_date, end_date):
    print('Getting listings for {}'.format(d.strftime('%Y-%m-%d')))
    game_ids = fetch_game_listings(d)
    for gid in game_ids:
        load_game.delay(gid)
