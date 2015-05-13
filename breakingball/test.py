from models import Session
from load import GameLoader
from utils import fetch_game_listings, daterange
import datetime as dt

session = Session()


for d in daterange(dt.date(2015, 5, 10), dt.date(2015, 5, 13)):
    game_ids = fetch_game_listings(d)
    for game_id in game_ids:
        g = GameLoader(game_id, session)
        g.fetch_all()
        g.parse_all()
