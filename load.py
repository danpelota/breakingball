import config
from bs4 import BeautifulSoup
import datetime as dt
import requests
import re
from sqlalchemy import create_engine, exists
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String, Date, Numeric, DateTime


Base = declarative_base()
engine = create_engine(config.DB_URL, echo=True)
Session = sessionmaker(bind=engine)

class Game(Base):
    __tablename__ = 'games'
    game_id = Column(String, primary_key=True)
    game_date = Column(Date, nullable=False)
    game_datetime = Column(DateTime)
    game_type = Column(String, nullable=False, default='')
    venue = Column(String, nullable=False, default='')
    url = Column(String, nullable=False)
    inning = Column(Integer)
    home_games_back = Column(Numeric)
    away_games_back = Column(Numeric)
    home_games_back_wildcard = Column(Numeric)
    away_games_back_wildcard = Column(Numeric)
    away_name_abbrev = Column(String, nullable=False, default='')
    home_name_abbrev = Column(String, nullable=False, default='')
    away_team_id = Column(String, nullable=False, default='')
    away_team_city = Column(String, nullable=False, default='')
    away_team_name = Column(String, nullable=False, default='')
    away_division = Column(String, nullable=False, default='')
    home_team_id = Column(String, nullable=False, default='')
    home_team_city = Column(String, nullable=False, default='')
    home_team_name = Column(String, nullable=False, default='')
    home_division = Column(String, nullable=False, default='')
    away_win = Column(Integer)
    home_win = Column(Integer)
    away_loss = Column(Integer)
    home_loss = Column(Integer)
    status = Column(String, nullable=False, default='')
    inning = Column(Integer)
    outs = Column(Integer)
    away_team_runs = Column(Integer)
    home_team_runs = Column(Integer)
    away_team_hits = Column(Integer)
    home_team_hits = Column(Integer)
    away_team_errors = Column(Integer)
    home_team_errors = Column(Integer)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        year = self.game_id[4:8]
        month = self.game_id[9:11]
        day = self.game_id[12:14]
        self.game_date = dt.date(int(year), int(month), int(day))
        self.url = ("http://gd2.mlb.com/components/game/mlb/"
                    "year_{0:04}/month_{1:02}/day_{2:02}/{3}/").format(
                        self.game_date.year, self.game_date.month,
                        self.game_date.day, self.game_id)

    def __repr__(self):
        return "<Game(game_id='{}', url='{}')>".format(self.game_id, self.url)

    def update_details(self):
        game_request = requests.get(self.url + "linescore.xml")
        try:
            game_request.raise_for_status()
        except requests.HTTPError:
            print("No game data found: {}".format(self))
            return
        soup = BeautifulSoup(game_request.content, 'xml')
        details = soup.find('game').attrs

        if details.get('home_games_back', '') == '-':
            details['home_games_back'] = '0'
            details['home_games_back_wildcard'] = '0'
        if details.get('home_games_back_wildcard', '') == '-':
            details['home_games_back_wildcard'] = '0'

        if details.get('away_games_back', '') == '-':
            details['away_games_back'] = '0'
            details['away_games_back_wildcard'] = '0'
        if details.get('away_games_back_wildcard', '') == '-':
            details['away_games_back_wildcard'] = '0'
        try:
            time = dt.datetime.strptime(details.get('time'), "%H:%M").time()
            details['game_datetime'] = dt.datetime.combine(self.game_date, time)
        except ValueError:
            # time is formatted as "TBD", or something similar
            pass

        for key in details:
            setattr(self, key, details[key])


def fetch_game_listings(date):
    base_url = "http://gd2.mlb.com/components/game/mlb/"
    date_pattern = "year_{0:04}/month_{1:02}/day_{2:02}/".format(
        date.year, date.month, date.day)
    date_url = base_url + date_pattern
    request = requests.get(date_url)
    try:
        request.raise_for_status()
    except requests.HTTPError:
        print("No game data on{}".format(game_date.strftime("%Y-%m-%d")))
    soup = BeautifulSoup(request.content)
    links = soup.find_all("a", href=re.compile("gid_"))
    gids = [l.text.strip().strip("/") for l in links]
    return gids


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + dt.timedelta(n)


if __name__ == "__main__":
    session = Session()
    # Base.metadata.drop_all(engine)
    # Base.metadata.create_all(engine)

    # Pull games from the last week
    week_ago = dt.date.today() - dt.timedelta(7)
    for game_date in daterange(week_ago, dt.date.today()):
        game_ids = fetch_game_listings(game_date)
        for game_id in game_ids:
            q = session.query(Game).filter(Game.game_id==game_id)
            game = q.first()
            if not game:
                game = Game(game_id=game_id)
                session.add(game)
            game.update_details()
    session.commit()
