from download import logging
import config
import random
from glob import glob
import os
import datetime as dt
from bs4 import BeautifulSoup
from utils import gid_to_date, gid_to_url, try_int, try_float
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String, Date, Numeric, DateTime, Boolean

Base = declarative_base()
engine = create_engine(config.DB_URL, echo=True)
#engine = create_engine(config.DB_URL)
Session = sessionmaker(bind=engine)


class Game(Base):
    __tablename__ = 'games'
    game_id = Column(String, primary_key=True)
    game_date = Column(Date, nullable=False)
    game_datetime = Column(DateTime)
    season = Column(Integer, nullable=False)
    venue = Column(String, nullable=False, default='')
    game_type = Column(String, nullable=False, default='')
    inning = Column(Integer)
    outs = Column(Integer)
    top_inning = Column(Boolean)
    status = Column(String, nullable=False, default='')
    home_team_id = Column(Integer)
    away_team_id = Column(Integer)
    home_team_id = Column(Integer)
    away_team_id = Column(Integer)
    home_team_runs = Column(Integer)
    away_team_runs = Column(Integer)
    home_team_hits = Column(Integer)
    away_team_hits = Column(Integer)
    home_team_errors = Column(Integer)
    away_team_errors = Column(Integer)
    url = Column(String, nullable=False)

    def __repr__(self):
        return("<{}>".format(self.game_id))


class Team(Base):
    __tablename__ = 'teams'
    team_id = Column(Integer, primary_key=True)
    season = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    short_name = Column(String, nullable=False)
    league = Column(String, nullable=False, default='')
    division = Column(String, nullable=False, default='')

    def __repr__(self):
        return("<{}>".format(self.name))

    @classmethod
    def from_soup(cls, boxscore_soup, linescore_soup, homeaway='home'):
        # TODO: Prefer linescore, since it's posted before the boxscore (or use
        # game.xml
        boxscore = boxscore_soup.find('boxscore')
        game = linescore_soup.find('game')
        team_id = try_int(boxscore.get(homeaway + '_id'))
        season = try_int(boxscore.get('game_id')[:4])
        name = boxscore.get(homeaway + '_fname')
        short_name = boxscore.get(homeaway + '_sname')
        # League is a two-character code (e.g., 'AN'), with the home team's
        # league first and the away team second. If away, use second. If
        # home, use first.
        league = game.get('league', '  ')[homeaway == 'away']
        division = game.get(homeaway + '_division', '')
        return cls(team_id=team_id, season=season, name=name,
                   short_name=short_name, league=league, division=division)


class Team_Stats(Base):
    __tablename__ = 'team_stats'
    game_id = Column(String, primary_key=True)
    team_id = Column(Integer, primary_key=True)
    at_home = Column(Boolean)
    games_back = Column(Numeric)
    games_back_wildcard = Column(Numeric)
    wins = Column(Integer)
    losses = Column(Integer)
    winrate = Column(Numeric)
    avg = Column(Numeric)
    at_bats = Column(Integer)
    runs = Column(Integer)
    hits = Column(Integer)
    doubles = Column(Integer)
    triples = Column(Integer)
    home_runs = Column(Integer)
    rbis = Column(Integer)
    walks = Column(Integer)
    putouts = Column(Integer)
    da = Column(Integer)
    strikeouts = Column(Integer)
    left_on_base = Column(Integer)
    era = Column(Numeric)


class Pitcher(Base):
    __tablename__ = 'pitchers'

    pitcher_id = Column(Integer, primary_key=True)
    game_id = Column(String, primary_key=True)
    team_id = Column(Integer)
    name = Column(String, nullable=False, default = '')
    full_name = Column(String, nullable=False, default = '')
    pos = Column(String, nullable=False, default = '')
    out = Column(Integer)
    batters_faced = Column(Integer)
    hr = Column(Integer)
    bb = Column(Integer)
    so = Column(Integer)
    er = Column(Integer)
    r = Column(Integer)
    h = Column(Integer)
    w = Column(Integer)
    l = Column(Integer)
    sv = Column(Integer)
    era = Column(Numeric)
    np = Column(Integer)
    s = Column(Integer)
    bs = Column(Integer)

    @classmethod
    def from_soup(cls, boxscore_soup, linescore_soup, homeaway='home'):
        # TODO: Prefer linescore, since it's posted before the boxscore (or use
        # game.xml
        pitching = boxscore_soup.find('pitching', team_flag=homeaway)
        boxscore = boxscore_soup.find('boxscore')
        game = linescore_soup.find('game')

        # There are multiple pitchers per game here
        team_id = try_int(boxscore.get(homeaway + '_id'))
        season = try_int(boxscore.get('game_id')[:4])
        name = boxscore.get(homeaway + '_fname')
        short_name = boxscore.get(homeaway + '_sname')
        # League is a two-character code (e.g., 'AN'), with the home team's
        # league first and the away team second. If away, use second. If
        # home, use first.
        league = game.get('league', '  ')[homeaway == 'away']
        division = game.get(homeaway + '_division', '')
        return cls(team_id=team_id, season=season, name=name,
                   short_name=short_name, league=league, division=division)



def load_from_path(path, session):
    linescore_path = os.path.join(path, 'linescore.xml')
    boxscore_path = os.path.join(path, 'boxscore.xml')
    try:
        with open(linescore_path, 'r') as lp:
            linescore_soup = BeautifulSoup(lp)
        with open(boxscore_path, 'r') as bp:
            boxscore_soup = BeautifulSoup(bp)
    except FileNotFoundError:
        logging.warn('No game data: {}'.format(path))
        return

    game = Game.from_soup(linescore_soup)
    home_team = Team.from_soup(boxscore_soup, linescore_soup, 'home')
    away_team = Team.from_soup(boxscore_soup, linescore_soup, 'away')
    home_stats = Team_Stats.from_soup(boxscore_soup, linescore_soup, 'home')
    away_stats = Team_Stats.from_soup(boxscore_soup, linescore_soup, 'away')
    session.add_all([game, home_stats, away_stats])
    session.merge(home_team)
    session.merge(away_team)


if __name__ == '__main__':
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
  #  session = Session()
  #  paths = [
  #      'xml/gid_2008_08_25_chnmlb_pitmlb_1/',
  #      'xml/gid_2009_05_16_bosmlb_seamlb_1/',
  #      'xml/gid_2010_04_30_nynmlb_phimlb_1/',
  #      'xml/gid_2011_06_01_houmlb_chnmlb_1/',
  #      'xml/gid_2012_07_17_seamlb_kcamlb_1/'
  #  ]

    #games = [Game.from_path(path) for path in paths]
    #session.add_all(games)

    # TODO: Fix double headers
  #  all_paths = glob('xml/gid_*')
  #  #sampled_paths = random.sample(all_paths, 1000)
  #  loaded = 0
  #  for path in all_paths:
  #      if 'bak' in path[-4:]:
  #          #TODO: Better way to check this via regex?
  #          continue
  #      load_from_path(path, session=session)
  #      loaded += 1
  #      if loaded % 100 == 0:
  #          print("{} loaded".format(loaded))
    #empty_path = 'xml/gid_2012_09_24_tboint_canint_1'
    #load_from_path(empty_path, session=session)
    #session.commit()
    #with open(os.path.join(paths[1], 'boxscore.xml'), 'r') as box:
    #    box_soup = BeautifulSoup(box)
    #with open(os.path.join(paths[1], 'linescore.xml'), 'r') as line:
    #    line_soup = BeautifulSoup(line)

    #teamstats = Team_Stats.from_soup(box_soup, line_soup, 'home')
    # Check xml/gid_2008_07_13_wftmin_uftmin_1
   # path = 'xml/gid_2008_07_13_wftmin_uftmin_1'
   # load_from_path(path, session=session)

    #session.close_all()
