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

    def __init__(self, **kwargs):
        super(Game, self).__init__(**kwargs)
        self.game_date = gid_to_date(self.game_id)
        self.season = self.game_date.year
        self.url = gid_to_url(self.game_id)

    def __repr__(self):
        return("<{}>".format(self.game_id))

    @classmethod
    def from_soup(cls, linescore_soup):
        ls_game = linescore_soup.find('game')
        game_id = 'gid_' + ls_game.get('gameday_link')
        self = cls(game_id=game_id)
        time_text = '{} {}'.format(ls_game.get('time'), ls_game.get('ampm'))
        try:
            game_time = dt.datetime.strptime(time_text, '%I:%M %p').time()
            self.game_datetime = dt.datetime.combine(self.game_date, game_time)
        except ValueError:
            logging.warn('Could not parse time for {}'.format(game_id))
        self.venue = ls_game.get('venue')
        self.game_type = ls_game.get('game_type')
        self.inning = try_int(ls_game.get('inning'))
        self.outs = try_int(ls_game.get('outs'))
        self.top_inning = ((ls_game.get('top_inning') is not None) &
                           (ls_game.get('top_inning') == 'Y'))
        self.status = ls_game.get('status')
        self.home_team_id = try_int(ls_game.get('home_team_id'))
        self.away_team_id = try_int(ls_game.get('away_team_id'))
        self.home_team_runs = try_int(ls_game.get('home_team_runs'))
        self.away_team_runs = try_int(ls_game.get('away_team_runs'))
        self.home_team_hits = try_int(ls_game.get('home_team_hits'))
        self.away_team_hits = try_int(ls_game.get('away_team_hits'))
        self.home_team_errors = try_int(ls_game.get('home_team_errors'))
        self.away_team_errors = try_int(ls_game.get('away_team_errors'))

        return self

    @classmethod
    def from_path(cls, path):
        with open(os.path.join(path, 'linescore.xml')) as linescore:
            ls_soup = BeautifulSoup(linescore)
        self = cls.from_soup(ls_soup)
        return self


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
    batting_avg = Column(Numeric)
    ab = Column(Integer)
    r = Column(Integer)
    h = Column(Integer)
    d = Column(Integer)
    t = Column(Integer)
    hr = Column(Integer)
    rbi = Column(Integer)
    bb = Column(Integer)
    po = Column(Integer)
    da = Column(Integer)
    so = Column(Integer)
    lob = Column(Integer)

    @classmethod
    def from_soup(cls, boxscore_soup, linescore_soup, homeaway='home'):
        ls_game = linescore_soup.find('game')
        bs_boxscore = boxscore_soup.find('boxscore')
        bs_batting = boxscore_soup.find('batting', team_flag=homeaway)
        game_id = 'gid_' + ls_game.get('gameday_link')
        team_id = try_int(bs_boxscore.get(homeaway + '_id'))
        at_home = (homeaway == 'home')
        games_back_text = ls_game.get(homeaway + '_games_back')
        games_back_wildcard_text = ls_game.get(homeaway + '_games_back')
        if games_back_text == '-':
            games_back = 0
            games_back_wildcard = 0
        elif games_back_wildcard_text == '-':
            games_back_wildcard = 0
            games_back = try_float(games_back_text)
        else:
            games_back = try_float(games_back_text)
            games_back_wildcard = try_float(games_back_wildcard_text)
        wins = try_int(bs_boxscore.get(homeaway + '_wins'))
        losses = try_int(bs_boxscore.get(homeaway + '_loss'))
        winrate = 0 if (wins + losses) == 0 else wins / (wins + losses)
        batting_avg = try_float(bs_batting.get('avg'))
        ab = try_int(bs_batting.get('ab'))
        r = try_int(bs_batting.get('r'))
        h = try_int(bs_batting.get('h'))
        d = try_int(bs_batting.get('d'))
        t = try_int(bs_batting.get('t'))
        hr = try_int(bs_batting.get('hr'))
        rbi = try_int(bs_batting.get('rbi'))
        bb = try_int(bs_batting.get('bb'))
        po = try_int(bs_batting.get('po'))
        da = try_int(bs_batting.get('da'))
        so = try_int(bs_batting.get('so'))
        lob = try_int(bs_batting.get('lob'))
        return cls(game_id=game_id, team_id=team_id, at_home=at_home,
                   games_back=games_back,
                   games_back_wildcard=games_back_wildcard, wins=wins,
                   losses=losses, winrate=winrate, batting_avg=batting_avg,
                   ab=ab, r=r, h=h, d=d, t=t, hr=hr, rbi=rbi, bb=bb, po=po,
                   da=da, so=so, lob=lob)


class Pitcher(Base):
    __tablename__ = 'pitchers'

    pitcher_id = Column(Integer, primary_key=True)
    game_id = Column(String, primary_key=True)
    team_id = Column(Integer)
    name = Column(String, nullable=False, default = '')
    full_name = Column(String, nullable=False, default = '')
    pos = Column(String, nullable=False, default = '')
    out = Column(Integer)
    bf = Column(Integer)
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
    session = Session()
    paths = [
        'xml/gid_2008_08_25_chnmlb_pitmlb_1/',
        'xml/gid_2009_05_16_bosmlb_seamlb_1/',
        'xml/gid_2010_04_30_nynmlb_phimlb_1/',
        'xml/gid_2011_06_01_houmlb_chnmlb_1/',
        'xml/gid_2012_07_17_seamlb_kcamlb_1/'
    ]

    #games = [Game.from_path(path) for path in paths]
    #session.add_all(games)

    # TODO: Fix double headers
    all_paths = glob('xml/gid_*')
    #sampled_paths = random.sample(all_paths, 1000)
    loaded = 0
    for path in all_paths:
        if 'bak' in path[-4:]:
            #TODO: Better way to check this via regex?
            continue
        load_from_path(path, session=session)
        loaded += 1
        if loaded % 100 == 0:
            print("{} loaded".format(loaded))
    #empty_path = 'xml/gid_2012_09_24_tboint_canint_1'
    #load_from_path(empty_path, session=session)
    session.commit()
    #with open(os.path.join(paths[1], 'boxscore.xml'), 'r') as box:
    #    box_soup = BeautifulSoup(box)
    #with open(os.path.join(paths[1], 'linescore.xml'), 'r') as line:
    #    line_soup = BeautifulSoup(line)

    #teamstats = Team_Stats.from_soup(box_soup, line_soup, 'home')
    # Check xml/gid_2008_07_13_wftmin_uftmin_1
   # path = 'xml/gid_2008_07_13_wftmin_uftmin_1'
   # load_from_path(path, session=session)

    #session.close_all()
