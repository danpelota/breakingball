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


class TeamStats(Base):
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
    position = Column(String, nullable=False, default = '')
    outs = Column(Integer)
    batters_faced = Column(Integer)
    home_runs = Column(Integer)
    walks = Column(Integer)
    strikeouts = Column(Integer)
    earned_runs = Column(Integer)
    runs = Column(Integer)
    hits = Column(Integer)
    wins = Column(Integer)
    losses = Column(Integer)
    saves = Column(Integer)
    era = Column(Numeric)
    pitches_thrown = Column(Integer)
    strikes = Column(Integer)
    blown_saves = Column(Integer)
    holds = Column(Integer)
    season_innings_pitched = Column(Numeric)
    season_hits = Column(Integer)
    season_runs = Column(Integer)
    season_earned_runs = Column(Integer)
    season_walks = Column(Integer)
    season_strikeouts = Column(Integer)
    game_score = Column(Integer)
    blown_save = Column(Boolean, default=False)
    save = Column(Boolean, default=False)
    loss = Column(Boolean, default=False)
    win = Column(Boolean, default=False)


class Batter(Base):
    __tablename__ = 'batters'
    game_id = Column(String, primary_key=True)
    team_id = Column(Integer, primary_key=True)
    batter_id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, default='')
    full_name = Column(String, nullable=False, default='')
    # Hitting
    avg = Column(Numeric)
    batting_order = Column(Integer)
    at_bats = Column(Integer)
    strikeouts = Column(Integer)
    flyouts = Column(Integer)
    hits = Column(Integer)
    doubles = Column(Integer)
    triples = Column(Integer)
    home_runs = Column(Integer)
    walks = Column(Integer)
    hit_by_pitch = Column(Integer)
    sac_bunts = Column(Integer)
    sac_flys = Column(Integer)
    rbi = Column(Integer)
    assists = Column(Integer)
    runs = Column(Integer)
    left_on_base = Column(Integer)
    caught_stealing = Column(Integer)
    stolen_bases = Column(Integer)
    season_walks = Column(Integer)
    season_hits = Column(Integer)
    season_home_runs = Column(Integer)
    season_runs = Column(Integer)
    season_rbi = Column(Integer)
    season_strikeouts = Column(Integer)
    # Fielding
    position = Column(String)
    putouts = Column(String)
    errors = Column(Integer)
    putouts = Column(Integer)
    fielding = Column(Numeric)


class AtBat(Base):
    __tablename__ = 'at_bats'
    at_bat_number = Column(Integer, primary_key=True)
    game_id = Column(String, primary_key=True)
    inning = Column(Integer)
    inning_half = Column(String, nullable=False, default='')
    balls = Column(Integer)
    strikes = Column(Integer)
    outs = Column(Integer)
    start_time = Column(DateTime)
    batter_id = Column(Integer)
    pitcher_id = Column(Integer)
    stands = Column(String, nullable=False, default='')
    p_throws = Column(String, nullable=False, default='')
    description = Column(String, nullable=False, default='')
    event_num = Column(Integer)
    event = Column(String, nullable=False, default='')
    score = Column(Boolean)
    home_team_runs = Column(Integer)
    away_team_runs = Column(Integer)


class Pitch(Base):
    __tablename__ = 'pitches'
    game_id = Column(String, primary_key=True)
    pitch_id = Column(Integer, primary_key=True)
    at_bat_number = Column(Integer, nullable=False)
    description = Column(String, nullable=False, default='')
    type = Column(String, nullable=False, default='')
    timestamp = Column(DateTime)
    x = Column(Numeric)
    y = Column(Numeric)
    event_num = Column(Integer)
    sv_id = Column(String, nullable=False, default='')
    play_guid = Column(String, nullable=False, default='')
    start_speed = Column(Numeric)
    end_speed = Column(Numeric)
    sz_top = Column(Numeric)
    sz_bottom = Column(Numeric)
    pfx_x = Column(Numeric)
    pfx_z = Column(Numeric)
    x0 = Column(Numeric)
    y0 = Column(Numeric)
    z0 = Column(Numeric)
    vx0 = Column(Numeric)
    vy0 = Column(Numeric)
    vz0 = Column(Numeric)
    ax = Column(Numeric)
    ay = Column(Numeric)
    az = Column(Numeric)
    break_y = Column(Numeric)
    break_angle = Column(Numeric)
    break_length = Column(Numeric)
    pitch_type = Column(String, nullable=False, default='')
    type_confidence = Column(Numeric)
    zone = Column(Integer)
    nasty = Column(Integer)
    spin_dir = Column(Numeric)
    spin_rate = Column(Numeric)


class Runner(Base):
    __tablename__ = 'runners'
    game_id = Column(String, primary_key=True)
    at_bat_number = Column(Integer, primary_key=True)
    runner_id = Column(Integer, primary_key=True)
    start = Column(String, nullable=False, default='')
    end = Column(String, nullable=False, default='')
    event = Column(String, nullable=False, default='')
    event_num = Column(Integer)
    score = Column(Boolean)
    rbi = Column(Boolean)
    earned = Column(Boolean)


if __name__ == '__main__':
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
