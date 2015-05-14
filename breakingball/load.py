from bs4 import BeautifulSoup
import requests
import re
from utils import gid_to_url, gid_to_date, try_int, try_float, daterange, \
    fetch_game_listings
import datetime as dt
from pytz import timezone
from download import logging
from models import Session, Game, Team, TeamStats, Pitcher, Batter, Runner, AtBat,\
    Pitch
from sqlalchemy.sql import exists
from sqlalchemy import exc
from multiprocessing import Pool


class GameLoader:
    def __init__(self, game_id, sessionmaker):
        self.game_id = game_id
        self.session = sessionmaker()
        self.game_date = gid_to_date(game_id)
        self.season = self.game_date.year
        self.base_url = gid_to_url(game_id)
        self.url = gid_to_url(game_id)
        self.to_load = []

    def fetch_linescore(self):
        url = self.base_url + 'linescore.xml'
        r = requests.get(url)
        soup = BeautifulSoup(r.content).find('game')
        self.linescore = soup

    def fetch_boxscore(self):
        url = self.base_url + 'boxscore.xml'
        r = requests.get(url)
        soup = BeautifulSoup(r.content).find('boxscore')
        self.boxscore = soup

    def fetch_innings(self):
        list_url = self.base_url + 'inning/'
        r = requests.get(list_url)
        inning_soup = BeautifulSoup(r.content)
        urls = inning_soup.find_all('a', href=re.compile(r'[0-9]\.xml$'))
        innings_r = [requests.get(list_url + url.get('href')) for url in urls]
        innings_soup = [BeautifulSoup(x.content).find('inning') for x in innings_r]
        self.innings = innings_soup
        self.atbats = []
        self.pitches = []
        self.runners = []
        for inning in innings_soup:
            for atbat in inning.find_all('atbat'):
                self.atbats.append(atbat)
            for pitch in inning.find_all('pitch'):
                self.pitches.append(pitch)
            for runner in inning.find_all('runner'):
                self.runners.append(runner)

    def parse_game(self):
        game = {}
        game['game_id'] = self.game_id
        game['url'] = self.url
        game['game_date'] = self.game_date
        game['season'] = self.season
        time_text = '{} {}'.format(self.linescore.get('time'), self.linescore.get('ampm'))
        try:
            game_time = dt.datetime.strptime(time_text, '%I:%M %p').time()
            game['game_datetime'] = dt.datetime.combine(self.game_date, game_time)
        except ValueError:
            logging.warn('Could not parse time for {}'.format(self.game_id))

        game['venue'] = self.linescore.get('venue')
        game['game_type'] = self.linescore.get('game_type', '')
        game['inning'] = try_int(self.linescore.get('inning'))
        game['outs'] = try_int(self.linescore.get('outs'))
        game['top_inning'] = ((self.linescore.get('top_inning') is not None) &
                              (self.linescore.get('top_inning') == 'Y'))
        game['status'] = self.linescore.get('status')
        game['home_team_id'] = try_int(self.linescore.get('home_team_id'))
        game['away_team_id'] = try_int(self.linescore.get('away_team_id'))
        game['home_team_runs'] = try_int(self.linescore.get('home_team_runs'))
        game['away_team_runs'] = try_int(self.linescore.get('away_team_runs'))
        game['home_team_hits'] = try_int(self.linescore.get('home_team_hits'))
        game['away_team_hits'] = try_int(self.linescore.get('away_team_hits'))
        game['home_team_errors'] = try_int(self.linescore.get('home_team_errors'))
        game['away_team_errors'] = try_int(self.linescore.get('away_team_errors'))
        game = dict((k, v) for k, v in game.items() if v is not None)
        self.to_load.append(Game(**game))

    def parse_team(self, homeaway='home'):
        if self.boxscore is None:
            logging.warn('No boxscore available to parse')
            return
        team = {}
        team['team_id'] = try_int(self.boxscore.get(homeaway + '_id'))
        team['season'] = self.season
        team['name'] = self.boxscore.get(homeaway + '_fname')
        team['short_name'] = self.boxscore.get(homeaway + '_sname')
        # League is a two-character code (e.g., 'AN'), with the home team's
        # league first and the away team second. If away, use second. If
        # home, use first.
        team['league'] = self.linescore.get('league', '  ')[homeaway == 'away']
        team['division'] = self.linescore.get(homeaway + '_division', '')
        team = dict((k, v) for k, v in team.items() if v is not None)
        self.session.merge(Team(**team))
        self.session.commit()

    def parse_team_stats(self, homeaway='home'):
        team_stats = {}
        batting = self.boxscore.find('batting', team_flag=homeaway)
        pitching = self.boxscore.find('pitching', team_flag=homeaway)
        team_stats['game_id'] = self.game_id
        team_stats['team_id'] = try_int(self.boxscore.get(homeaway + '_id'))
        team_stats['at_home'] = (homeaway == 'home')
        games_back_text = self.linescore.get(homeaway + '_games_back')
        games_back_wildcard_text = self.linescore.get(homeaway + '_games_back')
        if games_back_text == '-':
            team_stats['games_back'] = 0
            team_stats['games_back_wildcard'] = 0
        elif games_back_wildcard_text == '-':
            team_stats['games_back_wildcard'] = 0
            team_stats['games_back'] = try_float(games_back_text)
        else:
            team_stats['games_back'] = try_float(games_back_text)
            team_stats['games_back_wildcard'] = try_float(games_back_wildcard_text)
        wins = try_int(self.boxscore.get(homeaway + '_wins', 0))
        losses = try_int(self.boxscore.get(homeaway + '_loss', 0))
        team_stats['wins'] = wins
        team_stats['losses'] = losses
        team_stats['winrate'] = 0 if (wins + losses) == 0 else wins / (wins + losses)
        team_stats['avg'] = try_float(batting.get('avg'))
        team_stats['at_bats'] = try_int(batting.get('ab'))
        team_stats['runs'] = try_int(batting.get('r'))
        team_stats['hits'] = try_int(batting.get('h'))
        team_stats['doubles'] = try_int(batting.get('d'))
        team_stats['triples'] = try_int(batting.get('t'))
        team_stats['home_runs'] = try_int(batting.get('hr'))
        team_stats['rbis'] = try_int(batting.get('rbi'))
        team_stats['walks'] = try_int(batting.get('bb'))
        team_stats['putouts'] = try_int(batting.get('po'))
        team_stats['da'] = try_int(batting.get('da'))
        team_stats['strikeouts'] = try_int(batting.get('so'))
        team_stats['left_on_base'] = try_int(batting.get('lob'))
        team_stats['era'] = try_float(pitching.get('era'))
        team_stats = dict((k, v) for k, v in team_stats.items() if v is not None)
        self.to_load.append(TeamStats(**team_stats))

    def parse_batters(self):
        for batter in self.boxscore.find_all('batter'):
            b = {}
            b['game_id'] = self.game_id
            homeaway = batter.parent.get('team_flag')
            b['team_id'] = try_int(self.boxscore.get(homeaway + '_id'))
            b['batter_id'] = try_int(batter.get('id'))
            b['name'] = batter.get('name')
            b['full_name'] = batter.get('name_display_first_last')
            b['avg'] = try_float(batter.get('avg'))
            b['batting_order'] = try_int(batter.get('bo'))
            b['at_bats'] = try_int(batter.get('ab'))
            b['strikeouts'] = try_int(batter.get('so'))
            b['flyouts'] = try_int(batter.get('ao'))
            b['hits'] = try_int(batter.get('h'))
            b['doubles'] = try_int(batter.get('d'))
            b['triples'] = try_int(batter.get('t'))
            b['home_runs'] = try_int(batter.get('hr'))
            b['walks'] = try_int(batter.get('bb'))
            b['hit_by_pitch'] = try_int(batter.get('hbp'))
            b['sac_bunts'] = try_int(batter.get('sac'))
            b['sac_flys'] = try_int(batter.get('fs'))
            b['rbi'] = try_int(batter.get('rbi'))
            b['assists'] = try_int(batter.get('a'))
            b['runs'] = try_int(batter.get('r'))
            b['left_on_base'] = try_int(batter.get('lob'))
            b['caught_stealing'] = try_int(batter.get('cs'))
            b['stolen_bases'] = try_int(batter.get('sb'))
            b['season_walks'] = try_int(batter.get('s_bb'))
            b['season_hits'] = try_int(batter.get('s_h'))
            b['season_home_runs'] = try_int(batter.get('s_hr'))
            b['season_runs'] = try_int(batter.get('s_r'))
            b['season_rbi'] = try_int(batter.get('s_rbi'))
            b['season_strikeouts'] = try_int(batter.get('s_so'))
            b['position'] = batter.get('pos')
            b['putouts'] = try_int(batter.get('po'))
            b['errors'] = try_int(batter.get('e'))
            b['fielding'] = try_float(batter.get('fldg'))
            b = dict((k, v) for k, v in b.items() if v is not None)
            self.to_load.append(Batter(**b))

    def parse_pitchers(self):
        for pitcher in self.boxscore.find_all('pitcher'):
            p = {}
            p['pitcher_id'] = try_int(pitcher.get('id'))
            p['game_id'] = self.game_id
            homeaway = pitcher.parent.get('team_flag')
            p['team_id'] = try_int(self.boxscore.get(homeaway + '_id'))
            p['name'] = pitcher.get('name')
            p['full_name'] = pitcher.get('name_display_first_last')
            p['position'] = pitcher.get('pos')
            p['outs'] = try_int(pitcher.get('out'))
            p['batters_faced'] = try_int(pitcher.get('bf'))
            p['home_runs'] = try_int(pitcher.get('hr'))
            p['walks'] = try_int(pitcher.get('bb'))
            p['strikeouts'] = try_int(pitcher.get('so'))
            p['earned_runs'] = try_int(pitcher.get('er'))
            p['runs'] = try_int(pitcher.get('r'))
            p['hits'] = try_int(pitcher.get('h'))
            p['wins'] = try_int(pitcher.get('w'))
            p['losses'] = try_int(pitcher.get('l'))
            p['saves'] = try_int(pitcher.get('sv'))
            p['era'] = try_float(pitcher.get('era'))
            p['pitches_thrown'] = try_int(pitcher.get('np'))
            p['strikes'] = try_int(pitcher.get('s'))
            p['blown_saves'] = try_int(pitcher.get('bs'))
            p['holds'] = try_int(pitcher.get('hld'))
            p['season_innings_pitched'] = try_float(pitcher.get('s_ip'))
            p['season_hits'] = try_int(pitcher.get('s_h'))
            p['season_runs'] = try_int(pitcher.get('s_r'))
            p['season_earned_runs'] = try_int(pitcher.get('s_er'))
            p['season_walks'] = try_int(pitcher.get('s_bb'))
            p['season_strikeouts'] = try_int(pitcher.get('s_so'))
            p['game_score'] = try_int(pitcher.get('game_score'))
            p['blown_save'] = pitcher.get('blown_save')
            p['save'] = pitcher.get('save')
            p['loss'] = pitcher.get('loss')
            p['win'] = pitcher.get('win')
            p = dict((k, v) for k, v in p.items() if v is not None)
            self.to_load.append(Pitcher(**p))

    def parse_atbats(self):
        for atbat in self.atbats:
            ab = {}
            ab['at_bat_number'] = int(atbat.get('num'))
            ab['game_id'] = self.game_id
            ab['inning'] = try_int(atbat.parent.parent.get('num'))
            ab['inning_half'] = atbat.parent.name
            ab['balls'] = try_int(atbat.get('b'))
            ab['strikes'] = try_int(atbat.get('s'))
            ab['outs'] = try_int(atbat.get('o'))
            try:
                t = dt.datetime.strptime(atbat.get('start_tfs_zulu', ''), '%Y-%m-%dT%H:%M:%SZ')
                ab['start_time'] = t.replace(tzinfo=timezone('UTC')).\
                    astimezone(timezone('America/New_York'))
            except ValueError:
                logging.warning('Could not parse timestamp: Game {}; inning{}'.format(
                    self.game_id, ab['inning']))
            ab['batter_id'] = try_int(atbat.get('batter'))
            ab['pitcher_id'] = try_int(atbat.get('pitcher'))
            ab['stands'] = atbat.get('stand')
            ab['p_throws'] = atbat.get('p_throws')
            ab['description'] = atbat.get('des')
            ab['event_num'] = try_int(atbat.get('event_num'))
            ab['event'] = atbat.get('event')
            ab['score'] = atbat.get('score', 'F') == 'T'
            ab['home_team_runs'] = try_int(atbat.get('home_team_runs'))
            ab['away_team_runs'] = try_int(atbat.get('away_team_runs'))
            ab = dict((k, v) for k, v in ab.items() if v is not None)
            self.to_load.append(AtBat(**ab))

    def parse_pitches(self):
        for pitch in self.pitches:
            p = {}
            p['game_id'] = self.game_id
            p['pitch_id'] = try_int(pitch.get('id'))
            p['at_bat_number'] = try_int(pitch.parent.get('num'))
            p['description'] = pitch.get('des')
            p['type'] = pitch.get('type')
            try:
                t = dt.datetime.strptime(pitch.get('tfs_zulu', ''), '%Y-%m-%dT%H:%M:%SZ')
                p['timestamp'] = t.replace(tzinfo=timezone('UTC')).\
                    astimezone(timezone('America/New_York'))
            except ValueError:
                logging.warning('Could not parse timestamp: Game {}; pitch {}'.format(
                    self.game_id, p['pitch_id']))
            p['x'] = try_float(pitch.get('x'))
            p['y'] = try_float(pitch.get('y'))
            p['event_num'] = try_int(pitch.get('event_num'))
            p['sv_id'] = pitch.get('sv_id')
            p['play_guid'] = pitch.get('play_guid')
            p['start_speed'] = try_float(pitch.get('start_speed'))
            p['end_speed'] = try_float(pitch.get('end_speed'))
            p['sz_top'] = try_float(pitch.get('sz_top'))
            p['sz_bottom'] = try_float(pitch.get('sz_bot'))
            p['pfx_x'] = try_float(pitch.get('pfx_x'))
            p['pfx_z'] = try_float(pitch.get('pfx_z'))
            p['x0'] = try_float(pitch.get('x0'))
            p['y0'] = try_float(pitch.get('y0'))
            p['z0'] = try_float(pitch.get('z0'))
            p['vx0'] = try_float(pitch.get('vx0'))
            p['vy0'] = try_float(pitch.get('vy0'))
            p['vz0'] = try_float(pitch.get('vz0'))
            p['ax'] = try_float(pitch.get('ax'))
            p['ay'] = try_float(pitch.get('ay'))
            p['az'] = try_float(pitch.get('az'))
            p['break_y'] = try_float(pitch.get('break_y'))
            p['break_angle'] = try_float(pitch.get('break_angle'))
            p['break_length'] = try_float(pitch.get('break_length'))
            p['pitch_type'] = pitch.get('pitch_type')
            p['type_confidence'] = try_float(pitch.get('type_confidence'))
            p['zone'] = try_int(pitch.get('zone'))
            p['nasty'] = try_int(pitch.get('nasty'))
            p['spin_dir'] = try_float(pitch.get('spin_dir'))
            p['spin_rate'] = try_float(pitch.get('spin_rate'))
            p = dict((k, v) for k, v in p.items() if v is not None)
            self.to_load.append(Pitch(**p))

    def parse_runners(self):
        for runner in self.runners:
            r = {}
            r['game_id'] = self.game_id
            r['at_bat_number'] = try_int(runner.parent.get('num'))
            r['runner_id'] = try_int(runner.get('id'))
            r['start'] = runner.get('start')
            r['end'] = runner.get('end')
            r['event'] = runner.get('event')
            r['event_num'] = runner.get('event_num')
            r['score'] = (runner.get('score') is not None) & (runner.get('score') == 'T')
            r['rbi'] = (runner.get('rbi') is not None) & (runner.get('rbi') == 'T')
            r['earned'] = (runner.get('earned') is not None) & (runner.get('earned') == 'T')
            r = dict((k, v) for k, v in r.items() if v is not None)
            self.to_load.append(Runner(**r))

    def fetch_all(self):
        self.fetch_linescore()
        self.fetch_boxscore()
        self.fetch_innings()

    def parse_all(self):
        if (self.linescore is not None) & (self.boxscore is not None):
            self.parse_game()
            self.parse_team('home')
            self.parse_team('away')
            self.parse_team_stats('home')
            self.parse_team_stats('away')
            self.parse_pitchers()
            self.parse_batters()
            self.parse_atbats()
            self.parse_pitches()
            self.parse_runners()

    def load(self, skip_if_final=True):
        loaded = self.session.query(exists().where(
            (Game.game_id == self.game_id) & (Game.status == 'Final'))).scalar()
        if skip_if_final & loaded:
            return
        self.fetch_all()
        self.parse_all()
        try:
            self.session.add_all(self.to_load)
            self.session.commit()
        except exc.IntegrityError:
            self.session.rollback()
            [self.session.merge(x) for x in self.to_load]
            self.session.commit()
        self.session.close()


if __name__ == '__main__':

    def loadgame(gid):
        g = GameLoader(gid, Session)
        g.load()
    p = Pool(16)

    for d in daterange(dt.date(2008, 1, 1), dt.date(2015, 5, 14)):
        print('Getting listings for {}'.format(d.strftime('%Y-%m-%d')))
        game_ids = fetch_game_listings(d)
        p.map(loadgame, game_ids)
