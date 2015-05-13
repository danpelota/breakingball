from bs4 import BeautifulSoup
import requests
import re
from utils import gid_to_url, gid_to_date, try_int, try_float, dict_to_db
import datetime as dt
from download import logging
from models import Session, Game, Team, Team_Stats, Pitcher

class GameLoader:
    def __init__(self, game_id, session):
        self.game_id = game_id
        self.session = session
        self.game_date = gid_to_date(game_id)
        self.season = self.game_date.year
        self.base_url = gid_to_url(game_id)
        self.url = gid_to_url(game_id)

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
            logging.warn('Could not parse time for {}'.format(game_id))

        game['venue'] = self.linescore.get('venue')
        game['game_type'] = self.linescore.get('game_type')
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
        self.session.merge(Game(**game))

    def parse_team(self, homeaway='home'):
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
        self.session.merge(Team(**team))

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
        wins = try_int(self.boxscore.get(homeaway + '_wins'))
        losses = try_int(self.boxscore.get(homeaway + '_loss'))
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
        team_stats['era'] = try_int(batting.get('era'))
        self.session.merge(Team_Stats(**team_stats))

    def parse_pitchers(self):
        #pitching = self.boxscore.find('pitching')
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
            p['season_innings_pitched'] = try_int(pitcher.get('s_ip'))
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
            self.session.merge(Pitcher(**p))


    def parse_all(self):
        self.parse_game()
        self.parse_team('home')
        self.parse_team('away')
        self.parse_team_stats('home')
        self.parse_team_stats('away')
        self.parse_pitchers()
        self.session.commit()











# game_id = 'gid_2015_04_25_houmlb_oakmlb_1'
# url = gid_to_url(game_id)
# game_date = gid_to_date(game_id)

# boxscore = BeautifulSoup(requests.get(url + 'boxscore.xml').content)
# inning_links = BeautifulSoup(requests.get(url + 'inning').content)
# available_innings = inning_links.find_all('a', href=re.compile(r'[0-9]\.xml$'))
# inning_urls = [url + '/inning/' + x.get('href') for x in available_innings]
# inning_content = ''.join([str(requests.get(x).content) for x in inning_urls])
# innings = BeautifulSoup(inning_content)

# game = {}
# game['game_id'] = game_id
# game['game_date'] = game_date
# time_text = '{} {}'.format(linescore.get('time'), linescore.get('ampm'))
# try:
#     game_time = dt.datetime.strptime(time_text, '%I:%M %p').time()
#     game['game_datetime'] = dt.datetime.combine(game_date, game_time)
# except ValueError:
#     logging.warn('Could not parse time for {}'.format(game_id))
# game['season'] = game_date.year
# game['venue'] = linescore.get('venue')
# game['game_type'] = linescore.get('game_type')
# game['inning'] = linescore.get('inning')
# game['outs'] = try_int(linescore.get('outs'))
# game['top_inning'] = ((linescore.get('top_inning') is not None) &
#                       (linescore.get('top_inning') == 'Y'))
# game['status'] = linescore.get('status')
# game['home_team_id'] = try_int(linescore.get('home_team_id'))
# game['away_team_id'] = try_int(linescore.get('away_team_id'))
# game['home_team_runs'] = try_int(linescore.get('home_team_runs'))
# game['away_team_runs'] = try_int(linescore.get('away_team_runs'))
# game['home_team_hits'] = try_int(linescore.get('home_team_hits'))
# game['away_team_hits'] = try_int(linescore.get('away_team_hits'))
# game['home_team_errors'] = try_int(linescore.get('home_team_errors'))
# game['away_team_errors'] = try_int(linescore.get('away_team_errors'))
#
#
