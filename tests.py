import unittest
import requests
from utils import gid_to_url, gid_to_date, date_to_url
from load import fetch_game_listings
import datetime as dt
from urllib.parse import urljoin


class UtilsTest(unittest.TestCase):
    def test_gid_to_date(self):
        expected = [
            ('gid_2015_04_06_bosmlb_phimlb_1', dt.date(2015, 4, 6)),
            ('gid_2000_04_30_chamlb_detmlb_1', dt.date(2000, 4, 30)),
            ('gid_2008_03_01_atlmlb_houmlb_1', dt.date(2008, 3, 1))
        ]
        for gid, game_date in expected:
            self.assertEqual(gid_to_date(gid), game_date)

    def test_gid_to_url(self):
        expected = [
            ('gid_2008_03_20_detmlb_atlmlb_1',
             urljoin('http://gd2.mlb.com/components/game/mlb/year_2008/',
                     'month_03/day_20/gid_2008_03_20_detmlb_atlmlb_1')),
            ('gid_2012_07_17_anamlb_detmlb_1',
             urljoin('http://gd2.mlb.com/components/game/mlb/year_2012/',
                     'month_07/day_17/gid_2012_07_17_anamlb_detmlb_1'))
        ]

        for gid, url in expected:
            self.assertEqual(gid_to_url(gid), url)

    def test_date_to_url(self):
        expected = [
            (dt.date(2014, 7, 12),
             urljoin('http://gd2.mlb.com/components/game/mlb/year_2014/',
                     'month_07/day_12/')),
            (dt.date(2008, 4, 28),
             urljoin('http://gd2.mlb.com/components/game/mlb/year_2008/',
                     'month_04/day_28/')),
            (dt.date(2016, 8, 10),
             urljoin('http://gd2.mlb.com/components/game/mlb/year_2016/',
                     'month_08/day_10/')),
        ]

        for game_date, url in expected:
            self.assertEqual(date_to_url(game_date), url)


class LoadTests(unittest.TestCase):
    def test_fetch_game_listings(self):
        fetched = fetch_game_listings(dt.date(2012, 7, 17))
        expected = [
            'gid_2012_07_17_anamlb_detmlb_1',
            'gid_2012_07_17_arimlb_cinmlb_1',
            'gid_2012_07_17_balmlb_minmlb_1',
            'gid_2012_07_17_chamlb_bosmlb_1',
            'gid_2012_07_17_clemlb_tbamlb_1',
            'gid_2012_07_17_flomlb_chnmlb_1',
            'gid_2012_07_17_houmlb_sdnmlb_1',
            'gid_2012_07_17_miamlb_chnmlb_1',
            'gid_2012_07_17_nynmlb_wasmlb_1',
            'gid_2012_07_17_phimlb_lanmlb_1',
            'gid_2012_07_17_pitmlb_colmlb_1',
            'gid_2012_07_17_seamlb_kcamlb_1',
            'gid_2012_07_17_sfnmlb_atlmlb_1',
            'gid_2012_07_17_slnmlb_milmlb_1',
            'gid_2012_07_17_texmlb_oakmlb_1',
            'gid_2012_07_17_tormlb_nyamlb_1'
        ]
        self.assertTrue(len(expected) == len(fetched))
        self.assertTrue(sorted(expected) == sorted(fetched))

        no_games = fetch_game_listings(dt.date(2080, 1, 1))
        self.assertEqual(no_games, [])



if __name__ == "__main__":
    unittest.main()
