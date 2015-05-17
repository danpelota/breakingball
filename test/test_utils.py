import datetime as dt
import unittest
from breakingball.utils import try_int, try_float, fetch_game_listings, \
    gid_to_date, gid_to_url, date_to_url, daterange


class TestUtils(unittest.TestCase):

    def test_try_int(self):
        self.assertEqual(try_int('3'), 3)
        self.assertEqual(try_int('a'), None)
        self.assertEqual(try_int(None), None)

    def test_try_float(self):
        self.assertEqual(try_float('0.133'), .133)
        self.assertEqual(try_float('-15.2'), -15.2)
        self.assertEqual(try_float('text'), None)
        self.assertEqual(try_float(None), None)

    def test_fetch_game_listings(self):
        gids = fetch_game_listings(dt.date(2015, 5, 9))
        games = [
            'gid_2015_05_09_atlmlb_wasmlb_1',
            'gid_2015_05_09_balmlb_nyamlb_1',
            'gid_2015_05_09_bosmlb_tormlb_1',
            'gid_2015_05_09_chnmlb_milmlb_1',
            'gid_2015_05_09_cinmlb_chamlb_1',
            'gid_2015_05_09_cinmlb_chamlb_2',
            'gid_2015_05_09_houmlb_anamlb_1',
            'gid_2015_05_09_kcamlb_detmlb_1',
            'gid_2015_05_09_lanmlb_colmlb_1',
            'gid_2015_05_09_miamlb_sfnmlb_1',
            'gid_2015_05_09_minmlb_clemlb_1',
            'gid_2015_05_09_nynmlb_phimlb_1',
            'gid_2015_05_09_oakmlb_seamlb_1',
            'gid_2015_05_09_sdnmlb_arimlb_1',
            'gid_2015_05_09_slnmlb_pitmlb_1',
            'gid_2015_05_09_texmlb_tbamlb_1'
        ]
        self.assertTrue(set(gids) == set(games))

    def test_gid_to_date(self):
        self.assertEqual(gid_to_date('gid_2015_04_27_phimlb_slnmlb_1'),
                         dt.date(2015, 4, 27))

    def test_gid_to_url(self):
        self.assertEqual(gid_to_url('gid_2015_04_27_phimlb_slnmlb_1'),
                         ('http://gd2.mlb.com/components/game/mlb/year_2015/'
                          'month_04/day_27/gid_2015_04_27_phimlb_slnmlb_1/'))

    def test_date_to_url(self):
        self.assertEqual(date_to_url(dt.date(2014, 5, 4)),
                         ('http://gd2.mlb.com/components/game/mlb/year_2014/'
                          'month_05/day_04/'))

    def test_daterange(self):
        dr = daterange(dt.date(2013, 6, 1), dt.date(2013, 6, 7))
        dates = [x for x in dr]
        self.assertEqual(len(dates), 7)
        self.assertEqual(dates[0], dt.date(2013, 6, 1))
        self.assertEqual(dates[6], dt.date(2013, 6, 7))


if __name__ == "__main__":
    unittest.main()
