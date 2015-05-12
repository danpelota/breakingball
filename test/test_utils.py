import datetime as dt
import unittest
from breakingball.utils import try_int, try_float, gid_to_date, gid_to_url, \
    date_to_url, daterange


class Test(unittest.TestCase):

    def test_try_int(self):
        self.assertEqual(try_int('3'), 3)
        self.assertEqual(try_int('a'), None)
        self.assertEqual(try_int(None), None)

    def test_try_float(self):
        self.assertEqual(try_float('0.133'), .133)
        self.assertEqual(try_float('-15.2'), -15.2)
        self.assertEqual(try_float('text'), None)
        self.assertEqual(try_float(None), None)

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
