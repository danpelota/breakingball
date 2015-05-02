import datetime as dt

def gid_to_date(game_id):
    year = game_id[4:8]
    month = game_id[9:11]
    day = game_id[12:14]
    game_date = dt.date(int(year), int(month), int(day))
    return game_date

def gid_to_url(game_id):
    game_date = gid_to_date(game_id)
    url = ("http://gd2.mlb.com/components/game/mlb/"
           "year_{0:04}/month_{1:02}/day_{2:02}/{3}/").format(
               game_date.year, game_date.month, game_date.day, game_id)
    return url
