# breakingball
Scripts to download and analyze MLB game data.

## Downloading Game Data

Start celery workers (with optional concurrency)

    celery -A gameloader worker --loglevel=info --concurrency=4

Running `load.py` will first scrape game listings from a given date (or range
of dates) and then delegate the download and extraction process to celery
workers (performed asynchronously). To load game data from a single date:

    ./load.py --start-date 2015-05-13

To load game data from a date range:

    ./load.py --start-date 2015-05-13 --end-date 2015-05-20

By default, the loader will skip games that exist in the database with a status
of 'final'. To force a download and refresh of all game data, including those
marked as final:

    ./load.py --start-date 2015-05-13 --refresh
