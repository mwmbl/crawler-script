Mwmbl Crawler Script
====================

Usage:

```
python main.py [-j n] [-u url1 url2 ...]
```

where n is the number of threads you want to run in parallel. If you specify URLs using the `-u` option, then just those URLs will be crawled instead of retrieving batches from the server.

Installing
----------

Clone this repo, install [poetry](https://python-poetry.org/docs/) if necessary, cd into the repo and type

```
poetry install
poetry shell
```

then the `main.py` command as documented above.
