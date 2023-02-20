Mwmbl Crawler Script
====================

## Usage:

```
python main.py [-j n] [-u url1 url2 ...]
```

where n is the number of threads you want to run in parallel. If you specify URLs using the `-u` option, then just those URLs will be crawled instead of retrieving batches from the server.

Example: 
```
python main.py -j 4 -u https://example1.com https://example2.com https://example3.com 
```

### Crawling First 100 Pages:
```
python main.py [-j n] [-u url1 url2 ...] [--site SITE]
```
Using the `--site` option will crawl the first 100 pages of the given domain. 

Example:
```
python main.py -j 4 -u https://example.com --site https://example.com
```
## Installing
----------

Clone this repo, install [poetry](https://python-poetry.org/docs/) if necessary, cd into the repo and type

```
poetry install
poetry shell
```

then the `main.py` command as documented above.
