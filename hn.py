import logging
import re
import sqlite3
import sys
import urllib
from concurrent.futures import ThreadPoolExecutor
from html import unescape
from urllib.parse import urlparse

import requests

from main import send_batch, get_user_id


DATABASE_PATH = 'hn.db'
HREF_REGEX = re.compile(r'href="([^"]+)"')
HN_URL = 'https://news.ycombinator.com/'
NUM_ITEMS_TO_FETCH = 500
NUM_THREADS = 50


# Create a sqlite database to store IDs that have been retrieved
def create_id_database():
    conn = sqlite3.connect(DATABASE_PATH)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS ids
        (id INTEGER PRIMARY KEY)
    ''')
    conn.commit()
    conn.close()


def add_ids(hn_ids: list[int]):
    conn = sqlite3.connect(DATABASE_PATH)
    c = conn.cursor()
    hn_ids_prepared = [(i,) for i in hn_ids]
    c.executemany('''
        INSERT OR REPLACE INTO ids (id) VALUES (?)
    ''', hn_ids_prepared)
    conn.commit()
    conn.close()


def ids_exist(hn_ids: list[int]) -> list[int]:
    conn = sqlite3.connect(DATABASE_PATH)
    c = conn.cursor()
    c.execute('''
        SELECT id FROM ids WHERE id IN ({})
    '''.format(','.join('?' * len(hn_ids))), hn_ids)
    ids = [row[0] for row in c.fetchall()]
    conn.close()
    return ids


def get_hn_urls(most_recent_ids):
    """
    Get a batch of 100 URLs from the Hacker News API
    """
    # Retrieve all items starting from max_item downwards that haven't been retrieved before
    existing_ids = set(ids_exist(most_recent_ids))
    non_existing_ids = [i for i in most_recent_ids if i not in existing_ids]

    # Call fetch_urls_for_item in parallel using threads
    pool = ThreadPoolExecutor(max_workers=NUM_THREADS)
    futures = [pool.submit(fetch_urls_for_item, item_id) for item_id in non_existing_ids]
    urls = []
    for future in futures:
        try:
            urls += future.result()
        except Exception as e:
            logging.error(f"Error fetching URLs: {e}")

    return urls


def fetch_urls_for_item(item_id):
    # Extract URLs from the text
    new_urls = []
    item = requests.get(f'https://hacker-news.firebaseio.com/v0/item/{item_id}.json').json()
    if item is not None:
        text = item.get('text', '')
        timestamp = item['time'] * 1000
        for line in text.split():
            # Find all URLs in the text
            matches = HREF_REGEX.findall(line)
            for url in matches:
                # url = match.group(1)
                # Decode URL in format https:&#x2F;&#x2F;news.ycombinator.com&#x2F;item?id=33268319
                decoded_url = unescape(url)
                parsed_url = urlparse(decoded_url)
                if parsed_url.netloc:
                    new_urls.append((decoded_url, timestamp))
                    print(f"Found URL in text for item {item_id}: {decoded_url}")

        if 'url' in item:
            new_urls.append((item['url'], timestamp))
            print(f"Found URL in item {item_id} of type {item['type']}: {item['url']}")
    return new_urls


def main():
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    user_id = get_user_id()

    create_id_database()
    max_item = requests.get('https://hacker-news.firebaseio.com/v0/maxitem.json').json()
    while True:
        items = []
        ids_processed = []
        while len(items) < 10:
            most_recent_ids = list(range(max_item, max_item - NUM_ITEMS_TO_FETCH, -1))
            urls_and_timestamps = get_hn_urls(most_recent_ids)
            max_item -= NUM_ITEMS_TO_FETCH
            if not urls_and_timestamps:
                continue

            time = max(t for _, t in urls_and_timestamps)
            urls = [url for url, _ in urls_and_timestamps]

            item = {
                'url': HN_URL,
                'status': 200,
                'timestamp': time,
                'content': {
                    'title': "Hacker News",
                    'extract': "",
                    'links': urls,
                    'extra_links': [],
                    'links_only': True,
                },
                'error': None
            }
            items.append(item)
            ids_processed += most_recent_ids

        print(f"Sending batch of {len(items)}")
        send_batch(items, user_id)
        add_ids(ids_processed)



if __name__ == '__main__':
    main()

