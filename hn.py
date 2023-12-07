import json
import logging
import re
import sqlite3
import sys
import time
import urllib
from concurrent.futures import ThreadPoolExecutor
from html import unescape
from urllib.parse import urlparse

import requests
from lxml.etree import ParserError

from justext.core import html_to_dom
from main import send_batch, get_user_id, DEFAULT_ENCODING, DEFAULT_ENC_ERRORS, NUM_TITLE_CHARS, NUM_EXTRACT_CHARS

DATABASE_PATH = 'hn.db'
HREF_REGEX = re.compile(r'href="([^"]+)"')
HN_URL = 'https://news.ycombinator.com/'
NUM_ITEMS_TO_FETCH = 100
NUM_THREADS = 50
HN_ITEM_URL = 'https://news.ycombinator.com/item?id={}'


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


def get_hn_items(most_recent_ids):
    """
    Get a batch of 100 URLs from the Hacker News API
    """
    # Retrieve all items starting from max_item downwards that haven't been retrieved before
    existing_ids = set(ids_exist(most_recent_ids))
    non_existing_ids = [i for i in most_recent_ids if i not in existing_ids]

    # Call fetch_urls_for_item in parallel using threads
    pool = ThreadPoolExecutor(max_workers=NUM_THREADS)
    futures = [pool.submit(fetch_item, item_id) for item_id in non_existing_ids]
    items = []
    for future in futures:
        try:
            item = future.result()
            assert item is not None
            print("Got item", item)
            items.append(item)
        except Exception as e:
            logging.exception(f"Error fetching items")
            raise e

    return items


def fetch_item(item_id):
    # Extract URLs from the text
    item_data = requests.get(f'https://hacker-news.firebaseio.com/v0/item/{item_id}.json').json()
    js_timestamp = int(time.time() * 1000)
    if item_data is not None:
        new_urls = []
        content_str = item_data.get('text', '')
        paragraphs = None
        if content_str:
            content = unescape(item_data.get('text'))
            try:
                dom = html_to_dom(content, DEFAULT_ENCODING, None, DEFAULT_ENC_ERRORS)
                links = dom.xpath('//a')
                for link in links:
                    url = link.get('href')
                    if url:
                        new_urls.append(url)
                        print(f"Found URL in text for item {item_id}: {url}")
                paragraphs = [p.text for p in dom.xpath('//p') if p.text]
            except ParserError:
                print(f"Error parsing HTML for item {item_id}")

        if paragraphs is None:
            paragraphs = []

        if 'url' in item_data:
            new_urls.append(item_data['url'])
            print(f"Found URL in item {item_id} of type {item_data['type']}: {item_data['url']}")

        if 'title' in item_data and item_data['title']:
            title = unescape(item_data['title'])
            extract = ' '.join(paragraphs)
        else:
            title = paragraphs[0] if paragraphs else ''
            extract = ' '.join(paragraphs[1:])

        if len(title) > NUM_TITLE_CHARS:
            title = title[:NUM_TITLE_CHARS - 1] + '…'

        if len(extract) > NUM_EXTRACT_CHARS:
            extract = extract[:NUM_EXTRACT_CHARS - 1] + '…'

        return {
            'url': HN_ITEM_URL.format(item_id),
            'status': 200,
            'timestamp': js_timestamp,
            'content': {
                'title': title,
                'extract': extract,
                'links': new_urls,
                'extra_links': [],
                'links_only': False,
            },
            'error': None
        }

    return {
        'url': HN_URL.format(item_id),
        'status': 404,
        'timestamp': js_timestamp,
    }


def main():
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    user_id = get_user_id()

    create_id_database()
    max_item = requests.get('https://hacker-news.firebaseio.com/v0/maxitem.json').json()
    while True:
        most_recent_ids = list(range(max_item, max_item - NUM_ITEMS_TO_FETCH, -1))
        items = get_hn_items(most_recent_ids)

        # Only keep items with an extract and a title, or links
        useful_items = [
            item for item in items
            if 'content' in item
            and ((item['content']['title'] and item['content']['extract'])
                 or item['content']['links'])
        ]

        max_item -= NUM_ITEMS_TO_FETCH

        if len(useful_items) == 0:
            continue

        print(f"Sending batch of {len(useful_items)}")
        print("Items", json.dumps(useful_items, indent=2), sep='\n')
        retries = 0
        while True:
            status = send_batch(useful_items, user_id)
            if status == 200:
                add_ids(most_recent_ids)
                break
            elif status in {502, 504}:
                retries += 1
                if retries > 10:
                    raise Exception(f"Error sending batch: {status}")
                print("Got 502, retrying")
                time.sleep(5)
            else:
                raise Exception(f"Error sending batch: {status}")


if __name__ == '__main__':
    main()

