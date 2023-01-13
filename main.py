import json
import logging
import re
import sys
import time
from logging import getLogger
from urllib.parse import urlparse, urlunsplit
from urllib.robotparser import RobotFileParser
from uuid import uuid4

import requests
from requests import ReadTimeout, TooManyRedirects
from xdg import xdg_config_home

from justext import core, utils
from justext.core import html_to_dom
from justext.paragraph import Paragraph

DOMAIN = 'https://api.mwmbl.org/'
CRAWLER_ONLINE_URL = DOMAIN + 'crawler/'
POST_BATCH_URL = DOMAIN + 'crawler/batches/'
POST_NEW_BATCH_URL = DOMAIN + 'crawler/batches/new'

TIMEOUT_SECONDS = 3
MAX_FETCH_SIZE = 1024*1024
MAX_URL_LENGTH = 150
BAD_URL_REGEX = re.compile(r'\/\/localhost\b|\.jpg$|\.png$|\.js$|\.gz$|\.zip$|\.pdf$|\.bz2$|\.ipynb$|\.py$')
MAX_NEW_LINKS = 50
MAX_EXTRA_LINKS = 50
NUM_TITLE_CHARS = 65
NUM_EXTRACT_CHARS = 155
DEFAULT_ENCODING = 'utf8'
DEFAULT_ENC_ERRORS = 'replace'



logging.basicConfig(stream=sys.stdout, level=logging.INFO)

logger = getLogger(__name__)


def fetch(url):
    """
    Fetch with a maximum timeout and maximum fetch size to avoid big pages bringing us down.

    https://stackoverflow.com/a/22347526
    """

    r = requests.get(url, stream=True, timeout=TIMEOUT_SECONDS)

    size = 0
    start = time.time()

    content = b""
    for chunk in r.iter_content(1024):
        if time.time() - start > TIMEOUT_SECONDS:
            raise ValueError('Timeout reached')

        content += chunk

        size += len(chunk)
        if size > MAX_FETCH_SIZE:
            logger.info("Maximum size reached")
            break

    return r.status_code, content


def robots_allowed(url):
    try:
        parsed_url = urlparse(url)
    except ValueError:
        logger.info(f"Unable to parse URL: {url}")
        return False

    if parsed_url.path.rstrip('/') == '' and parsed_url.query == '':
        logger.info(f"Allowing root domain for URL: {url}")
        return True

    robots_url = urlunsplit((parsed_url.scheme, parsed_url.netloc, 'robots.txt', '', ''))

    parse_robots = RobotFileParser(robots_url)

    try:
        status_code, content = fetch(robots_url)
    except (ValueError, ConnectionError):
        logger.exception(f"Robots error: {robots_url}")
        return True

    if status_code != 200:
        logger.info(f"Robots status code: {status_code}")
        return True

    parse_robots.parse(content.decode('utf-8').splitlines())
    allowed = parse_robots.can_fetch('Mwmbl', url)
    logger.info(f"Robots allowed for {url}: {allowed}")
    return allowed


def get_new_links(paragraphs: list[Paragraph]):
    new_links = set()
    extra_links = set()
    for paragraph in paragraphs:
        if len(paragraph.links) > 0:
            for link in paragraph.links:
                if link.startswith('http') and len(link) <= MAX_URL_LENGTH:
                    if BAD_URL_REGEX.search(link):
                        logger.info(f"Found bad URL: {link}")
                        continue
                    try:
                        parsed_url = urlparse(link)
                    except ValueError:
                        logger.info(f"Unable to parse link {link}")
                        continue
                    url_without_hash = urlunsplit((parsed_url.scheme, parsed_url.netloc, parsed_url.path, parsed_url.query, ''))
                    if paragraph.class_type == 'good':
                        if len(new_links) < MAX_NEW_LINKS:
                            new_links.add(url_without_hash)
                    else:
                        if len(extra_links) < MAX_EXTRA_LINKS and url_without_hash not in new_links:
                            extra_links.add(url_without_hash)
                if len(new_links) >= MAX_NEW_LINKS and len(extra_links) >= MAX_EXTRA_LINKS:
                    return new_links, extra_links
    return new_links, extra_links


def crawl_url(url):
    js_timestamp = int(time.time() * 1000)
    allowed = robots_allowed(url)
    if not allowed:
        return {
            'url': url,
            'status': None,
            'timestamp': js_timestamp,
            'content': None,
            'error': {
                'name': 'RobotsDenied',
                'message': 'Robots do not allow this URL',
            }
        }

    try:
        status_code, content = fetch(url)
    except (TimeoutError, ReadTimeout, TooManyRedirects) as e:
        return {
            'url': url,
            'status': None,
            'timestamp': js_timestamp,
            'content': None,
            'error': {
                'name': 'AbortError',
                'message': str(e),
            }
        }

    if len(content) == 0:
        return {
            'url': url,
            'status': status_code,
            'timestamp': js_timestamp,
            'content': None,
            'error': {
                'name': 'NoResponseText',
                'message': 'No response found',
            }
        }

    dom = html_to_dom(content, DEFAULT_ENCODING, None, DEFAULT_ENC_ERRORS)
    title_element = dom.xpath("//title")
    title = ""
    if len(title_element) > 0:
        title_text = title_element[0].text
        if title_text is not None:
            title = title_text.strip()

    if len(title) > NUM_TITLE_CHARS:
        title = title[:NUM_TITLE_CHARS - 1] + '…'

    paragraphs = core.justext_from_dom(dom, utils.get_stoplist("English"))

    new_links, extra_links = get_new_links(paragraphs)
    logger.info(f"Got new links {new_links}")
    logger.info(f"Got extra links {extra_links}")

    extract = ''
    for paragraph in paragraphs:
        if paragraph.class_type != 'good':
            continue
        extract += ' ' + paragraph.text.strip()
        if len(extract) > NUM_EXTRACT_CHARS:
            extract = extract[:NUM_EXTRACT_CHARS - 1] + '…'
            break

    return {
      'url': url,
      'status': status_code,
      'timestamp': js_timestamp,
      'content': {
        'title': title,
        'extract': extract,
        'links': sorted(new_links),
        'extra_links': sorted(extra_links),
      },
      'error': None
    }


def crawl_batch(batch):
    crawl_results = []
    for url in batch:
        logger.info(f"Crawling URL {url}")
        result = crawl_url(url)
        logger.info(f"Got crawl result: {result}")
        crawl_results.append(result)
    return crawl_results


def get_user_id():
    path = xdg_config_home() / 'mwmbl' / 'config.json'
    try:
        return json.loads(path.read_text())['user_id']
    except FileNotFoundError:
        user_id = str(uuid4())
        path.parent.mkdir(exist_ok=True, parents=True)
        path.write_text(json.dumps({'user_id': user_id}))
        return user_id


def send_batch(batch_items, user_id):
    batch = {
      'user_id': user_id,
      'items': batch_items,
    }

    logger.info("Sending batch", batch)

    response = requests.post(POST_BATCH_URL, json=batch, headers={'Content-Type': 'application/json'})
    logger.info(f"Response status: {response.status_code}, {response.content}")


def get_batch(user_id: str):
    response = requests.post(POST_NEW_BATCH_URL, json={'user_id': user_id})
    if response.status_code != 200:
        raise ValueError(f"No batch received, status code {response.status_code}, content {response.content}")

    urls_to_crawl = response.json()
    if len(urls_to_crawl) == 0:
        raise ValueError("No URLs in batch")

    return urls_to_crawl


def run_crawl_iteration(user_id):
    new_batch = get_batch(user_id)
    logger.info(f"Got batch with {len(new_batch)} items")
    crawled_batch = crawl_batch(new_batch)
    send_batch(crawled_batch, user_id)


def run_continuously():
    user_id = get_user_id()
    while True:
        try:
            run_crawl_iteration(user_id)
        except Exception:
            logger.exception("Exception running crawl iteration")
            time.sleep(10)


if __name__ == '__main__':
    run_continuously()
