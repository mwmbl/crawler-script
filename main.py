import logging
import re
import sys
import time
from logging import getLogger
from urllib.parse import urlparse, urlunsplit
from urllib.robotparser import RobotFileParser

import requests

from justext import core, utils
from justext.core import html_to_dom
from justext.paragraph import Paragraph

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
    parse_robots.read()
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

    status_code, content = fetch(url)

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
    titles = dom.xpath("//title")
    title = titles[0].text.strip() if len(titles) > 0 else None
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
        result = crawl_url(url)
        logger.info(f"Got crawl result: {result}")
        crawl_results.append(result)
    return crawl_results


if __name__ == '__main__':
    batch = crawl_batch([
        "https://blog.mwmbl.org/articles/fall-2022-update/",
        "https://google.com/?s=banana"
        ])
