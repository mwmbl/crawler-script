import requests

from justext import core, utils


def fetch(url):
    response = requests.get(url)
    paragraphs = core.justext(response.content, utils.get_stoplist("English"))
    for paragraph in paragraphs:
        print("Paragraph", paragraph.text, paragraph.links, paragraph.class_type)


if __name__ == '__main__':
    fetch("https://blog.mwmbl.org/articles/fall-2022-update/")
