import requests
import datetime
import re
import pandas as pd
from lxml import etree
from urllib.parse import unquote


def get_links(search, begin_date, end_date):
    data = []
    page = 0
    while True:
        page += 1
        print('Page:', page)
        try:
            news = get_links_from_page(search, begin_date, end_date, page)
        except:
            pass
        if len(news) == 0:
            break
        data += news
    return data


def get_links_from_page(search, begin_date, end_date, page):
    begin_date = begin_date.strftime('%Y-%m-%d')
    end_date = end_date.strftime('%Y-%m-%d')
    url = f'https://valor.globo.com/busca/?q={search}&order=recent&from={begin_date}T00:00:00-0300&to={end_date}T23:59:59-0300&page={page}'
    resp = requests.get(url)
    dom = etree.HTML(resp.content)
    news = dom.xpath('//div[contains(@class, "widget--info__text-container")]//a')
    links = [unquote(re.search(r'https.*ghtml', link.get('href')).group()) for link in news]
    return [link for link in links if '/patrocinado/' not in link
            and '/conteudo-de-marca/' not in link
            and '/noticia/' in link]


def get_news_content(url):
    resp = requests.get(url)
    dom = etree.HTML(resp.content)
    title = dom.xpath('//h1[@class="content-head__title"]')[0].text
    author = ''
    try:
        author = dom.xpath('//p[@class="content-publication-data__from"]')
        author = author[0].attrib['title'].split(',')[0]
    except:
        pass
    date = re.search(r'\d{4}/\d{2}/\d{2}', url).group()
    pattern = '//*[contains(@class, "content-text__container")\
        or contains(@class, "content-blockquote")\
        or contains(@class, "content-intertitle")\
        or contains(@class, "content-unordered-list")\
        or contains(@class, "content-ordered-list")]'
    paragraphs = dom.xpath(pattern)
    content = [''.join(p.itertext()).strip() for p in paragraphs]
    content = [p for p in content if p != '']
    text = ''
    for p in content:
        text = text + p + '\n'
    return pd.DataFrame({
        'date': [date],
        'url': [url],
        'title': [title],
        'author': [author],
        'content': [text]
    })


def scrape():
    search = 'economia'
    begin_date = datetime.date(2024, 1, 1)
    links = []
    print('Filtering links')
    while True:
        end_date = begin_date + datetime.timedelta(days=1)
        print(begin_date.strftime('%Y/%m/%d'), '-', end_date.strftime('%Y/%m/%d'))
        try:
            links += get_links(search, begin_date, end_date)
        except:
            pass
        begin_date = end_date + datetime.timedelta(days=1)
        if begin_date > datetime.date.today():
            break
    news = pd.DataFrame()
    print('Requesting news content')
    for link in links:
        news = pd.concat([news, get_news_content(link)], ignore_index=True)
    print('Saving news')
    # saving with \t to avoid unexpected results due to commas in the text
    news.to_csv(f'./{search}.tsv', index=False, sep='\t')


if __name__ == '__main__':
    scrape()
