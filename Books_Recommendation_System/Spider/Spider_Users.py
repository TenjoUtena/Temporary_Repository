import re
import time
import random
import pymysql
import requests
import pandas as pd
from lxml import etree
from bs4 import BeautifulSoup


def load_page(url):
    # 定义请求头，模拟浏览器
    headers = {
        # "Referer":"https://www.douban.com/people/fayolee/collect",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.72 Safari/537.36",
        "Connection": "close"
    }
    try:
        # 添加请求头
        requests.packages.urllib3.disable_warnings()
        html = requests.get(url, headers=headers, verify=False)
        html.encoding = html.encoding
        # time.sleep(random.randint(3, 5))
        if html.status_code == 200:
            return html
    except Exception as e:
        print('抓取失败：%s' % e)


def load_html(url1, url2):
    headers = {
        # 'host':'www.douban.com',
        "Referer": url2,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.72 Safari/537.36",
        "Connection": "close"
    }
    try:
        # 添加请求头
        requests.packages.urllib3.disable_warnings()
        html = requests.get(url1, headers=headers, verify=False)
        html.encoding = html.encoding
        time.sleep(random.randint(3, 4))
        if html.status_code == 200:
            return html
    except Exception as e:
        print('抓取失败：%s' % e)


def parse_1(html, url):
    try:
        soup = BeautifulSoup(html.text, 'lxml')
        html = etree.HTML(html.text)
        id = html.xpath('//*[@id="profile"]/div/div[2]/div[1]/div/div/text()')[0]
        print(id)
        read_num = html.xpath('//div[@id="book"]//a/text()')[2]
        read_num = re.findall('(.*)本读过', read_num)
        print(read_num)
        link = html.xpath('//*[@id="book"]/h2/span/a[3]/@href')
        check = re.findall('(.*)//', link[0])
        link[0] = link[0].replace(r'https://book.douban.com', '')
        link[0] = 'https://book.douban.com' + link[0]
        page_num = int(int(read_num[0]) / 15)
        print(page_num)
        nickname = soup.select('#db-usr-profile > div.info > h1')
        nickname = re.search('<h1>(.*?)<', str(nickname).replace(' ', ''), re.S)
        nickname = str(nickname.group(1)).replace('\n', '')
        print(nickname)
        dict = {}

        for l in link:
            for i in range(page_num):
                print('start!')
                baseurl = l + '?start=' + str(i * 15)
                page = load_html(baseurl, url)
                soup = BeautifulSoup(page.text, 'lxml')
                page = etree.HTML(page.text)
                lis = page.xpath('//ul[@class="interest-list"]/li')
                for li in lis:
                    try:
                        book_name = li.xpath('.//h2/a/@title')
                        print(book_name)
                        score = soup.select(
                            '#content > div.grid-16-8.clearfix > div.article > ul > li:nth-child(1) > div.info > div.short-note > div ')
                        score = re.search('rating(.*)-t', str(score))
                        score = score.group(1)
                    except:
                        continue
                    dict.update({book_name[0]: score})
        sql = """
        INSERT INTO USERES(id, nickname, read_num, read_book_and_score)
        VALUES(%s, %s, %s, %s)
        """
        data = [(id, nickname, read_num[0], str(dict))]
        try:
            print('success')
            cursor.executemany(sql, data)
            conn.commit()
        except Exception as e:
            print(e)
            conn.rollback()

    except Exception as e:
        print('抓取失败：%s' % e)


if __name__ == '__main__':
    conn = pymysql.connect(
        host="localhost",
        user="root",
        password="YUAN69188059*",
        database="douban",
        charset="utf8"
    )

    cursor = conn.cursor()
    books = pd.read_sql('select * from readers_urls', conn)

    sql = """CREATE TABLE USERES (
                     id VARCHAR(40) NOT NULL UNIQUE,
                     nickname VARCHAR(40),
                     read_num TEXT,
                     read_book_and_score TEXT
                         )"""
    try:
        cursor.execute(sql)
    except Exception as e:
        print(e)
        conn.rollback()
    cursor = conn.cursor()

    urls = books['urls'].values.tolist()
    links = random.sample(urls, 700)
    i = 1479
    while i < 1500:
        html = load_page(urls[i])
        parse_1(html, urls[i])
        print(i)
        i = i + 1
    print('end')
