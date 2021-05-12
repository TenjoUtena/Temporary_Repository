#该页面是爬取豆瓣top250书籍的代码，开始时用来练手用的
import requests
import time
import random
from lxml import etree
import re
import pymysql
from bs4 import BeautifulSoup

def load_page(url):
    #定义请求头，模拟浏览器
    headers = {
        "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.72 Safari/537.36"
    }
    try:
        # 添加请求头
        html = requests.get(url, headers=headers)
        html.encoding = html.encoding
        if html.status_code == 200:
            return html
    except Exception as e:
        print('抓取失败：%s' % e)


#获取25个页面
def get_data(baseurl):
    for i in range(0,10):
        url = baseurl + str(i*25)
        html = load_page(url)
        time.sleep(random.randint(3,5))
        parse(html)

#页面解析
def parse(html):
    i = -1
    authors = []
    html = etree.HTML(html.text)
    tables = html.xpath('//div[@class="indent"]/table')
    for table in tables:
        cover_urls = table.xpath('//a[@class="nbg"]/img/@src')
        ps = table.xpath('//p[@class="pl"]/text()')

    for p in ps:
        author = re.match('(.*?) / ', str(p))
        authors.append(author.group(1))

    divs = html.xpath("//div[@class='pl2']")
    for div in divs:
        links = div.xpath("./a/@href")
        for link in links:
            i = i + 1
            page = load_page(link)
            soup = BeautifulSoup(page.text, 'lxml')
            time.sleep(random.randint(3, 5))
            page = etree.HTML(page.text)
            try:

                lis = page.xpath('//ul/li[@class="comment-item"]')
                readers = ''

                for li in lis:
                    reader = li.xpath('.//a/@href')[1]
                    readers += reader
                texts = page.xpath('//div[@class="indent"]//div[@class="intro"]/p/text()')
                contents = ''
                for text in texts:
                    contents += str(text)

                book_names = page.xpath(".//span[@property='v:itemreviewed']/text()")
                book_name = book_names[0]
                print(book_name)
                scores = page.xpath('//*[@id="interest_sectl"]/div/div[2]/strong/text()')
                score = scores[0]
                rating_nums = page.xpath('.//span[@property="v:votes"]/text()')
                rating_num = rating_nums[0]
                info = str(soup.select('#info'))
                ISBN = re.search('ISBN:</span> (.*)<br/>', info)
                ISBN = str(ISBN.group(1))
                press = re.search('出版社:</span> (.*)<br/>', info)
                press = str(press.group(1))
                publishing_year = re.search('出版年:</span> (.*?)-\d', info)
                publishing_year = str(publishing_year.group(1))
                page_num = re.search('页数:</span> (.*)<br/>', info)
                page_num = str(page_num.group(1))
                price = re.search('定价:</span> (.*)<br/>', info)
                price = str(price.group(1))

                sql = """
                        INSERT INTO BOOKS_TOP250(book_name, press, publishing_year, score, rating_num, page_num, price,
                        ISBN, content_introduction, readers, cover_url, author)
                        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                data = [(book_name, press, publishing_year, score, rating_num, page_num, price,
                         ISBN, contents, readers, cover_urls[i], authors[i])]
                try:
                    cursor.executemany(sql, data)
                    conn.commit()
                    print('success')
                except Exception as e:
                    print(e)
                    conn.rollback()
            except:
                continue

def get_tag_url(html):
    base = 'https://book.douban.com/'
    html = etree.HTML(html.text)
    trs = html.xpath("//table[@class='tagCol']//tr")
    for tr in trs:
        links = tr.xpath(".//a@href")
    print(links)


if __name__ == '__main__':
    baseurl = 'https://book.douban.com/top250?start='

    conn = pymysql.connect(
        host="localhost",
        user="root",
        password="YUAN69188059*",
        database="douban",
        charset="utf8"
    )
    cursor = conn.cursor()

    sql = """CREATE TABLE BOOKS_TOP250 (
                     BOOK_NAME TEXT,
                     AUTHOR TEXT,
                     PRESS TEXT,
                     PUBLISHING_YEAR TEXT,
                     SCORE TEXT,
                     RATING_NUM TEXT,
                     PAGE_NUM TEXT,
                     PRICE TEXT,
                     ISBN TEXT,
                     CONTENT_INTRODUCTION TEXT,
                     COVER_URL TEXT,
                     READERS TEXT)"""
    try:
        cursor.execute(sql)
    except Exception as e:
        print(e)
        conn.rollback()
    cursor = conn.cursor()

    get_data(baseurl)
    conn.close
