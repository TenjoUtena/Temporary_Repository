#该页面为尝试用多线程爬取的代码，但是失败了
import re
import time
import queue
import random
import pymysql
import threading
import requests
from lxml import etree
from bs4 import BeautifulSoup


def load_page(url):
    #定义请求头，模拟浏览器
    headers = {
        "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.72 Safari/537.36",
        "Connection":"close"
    }
    try:
        # 添加请求头
        html = requests.get(url, headers=headers)
        html.encoding = html.encoding
        if html.status_code == 200:
            return html
    except Exception as e:
        print('抓取失败：%s' % e)


#页面解析
def parse_1(html):
    try:
        i = 0
        authors = []
        html = etree.HTML(html.text)
        ps = []
        tables = html.xpath('//div[@class="indent"]/table')
        for table in tables:
            cover_url = table.xpath('//a[@class="nbg"]/img/@src')
            ps = table.xpath('//p[@class="pl"]/text()')
        for p in ps:
            author = re.match('(.*?) / ', str(p))
            authors.append(author.group(1))
        while i < 25:
            sql = """
                    INSERT INTO BOOKS_TOP250(cover_url, author)
                    VALUES(%s, %s)
                """
            data = [(cover_url[i], authors[i])]
            # try:
            #     cursor.executemany(sql, data)
            #     conn.commit()
            #     print('success')
            # except Exception as e:
            #     print(e)
            #     conn.rollback()
            i = i + 1
    except Exception as e:
        print('%s', e)

    links = []
    divs = html.xpath("//div[@class='pl2']")
    for div in divs:
        links.append(div.xpath("./a/@href"))
    return links


def parse_2(html):
    try:
        soup = BeautifulSoup(html.text, 'lxml')
        html = etree.HTML(html.text)
        lis = html.xpath('//ul/li[@class="comment-item"]')
        readers = ''
        for li in lis:
            reader = li.xpath('.//a/@href')[1]
            readers += reader

        texts = html.xpath('//div[@class="indent"]//div[@class="intro"]/p/text()')
        contents = ''
        for text in texts:
            contents += str(text)

        book_name = html.xpath(".//span[@property='v:itemreviewed']/text()")
        print(book_name)
        score = html.xpath('//*[@id="interest_sectl"]/div/div[2]/strong/text()')
        print(score)
        rating_num = html.xpath('.//span[@property="v:votes"]/text()')
        print(rating_num)
        info = str(soup.select('#info'))
        ISBN = re.search('ISBN:</span> (.*)<br/>', info)
        ISBN = str(ISBN.group(1))
        print(ISBN)
        press = re.search('出版社:</span> (.*)<br/>', info)
        press = str(press.group(1))
        print(press)
        publishing_year = re.search('出版年:</span> (.*?)-\d', info)
        publishing_year = str(publishing_year.group(1))
        print(publishing_year)
        page_num = re.search('页数:</span> (.*)<br/>', info)
        page_num = str(page_num.group(1))
        print(page_num)
        price = re.search('定价:</span> (.*)<br/>', info)
        price = str(price.group(1))
        print(price)

        # sql = """
        #      INSERT INTO BOOKS_TOP250(book_name, press, publishing_year, score, rating_num, page_num, price,
        #      ISBN, content_introduction, readers)
        #      VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        #     """
        # data = [(book_name[0], press, publishing_year, score[0], rating_num[0], page_num, price,
        #      ISBN, contents, readers)]
        # try:
        #     cursor.executemany(sql, data)
        #     conn.commit()
        #     print('success')
        # except Exception as e:
        #     print(e)
        #     conn.rollback()
    except Exception as e:
        print(e)

def get_tag_url(html):
    base = 'https://book.douban.com/'
    html = etree.HTML(html.text)
    trs = html.xpath("//table[@class='tagCol']//tr")
    for tr in trs:
        links = tr.xpath(".//a@href")
    print(links)

def do_craw_1(url1_q: queue.Queue,html1_q: queue.Queue):
    while True:
        if url1_q.empty():
            break
        url = url1_q.get()
        html = load_page(url)
        html1_q.put(html)
        time.sleep(random.randint(3, 5))

def do_parse_1(html1_q: queue.Queue,url2_q: queue.Queue):
    while True:
        # if html1_q.empty():
        #     break
        html = html1_q.get()
        links = parse_1(html)
        for link in links:
            link = str(link).replace("'","")
            link = link.replace('[','')
            link = link.replace(']','')
            print(link)
            url2_q.put(link)


def do_craw_2(url2_q: queue.Queue,html2_q: queue.Queue):
    while True:
        # if url2_q.empty():
        #     break
        url = url2_q.get()
        html = load_page(url)
        html2_q.put(html)
        time.sleep(random.randint(3, 5))

def do_parse_2(html2_q: queue.Queue):
    while True:
        # if html2_q.empty():
        #     break
        html = html2_q.get()
        parse_2(html)


if __name__ == '__main__':
    baseurl = 'https://book.douban.com/top250?start='
    url1_q = queue.Queue()
    html1_q = queue.Queue()
    url2_q = queue.Queue()
    html2_q = queue.Queue()
    urls = []
    for i in range(2, 3):
        url = baseurl + str(i * 25)
        url1_q.put(url)

    # conn = pymysql.connect(
    #     host="localhost",
    #     user="root",
    #     password="YUAN69188059*",
    #     database="douban",
    #     charset="utf8"
    # )
    # cursor = conn.cursor()
    # sql = """CREATE TABLE BOOKS_TOP250 (
    #              BOOK_NAME TEXT,
    #              AUTHOR TEXT,
    #              PRESS TEXT,
    #              PUBLISHING_YEAR TEXT,
    #              SCORE TEXT,
    #              RATING_NUM TEXT,
    #              PAGE_NUM TEXT,
    #              PRICE TEXT,
    #              ISBN TEXT,
    #              CONTENT_INTRODUCTION TEXT,
    #              COVER_URL TEXT,
    #              READERS TEXT)"""
    # try:
    #     cursor.execute(sql)
    # except Exception as e:
    #     print(e)
    #     conn.rollback()
    # cursor = conn.cursor()

    t = []
    T = []
    # t.append(threading.Thread(target=do_craw_1, args=(url1_q, html1_q)))
    # t.append(threading.Thread(target=do_parse_1, args=(html1_q, url2_q)))
    # t.append(threading.Thread(target=do_craw_2, args=(url2_q, html2_q)))
    # t.append(threading.Thread(target=do_parse_2, args=(html2_q,)))
    #
    # for idx in range(4):
    #     t[idx].start()
    #     t[idx].join()
    #     print('end')

    t1 = threading.Thread(target=do_craw_1, args=(url1_q, html1_q))
    t1.start()

    t2 = threading.Thread(target=do_parse_1, args=(html1_q, url2_q))
    t2.start()

    for idx in range(3):
        t.append(threading.Thread(target=do_craw_2, args=(url2_q, html2_q)))
        t[idx].start()
        T.append(threading.Thread(target=do_parse_2, args=(html2_q,)))
        T[idx].start()

    t1.join()
    print('end')
    t2.join()
    print('end')
    for idx in range(3):
        t[idx].join()
        print('end')
        T[idx].join()

    #conn.close