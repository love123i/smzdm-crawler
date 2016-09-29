#!/usr/bin/env python
# encoding:utf-8
# main.py
# 2016/9/25  17:46
from __future__ import unicode_literals
from colors import *
import requests
from bs4 import BeautifulSoup
import traceback,sys,re,json,time,random
from datetime import datetime,timedelta
from db import DB

DEBUG_MODE = True

class Web_Crawler(object):
    @staticmethod
    def get_soup(content='', features='html5lib'):
        return BeautifulSoup(content, features)

    def __init__(self, host):
        self.host = host
        self.session = requests.session()
        self.session.headers = self._get_headers()

    def set_host(self, host):
        self.host = host
        self.session.headers = self._get_headers()



    def _get_headers(self):
        HTTP_HEADERS = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*',
            'Connection': 'keep-alive',
            'Accept-Encoding': 'gzip, deflate',
            'Host': self.host,
        }

    def http_get(self, url):
        return self.session.get(url)

    def http_post(self, url, data):
        return self.session.post(url, data)



class SMZDM(Web_Crawler):

    def __init__(self):
        Web_Crawler.__init__(self,'www.smzdm.com')

    def crawl_youhui(self, page=1, show_debug=False):
        global logger

        url = 'http://www.smzdm.com/youhui/p%s' % page
        reponse = self.http_get(url)
        soup = Web_Crawler.get_soup(reponse.content)

        #print yellow(soup.prettify())
        soup_item_list = soup.find_all('div', {'class':'list list_preferential '})
        #logger.debug('page:%s count:%s' % (green(page), green(len(soup_item_list))))

        item_list = []
        for index,item_origin in enumerate(soup_item_list):
            #logger.debug('--->%s' % index)
            item = {}
            try:
                #article_id  = item_origin['articleid']
                item_time   = int(item_origin['timesort'])
                _temp_url_price_title = item_origin.find('a', {'href':True, 'target':'_blank'})
                url         = _temp_url_price_title['href']
                title       = _temp_url_price_title.text
                price_str   = _temp_url_price_title.find('span').text
                img_url     = item_origin.find('img', {'src':True, 'alt':True})['src']
                content     = item_origin.find('div', {'class':'lrInfo'}).text
                worth       = int(item_origin.find('a', {'class':'good'}).find('span', {'class':'scoreTotal'})['value'])
                unworth     = int(item_origin.find('a', {'class':'bad'}).find('span', {'class':'scoreTotal'})['value'])
                #mall        = item_origin.find('a', {'class':'mall', 'href':True}).text
                favoriate   = int(item_origin.find('a', {'class':'fav'}).text)
                comment     = int(item_origin.find('a', {'class':'comment'}).text)
                _temp_dimens = item_origin.find('div', {'class':'buy'}).find('a', {'href':True, 'onclick':re.compile(r'gtmAddToCart.*')})

                if _temp_dimens is None:
                    logger.debug('====>%s, page:%s, index:%s, %s' % (green(_temp_dimens), page, index, green(title)))
                    logger.debug('url fetch failed. 无法获取到 gtmAddToCart函数的参数. 怀疑是专题类，略过')
                    continue

                _temp_dimens = _temp_dimens['onclick']
                _temp_dimens = re.match(r'.*({.*}).*', _temp_dimens).group(1)
                _temp_dimens = json.loads(_temp_dimens.replace('"','``').replace('\'', '"'))
                if show_debug:  logger.debug(green(_temp_dimens))
                #_temp_dimens = json.loads(_temp_dimens)
                #print _temp_dimens
                article_id  = _temp_dimens.get('id')
                price       = _temp_dimens.get('price',price_str)
                title       = _temp_dimens.get('name')
                brand       = _temp_dimens.get('brand')
                mall        = _temp_dimens.get('mall')
                mall_url    = _temp_dimens.get('dimension10')
                category    = _temp_dimens.get('category')
                dirty       = _temp_dimens.get('dimension32')

                item['id']  = article_id
                item['brand'] = brand
                item['mall_url'] = mall_url
                item['category'] = category
                item['dirty'] = dirty
                item['url']         = url
                item['title']       = title
                item['img_url']     = img_url
                item['price']       = -1 if '无' in price else int(price)
                item['time']        = item_time
                item['content']     = content
                item['worth']       = int(worth)
                item['unworth']     = int(unworth)
                item['mall']        = mall
                item['favoriate']   = int(favoriate)
                item['comment']     = int(comment)
                item['worth_rate']  = '%.2f%%' % (float(worth) / (worth+unworth+0.000001) * 100)
                item_list.append(item)
            except Exception as e:
                logger.debug(yellow(_temp_dimens))
                logger.debug(item_origin)
                traceback.print_exc()
                logger.error('failed in page:%s -> item:%s, exception: %s' % (page, index, red(e), ))
            finally:
                pass

            if (item['worth'] > 100) or (item['worth']+item['unworth']>20 and (float(item['worth']) / (item['worth']+item['unworth']+0.000001))>0.8) or (item['comment']>50):
                show_debug and logger.info(red('\npage:%s item %s: ↓↓↓' % (page, index)))
                logger.debug(red('\npage:%s item %s: ↓↓↓' % (page, index)))
                for k,v in item.items():
                    show_debug and logger.info('%s => %s' % (green(k),red(v)))
            #if index == 1: break

        return item_list

    def crawl_faxian(self, show_more=False):
        global logger

        time_current    = time.time()
        time_line       = time.time() - 24*60*60

        url_orign   = 'http://faxian.smzdm.com/a/json_more?timesort=%s'
        item_list   = []
        # while time_current > time_line:
        #     url = url_orign % time_current
        #     response = self.http_get(url)
        #     items    = json.loads(response.content, encoding='utf-8')
        #
        #     for k,v in items[-1].items():
        #         print "%s => %s" % (green(k),green(v))
        #
        #     object_data = json.loads(items[-1]['gtm']['object'].replace('"', '``').replace('\'', '"'), encoding='utf-8')
        #     for k,v in object_data.items():
        #         print "%s => %s" % (red(k),red(v))
        #
        #     for item_origin in items:
        #         item = {}
        #
        #         object_data = json.loads(item_origin['gtm']['object'].replace('"', '``').replace('\'', '"'),
        #                                  encoding='utf-8')
        #
        #         item_time   = int(item_origin['timesort'])
        #         url         = object_data['href']
        #         title       = object_data['name']
        #         price_str   = object_data['price']
        #         img_url     = item_origin['article_pic_url']
        #         content     = item_origin['article_content']
        #         worth       = int(item_origin.find('a', {'class': 'good'}).find('span', {'class': 'scoreTotal'})['value'])
        #         unworth = int(item_origin.find('a', {'class': 'bad'}).find('span', {'class': 'scoreTotal'})['value'])
        #         favoriate = int(item_origin.find('a', {'class': 'fav'}).text)
        #         comment = int(item_origin.find('a', {'class': 'comment'}).text)
        #         _temp_dimens = item_origin.find('div', {'class': 'buy'}).find('a', {'href': True, 'onclick': re.compile(
        #             r'gtmAddToCart.*')})
        #
        #         if _temp_dimens is None:
        #             logger.debug('====>%s, page:%s, index:%s, %s' % (green(_temp_dimens), page, index, green(title)))
        #             logger.debug('url fetch failed. 无法获取到 gtmAddToCart函数的参数. 怀疑是专题类，略过')
        #             continue
        #
        #         _temp_dimens = _temp_dimens['onclick']
        #         _temp_dimens = re.match(r'.*({.*}).*', _temp_dimens).group(1)
        #         _temp_dimens = json.loads(_temp_dimens.replace('"', '``').replace('\'', '"'))
        #         if show_more:  logger.debug(green(_temp_dimens))
        #         # _temp_dimens = json.loads(_temp_dimens)
        #         # print _temp_dimens
        #         article_id = _temp_dimens.get('id')
        #         price = _temp_dimens.get('price', price_str)
        #         title = _temp_dimens.get('name')
        #         brand = _temp_dimens.get('brand')
        #         mall = _temp_dimens.get('mall')
        #         mall_url = _temp_dimens.get('dimension10')
        #         category = _temp_dimens.get('category')
        #         dirty = _temp_dimens.get('dimension32')
        #
        #         item['id'] = article_id
        #         item['brand'] = brand
        #         item['mall_url'] = mall_url
        #         item['category'] = category
        #         item['dirty'] = dirty
        #         item['url'] = url
        #         item['title'] = title
        #         item['img_url'] = img_url
        #         item['price'] = price
        #         item['time'] = item_time
        #         item['content'] = content
        #         item['worth'] = worth
        #         item['unworth'] = unworth
        #         item['mall'] = mall
        #         item['favoriate'] = favoriate
        #         item['comment'] = comment
        #         item['worth_rate'] = '%.2f%%' % (float(worth) / (worth + unworth + 0.000001) * 100)
        #
        #         item_list.append(item)
        #
        #
        #     time_current    = items[-1]['timesort']
        #     break

        return item_list

def prettify_product_index(item):
    print "时间:%s   值:%s/%s  %s  评论:%s   价格:%s元  商品:%s => %s" % (
        time.strftime("%y-%m-%d %H:%M", time.localtime(item['time'])),
        red("%3s" % item['worth']),
        yellow("%-2s" % item['unworth']),
        red(item['worth_rate']),
        blue("%-2s" % item['comment']),
        green("%-5s" % item['price']),
        red(item['title']),
        red(item['url'])
    )

def crawl_youhui(smzdm, start=1,end=30, time_after=0,show=False, save_db=False, sleep_time=3):
    for i in range(start, end+1):
        print green('crawl page:%s') % red(i)
        result     = smzdm.crawl_youhui(page=i, show_debug=False)
        for item in result:
            save_db and save(item)
            if (item['worth'] > 100) or (item['worth'] + item['unworth'] > 20 and (
                float(item['worth']) / (item['worth'] + item['unworth'] + 0.000001)) > 0.8) or (item['comment'] > 50):
                show and prettify_product_index(item)
        if result[-1]['time']<time_after:
            return

        sleep_time = random.random() * sleep_time
        print green('sleep for %s') % blue(sleep_time)
        time.sleep(sleep_time)

def save(data):
    global db
    try:
        rt  = db.insert('Products', 'id', data)
    except Exception as e:
        logger.error(red("db save error: %s" % e))

def print_youhui_from_db(timeline=0, sortby=None, direction=None):
    #timestamp = int(time.mktime((datetime.now() - timedelta(days=1)).replace(hour=0, minute=0, second=0).timetuple()))
    data   = db.find('Products', {'time':{'$gt':timeline}})
    if sortby is not None:
        data = data.sort(sortby, direction)

    for item in data:
        prettify_product_index(item)

if __name__ == '__main__':
    global db
    db  = DB("127.0.0.1", 27017, db='SMZDM')

    global logger
    logger = Logger(cmd_mode=True, level=Logger.LOG_LEVEL_ERROR)

    smzdm = SMZDM()

    #smzdm.crawl_faxian(show_more=True)

    # :example: 抓取什么值得买数据
    crawl_youhui(smzdm,1,100, time_after=time.time()-24*60*60, show=False,save_db=True, sleep_time=2)

    # :example: 从数据库中提取指定条件的数据
    #print_youhui_from_db(timeline=time.time()-24*60*60, sortby='worth', direction=-1)
    #
    print_youhui_from_db(timeline=time.time() - 24 * 60 * 60, sortby='price', direction=1)

    #新思路：各功能只负责爬取各优惠信息的地址URL，各优惠的具体信息则到具体的页面去爬取