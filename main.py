#!/usr/bin/env python
# encoding:utf-8
# main.py
# 2016/9/25  17:46
from __future__ import unicode_literals
from colors import *
import requests
from bs4 import BeautifulSoup
import traceback,sys,re,json,time,random
import threading
from datetime import datetime,timedelta
from db import DB
#from concurrent.future import ThreadPoolExecutor  # python 3
from multiprocessing.dummy import Pool

DEBUG_MODE = True

class Web_Crawler(object):
    time_http_interval = 1
    time_http_last = 0
    lock = threading.Lock()

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

    # 每一次访问至少间隔 time_http_interval
    def time_wait(self, st=None):
        #st and logger.debug(green('-->  %s' % st))
        with Web_Crawler.lock:
            #logger.error(red('last visit:%s' % Web_Crawler.time_http_last))
            now = time.time()
            last_visit = Web_Crawler.time_http_last

            interval = now - last_visit
            Web_Crawler.time_http_last = now
            if interval>=Web_Crawler.time_http_interval:
                #logger.debug(red('interval:%s  sleep:%s' % (interval, 0)))
                return

            wait = Web_Crawler.time_http_interval - interval
            logger.debug(red('sleep:%s' % wait))
            time.sleep(wait)
        #logger.debug(red('     over\n'))

    def http_get(self, url):
        self.time_wait('get:  %s' % url)
        return self.session.get(url)

    def http_post(self, url, data):
        self.time_wait('post:  %s' % url)
        return self.session.post(url, data)



class SMZDM(Web_Crawler):

    def __init__(self):
        Web_Crawler.__init__(self,'www.smzdm.com')
        self.count_parse_product = 0

    def crawl_youhui(self, page=1, show_debug=False):
        '''
        crawl_youhui(smzdm,1,100, time_after=time.time()-24*60*60, show=False,save_db=True, sleep_time=2)
        :param page:
        :param show_debug:
        :return:
        '''
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

    def crawl_faxian(self, time_current=time.time(), time_line=time.time() - 24 * 60 * 60, sleep_time=2, show_more=False, saver=False):
        """
        :param saver: False|function
        """
        global logger
        url_orign   = 'http://faxian.smzdm.com/a/json_more?timesort=%s'
        item_list   = []

        while time_current > time_line:
            logger.debug('抓取 %s 期间的数据' % red(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time_current))))

            url = url_orign % time_current
            response = self.http_get(url)
            items = json.loads(response.content, encoding='utf-8')
            logger.debug('抓取 %s 条数据' % red(len(items)))

            if len(items)==0:
                break

            if show_more:
                for k,v in items[-1].items():
                    logger.info("%s => %s" % (green(k),green(v)))

                object_data = json.loads(items[-1]['gtm']['object'].replace('"', '``').replace('\'', '"'), encoding='utf-8')
                for k,v in object_data.items():
                    logger.info("%s => %s" % (red(k),red(v)))

            for item_origin in items:
                item = {}
                item['id'] = item_origin['article_id']
                item['url'] = item_origin['article_url']
                item['timesort'] = item_origin['timesort']
                saver and saver(item)
                item_list.append(item)

            time_current    = items[-1]['timesort']

            #sleep = random.random() * sleep_time
            #logger.debug(green('sleep for %s') % blue(sleep))
            #time.sleep(sleep_time)

        return item_list

    def parse_product(self, url, get_comment=True, verbose=True):
        logger.error('handle url:%s' % red(url))

        response = self.http_get(url)
        soup = self.get_soup(response.content)
        verbose and logger.info(soup.prettify()) and logger.info(red('='*30))

        soup_onclick = soup.find('a', {'href':True, 'onclick':re.compile(r'^change_direct_url.*')})
        verbose and logger.info(soup_onclick)
        temp_onclick = soup_onclick['onclick']
        temp_onclick = re.match(r'.*({.*}).*', temp_onclick).group(1)
        temp = json.loads(temp_onclick.replace('"', '``').replace('\'', '"'))
        #print green(temp)
        item = {}
        item['id']      = temp.get('id',None)
        item['title']   = temp.get('name',None)
        item['price']   = temp.get('price',0)
        try:
            item['price'] = int(item['price'])
        except:
            pass

        item['brand']   = temp.get('brand',None)
        item['mall']    = temp.get('mall',None)
        item['category'] = temp.get('category',None)
        item['metric1'] = temp.get('metric1',None)
        # 商城host？ temp['dimension10']
        # 海淘？ temp['dimension9']
        item['image_url'] = soup_onclick.find('img',{'alt':True,'src':True}).get('src',None)

        content = soup.find('div',{'class':'item-box item-preferential'})
        if content:
            item['content'] = content.text.rstrip()
            temp = content.find('div',{'class':'inner-block'})
            item['content_1'] = temp.text.strip() if temp else ''
            temp = content.find('div', {'class': 'baoliao-block'})
            item['content_2'] = temp.text.strip() if temp else ''
            temp = content.find('div', {'class': 'baoliao-block news_content'})
            item['content_3'] = temp.text.strip() if temp else ''

        imgList = soup.find('ul',{'class':'smallImgList'}).find_all('a',{'href':True,'rel':True})
        if imgList:
            item['big_image_url'] = list()
            item['small_image_url'] = list()
            for index,img in enumerate(imgList):
                img_url = img.get('rel', None)
                img_url and item['big_image_url'].append(img_url)
                img_url = img.img.get('src', None)
                img_url and item['small_image_url'].append(img_url)

        try:
            item['worth'] = soup.find('span',{'id':'rating_worthy_num'}).text
            item['unworth'] = soup.find('span',{'id':'rating_unworthy_num'}).text
            item['worth'] = int(item['worth'])
            item['unworth'] = int(item['unworth'])
            item['worth_rate'] = item['worth'] / (0.00000001 + item['worth'] + item['unworth'])
        except:
            item['worth'] = 0
            item['unworth'] = 0
            item['worth_rate'] = 0

        ######################### 抓取评论 #######################
        item['comment'] = int(soup.find('em', {'class': 'commentNum'}).text)
        current_soup = soup
        comments = list()
        while(current_soup):
            comment_list = current_soup.find('div', {'class': 'tab_info', 'id': 'commentTabBlockNew'})
            comment_list = comment_list.find_all('li', {'class': 'comment_list'})

            for comment_li in comment_list:
                comment_id = re.match(r'.*_(\d*)', comment_li['id']).group(1)

                comments.append(comment_li.find('p', {'class': 'p_content_%s' % comment_id}).text)

            # comment_list = soup.find('div', {'class': 'tab_info', 'id': 'commentTabBlockHot'})


            next_page = current_soup.find('li',{'class':'pagedown'})
            current_soup = None
            if get_comment and next_page:
                next_url = next_page.a.get('href',None)
                if next_url:
                    html = self.http_get(next_url)
                    if html.status_code==200:
                        current_soup = self.get_soup(html.content)

        item['comments'] = comments.reverse()


        if verbose:
            for k, v in item.items():
                if not isinstance(v,(list,set,dict)):
                    logger.info('%s => %s' % (green(k), red(v)))
                elif isinstance(v, (list,set)):
                    for k2,v2 in enumerate(v):
                        logger.info('%s => list|set: %s  => %s' % (green(k),  green(k2), green(v2)))
                elif isinstance(v, (dict)):
                    for k2,v2 in v.items():
                        logger.info('%s => dict => %s => %s' % (green(k),  green(k2), red(v2)))

        # item['time'] = item_time
        # item['favoriate'] = favoriate


        self.count_parse_product += 1

        logger.debug(green("---------------------\nsucess count:%s") % red(self.count_parse_product))
        return item if item['id'] and item['title'] else None


def prettify_product_index(item):
    print "时间:%s   值:%s/%s  %s  评论:%s   价格:%s元  商品:%s => %s" % (
        time.strftime("%y-%m-%d %H:%M", time.localtime(item.get('time',0))),
        red("%3s" % item.get('worth',-1)),
        yellow("%-2s" % item.get('unworth',-1)),
        red(item.get('worth_rate',-1)),
        blue("%-2s" % item.get('comment',-1)),
        green("%-5s" % item.get('price',-1)),
        red(item.get('title','null')),
        red(item.get('url','null'))
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

def save(data, model='Products', key='id'):
    global db
    try:
        rt  = db.insert(model, key, data)
    except Exception as e:
        logger.error(red("db save error: %s" % e))

def print_youhui_from_db(timeline=0, sortby=None, direction=None):
    #timestamp = int(time.mktime((datetime.now() - timedelta(days=1)).replace(hour=0, minute=0, second=0).timetuple()))
    data   = db.find('Products', {'time':{'$gt':timeline}})
    if sortby is not None:
        data = data.sort(sortby, direction)

    for item in data:
        prettify_product_index(item)

def task_crawl_faxian(show_more=False, sleep_time=2):
    config = dict()
    with open('faxian.config', 'a+') as cf:
        try:
            st = cf.read()
            #print(st)
            config = json.loads(st)
            #print red(config)
        except Exception as e:
            logger.error('crawl_faxian.config read error: %s' % e)

    while(True):
        now = time.time()
        time_line = config.get('last_time', now - 1 * 60 * 60)
        # 异常时间 或 一次抓取超过10天的数据，统统重置为获取最近36个小时的数据
        if (time_line >= now) or (now - time_line > 10 * 24 * 60 * 60):
            time_line = now - 36 * 60 * 60

        logger.error(green('crawl faxian in time:%s -> %s' % (time_line, now)))

        saver = lambda x:save(x, model='ArticleList', key='id')
        article_list = smzdm.crawl_faxian(show_more=False, sleep_time=2, time_line=time_line)

        for item in article_list:
            saver(item)

        config['last_time'] = now
        #print red(config)
        with  open('faxian.config', 'w') as cf:
            cf.write(json.dumps(config))

        # 休眠N分钟后继续抓取
        sleep_minute = 10
        logger.error(red('抓取完一波，等待%s分钟后继续!' % sleep_minute))
        time.sleep(sleep_minute*60)


def task_parse_product( params):
    for param in params:
        func = param[0]
        param = param[1:]
        func(*param)

def parse_product():
    pool = Pool(5)

    article_list = db.find('ArticleList',{}).limit(100).sort('timesort',-1)

    # for record in article_list:
    #     id = record['id']
    #     url = record['url']
    url = 'http://www.smzdm.com/p/6472917/'

    task_params = [[smzdm.parse_product, url,False,False] for i in range(10)]
    #pool.map_async(smzdm.parse_product, urls)


    # for param in task_params:
    #     pool.apply_async(smzdm.parse_product, (param))

    # for i in range(4):
    #     pool.apply_async(task_parse_product, (task_params,))
    # pool.close()
    # pool.join()

    task_params = [[url, False, False] for i in range(40)]
    for param in task_params:
        smzdm.parse_product(*param)



    print 'ok'
    return


    url = 'http://www.smzdm.com/p/6472917/'
    product = None
    try:
        product = smzdm.parse_product(url, verbose=False)
        #product['time'] =
    except Exception as e:
        logger.error('parse_product error: %s' % red(e))
    product and product['id'] and save(product)


if __name__ == '__main__':
    t1 = datetime.now()
    print green(t1)



    global db
    global logger
    db  = DB("127.0.0.1", 27017, db='SMZDM')
    logger = Logger(cmd_mode=True, level=Logger.LOG_LEVEL_INFO)

    smzdm = SMZDM()

    # 放置到线程1中执行，常驻内存。每抓取一轮sleep一段时间，之后恢复抓取。中断后可继续上一次的执行
    task_crawl_faxian()

    # 放置到线程2中执行，常驻内存。解析一波结束后，sleep一小段时间，之后继续获取需重新获取的内容（最近24小时）
    #parse_product()



    # :example: 抓取什么值得买数据
    #

    # :example: 从数据库中提取指定条件的数据
    #print_youhui_from_db(timeline=time.time()-24*60*60, sortby='worth', direction=-1)
    #
    #print_youhui_from_db(timeline=time.time() - 24 * 60 * 60, sortby='price', direction=1)

    #新思路：各功能只负责爬取各优惠信息的地址URL，各优惠的具体信息则到具体的页面去爬取

    t2 = datetime.now()
    print green(t2)
    print red(t2 - t1)