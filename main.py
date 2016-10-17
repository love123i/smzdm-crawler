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
            #'Accept':'application/json, text/javascript, */*; q=0.01',
            'Accept-Encoding':'gzip, deflate, sdch',
            'Accept-Language':'zh-CN,zh;q=0.8',
            'Connection': 'keep-alive',
            #'Accept-Encoding': 'gzip, deflate',
            #'Host': self.host,
            'Referer':'http://faxian.smzdm.com/',
            'Host': 'faxian.smzdm.com',
        }

    # 每一次访问至少间隔 time_http_interval
    def time_wait(self, st=None):
        #st and logger.debug(green('-->  %s' % st))
        with Web_Crawler.lock:
            #logger.error(red('last visit:%s' % Web_Crawler.time_http_last))
            now = int(time.time())
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

    def crawl_faxian(self, time_line, sleep_time=2, show_more=False, saver=False):
        """
        :param saver: False|function
        """
        global logger
        #url_orign   = 'http://faxian.smzdm.com/a/json_more?timesort=%s'
        url_orign = 'http://faxian.smzdm.com/json_more?timesort=%s'
        item_list   = []

        time_current = time.time()
        # time_line = time.time() - 24 * 60 * 60



        while time_current > time_line:
            time_current = int(time_current)
            logger.debug('抓取 %s 期间的数据' % red(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time_current))))

            url = url_orign % time_current
            response = self.http_get(url)

            print green(url), green(response.status_code)
            #print green(response.content)
            items = json.loads(response.content, encoding='utf-8')
            logger.debug('抓取 %s 条数据' % red(len(items)))

            if len(items)==0:
                logger.debug('抓取个数为0, url=%s' % red(url))
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

            logger.debug(green("获取下一时间点的【发现频道】数据, next_time_point: %s" % (time_current,)))

            #sleep = random.random() * sleep_time
            #logger.debug(green('sleep for %s') % blue(sleep))
            #time.sleep(sleep_time)

        return item_list

    def parse_product(self, url, get_comment=True, verbose=True, saver=None):
        logger.debug('handle url:%s' % red(url))

        response = self.http_get(url)
        if response.status_code!=200:
            logger.error(red('crawl_faxian url:%s: error: %s' % (url, response.status_code)))

        soup = self.get_soup(response.content)
        verbose and logger.info(soup.prettify()) and logger.info(red('='*30))

        try:
            soup_onclick = soup.find('a', {'href':True, 'onclick':re.compile(r'^change_direct_url.*')})

            verbose and logger.info(soup_onclick)
            temp_onclick = soup_onclick['onclick']

            temp_onclick = re.match(r'.*({.*}).*', temp_onclick).group(1)
            #print green(temp_onclick)
            temp = json.loads(temp_onclick.replace('"', '``').replace("'", '"'), strict=False)
            #print green(temp)
        except Exception as e:
            logger.error(red('crawl_faxian url:%s: error: %s, stack:\n%s' % (url, e, red(traceback.format_exc()))))
            return False

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
            item['content'] = content.text.strip()
            temp = content.find('div',{'class':'inner-block'})
            item['content_1'] = temp.text.strip() if temp else ''
            temp = content.find('div', {'class': 'baoliao-block'})
            item['content_2'] = temp.text.strip() if temp else ''
            temp = content.find('div', {'class': 'baoliao-block news_content'})
            item['content_3'] = temp.text.strip() if temp else ''

        imgList = soup.find('ul',{'class':'smallImgList'})
        #print red(imgList)
        if imgList:
            imgList = imgList.find_all('a', {'href': True, 'rel': True})
            #print green(imgList)
            item['big_image_url'] = list()
            item['small_image_url'] = list()
            #print green(imgList)
            for index,img in enumerate(imgList):
                img_url = img.get('rel', None)
                img_url and item['big_image_url'].append(img_url)
                img_url = img.img.get('src', None)
                img_url and item['small_image_url'].append(img_url)
                index+=1

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

            if comment_list:
                comment_list = comment_list.find_all('li', {'class': 'comment_list'})

                for comment_li in comment_list:
                    comment_id = re.match(r'.*_(\d*)', comment_li['id']).group(1)
                    #print comment_li.find('p', {'class': 'p_content_%s' % comment_id}).text
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

        comments.reverse()
        item['comments'] = comments
        #print 'comment:%s  comments: %s' % (item['comment'], item['comments'])

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

        item = item if item['id'] and item['title'] else None
        item['last_update'] = time.time()
        if item and saver:
            saver(item)
        return item


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

def save(data, model=None, key='id', unexist=False):
    global db
    try:
        if unexist:
            rt  = db.insert_unexist(model, key, data)
        else:
            rt  = db.insert(model, key, data)
        return rt
    except Exception as e:
        logger.error(red("db save error: %s" % e))
        raise

def print_youhui_from_db(timeline=0, sortby=None, direction=None):
    '''
    :example: 从数据库中提取指定条件的数据
    print_youhui_from_db(timeline=time.time()-24*60*60, sortby='worth', direction=-1)

    print_youhui_from_db(timeline=time.time() - 24 * 60 * 60, sortby='price', direction=1)
    '''

    #timestamp = int(time.mktime((datetime.now() - timedelta(days=1)).replace(hour=0, minute=0, second=0).timetuple()))
    data   = db.find(DB_TABLE_PRODUCT, {'time':{'$gt':timeline}})
    if sortby is not None:
        data = data.sort(sortby, direction)

    for item in data:
        prettify_product_index(item)

def task_crawl_faxian(daemon=True):
    '''
    任务1——抓取发现频道的指定时间内的数据
    '''
    smzdm = SMZDM()

    ############# 读取配置
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
        now = int(time.time())
        time_line = config.get('last_time', now - 24 * 60 * 60)
        # 异常时间 或 一次抓取超过10天的数据，统统重置为获取最近36个小时的数据
        if (time_line >= now) or (now - time_line > 10 * 24 * 60 * 60):
            time_line = now - 36 * 60 * 60

        time_st1 = time.strftime( '%Y-%m-%d %X', time.localtime(time_line))
        time_st2 = time.strftime('%Y-%m-%d %X', time.localtime(now))
        logger.record('%s 抓取【发现频道】, 时间范围: %s -> %s' % (tool_time_strftime(), red(time_st1), red(time_st2)))

        article_list = list()
        crawl_count = 0
        insert_count = 0
        try:
            ############# 开始抓取数据配置
            saver = lambda x:save(x, model=DB_TABLE_LIST, key='id', unexist=True)
            article_list = smzdm.crawl_faxian(show_more=False, sleep_time=2, time_line=time_line)
            print 'len:%s' % len(article_list)

            ############# 对新抓取到的商品url，设置其未解析状态
            for item in article_list:

                item['parsed'] = 0
                item['create'] = now
                rt = saver(item)
                crawl_count += 1
                #print item
                if rt:
                    #print 'rt:%s' % rt
                    insert_count += 1

            config['last_time'] = now
            #print red(config)
            ############# 保存配置
            with  open('faxian.config', 'w') as cf:
                cf.write(json.dumps(config))
        except Exception as e:
            logger.error(red('task_crawl_faxian: error: %s, stack:\n%s' % (e, red(traceback.format_exc()))))

        if not daemon:
            return

        # 休眠N分钟后继续抓取
        sleep_minute = 30
        logger.record(red('%s 【发现频道——Over】 共抓取%s条数据, 有效新数据%s条， 等待%s分钟后继续, 下一轮时间: %s!' % (tool_time_strftime(), crawl_count, insert_count, sleep_minute, tool_time_strftime(timestamp=now+sleep_minute*60))))
        time.sleep(sleep_minute*60)


def task_parse_product(daemon=True):
    '''
    任务——解析所有未解析的商品具体信息
    '''
    ############# 参数配置
    limit = 1000
    verbose = False
    get_comment_interval = 6 * 60 * 60

    now = int(time.time())
    smzdm = SMZDM()

    ############# 初始化——读取配置
    config = dict()
    with open('parse_product.config', 'a+') as cf:
        try:
            st = cf.read()
            config = json.loads(st)
        except Exception as e:
            logger.error(red('crawl_faxian.config read error: %s' % e))
    last_get_comment_time = config.get('last_get_comment_time', now - get_comment_interval)

    get_comment=True if last_get_comment_time+get_comment_interval<=now else False

    saver = lambda x: save(x, model=DB_TABLE_PRODUCT, key='id')


    while True:
        try:
            ############# 从数据库中获取需解析的商品列表
            # 只解析【未被解析过的】、【距离上次解析超过3小时，且商品信息创建时间小于36小时，即: 只维护3天内的商品信息】
            article_list = db.find(DB_TABLE_LIST, {'$or':[{'parsed':0}, {'parsed':{'$ne':0, '$lt':now-3*60*60}, 'create':{'$gt':now-36*60*60}}]} ).limit(limit).sort('timesort', -1)
            logger.record('%s 【商品数据】 开始解析，数量:%s, 是否抓取更多评论:%s' % (tool_time_strftime(), red(article_list.count() if article_list.count()<limit else limit), red(get_comment)))

            ############# 开始解析
            count = 0
            for item in article_list:
                if item.get('url',None) is not None:
                    count += 1
                    try:
                        smzdm.parse_product(item['url'], get_comment=get_comment, verbose=verbose, saver=saver)
                    except Exception,e:
                        logger.error(red('smzdm.parse_product error! url: %s exception:"%s", stack:%s' % (item['url'], e, red(traceback.format_exc()))))
                        count -= 1
                        continue
                    item['parsed'] = now
                    save(item, model=DB_TABLE_LIST, key='id')
            logger.record('%s 【商品数据】 本次解析成功，数量: %s' % (tool_time_strftime(), red(count)))

            ############# 保存配置
            config['last_get_comment_time'] = now
            with  open('parse_product.config', 'w') as cf:
                cf.write(json.dumps(config))
        except Exception as e:
            logger.error(red('task_parse_product error! exception:"%s", stack:%s' % (e, traceback.format_exc())))

        if not daemon:
            return

        # 休眠N分钟后继续抓取
        sleep_minute = 15
        logger.record(red('%s 【商品数据】 解析完一波，等待%s分钟后继续, 下一轮时间: %s!' % (tool_time_strftime(), sleep_minute, tool_time_strftime(timestamp=now+sleep_minute*60))))
        time.sleep(sleep_minute * 60)

def task_clean():
    while True:
        ################# 清理异常数据
        logger.record('%s【数据库异常数据清理——Start】' % tool_time_strftime())
        count = 0
        rt = db.remove(DB_TABLE_LIST, {'url':{'$exists':False}})
        count += rt.get('n',0)
        rt = db.remove(DB_TABLE_PRODUCT, {'id':{'$exists':False}})
        count += rt.get('n', 0)
        logger.record('【数据库异常数据清理——Over】共清理 %s 条异常数据' % red(count))

        ################# 清理过时数据
        time_out_hour = 36
        logger.record('【数据库过时数据清理——Start】超时时间: %s小时' % red(time_out_hour))
        count = 0
        rt = db.remove(DB_TABLE_LIST, {'create':{'$lt':time_out_hour*60*60}})
        count += rt.get('n', 0)
        rt = db.remove(DB_TABLE_PRODUCT, {'last_update':{'$lt':time_out_hour* 60*60}})
        count += rt.get('n', 0)
        logger.record('【数据库过时数据清理——Over】清理 %s 条过时数据' % red(count))
        ################# 休眠
        sleep_hour = 12
        logger.record(red('%s 【数据库数据清理】 清理完一波，等待%s小时后继续, 下一轮时间: %s!' % (tool_time_strftime(), sleep_hour, tool_time_strftime(timestamp=time.time()+sleep_hour*60*60))))
        time.sleep(sleep_hour*60*60)

DB_TABLE_LIST = 'ArticleList'
DB_TABLE_PRODUCT = 'Products'

def tool_time_strftime(format="%Y-%m-%d %H:%M:%S", timestamp=None):
    p_tuple = None if not timestamp else time.localtime(timestamp)
    return time.strftime(format) if not p_tuple else time.strftime(format, p_tuple)

def run():
    # 放置到线程1中执行，常驻内存。每抓取一轮sleep一段时间，之后恢复抓取。中断后可继续上一次的执行
    t1 = threading.Thread(target=task_crawl_faxian, args=(True,))
    t1.start()

    # 放置到线程2中执行，常驻内存。解析一波结束后，sleep一小段时间，之后继续获取需重新获取的内容（最近24小时）
    t2 = threading.Thread(target=task_parse_product, args=(True,))
    t2.start()

    # 放置到线程3中执行，常驻内存。每隔1天运行一次
    t3 = threading.Thread(target=task_clean)
    t3.start()

    t1.join()
    t2.join()
    t3.join()

if __name__ == '__main__':
    ############ 记录程序开始时间
    time1 = datetime.now()


    ############ 初始化【数据库】、【日志】
    global db
    global logger
    db  = DB("127.0.0.1", 27017, db='SMZDM')
    logger = Logger(cmd_mode=True, level=Logger.LOG_LEVEL_DEBUG)
    logger.record('开始时间:%s' % green(time1))

    run()

    ############ 记录程序结束时间
    time2 = datetime.now()
    logger.record('结束时间:%s' % green(time2))
    logger.record('总消耗时间:%s' % green(time2 - time1))