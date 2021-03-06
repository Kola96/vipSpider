import random
import hashlib
import logging
import queue
import threading
import sqlite3
from datetime import datetime
from math import ceil

import requests
import yagmail


# 商品id爬虫线程数
PID_SPIDER_THREAD_NUM = 5
# 商品爬虫线程数
PRODUCT_SPIDER_THREAD_NUM = 20
# 关键词队列
KEYWORD_QUEUE = queue.Queue()
# 商品id队列
PID_QUEUE = queue.Queue()
# item队列
ITEM_QUEUE = queue.Queue()
RECEIVERS = ['372529797@qq.com', '1351696477@qq.com', '1215063438@qq.com']
# 日志设置
logging.basicConfig(level='INFO', format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s:\n%(message)s')


class Product:
    product_id = None
    brand_id = None
    brand_sn = None
    brand_show_name = None
    title = None
    status = None
    sale_price = None
    market_price = None
    url = None

    def __str__(self):
        return f'[{self.brand_show_name}]{self.title} - {self.url}'


def gen_random_api_key() -> str:
    r_num = random.random()
    api_key = hashlib.md5(str(r_num).encode()).hexdigest()
    return api_key


def gen_mars_cid():
    # return '1647710806882_47b28aad3c45494f56c8143933cf180b'
    return str(int(datetime.now().timestamp() * 1000)) + '_' + hashlib.md5(str(random.random).encode()).hexdigest()


def load_keyword_and_brand():
    conn = sqlite3.connect('vipSpider.db')
    cursor = conn.cursor()
    cursor.execute('SELECT keyword, brand_code FROM vip_keyword')
    keyword_pairs = cursor.fetchall()
    for item in keyword_pairs:
        KEYWORD_QUEUE.put(item)
        logging.info(f'({item[0]}, {item[1]}) 进入关键词队列')


def mock_keyword():
    KEYWORD_QUEUE.put(('COUNT', '10000630'))


def vip_keyword_spider(pid):
    session = requests.session()
    session.headers = {
        'referer': 'https://category.vip.com/'
    }
    while 1:
        try:
            p = KEYWORD_QUEUE.get(timeout=5)
        except queue.Empty:
            logging.info(f'[PID-{pid}] 关键词队列已空, 爬虫退出')
            break
        page_offset = 0
        app_key = gen_random_api_key()
        mars_cid = gen_mars_cid()
        while 1:
            url = f'https://mapi.vip.com/vips-mobile/rest/shopping/pc/search/product/rank?app_name=shop_pc&app_version=4.0&warehouse=VIP_SH&fdc_area_id=103102111&client=pc&mobile_platform=1&province_id=103102&api_key={app_key}&mars_cid={mars_cid}&wap_consumer=a&standby_id=nature&keyword={p[0]}&brandStoreSns={p[1]}&sort=0&pageOffset={page_offset}&channelId=1&gPlatform=PC&batchSize=50'
            resp = session.get(url)
            if resp.status_code == 200:
                json = resp.json()
                if json['data']['total'] == 0:
                    logging.error(f'没有搜索到商品，请检查关键词: {p}')
                    break
            else:
                KEYWORD_QUEUE.put(p)
                logging.error(f'关键词：{p} 未能完整爬取，重新加入队列')
                break
            try:
                product_ids = [item['pid'] for item in json['data']['products']]
            except KeyError:
                continue
            pid_url = f'https://mapi.vip.com/vips-mobile/rest/shopping/pc/product/module/list/v2?app_name=shop_pc&app_version=4.0&warehouse=VIP_SH&api_key={gen_random_api_key()}&productIds={"%2C".join(product_ids)}&scene=search&standby_id=nature'
            url_item = (p[0], pid_url)
            PID_QUEUE.put(url_item)
            is_last = json['data']['isLast']
            if is_last:
                logging.info(f'[PID-{pid}] {p} 爬取完毕')
                break
            else:
                page_offset += 50


def vip_prod_spider(pid):
    session = requests.session()
    session.headers = {
        'referer': 'https://category.vip.com/'
    }
    while 1:
        try:
            url_item = PID_QUEUE.get(timeout=5)
        except queue.Empty:
            logging.info(f'[PID-{pid}] pid队列已空, 爬虫退出')
            break
        keyword = url_item[0]
        url = url_item[1]
        resp = session.get(url)
        products = resp.json()['data']['products']
        for prod in products:
            if keyword in prod['title']:
                item = Product()
                item.product_id = prod['productId']
                item.brand_id = prod['brandId']
                item.brand_sn = prod['brandStoreSn']
                item.brand_show_name = prod['brandShowName']
                item.title = prod['title'].replace('"', '').replace('\'', '')
                item.sale_price = prod['price']['salePrice']
                item.market_price = prod['price']['marketPrice']
                item.status = prod['status']
                item.url = f'https://detail.vip.com/detail-{prod["brandId"]}-{prod["productId"]}.html'
                logging.debug(item)
                ITEM_QUEUE.put(item)


def vip_saver():
    conn = sqlite3.connect('vipSpider.db')
    cursor = conn.cursor()
    content = []
    seq = 0
    while 1:
        try:
            item = ITEM_QUEUE.get(timeout=1)
        except queue.Empty:
            logging.info(f'item队列已空, saver退出')
            cursor.close()
            conn.close()
            break
        sql = f'SELECT status FROM vip_product WHERE `prod_id` = {item.product_id}'
        cursor.execute(sql)
        res = cursor.fetchone()
        if res is not None:
            status = res[0]
        else:
            status = None
        if status is None:
            sql = f'''INSERT INTO vip_product (`prod_id`, `title`, `brand_id`, `brand_sn`, `brand_name`, `status`, `url`) VALUES ({item.product_id}, '{item.title}', {item.brand_id}, {item.brand_sn}, '{item.brand_show_name}', '{item.status}', '{item.url}')'''
            logging.info('新增: ' + str(item))
            cursor.execute(sql)
            conn.commit()
            p = f'{seq + 1}. [新品][{item.brand_show_name}] <a href={item.url}>{item.title}</a> 售价：{item.sale_price} 原价：{item.market_price}'
            seq += 1
            content.append(p)
        else:
            if status == '1' and item.status == '0':
                p = f'{seq + 1}. [重新上架][{item.brand_show_name}] <a href={item.url}>{item.title}</a> 售价：{item.sale_price} 原价：{item.market_price}'
                logging.info('重新上架: ' + str(item))
                content.append(p)
            else:
                pass
            update_sql = f'''UPDATE vip_product SET `title`='{item.title}', `status`='{item.status}', `url`='{item.url}', `update_time`='{datetime.now()}' WHERE `prod_id` = {item.product_id}'''
            logging.debug('更新: ' + str(item))
            cursor.execute(update_sql)
            conn.commit()
    if len(content) > 0:
        send_mail(content)
        logging.info('已发送上新提醒邮件')
    else:
        logging.info('没有爬取到新品')


def send_mail(content):
    yag = yagmail.SMTP(
        user='vip_spider@163.com',
        host='smtp.163.com',
        port='465',
        password='UZOVRUQJZSXJKKIW'
    )
    if len(content) <= 500:
        yag.send(to=RECEIVERS, subject=f'唯品会上新提醒 {datetime.now()}', contents=content)
    else:
        p_num = ceil(len(content) / 500)
        for p in range(p_num):
            yag.send(to=RECEIVERS, subject=f'唯品会上新提醒 [{p + 1}/{p_num}] {datetime.now()}', contents=content[p * 500:(p + 1) * 500])


if __name__ == '__main__':
    load_keyword_and_brand()

    thread_list = []
    for i in range(PID_SPIDER_THREAD_NUM):
        t = threading.Thread(target=vip_keyword_spider, args=(i,))
        thread_list.append(t)
        t.start()
        logging.info(f'keywordSpider线程 <{i}> 启动')

    for i in range(PRODUCT_SPIDER_THREAD_NUM):
        t = threading.Thread(target=vip_prod_spider, args=(i,))
        thread_list.append(t)
        t.start()
        logging.info(f'pidSpider线程 <{i}> 启动')

    threading.Thread(target=vip_saver).start()
