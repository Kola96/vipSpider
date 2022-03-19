import random
import hashlib
import logging
import queue
import threading
import sqlite3
from datetime import datetime

import requests
import yagmail


# 商品id爬虫线程数
PID_SPIDER_THREAD_NUM = 10
# 商品爬虫线程数
PRODUCT_SPIDER_THREAD_NUM = 10
# 关键词队列
KEYWORD_QUEUE = queue.Queue()
# 商品id队列
PID_QUEUE = queue.Queue()
# item队列
ITEM_QUEUE = queue.Queue()
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


def load_keyword_and_brand():
    conn = sqlite3.connect('vipSpider.db')
    cursor = conn.cursor()
    cursor.execute('SELECT keyword, brand_code FROM vip_keyword')
    keyword_pairs = cursor.fetchall()
    for item in keyword_pairs:
        KEYWORD_QUEUE.put(item)
        logging.debug(f'({item[0]}, {item[1]}) 进入关键词队列')


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
        while 1:
            url = f'https://mapi.vip.com/vips-mobile/rest/shopping/pc/search/product/rank?app_name=shop_pc&app_version=4.0&warehouse=VIP_SH&fdc_area_id=103102111&client=pc&mobile_platform=1&province_id=103102&api_key={gen_random_api_key()}&mars_cid=1647519945895_0eb566b2dba9d095fbf844e26ee77f26&wap_consumer=a&standby_id=nature&keyword={p[0]}&brandStoreSns={p[1]}&sort=0&pageOffset={page_offset}&channelId=1&gPlatform=PC&batchSize=50'
            resp = session.get(url)
            json = resp.json()
            try:
                product_ids = [item['pid'] for item in json['data']['products']]
            except KeyError:
                continue
            pid_url = f'https://mapi.vip.com/vips-mobile/rest/shopping/pc/product/module/list/v2?app_name=shop_pc&app_version=4.0&warehouse=VIP_SH&api_key={gen_random_api_key()}&productIds={"%2C".join(product_ids)}&scene=search&standby_id=nature'
            PID_QUEUE.put(pid_url)
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
            url = PID_QUEUE.get(timeout=5)
        except queue.Empty:
            logging.info(f'[PID-{pid}] pid队列已空, 爬虫退出')
            break
        resp = session.get(url)
        products = resp.json()['data']['products']
        for prod in products:
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
    while 1:
        try:
            item = ITEM_QUEUE.get(timeout=5)
        except queue.Empty:
            logging.info(f'item队列已空, saver退出')
            cursor.close()
            conn.close()
            break
        sql = f'SELECT COUNT(1) FROM vip_product WHERE `prod_id` = {item.product_id}'
        cursor.execute(sql)
        count = cursor.fetchone()[0]
        if count <= 0:
            sql = f'''INSERT INTO vip_product (`prod_id`, `title`, `brand_id`, `brand_sn`, `brand_name`, `status`, `url`) VALUES ({item.product_id}, '{item.title}', {item.brand_id}, {item.brand_sn}, '{item.brand_show_name}', '{item.status}', '{item.url}')'''
            logging.info('新增: ' + str(item))
            cursor.execute(sql)
            conn.commit()
            p = f'新品：[{item.brand_show_name}] <a href={item.url}>{item.title}</a> 售价：{item.sale_price} 原价：{item.market_price}'
            content.append(p)
        else:
            logging.debug('商品已存在')
            logging.debug(item)
            # sql = f'''UPDATE vip_product SET `title`='{item.title}', `status`='{item.status}', `url`='{item.url}', `update_time`='{datetime.now()}' WHERE `prod_id` = {item.product_id}'''
            # logging.info('更新: ' + str(item))
            # cursor.execute(sql)
            # conn.commit()
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
    yag.send(to=['372529797@qq.com'], subject=f'唯品会上新提醒 {datetime.now()}', contents=content)


if __name__ == '__main__':
    load_keyword_and_brand()
    for i in range(PID_SPIDER_THREAD_NUM):
        threading.Thread(target=vip_keyword_spider, args=(i,)).start()
        logging.info(f'keywordSpider线程 <{i}> 启动')

    for i in range(PRODUCT_SPIDER_THREAD_NUM):
        threading.Thread(target=vip_prod_spider, args=(i,)).start()
        logging.info(f'pidSpider线程 <{i}> 启动')

    threading.Thread(target=vip_saver).start()
