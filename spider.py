#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import re
import time
import urllib.request

import pymysql
import threading

from warnings import filterwarnings
from hdfs.client import Client
from io import StringIO
from pyhive import hive
from urllib.request import quote
from bs4 import BeautifulSoup

filterwarnings('ignore', category=pymysql.Warning)


def run_time(func):
    def wrapper(*args, **kwargs):
        st = time.time()
        res = func(*args, **kwargs)
        et = time.time()
        print('%s 运行了 %s ms' % (func.__name__, (et - st) * 1000))
        return res

    return wrapper


# @run_time
def get_html(url):
    header = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, sdch',
        'Accept-Language': 'zh-CN,zh;q=0.8',
        'Connection': 'keep-alive',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.235'
    }
    req = urllib.request.Request(url, headers=header)
    rep = urllib.request.urlopen(req)
    html = rep.read().decode('utf-8')
    # html.decode('utf-8')
    soup = BeautifulSoup(html, 'html.parser')
    return soup


# 从maps7.com网站上保存所有省份和下面城市的列表，生成{城市:省份}的字典
def find_all_province():
    url = 'http://www.maps7.com/china_province.php'
    response = urllib.request.urlopen(url)
    html = response.read().decode('utf-8')
    soup = BeautifulSoup(html, 'html.parser')
    soup = soup.body.div.div
    # n=0
    city_dic = {}
    province = ''
    for i in soup.find_all('a'):
        if i.h4:
            province = i.h4.string
        elif province != '':
            city_dic[i.string] = province
    # 北京地区比较特殊，要单独处理
    for i in soup.find_all('a'):
        if i.h4:
            break
        elif i['href'][0] != '#' and i.string != '北京市':
            city_dic[i.string] = '北京市'
    with open('city_dic', 'w') as f:
        f.write(str(city_dic))


# 读取省城市字典文件,并返回一个字典
def read_dic(file_name):
    with open(file_name, 'r') as f:
        l = f.read()
    d = eval(l)
    return d


@run_time
def get_city_url(soup):
    city_dic = {}
    city = soup.find_all('a')
    for c in city:
        if c.get('href')[:4] == 'city':
            city_name = c.string
            city_dic[city_name] = c.get('href')
    print('********一共<<%s>>个城市。********' % (len(city_dic)))
    return city_dic


threadLock = threading.Lock()
threads = []


class myThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        print("开启线程： " + self.name)
        # 获取锁，用于线程同步
        threadLock.acquire()
        spider_run()
        # 释放锁，开启下一个线程
        threadLock.release()


@run_time
def region_page(city_dic):
    global APC, province_name
    # result = {}
    # n = 0
    f = StringIO()
    station_lat_lng = {}
    for city_name, city_url in city_dic.items():
        s = get_html('http://www.86pm25.com/' + city_url)
        trs = s.find_all(name='tr')
        row = []
        for tr in trs:
            td = tr.find_all(name='td')
            if td:
                if td[2].string != '—':
                    AQI = td[2].string
                    if 50 > int(td[2].string):
                        APC = '优'
                    elif 100 > int(td[2].string):
                        APC = '良'
                    elif 150 > int(td[2].string):
                        APC = '轻度污染'
                    elif 200 > int(td[2].string):
                        APC = '中度污染'
                    elif 300 > int(td[2].string):
                        APC = '重度污染'
                    elif 500 > int(td[2].string):
                        APC = '严重污染'
                else:
                    APC = '未知'
                    AQI = '0'
                city_dic = read_dic('./city_dic')
                for k, v in city_dic.items():
                    if city_name in k:
                        province_name = v
                uptime = s.find('div', attrs=('class', 'remark')).string[3:]
                dt = '{}-{}-{} {}:00:00'.format(uptime[0:4], uptime[5:7], uptime[8:10], uptime[12:14])
                insertTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                timeArray = time.strptime(dt, "%Y-%m-%d %H:%M:%S")
                uptime = str(time.mktime(timeArray))
                country = td[0].string
                station = td[1].string
                address = province_name + city_name + td[0].string + td[1].string
                level = APC
                if td[4].string.replace('μg/m³', '') == '—' or td[4].string.replace('μg/m³', '') == '':
                    PM25 = '0'
                else:
                    PM25 = td[4].string.replace('μg/m³', '')
                if td[5].string.replace('μg/m³', '') == '—' or td[5].string.replace('μg/m³', '') == '' or td[
                    5].string.replace('μg/m³', '') == '-':
                    PM10 = '0'
                else:
                    PM10 = td[5].string.replace('μg/m³', '')
                # print(address)
                # lat_lng = get_lnglat(address.replace(',', ''))
                # latitude = str(lat_lng[0]) + ','
                # longtitude = str(lat_lng[1]) + '\n'
                # station_lat_lng[address.replace(',', '')] = latitude+longtitude
                coordinate_dic = read_dic('./station_coordinate')
                if address in coordinate_dic:
                    coordinate = coordinate_dic[address]
                else:
                    lat_lng = get_lnglat(address)
                    latitude = str(lat_lng[0]) + ','
                    longtitude = str(lat_lng[1]) + '\n'
                    coordinate = latitude + longtitude
                    coordinate_dic[address] = coordinate
                    with open('station_coordinate', 'w') as station_lat_lng_f:
                        station_lat_lng_f.write(str(coordinate_dic))
                rows = ','.join(
                    (province_name, city_name, country, station, address, dt, insertTime, AQI, level, PM25, PM10,
                     coordinate))
                f.write(rows)
        # n = n + len(row)
        # print("********城市<<%s>>一共<<%d>>个监测站,已保存了<<%d>>个监测站数据。********" % (city_name, len(row), n))
        # result[city_name] = row
    # return result
    # with open('station_coordinate','w') as f:
    #     f.write(str(station_lat_lng))
    return f


def get_lnglat(address):
    url = 'http://api.map.baidu.com/geocoder/v2/'
    output = 'json'
    ak = 'C92Dy2qwPGTGqR2y6LCjEglKSi0eHmvC'
    add = quote(address)  # 由于本文地址变量为中文，为防止乱码，先用quote进行编码
    uri = url + '?' + 'address=' + add + '&output=' + output + '&ak=' + ak
    req = urllib.request.urlopen(uri)
    res = req.read().decode()
    temp = json.loads(res)
    print(temp)
    if temp['status'] != 0:
        lat, lng = 0, 0
    else:
        lat = temp['result']['location']['lat']
        lng = temp['result']['location']['lng']
    return lat, lng


@run_time
def save_to_csv(result):
    with open('result.csv', 'w') as f:
        # j = json.dumps(result, ensure_ascii=False)
        #        print(j)
        # f.write(j.replace('"},', '"},\n').replace('}],', '}],\n'))
        f.write(result.getvalue())


def insert_to_mysql(result_file):
    db = pymysql.connect("192.168.52.226", "root", "root123", "air_quality", local_infile=True)
    cursor = db.cursor()
    sql_real = "LOAD DATA LOCAL INFILE '{}' INTO TABLE {} CHARACTER SET utf8 FIELDS TERMINATED BY ','".format(
        result_file,
        'PM')
    sql_history = "LOAD DATA LOCAL INFILE '{}' INTO TABLE {} CHARACTER SET utf8 FIELDS TERMINATED BY ','".format(
        result_file,
        'PM_history')
    print(sql_real)
    try:
        cursor.execute('TRUNCATE TABLE PM')
        db.commit()
        cursor.execute(sql_real)
        db.commit()
        cursor.execute(sql_history)
        db.commit()
    except:
        db.rollback()
    db.close()


def put_to_hdfs(result_file):
    client = Client("http://192.168.53.30:50070")
    if client.status('/tmp/result.csv',strict=False):
        client.delete('/tmp/result.csv')
        client.upload('/tmp', result_file)
    else:
        client.upload('/tmp', result_file)


def load_to_hive(result_file):
    put_to_hdfs(result_file)
    t = time.localtime()
    year = str(t[0])
    month = str(t[1])
    day = str(t[2])
    hour = str(t[3])
    sql = "load data inpath '{}' into table {} PARTITION(year='{}',month='{}',day='{}',hour='{}')".format(
        '/tmp/result.csv',
        'pm_history',
        year, month,
        day, hour)
    conn = hive.Connection(host='192.168.53.31', port=10000, username='hive', database='air_quality')
    print(sql)
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        # for l in cursor.fetchall():
        #     print(l)
    except:
        conn.rollback()
    conn.close()


@run_time
def spider_run():
    s = get_html('http://www.86pm25.com')
    d = get_city_url(s)

    # d = {'北京': 'city/beijing.html', '上海': 'city/shanghai.html', '天津': 'city/tianjin.html'}

    save_to_csv(region_page(d))
    insert_to_mysql('./result.csv')
    load_to_hive('./result.csv')
    # region_page(d)
    # print(get_lnglat('湖南省株洲荷塘区市监测站'))


if __name__ == '__main__':
    # 创建新线程
    thread1 = myThread()
    # thread2 = myThread()

    # 开启新线程
    thread1.start()
    # thread2.start()

    # 添加线程到线程列表
    threads.append(thread1)
    # threads.append(thread2)

    # 等待所有线程完成
    for t in threads:
        t.join()

    print("退出主线程")
