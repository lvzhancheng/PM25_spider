#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import re
import time
import urllib.request

import threading
# import PyMySQL
from io import StringIO

import math

import requests
from urllib.request import quote
from bs4 import BeautifulSoup


def run_time(func):
    def wrapper(*args, **kwargs):
        st = time.time()
        res = func(*args, **kwargs)
        et = time.time()
        print('%s 运行了 %s ms' % (func.__name__, (et - st) * 1000))
        return res

    return wrapper

#todo 实现测站坐标字典生成脚本
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


# 读取省城市字典文件
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

@run_time
def write_station_coordinate(city_dic):
    global  province_name
    station_lat_lng = {}
    for city_name, city_url in city_dic.items():
        s = get_html('http://www.86pm25.com/' + city_url)
        trs = s.find_all(name='tr')
        for tr in trs:
            td = tr.find_all(name='td')
            if td:
                city_dic = read_dic('city_dic')
                for k, v in city_dic.items():
                    if city_name in k:
                        province_name = v
                address = province_name + city_name + td[0].string + td[1].string
                lat_lng = get_lnglat(address)
                latitude = str(lat_lng[0]) + ','
                longtitude = str(lat_lng[1]) + '\n'
                station_lat_lng[address] = latitude+longtitude
    with open('station_coordinate','w') as f:
        f.write(str(station_lat_lng))

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


if __name__ == '__main__':
    write_station_coordinate()