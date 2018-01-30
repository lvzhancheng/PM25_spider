#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import time
import urllib.request
import pickle
import threading

from io import StringIO
from bs4 import BeautifulSoup



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
            city_dic[i.string.replace('市','')] = province
    # 北京地区比较特殊，要单独处理
    for i in soup.find_all('a'):
        if i.h4:
            break
        elif i['href'][0] != '#' and i.string != '北京市':
            city_dic[i.string.replace('市','')] = '北京市'
    return city_dic
d=find_all_province()
with open('city_dic','w') as f:
    f.write(str(d))

def read_city_dic(file_name):
    with open(file_name,'r') as f:
        l=f.read()
    d=eval(l)
    return d

test=read_city_dic('city_dic')
print(type(test),test)