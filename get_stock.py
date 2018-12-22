# -*- coding: utf-8 -*-
"""
Created on Thu Dec  6 00:51:19 2018

@author: taeil
"""

# ─────────────────────────────────────────────────────────────────────────────
#import io
#import numpy as np
import pandas as pd
import os
import requests
import sys
import threading
import datetime as dt
from bs4 import BeautifulSoup
from threading import Thread
from time import localtime, strftime
# ─────────────────────────────────────────────────────────────────────────────
class run_frgn(Thread):
    def __init__(self, stock_list):
        super().__init__()
        self.lock = threading.Lock()
        self.stock_list = stock_list
        print(len(self.stock_list))
        
    def run(self):
        try:
            outfile = 'frgn_%s.txt' % strftime("%Y%m%d%H%M%S", localtime())
            for n, stock in enumerate(self.stock_list):
                print("[{0}/{1}] {2} ☞ [{3}]".format(n, len(self.stock_list), stock, outfile))
                with self.lock:
                    frgn_read_to_csv(stock, outfile)
        except:
            print("[run_frgn] Unexpected error:", sys.exc_info()[0])
            pass


class run_sise(Thread):
    def __init__(self, stock_list):
        super().__init__()
        self.lock = threading.Lock()
        self.stock_list = stock_list
        print(len(self.stock_list))
        
    def run(self):
        try:
            outfile = 'sise_%s.txt' % strftime("%Y%m%d%H%M%S", localtime())
            for n, stock in enumerate(self.stock_list):
                print("[{0}/{1}] {2} ☞ [{3}]".format(n, len(self.stock_list), stock, outfile))
                with self.lock:
                    sise_read_to_csv(stock, outfile)
        except:
            print("[run_sise] Unexpected error:", sys.exc_info()[0])
            pass

    
def frgn_read_to_csv(stock, outfile):
    try:
        url = "http://finance.naver.com/item/frgn.nhn?code=%s" % stock
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'lxml')

        # 현재시간    
        timestamp = soup.select('.date')[0].text.strip()
        # 매도매수 거래원별 거래량
        frgn = soup.select('table.type2')[0] # 
        매도상위 = [x.text.replace('\n','') for x in frgn.select('.title.bg01') if x.text!='\n']
        매도거래량 = [x.text.replace('\n','') for x in frgn.select('.num.bg01') if x.text!='\n']
        매수상위 = [x.text.replace('\n','') for x in frgn.select('.title.bg02') if x.text!='\n']
        매수거래량 = [x.text.replace('\n','') for x in frgn.select('.num.bg02') if x.text!='\n']
        df = pd.DataFrame({'Timestamp':timestamp
                  ,'STOCK':stock
                  ,'매도상위':매도상위[0:5]\
                  ,'매도거래량':매도거래량[0:5]\
                  ,'매수상위':매수상위[0:5]\
                  ,'매수거래량':매수거래량[0:5]})
        # 외국계추정치
        frgn_total = soup.select('table.type2')[0].select('.total')[0]
        total = { 'Timestamp':timestamp
                 ,'STOCK':stock
                 ,'매도상위':frgn_total.select('.title')[0].text
                 ,'매도거래량':frgn_total.select('.num')[0].text.replace('\n','')
                 ,'매수상위':frgn_total.select('.title')[0].text
                 ,'매수거래량':frgn_total.select('.num')[1].text.replace('\n','') }
        df = df.append([total], ignore_index=True)

    except Exception as e:
        print('error: Error occurred in frgn_read_to_csv, scraping to make df', e)
        return 0       
    else:
        df.to_csv(outfile, header=None, index=None, sep='|', mode='a')
        return len(df.index)  # return rows count

def sise_read_to_csv(stock, outfile):
    try:
        url = "https://finance.naver.com/item/sise.nhn?code={0}&asktype=10".format(stock)
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'lxml')

        # 현재시간    
        timestamp = soup.select('.date')[0].text.strip()
        # 10호
        sise = soup.select('table.type2')[1] #
        매도잔량 = [x.text.replace('\n','').replace('\t','') for x in sise.select('.num.bg01')][0::2]
        매도호가 = [x.text.replace('\n','').replace('\t','') for x in sise.select('.num.bg01')][1::2]
        매수잔량 = [x.text.replace('\n','').replace('\t','') for x in sise.select('.num.bg02')][1::2]
        매수호가 = [x.text.replace('\n','').replace('\t','') for x in sise.select('.num.bg02')][0::2]
        df = pd.DataFrame({'Timestamp':timestamp
                  ,'STOCK':stock
                  ,'매도잔량':매도잔량\
                  ,'매도호가':매도호가\
                  ,'매수잔량':매수잔량\
                  ,'매수호가':매수호가})
        # 잔량합계
        sise_total = soup.select('table.type2')[2].select('.num')
        sise_total[0].text.replace('\n','').replace('\t','')
        total = { 'Timestamp':timestamp
                 ,'STOCK':stock
                 ,'매도잔량':sise_total[0].text.replace('\n','').replace('\t','')
                 ,'매도호가':None
                 ,'매수잔량':sise_total[1].text.replace('\n','').replace('\t','')
                 ,'매수호가':None }
        df = df.append([total], ignore_index=True)

    except Exception as e:
        print('error: Error occurred in sise_read_to_csv, scraping to make df', e)
        return 0       
    else:
        df.to_csv(outfile, header=None, index=None, sep='|', mode='a')
        return len(df.index)  # return rows count

url = "https://finance.naver.com/item/sise_time.nhn?code=005930&thistime=20181219200000&page=1"
response = requests.get(url)
soup = BeautifulSoup(response.text, 'lxml')
[x.text.replace('\n','').replace('\t','') for x in soup.select("table.type2")[0].select("tr > td")]





def get_naver_stock_list():
    try:
        naver = "https://finance.naver.com"
        url = naver + "/sise/sise_group.nhn?type=upjong"
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'lxml')

        # 업종별 현황
        now = dt.datetime.now().strftime('%Y%m%d')
        ind = soup.select("table.type_1")[0].select("tr a")
        
        # 업종내 개별종목
        stock_list = []
        for i in ind:
            ind_code = i['href'].split('no=')[1]
            ind_name = i.text
            
            response = requests.get(naver + i['href'])
            soup = BeautifulSoup(response.text, 'lxml')
            stocks = soup.select("table.type_5")[0].select("tr td a")
            for x in stocks[0:-1:2]:
                stock_code = x['href'][-6:]
                stock_name = x.text
                stock_list.append([now, ind_code, ind_name, stock_code, stock_name])
                
        df = pd.DataFrame(stock_list, columns=['입수일자','업종코드','업종명','업체코드','업체명'])
        return df
        
    except Exception as e:
        print('error: Error occurred in get_stock_list, scraping to make df\n', e)
    



# 기본 디렉토리
os.chdir(r'C:\Users\taeil\Documents\get_stockpy')
# 업종별 종목 리스트
sl = get_naver_stock_list(); sl.to_csv('naver_stock_list.txt', header=None, index=None, sep='|', mode='a')
sl = list(sl.where(sl.입수일자==sl['입수일자'].max())['업체코드'].sort_values())
# 거래원별 자료 입수
run_frgn(sl).start(); run_sise(sl).start()