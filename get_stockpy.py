# -*- coding: utf-8 -*-
"""
Created on Wed Dec 26 10:00:38 2018

@author: taeil
"""

# ─────────────────────────────────────────────────────────────────────────────
#import io
#import numpy as np
import datetime as dt
import glob
import os
import pandas as pd
import pymysql
import requests
import sys
import threading
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
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

def start_stock():
    global sl, th_list
    
    now = strftime("%H%M%S", localtime())
    def is_working_time():
        nonlocal now
        if '090000' <= now and now <= '180000':
            return True
        else:
            return False
    
    def is_working_date():
        return True
    
    if is_working_time() and is_working_date():
        print('[start] Starting... %s' % now);
        t1 = run_frgn(sl); t1.setName('frgn_%s' % now); t1.start()
        t2 = run_sise(sl); t2.setName('sise_%s' % now); t2.start()
        t3 = threading.Timer(180, start_stock); t3.setName('start_%s' % now); t3.start()
        #th_list.extend([t1, t2, t3])  # 3분마다 실행
    else:
        t3 = threading.Timer(180, start_stock); t3.setName('start_%s' % now); t3.start()
        #th_list.extend([t3])  # 3분마다 실행

def txt_to_aws():
    def df_to_aws(df, tbname):
        pymysql.install_as_MySQLdb()
        eng = create_engine("mysql+mysqldb://bogard75:1!gkskgksk@getstockpy.cwlv0262o99p.us-east-2.rds.amazonaws.com/getstockpy", encoding='utf-8')
        conn = eng.connect()
    
        try:
            df.to_sql(name=tbname, con=conn, if_exists='append')
        except Exception as e:
            print('[error]', e)
            df_to_aws(df, tbname)
        finally:
            conn.close()
            
    def glob_files(expression, tbname, names):
        files = glob.glob(expression)
        for i, f in enumerate(files):
            print('[aws] inserting... {0}/{1}'.format(i, len(files)))
            df = pd.read_csv(f, delimiter='|', names=names)
            df.pipe(df_to_aws, tbname)

    #glob_files('frgn*.txt', 'tb_frgn', ['STOCK','Timestamp','매도거래량','매도상위','매수거래량','매수상위'])
    glob_files('sise*.txt', 'tb_sise', ['STOCK','Timestamp','매도잔량','매도호가','매수잔량','매수호가'])

def main(argv):
    # 1 기본 디렉토리 이동
    # os.chdir(r'C:\Users\taeil\Documents\get_stockpy') # home
    os.chdir(r'C:\aws_getstockpy')                      # aws_kyaksik
    
    # 2 종목리스트 입수
    sl = get_naver_stock_list(); #sl.to_csv('naver_stock_list.txt', header=None, index=None, sep='|', mode='a')
    sl = sl.where(sl.입수일자==sl['입수일자'].max())['업체코드'].sort_values()[0:500]
    sl = list(sl)
    print('[sl] {0} stocks collected ...\n'.format(len(sl)))
    
    # 3 주가스크래핑 (600초 간격)
    th_list = []
    t = start_stock()  # interval in seconds
    th_list.append(t)
    
    # 4 DB저장 (aws insert)
    #txt_to_aws()
    
if __name__ == "__main__":
    main(sys.argv[1:])





#dd = pd.DataFrame(sl)
#dd.columns = ['STOCK_CODE']
#dd.assign(FB=dd['STOCK_CODE'].str.slice(0,2)).groupby('FB').count().plot()

