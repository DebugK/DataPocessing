import datetime
import os
from os.path import join
import pandas as pd
import time
import math
import numpy as np
from glob import glob

# 전역변수 MAX TIMESTAMP COUNT 24*365=8760
# 366 * 24 = 8,784
# 19년 05,06,07,08,09 = 3648
# 19년 10,11,12 = 2208
# 20년 1,2,3,4 = 2904
max_timestamp_count = 8760

# 모든 컬럼명 출력 옵션(최대 10개)
pd.set_option('display.max_columns', 10)

# 소요시간 확인
start_time = time.time()

def times_categorize(times):
    if math.isnan(times):
        return -1
    return math.floor(times / 10) * 10

datapath=os.getcwd()
unzip_path = join(datapath,'01_Raw_Data', 'File')
# C:\Users\ahraj\PycharmProjects\DataPocessing\01_Raw_Data\File
print(unzip_path)
#files = os.listdir(unzip_path)
count = 0

##
# input_df = pd.concat([pd.read_csv(filename, index_col=False) for filename in glob(join(unzip_path,f"{20}*.csv"))])
#
# print(input_df.describe())
#
# input_df.rename(columns={'YMD':'ymd', '시간':'time', '고객번호':'customer', '계기번호':'meter', '사용량':'usage'},inplace=True)
# print(input_df.head())
# print(input_df.info())
#
# input_df["time"] = input_df.time // 100
# input_df['time'] = input_df['time'].astype(np.uint8)
# input_df['customer'] = input_df['customer'].astype(np.uint32)
# input_df['meter'] = input_df['meter'].astype(np.uint32)
# input_df['usage'] = input_df['usage'].astype(np.float32)
# print(input_df.head())
#
# input_df.to_pickle("combine_raw_data.pickle")





# input_df = pd.read_pickle("combine_raw_data.pickle")
# input_df['meter']=input_df['meter'].astype(np.uint32)
# print(input_df.head(30))
# print(input_df.info())
# print("계기번호 2개이상 고객번호 제거 전 : ", input_df['customer'].nunique())
# #print(input_df['customer'].value_counts())
#
# # 고객번호당 계기 여러개 인경우 전체 삭제
# duplicated = input_df.drop_duplicates(['meter','customer']).groupby(['customer']).size().where(lambda x : x!=1).dropna().index
# input_df = input_df.loc[~input_df['meter'].isin(duplicated)]
#
# print("계기번호 2개이상 고객번호 제거 후 :", input_df['customer'].nunique())
#
# group_customer_df = input_df[['customer','time']].groupby('customer')
#
# print(group_customer_df.count().sort_values(by='time',ascending=True))
#
# count_customer_df = group_customer_df.count().value_counts(ascending=True)
# print(count_customer_df)
#
# count_customer_df.to_pickle('count_customer_raw_data.pickle')
#








input_df = pd.read_pickle("count_customer_raw_data.pickle")

input_df=input_df.reset_index()
input_df.columns = ['time','amount']
print(input_df.describe(),len(input_df),input_df['amount'].sum())


input_df['percent'] = round((input_df['time']/max_timestamp_count) * 100,2)
input_df.sort_values(by='percent',ascending=True,inplace=True)
new_df = input_df[input_df['percent']<101]
weird_df = input_df[input_df['percent']>100]
#print("고객번호중 1년치 데이터만 가지고 있는 것들 : \n",new_df.tail(10), new_df['amount'].sum())
#print("고객번호중 1년치 이상의 데이터를 가지고 있는 것들 : \n",input_df.tail(10),weird_df['amount'].sum())


new_sum = new_df['amount'].sum()
_percent=99.73
print("1년치 사용량을 {}% 이상 데이터를 가지고있는 고객 호수".format(_percent))
print(round(new_df[new_df['percent']>_percent]['amount'].sum()/new_sum,2)*100,"%")
print("================percent별 분포도===================")
print(round(new_df.set_index('percent').groupby(times_categorize)['amount'].sum()/new_sum,2)*100)





# print("================percent별 갯수===================")
# print(input_df.groupby('percent').count())
# print(round(sum(input_df['percent']<80)/len(input_df)*100,2))
# print(round(sum(((input_df['percent']>=80)&(input_df['percent']<=100))/len(input_df)*100),2))
# print(len(input_df))
#
# #
#



Run_time = datetime.timedelta(seconds=(time.time() - start_time))

print(Run_time)



# for file in files :