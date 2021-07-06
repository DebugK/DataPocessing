import datetime
from os.path import join, basename
from glob import glob

import numpy as np
import pandas as pd
import os

import time
start_time = time.time()


datapath=os.getcwd()
raw_data_path = join(datapath,"02_Processing_Data","Trim_Data")


# raw data insert
raw_data_df = pd.concat([pd.read_csv(filename,
                                     names=['ymd','time','customer','meter','usage'],
                                     header=0,
                                     index_col=False)
                         for filename in glob(join(raw_data_path,f"{20}*.csv"))])
print(raw_data_df.info())
print(raw_data_df.head())

# # 고객번호(customer)가 null 인 행 제거
# # 고객번호가 Null 인 정보 없음.
# drop_na_customer_df = raw_data_df.dropna(subset=['customer'])
# print(drop_na_customer_df.info())
#
#
# # ymd를 datetime 타입으로 저장
# datetime_save_df = raw_data_df.assign(ymd=pd.to_datetime(raw_data_df['ymd'].astype(str)))
#
# # 월별 데이터 별도 저장
# assign_month_timestamp_df = datetime_save_df.assign(customer=datetime_save_df['customer'].astype(int),
#                                                     month=datetime_save_df['ymd'].dt.month,
#                                                     timestamp=datetime_save_df['ymd']+pd.to_datetime(datetime_save_df['time'],
#                                                                                                      unit='h'))
sorted_df=raw_data_df
#sorted_df = assign_month_timestamp_df.sort_values('meter','timestamp')
sorted_df.to_csv(join(datapath,"02_Processing_Data","combine.csv"),
                 index=None)



Run_time = datetime.timedelta(seconds=(time.time() - start_time))

print(Run_time)