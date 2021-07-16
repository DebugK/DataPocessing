import datetime
import os
#from os.path import join
import pandas as pd
import time
from glob import glob
import numpy as np
import math

# 모든 컬럼명 출력 옵션(최대 10개)
pd.set_option('display.max_columns', 10)

# 소요시간 확인
start_time = time.time()

# 전역변수 MAX TIMESTAMP COUNT 24*365=8760
max_timestamp_count = 8760

data_path = os.getcwd()
load_path = os.path.join(data_path, "02_Processing_Data", "MonthCombine")#.replace('\\', '/') + '/'


# 현재 왜 path가 이중으로 \\ 되는지 알 수가 없음 추후 수정해야함.
# FileNotFoundError: [Errno 2] No such file or directory: 'C:\\Users\\ahraj\\PycharmProjects\\DataPocessing\\02_Processing_Data\\MonthCombine\\trim_total_customer.pkl'
# filename = os.path.join(load_path,'trim_total_customer.pkl')


filename = 'trim_total_customer.pkl'
print(filename)

input_df = pd.read_pickle(filename)
input_df.reset_index(drop=True,inplace=True)
print(input_df.head(60))
# 현재상태
# customer , timestamp  기준으로 sorting 되어있음

#input_df.sort_values(['customer','timestamp','time'],inplace=True)

print(input_df.head(30))
input_df['com_usage']=input_df.groupby(['customer','meter']).sum().groupby(level=0).cumsum().reset_index()

print(input_df.tail(5))


Run_time = datetime.timedelta(seconds=(time.time() - start_time))

print(Run_time)

