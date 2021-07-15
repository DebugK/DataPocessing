import datetime
import os
from os.path import join
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
# 19년 05,06,07,08,09 = 3648
# 19년 10,11,12 = 2208
# 20년 1,2,3,4 = 2904
max_timestamp_count = 8760


def usage_categorize(usage):
    if math.isnan(usage):
        return -1
    return math.floor(usage / 10) * 10


datapath=os.getcwd()
unzip_path = join(datapath)
print(unzip_path)

input_file = join(unzip_path, ""+ ".csv")
# input_df = pd.concat([pd.read_csv(filename, index_col=0) for filename in glob(join(unzip_path,f"CUSTOMER_sample-{20}*.csv"))],axis=1)
input_df = pd.read_csv("CUSTOMER_sample-full.csv")


# input_df1= pd.read_csv("CUSTOMER_sample-20190(3648).csv")
# input_df2= pd.read_csv("CUSTOMER_sample-20191(2208).csv")
# input_df3= pd.read_csv("CUSTOMER_sample-2020(2904).csv")
#
# print(input_df1.tail(10))
# print(input_df1.describe())

print(input_df.tail(5))
print(input_df.describe())

sum_df = input_df.sort_values(by='usage',ascending=True)
print(sum_df)
print("===================================")
percent_df = round((sum_df/max_timestamp_count)*100)
print(percent_df)
percent_df = percent_df['usage'].value_counts().sort_values(ascending=True)
#percent_df = round((percent_df/max_timestamp_count)*100)
print(percent_df)

