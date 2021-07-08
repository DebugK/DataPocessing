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
max_timestamp_count = 8760

datapath=os.getcwd()
unzip_path = join(datapath, "02_Processing_Data", "Combine", "")

def usage_categorize(usage):
    if math.isnan(usage):
        return -1
    return math.floor(usage / 10) * 10

print(unzip_path)
df = pd.concat([pd.read_pickle(filename) for filename in glob(join(unzip_path,f"combine{20}*.pkl"))])

print(df)
print(df.info())




# 사용량 기준 데이터 갯수 확인
missing_usage_data_df = df.set_index("usage").groupby(usage_categorize).count()
print("사용량 기준 데이터 갯수 : \n",missing_usage_data_df)
# 분석 결과 : 0 ~ 10 사이의 사용량이 몰려있음
# 10 ~ 48000 사이 고객 제거  필요

drop_lager_customer_df =df.loc[~df['customer'].isin(
                                df.loc[df['usage'].gt(10),'customer'].drop_duplicates())]
print(df.loc[df['usage'].gt(10),'customer'])
missing_usage_data_df = drop_lager_customer_df.set_index("usage").groupby(usage_categorize).count()
print("사용량 기준 데이터 갯수(10 ~ 4800 사이 사용량 고객 제외) : \n",missing_usage_data_df)





# 고객번호 기준 각 컬럼별 데이터 갯수
missing_customer_data_df = drop_lager_customer_df.groupby('customer')
print(missing_customer_data_df.count())

# 고객번호 중 제대로 timestamp가 안들어가 있는 고객 수 확인
rate_df = missing_customer_data_df['timestamp'].count().lt(max_timestamp_count)
print("고객번호 중 제대로 timestamp가 안들어가 있는 고객 수\n", rate_df.value_counts())

# 고객번호 중 제대로 timestamp 가 안들어가 있는 고객 삭제
list_of_drop_customer = rate_df.index[rate_df == True].tolist()
drop_not_have_full_timestamp_df = drop_lager_customer_df[~drop_lager_customer_df['customer'].isin( i for i in list_of_drop_customer)]
print(drop_not_have_full_timestamp_df)

missing_customer_data_df = drop_not_have_full_timestamp_df.groupby('customer')


## 나중에 삭제 걍 프린트 용도
#
#
print(missing_customer_data_df.count())
# 고객번호 중 제대로 timestamp가 안들어가 있는 고객
rate_df = missing_customer_data_df['timestamp'].count().lt(max_timestamp_count)
# 분석 결과 : timestamp가 8760개가 안되는 고객번호 모두 삭제 ( 타임테이블에 누락된 값 들어있음)
# 8760 = 365*24
print("고객번호 중 제대로 timestamp가 안들어가 있는 고객\n", rate_df.value_counts())
#
#
## 나중에 삭제 걍 프린트 용도

# usage 갯수 기준으로 sorting
# 8760개 보다 작은 고객 수가 얼마나 되는지 check( rate 계산)

missing_usage_rate_df=missing_customer_data_df['usage'].count().lt(max_timestamp_count)

print("사용량이 빠져있는 고객들\n", missing_usage_rate_df.value_counts())

print(missing_customer_data_df.count()['usage'].sort_values())



# 저장하기 전에!!!
# usage float -> float32 변경
# meter float ->int32 변경 필요
# month -> int8 변경 필요
# df['meter'].astype(int)
# df['month'].astype(int)
# 결측값 제거해야 가능
# print(df.info())



















Run_time = datetime.timedelta(seconds=(time.time() - start_time))

print(Run_time)


