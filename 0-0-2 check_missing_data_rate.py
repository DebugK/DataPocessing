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

# 기준 퍼센트 ( 365일 중에 24시간 이하로 비어있는 고객을 찾기위함)
# 8760 - 24 =
standard_count = 8736

datapath=os.getcwd()
unzip_path = join(datapath, "02_Processing_Data", "Combine", "")

def usage_categorize(usage):
    if math.isnan(usage):
        return -1
    return math.floor(usage / 10) * 10

#######################################################
####       정리된 값을 다시 날짜별 파일로 저장          #####
####                                              #####
#######################################################
def save_dataframe(df,name) :
    save_path = join(datapath, "02_Processing_Data", "")

    df.to_pickle(
        save_path + name + ".pkl")
    print("☆☆☆☆☆☆☆☆☆☆☆☆☆☆☆☆SAVE SUCCESS☆☆☆☆☆☆☆☆☆☆☆☆☆☆☆☆☆☆")
    # pickle test
    # df = pd.read_pickle(save_path + "combine" + year + month + day + ".pkl")
    # print(df.describe())
    # print ( df[df['usage'].isna()])
    return

print(unzip_path)
# 1.  일별 데이터 가져와서 통합 df 로 만들기
# 2.  용량을 줄이기 위해 data별 type 재 지정
# df = pd.concat([pd.read_pickle(filename) for filename in glob(join(unzip_path,f"combine{20}*.pkl"))])
# df.reset_index()
# df['time']=df['time'].astype(np.uint8)
# df['customer']=df['customer'].astype(np.uint32)
# df['month']=df['month'].astype(np.uint8)
# df['meter']=df['meter'].astype(np.float32)
# df['usage']=df['usage'].astype(np.float32)
# print(df)
# print(df.info())
#
# # 사용량 기준 데이터 갯수 확인
# missing_usage_data_df = df.set_index("usage").groupby(usage_categorize).count()
# print("사용량 기준 데이터 갯수 : \n",missing_usage_data_df)
# # 분석 결과 : 0 ~ 10 사이의 사용량이 몰려있음
#
# # 10 ~ 48000 사이 고객 제거  필요
#
# drop_lager_customer_df =df.loc[~df['customer'].isin(
#                                 df.loc[df['usage'].gt(10),'customer'].drop_duplicates())]
# print(df.loc[df['usage'].gt(10),'customer'])
# missing_usage_data_df = drop_lager_customer_df.set_index("usage").groupby(usage_categorize).count()
# print("사용량 기준 데이터 갯수(10 ~ 4800 사이 사용량 고객 제외) : \n",missing_usage_data_df)
#
# # 고객번호 기준 각 컬럼별 데이터 갯수
# missing_customer_data_df = drop_lager_customer_df.groupby('customer')
# print(missing_customer_data_df.count())
#
# # 고객번호 중 제대로 timestamp가 안들어가 있는 고객 수 확인
# rate_df = missing_customer_data_df['timestamp'].count().ne(max_timestamp_count)
# print("고객번호 중 제대로 timestamp가 안들어가 있는 고객 수\n", rate_df.value_counts())
#
# # 고객번호 중 제대로 timestamp 가 안들어가 있는 고객 삭제
# list_of_drop_customer = rate_df.index[rate_df == True].tolist()
# drop_not_have_full_timestamp_df = drop_lager_customer_df[~drop_lager_customer_df['customer'].isin( i for i in list_of_drop_customer)]
# print(drop_not_have_full_timestamp_df)
#
#
# # timestamp, customer, time 기준으로 정렬
# drop_not_have_full_timestamp_df.sort_values(by=['customer','timestamp','time'],ascending=True,inplace=True)
# print(drop_not_have_full_timestamp_df.head(30))
#
# 현재까지
# save_dataframe(drop_not_have_full_timestamp_df,"save_all_day")






save_path = join(datapath, "02_Processing_Data", "")
print(save_path)
drop_not_have_full_timestamp_df = pd.read_pickle(save_path + "combinesave_all_day.pkl")




missing_customer_data_df = drop_not_have_full_timestamp_df.groupby('customer')
## 나중에 삭제 걍 프린트 용도
#
#
print(missing_customer_data_df.count())
# 고객번호 중 제대로 timestamp가 안들어가 있는 고객
rate_df = missing_customer_data_df['timestamp'].count().ne(max_timestamp_count)
# 분석 결과 : timestamp가 8760개가 안되는 고객번호 모두 삭제 ( 타임테이블에 누락된 값 들어있음)
# 8760 = 365*24
print("고객번호 중 제대로 timestamp가 안들어가 있는 고객\n", rate_df.value_counts())




#
#
##
# usage 갯수 기준으로 sorting
print(missing_customer_data_df.count()['usage'].sort_values(ascending=True))
missing_customer_data_df.count()['usage'].sort_values().to_csv("CUSTOMER_sample-full-9974.csv")

percent_df = round((missing_customer_data_df.count()/max_timestamp_count)*100,2)

print("고객번호중 24시간 이하로 결측치가 존재하는 고객 호수: \n",percent_df[(percent_df>=99.74) & (percent_df<=100)]['usage'].notnull().sum())
final_customer_df = percent_df[percent_df['usage']>=99.74]['usage'].reset_index()
print("최종 고객번호 :\n",final_customer_df.tail())

drop_having_nan_over_24h_df=drop_not_have_full_timestamp_df[drop_not_have_full_timestamp_df['customer'].isin(final_customer_df['customer'])]
print(drop_having_nan_over_24h_df.nunique())



# 8760개 보다 작은 고객 수가 얼마나 되는지 check( rate 계산)
#
missing_usage_rate_df=missing_customer_data_df['usage'].count().eq(max_timestamp_count)
print("사용량이 빠져있는 고객들\n", missing_usage_rate_df.value_counts())

print("결측치 채우기전:\n",drop_having_nan_over_24h_df.isnull().sum())
print("1시 ~ 23시 결측치 갯수 :\n",drop_having_nan_over_24h_df[drop_having_nan_over_24h_df['time'] > 0]['usage'].isnull().sum())

#drop_not_have_full_timestamp_df['usage']=drop_not_have_full_timestamp_df[drop_not_have_full_timestamp_df['time'] > 0]['usage'].fillna(method='ffill')

# drop_not_have_full_timestamp_df['usage'].fillna(method='ffill',inplace=True)
drop_having_nan_over_24h_df['meter'].fillna(method='ffill',inplace=True)


# 결측치 채우기 (pad + back 하나씩 만들어서 더하고 나누기 2 )
# 결측치 앞뒤 값의 중간값 사용하기 위함.
fill_na_usage_pad_df = drop_having_nan_over_24h_df['usage'].fillna(method='ffill')
fill_na_usage_back_df = drop_having_nan_over_24h_df['usage'].fillna(method='bfill')
sum_fill_na_usage_df = (fill_na_usage_pad_df + fill_na_usage_back_df)/2

print("결측치 중간값 결과 : \n",sum_fill_na_usage_df.head())
print(drop_having_nan_over_24h_df.head(10))

drop_having_nan_over_24h_df['usage']= sum_fill_na_usage_df

print("결측치 결측치 넣은 결과 : \n",sum_fill_na_usage_df.head())
print(drop_having_nan_over_24h_df.head(10))

print("결측치 채운 값 :\n",drop_having_nan_over_24h_df.isnull().sum())


# 저장하기 전에!!!
# usage float -> float32 변경
# meter float ->int32 변경 필요
# month -> int8 변경 필요
drop_having_nan_over_24h_df['meter']=drop_having_nan_over_24h_df['meter'].round().astype(np.uint32)
drop_having_nan_over_24h_df['month']=drop_having_nan_over_24h_df['month'].astype(np.uint8)
# 결측값 제거해야 가능
# print(df.info())

# check 용 : 실제로 돌릴떄 코드 삭제 필요 #
print(drop_not_have_full_timestamp_df.info())
print("=====================================================================")
group_by_customer_meter=drop_having_nan_over_24h_df.groupby(['customer','meter'])
print(group_by_customer_meter.count())

rate_df = group_by_customer_meter['timestamp'].count().ne(max_timestamp_count)
print("고객번호 중 제대로 timestamp가 안들어가 있는 고객\n", rate_df.value_counts(),"\n\n",new_df.head(10))
#


save_path = join(datapath, "02_Processing_Data", "MonthCombine", "")

pd.to_pickle(drop_having_nan_over_24h_df, "trim_total_customer.pkl")

Run_time = datetime.timedelta(seconds=(time.time() - start_time))

print(Run_time)


