import datetime
import numpy as np
import pandas as pd
import time
import requests
import json

# 모든 컬럼명 출력 옵션(최대 10개)
pd.set_option('display.max_columns', 20)

# 소요시간 확인
start_time = time.time()

# 전역변수 MAX TIMESTAMP COUNT 24*365=8760
max_timestamp_count = 8760

start_day = '20190502'
end_day = '20200430'

filename = 'add_weather_imformation.pkl'
print(filename)

input_df = pd.read_pickle(filename)
customer_info_df = pd.read_csv('(25269)-sample_customer.csv')

customer_info_df['customer'] = customer_info_df['customer'].astype(np.uint32)
customer_info_df['meter'] = customer_info_df['meter'].astype(np.uint64)
customer_info_df['contract-power'] = customer_info_df['contract-power'].astype(np.uint8)
customer_info_df['purpose-of-use'] = customer_info_df['purpose-of-use'].astype(np.uint8)
# customer_info_df['dwelling'] = customer_info_df['dwelling'].astype(np.uint8)

customer_info_df['sale-code-1'] = customer_info_df['sale-code-1'].astype(np.uint8)
customer_info_df['sale-code-2'] = customer_info_df['sale-code-2'].astype(np.uint8)

input_df['com_usage'] = round(input_df['com_usage'],3)
input_df['avgTa'] = round(input_df['avgTa'],2)
input_df['avgRhm'] = round(input_df['avgRhm'],2)


print(customer_info_df.info())
print(input_df.tail())


# sample_customer_df = input_df['customer'].drop_duplicates()
# merge_df = pd.merge(sample_customer_df,customer_info_df,how='inner',on='customer')
# print(merge_df.head())
# print(merge_df.describe())
# merge_df.to_csv('sample_customer.csv')

#
# sample_df = input_df[:max_timestamp_count*5]
# customer_info_df.drop(['meter-now'],axis=1,inplace=True)
# merge_df = pd.merge(sample_df,customer_info_df,how='left',on='customer')


# merge_df['meter'] = merge_df['meter_y']
# 실제 merge 시켜야 하는 값이 들어왔을때 실행 되어야 하는 코드
customer_info_df.drop(['meter-now'],axis=1,inplace=True)
merge_df = pd.merge(input_df,customer_info_df,how='left',on='customer')
merge_df.drop(['meter_x','date'],axis=1,inplace=True)
merge_df.rename(columns={'meter_y':'meter'},inplace=True)
print(merge_df.tail(30))


merge_df.to_pickle("sample-combine-customerinfo-weatherinfo.pkl")
#print("nunique:",input_df.nunique())



Run_time = datetime.timedelta(seconds=(time.time() - start_time))

print(Run_time)
