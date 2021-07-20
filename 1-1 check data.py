import datetime
import numpy as np
import pandas as pd
import time
import matplotlib.pyplot as plt
#%matplotlib inline
#from calendar import get_calendar

# 모든 컬럼명 출력 옵션(최대 10개)
pd.set_option('display.max_columns', 30)

# 소요시간 확인
start_time = time.time()

# 전역변수 MAX TIMESTAMP COUNT 24*365=8760
max_timestamp_count = 8760

# filename = 'sample-combine-customerinfo-weatherinfo.pkl'
# print(filename)
#
# input_df = pd.read_pickle(filename)
# print(input_df.info(), input_df['customer'].nunique())
# input_df.dropna(axis=0,inplace=True)
# input_df['com_usage'] = round(input_df['com_usage'],3)
# input_df['avgTa'] = round(input_df['avgTa'],2)
# input_df['avgRhm'] = round(input_df['avgRhm'],2)
# print(input_df.info(), input_df['customer'].nunique())
# print(input_df.head())
#
#
# input_df.to_pickle('last_data_set(25269).pkl')

filename = 'last_data_set(25269).pkl'
print(filename)

input_df = pd.read_pickle(filename)

weekday_names = ['Mon','Tue','Wed','Thu','Fri','Sat','Sun']
weekday = {idx:name for idx, name in zip(range(7),weekday_names)}
print (weekday)

sample_df = input_df[:max_timestamp_count*10]
print(sample_df.head(30))
print(sample_df.columns)
for each in range(0,7) :
    sample_df.loc[sample_df['weekday']==each].groupby('time').mean()['usage'].plot.line()

plt.legend(weekday)
plt.show()


Run_time = datetime.timedelta(seconds=(time.time() - start_time))

print(Run_time)
