import datetime
import numpy as np
import pandas as pd
import time
import requests
import json

# 모든 컬럼명 출력 옵션(최대 10개)
pd.set_option('display.max_columns', 10)

# 소요시간 확인
start_time = time.time()

# 전역변수 MAX TIMESTAMP COUNT 24*365=8760
max_timestamp_count = 8760

start_day = '20190502'
end_day = '20200430'

filename = 'accumulate_usage.pkl'
print(filename)

input_df = pd.read_pickle(filename)

# 기온 데이터 입력 (수원지역코드 : 119)

url = 'http://apis.data.go.kr/1360000/AsosDalyInfoService/getWthrDataList'
key = 'Pt0oUEUEzvyC0JdAZNhRi6i3nY5%2FDj9ulObntXRZG%2B5JtRztw8IPBsJaR%2BuAZylBWh8SNI3QmRYIzbDgzJrsSQ%3D%3D'

idx = pd.date_range(start_day, end_day, freq='D')
code = '119'
data = pd.DataFrame()
pageNo = 1

queryParams = '?' + \
              'ServiceKey=' + key + \
              '&pageNo=' + repr(pageNo) + \
              '&numOfRows=' + '999' + \
              '&dataType=' + 'JSON' + \
              '&dataCd=' + 'ASOS' + \
              '&dateCd=' + 'DAY' + \
              '&startDt=' + start_day + \
              '&endDt=' + end_day + \
              '&stnIds=' + code

result = requests.get(url + queryParams)
js = json.loads(result.content)
data_tmp = pd.DataFrame(js['response']['body']['items']['item'])
data = pd.concat([data_tmp, data], axis=0, ignore_index=True)


data = data[['tm', 'avgTa', 'avgRhm']]
data.columns = ['date', 'avgTa', 'avgRhm']
data['avgTa'] = data['avgTa'].astype(np.float16)
data['avgRhm'] = data['avgRhm'].astype(np.float16)
data['date'] = data['date'].astype(str)


print(data[['date', 'avgTa', 'avgRhm']].head(10))



input_df['date'] = input_df['timestamp'].dt.date
input_df['weekday'] = input_df['timestamp'].dt.weekday

input_df['date'] = input_df['date'].astype(str)
print('date time 변경 완료')
print(input_df.info())
print(data.info())

merge_df = pd.merge(input_df, data, how='right',on='date')
# merge_df = pd.concat([data,input_df],axis=1,key='date')

#print(data['date'].isin(input_df['date']))
#input_df['avgTa'] = data[data['date'].isin(input_df['date'])]['avgTa']

# print("nunique:",merge_df.nunique())
input_df.drop(['date'],axis=1,inplace=True)
input_df['weekday'] = input_df['weekday'].astype(np.int8)

print(merge_df.iloc[max_timestamp_count*3-2:max_timestamp_count*3+4])
merge_df.to_pickle('add_weather_imformation.pkl')

Run_time = datetime.timedelta(seconds=(time.time() - start_time))

print(Run_time)
