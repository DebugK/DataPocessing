import datetime
import os
import pandas as pd
import time


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
print(input_df.info())
# 현재상태
# customer , timestamp  기준으로 sorting 되어있음

# input_df.sort_values(['customer','timestamp','time'],inplace=True)
# print(input_df.head(30))

input_df['com_usage']=input_df[['customer','usage']].groupby('customer').transform(lambda x : x.cumsum()).fillna(0)
# 만약에 0부터 시작하고, 현재 시간 값 이 다음번에 누적이 되어야 한다면, lambda x : x.cumsum().shift() 사용
print("고객번호별로 누적치 값이 제대로 들어갔는지 확인 \n",input_df.iloc[2*max_timestamp_count-2:2*max_timestamp_count+2])

input_df.to_pickle('accumulate_usage.pkl')

Run_time = datetime.timedelta(seconds=(time.time() - start_time))

print(Run_time)

