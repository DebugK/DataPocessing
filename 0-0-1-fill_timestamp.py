# 일자별 데이터 CSV 파일 OPEN
# 0. 데이터값 자리수 통일 (고객번호 - 10자리, 시간대 - 1자리)
# 1. 마이너스 값을 가지고있는 계기번호 데이터 모두 삭제
# 2. timestamp, month 컬럼 생성
# 2.1 'ymd' 컬럼 삭제 (timestamp와 중복)
# 3. 비어있는 시간대 데이터 생성
# 4. 시간대 생성시 만들어진 결측값 채우기
# 4.1 계기번호 : 시간대가 0시 ~ 22시이면 뒤에있는 계기번호로 채움
#               시간대가 23시이면 앞에있는 계기번호로 채움
# 4.2 usage : 앞뒤 평균값으로 채움


# csv 파일 용량이 크므로, load의 편의성을 위해 pickle로 저장



import datetime
import os
from os.path import join
import pandas as pd
import time
import numpy as np

# 모든 컬럼명 출력 옵션(최대 10개)
pd.set_option('display.max_columns', 10)
# 소요시간 확인
start_time = time.time()


datapath=os.getcwd()
unzip_path = join(datapath,"01_Raw_Data","File")
save_path = join(datapath,"02_Processing_Data","Trim_Data")

year = '2019'
month = '11'
day = '01'

files = os.listdir(unzip_path)

input_file = join(unzip_path, year+month+day+".csv")
input_df = pd.read_csv(input_file,
                       names=['ymd', 'time', 'customer', 'meter', 'usage'],
                       #dtype={'ymd':np.int32, 'time':np.int8, 'customer':np.int32,'meter':np.int32,'usage':np.float32},
                       header=0,
                       index_col=False)

#######################################################
####                                              #####
####                                              #####
#######################################################
# 0. 데이터값 자리수 통일 (고객번호 - 10자리, 시간대 - 1자리)

# 고객번호(customer) 10자리로 통일
input_df['customer'] = input_df['customer'].astype(str).str.zfill(10)

# 시간 1자리로 통일
input_df["time"] = input_df.time // 100
print(input_df.head())




# 고객번호 별로 음수 사용량 퍼센트
df = input_df['customer'].groupby(input_df['usage']<=0).size().div(len(input_df)).apply(lambda x:f"{x:.2%}").to_frame(name='missing rate')
print(df)

# 고객 번호별로 timeline 빠져있는  퍼센트
df2 = input_df.groupby(['customer'])['usage'].count().to_frame(name='count')
df3 = df2.groupby(df2['count']<24).count().div(len(df2))

print(df2.head())
print(df3)




# 마이너스 사용량을 가진 고객 데이터 전체 삭제
drop_minus_usage_customer_df = input_df.loc[~input_df['customer'].isin(
                                input_df.loc[input_df['usage'].lt(0),'customer'].drop_duplicates())]

df = drop_minus_usage_customer_df['customer'].groupby(drop_minus_usage_customer_df['usage']<=0).size().div(len(drop_minus_usage_customer_df)).apply(lambda x:f"{x:.2%}").to_frame(name='missing rate')
print("마이너스 사용량 제거 :\n",df)
print(drop_minus_usage_customer_df.info())

# 고객정보에 timestamp 컬럼 추가
with_timestamp_df = drop_minus_usage_customer_df.assign(ymd=pd.to_datetime(drop_minus_usage_customer_df['ymd'].astype(str)))
# 'cust'열을 int 타입,
# 'month'열을 'ymd' 의 월 부분만 추출,
# 'timestamp'열을 'ymd'의 연월일과 'time'의 시간정보를 합하여 열 추가
with_timestamp_df = with_timestamp_df.assign(customer=with_timestamp_df['customer'].astype(int),
                                             month=with_timestamp_df['ymd'].dt.month,
                                             timestamp=with_timestamp_df['ymd'] + pd.to_timedelta(with_timestamp_df['time'], unit="h"))

print(with_timestamp_df.head())
print("=======FINAL======\n",with_timestamp_df[with_timestamp_df['customer'] == 242309818])
print("====================================================================================")


# timestamp를 가진 고객번호 생성
customer_df = pd.DataFrame()
customer_df['customer']= drop_minus_usage_customer_df['customer'].drop_duplicates().reset_index(drop=True)
customer_df.columns=['customer']

print(customer_df.columns)

pd_ts = pd.date_range(start=year+month+day,
                      end = None,
                      periods=24,
                      freq='h').to_frame(index=False,name='timestamp')

print(pd_ts)
customer_timestamp_df = pd.DataFrame(columns=['customer'])
#for i in range(0,len(customer_df)):
for i in range(0,1000):
    plus_customer_df = pd_ts.assign(customer=customer_df['customer'].iloc[i])

    customer_timestamp_df=pd.concat([plus_customer_df,customer_timestamp_df])
    del plus_customer_df

customer_timestamp_df['time'] = with_timestamp_df['timestamp'].dt.strftime('%H')

customer_timestamp_df = customer_timestamp_df.astype({'customer':'int32'})
customer_timestamp_df = customer_timestamp_df.astype({'time':'int32'})

print("고객번호 별 timestamp:\n", customer_timestamp_df.head())

merge_customer_timestamp_df = pd.merge(customer_timestamp_df, with_timestamp_df,
                                       how='left',
                                       on=['customer', 'timestamp', 'time'])
#246727519 : 시간대 사용량이 비어있는  고객
#1170024894 : 사용량이 0 인 고객
print("고객번호 별 timestamp:\n", customer_timestamp_df.head())
print("=======FINAL======\n",merge_customer_timestamp_df[merge_customer_timestamp_df['customer'] == 242309818])


# merge_customer_timestamp_df.to_csv(join(datapath,"02_Processing_Data","Combine","combine_"+year+month+day+".csv"),
#                  index=None)

#######################################################
####   4. 시간대 생성시 만들어진 결측값 채우기          #####
####                                              #####
#######################################################
# 4.1 계기번호 : 시간대가 0시 ~ 22시이면 뒤에있는 계기번호로 채움
#               시간대가 23시이면 앞에있는 계기번호로 채움

print("=======FINAL======\n",merge_customer_timestamp_df[(merge_customer_timestamp_df['time'] == 0)])














merge_customer_timestamp_df.to_pickle(datapath+"02_Processing_Data"+"Combine"+"combine"+year+month+day+".pkl")

# pickle test
df = pd.read_pickle(datapath+"02_Processing_Data"+"Combine"+"combine"+year+month+day+".pkl")
print(df.head(10))



Run_time = datetime.timedelta(seconds=(time.time() - start_time))

print(Run_time)


