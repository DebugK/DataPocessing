# 일자별 데이터 CSV 파일 OPEN
# 0. 데이터값 자리수 통일 (고객번호 - 10자리, 시간대 - 1자리)
# 1. 마이너스 값을 가지고있는 계기번호 데이터 모두 삭제
# 2. timestamp, month 컬럼 생성
# 2.1 'ymd' 컬럼 삭제 (timestamp와 중복)
# 3. 비어있는 시간대 데이터 생성
# 4. 시간대 생성시 만들어진 결측값 채우기 --> 1년치 데이터 만들고 이후에 넣기
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


#######################################################
####      0. 데이터값 자리수 통일                    #####
####        (고객번호 - 10자리, 시간대 - 1자리)       #####
#######################################################
def set_number_of_digits(df):
    # 고객번호(customer) 10자리로 통일
    df['customer'] = df['customer'].astype(str).str.zfill(10)

    # 시간 1자리로 통일
    #df["time"] = df.time // 100

    # print(df.describe())

    return df

#######################################################
####   0. usage가 음수 값을 가지고있는 퍼센트 확인      #####
####        (고객번호 - 10자리, 시간대 - 1자리)       #####
#######################################################
def check_minus_usage(df):
    # 고객번호 별로 음수 사용량 퍼센트
    new_df = df['customer'].groupby(df['usage'] <= 0).size().div(len(df)).apply(
        lambda x: f"{x:.2%}").to_frame(name='missing rate')
    # print(new_df)

    # 고객 번호별로 timeline 빠져있는  퍼센트
    df2 = df.groupby(['customer'])['usage'].count().to_frame(name='count')
    df3 = df2.groupby(df2['count'] < 24).count().div(len(df2))
    # print(df3)

#######################################################
#### 1. 마이너스 값을 가지고있는 계기번호 데이터 모두 삭제#####
####        (고객번호 - 10자리, 시간대 - 1자리)       #####
#######################################################
def drop_minus_usage(df):
    # 마이너스 사용량을 가진 고객 데이터 전체 삭제
    drop_minus_usage_customer_df = df.loc[~input_df['customer'].isin(
        df.loc[input_df['usage'].lt(0), 'customer'].drop_duplicates())]

    print_df = drop_minus_usage_customer_df['customer'].groupby(drop_minus_usage_customer_df['usage'] <= 0).size()\
                                                     .div(len(drop_minus_usage_customer_df)).apply(lambda x: f"{x:.2%}").to_frame(name='missing rate')
    # print("마이너스 사용량 제거 :\n", df)
    # print(drop_minus_usage_customer_df.info())
    return drop_minus_usage_customer_df



#######################################################
####        2. timestamp, month 컬럼 생성          #####
####                                              #####
#######################################################
def add_timestamp(df):
    # 고객정보에 timestamp 컬럼 추가
    with_timestamp_df = df.assign(ymd=pd.to_datetime(df['ymd'].astype(str)))
    # 'customer'열을 int 타입,
    # 'month'열을 'ymd' 의 월 부분만 추출,
    # 'timestamp'열을 'ymd'의 연월일과 'time'의 시간정보를 합하여 열 추가
    with_timestamp_df = with_timestamp_df.assign(customer=with_timestamp_df['customer'].astype(int),
                                                 month=with_timestamp_df['ymd'].dt.month,
                                                 timestamp=with_timestamp_df['ymd'] + pd.to_timedelta(with_timestamp_df['time'], unit="h"))
    # 중복되는 ymd 제거
    with_timestamp_df.drop(['ymd'], axis=1, inplace=True)
    # print("====================================================================================")
    return with_timestamp_df

#######################################################
####        3. 비어있는 시간대 데이터 생성            #####
####                                              #####
#######################################################
def make_full_timestamp(df):
    # timestamp를 가진 고객번호 생성
    customer_df = pd.DataFrame()
    customer_df['customer'] = df['customer'].drop_duplicates().reset_index(drop=True)

    # print(customer_df.columns)

    pd_ts = pd.date_range(start=year + month + day,
                          end=None,
                          periods=24,
                          freq='h').to_frame(index=False, name='timestamp')

    # print(pd_ts)
    customer_timestamp_df = pd.DataFrame(columns=['customer'])
    # for i in range(0,len(customer_df)):
    for i in range(0, 1000):
        plus_customer_df = pd_ts.assign(customer=customer_df['customer'].iloc[i])

        customer_timestamp_df = pd.concat([plus_customer_df, customer_timestamp_df])
        del plus_customer_df

    customer_timestamp_df['time'] = with_timestamp_df['timestamp'].dt.strftime('%H')
    customer_timestamp_df = customer_timestamp_df.astype({'customer': 'int32'})
    customer_timestamp_df = customer_timestamp_df.astype({'time': 'int32'})

    # print("고객번호 별 timestamp:\n", customer_timestamp_df.head())
    return customer_timestamp_df

#######################################################
####        3. 비어있는 시간대 데이터 생성            #####
####                                              #####
#######################################################
def merge_df(customer_timestamp_df,with_timestamp_df) :
    merge_customer_timestamp_df = pd.merge(customer_timestamp_df, with_timestamp_df,
                                           how='left',
                                           on=['customer', 'timestamp', 'time'])
    # 246727519 : 시간대 사용량이 비어있는  고객
    # 1170024894 : 사용량이 0 인 고객
    # print("고객번호 별 timestamp:\n", customer_timestamp_df.head())
    # print("=======FINAL======\n", merge_customer_timestamp_df[merge_customer_timestamp_df['time'] == 0])

    return merge_customer_timestamp_df

#######################################################
####       정리된 값을 다시 날짜별 파일로 저장          #####
####                                              #####
#######################################################
def save_dataframe(df) :
    save_path = join(datapath, "02_Processing_Data", "Combine", "")

    df.to_pickle(
        save_path + "combine" + year + month + day + ".pkl")

    # pickle test
    # df = pd.read_pickle(save_path + "combine" + year + month + day + ".pkl")
    # print(df.describe())
    # print ( df[df['usage'].isna()])
    print(save_path + year + month + day + ".pkl")
    return

#######################################################
####               month 결측값 채우기              #####
####                                              #####
#######################################################
def fill_month(df, month) :
    df['month'].fillna(value=month,inplace=True)
    # print("month  결측값 채우기")
    # print(df[df['month'].isna()])
    return df

datapath=os.getcwd()
unzip_path = join(datapath,"02_Processing_Data","Trim_Data")

files = os.listdir(unzip_path)
count = 0

for file in files :

    year = files[count][:4]
    month = files[count][4:6]
    day = files[count][6:8]

    # year = '2019'
    # month = '11'
    # day = '01'

    input_file = join(unzip_path, year+month+day+".csv")
    input_df = pd.read_csv(input_file,
                           names=['ymd', 'time', 'customer', 'meter', 'usage'],
                           #dtype={'ymd':np.int32, 'time':np.int8, 'customer':np.int32,'meter':np.int32,'usage':np.float32},
                           header=0,
                           index_col=False)

    # 자리수 통일
    input_df =set_number_of_digits(input_df)

    # 마이너스 사용량 퍼센트 체크
    #check_minus_usage(input_df)

    # 마이너스사용량을 가진 고객번호 전부 제거
    drop_minus_usage_customer_df = drop_minus_usage(input_df)

    # time stamp 생성
    with_timestamp_df = add_timestamp(drop_minus_usage_customer_df)

    # 고객번호별 timestamp 생성 ( 결측치 판단)
    customer_timestamp_df=make_full_timestamp(with_timestamp_df)

    # 고객번호별 timestamp 모두 존재하도록 merge
    merge_customer_timestamp_df= merge_df(customer_timestamp_df,with_timestamp_df)

    merge_customer_timestamp_df=fill_month(merge_customer_timestamp_df,month)

    save_dataframe(merge_customer_timestamp_df)

    count += 1
#for_End



# merge_customer_timestamp_df.to_csv(join(datapath,"02_Processing_Data","Combine","combine_"+year+month+day+".csv"),
#                  index=None)

#######################################################
####   4. 시간대 생성시 만들어진 결측값 채우기          #####
####                                              #####
#######################################################
# 4.1 계기번호 : 시간대가 0시 ~ 22시이면 뒤에있는 계기번호로 채움
#               시간대가 23시이면 앞에있는 계기번호로 채움
# month 결측값 채우기


merge_customer_timestamp_df['time']>=23
# time_zero_df = merge_customer_timestamp_df[(merge_customer_timestamp_df['meter'].isna())]['time']==0
# print("=======23시 결측값 Count======\n",time_zero_df,"\n",time_zero_df.value_counts(),type(time_zero_df))
#
# time_df = merge_customer_timestamp_df[time_zero_df]
# print(time_df.head(10))
# merge_customer_timestamp_df[time_zero_df]['month'].fillna(method='backfill')
# print("===================================================")
# print(merge_customer_timestamp_df[time_zero_df].head(10))
# if():
#
# else:
#     merge_customer_timestamp_df['meter'].fillna(method='ffill')

#print("고객번호 별 timestamp:\n", merge_customer_timestamp_df[time_zero_df])

















Run_time = datetime.timedelta(seconds=(time.time() - start_time))

print(Run_time)


