import os
from os.path import join
import pandas as pd
import time

start = time.time()


datapath=os.getcwd()
unzip_path = join(datapath,"01_Raw_Data","File")
save_path = join(datapath,"02_Processing_Data","Trim_Data")


files=os.listdir(unzip_path)
compare_file = join(datapath,"02_Processing_Data","standard_customer_list.csv")
compare_df = pd.read_csv(compare_file,
                         names=['customer', 'LP'],
                         header=0)

for file in files :
    input_file = join(unzip_path,file)
    input_df = pd.read_csv(input_file,
                           names=['ymd','time','customer','meter','usage'],
                           header=0,
                           index_col=False)
    # 고객번호(customer) 10자리로 통일
    input_df['customer']=input_df['customer'].astype(str).str.zfill(10)
    compare_df['customer'] = compare_df['customer'].astype(str).str.zfill(10)

    # 시간(time) 2자리로 통일 <= 자료추출 당시 시 분으로 표기되어있으며, 자리수 안맞음
    input_df["time"] = input_df.time//100

    #
    merge_df = pd.merge(input_df,compare_df,
                        how='left',
                        left_on=['customer'],
                        right_on=['customer'] )

    merge_df = merge_df[merge_df['LP'] == 1]
    merge_df.drop(['LP'],
                  axis=1,
                  inplace=True)
    merge_df.to_csv(join(save_path,file), encoding='utf-8-sig', index=False)

    del merge_df


print("time :", time.time() - start)