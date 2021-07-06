import datetime
from os.path import join, basename
from glob import glob

import numpy as np
import pandas as pd
import os


import pyarrow.csv as pacsv
import csv

from pyarrow import csv

import time
start_time = time.time()


data_path=os.getcwd()
raw_data_path = join(data_path, "02_Processing_Data", "Trim_Data")
raw_file = join(data_path, "02_Processing_Data", "combine.csv")


#pyarrow_table = pacsv.read_csv(raw_file)
#print("success")
#raw_data_df = pyarrow_table.to_pandas()

raw_data_df = pd.read_csv(raw_file,
                     names=['ymd', 'time', 'customer', 'meter', 'usage'],
                     header=0,
                     index_col=False,
                     chunksize=200000)

raw_data_df = pd.concat(raw_data_df)

print(raw_data_df.shape)
print(raw_data_df.info())

raw_data_df['time'] = raw_data_df['time'].astype(np.int16)
raw_data_df['customer'] = raw_data_df['customer'].astype(np.int16)
raw_data_df['meter'] = raw_data_df['meter'].astype(np.int16)
raw_data_df['usage'] = raw_data_df['usage'].astype(np.float16)


print(raw_data_df.shape)
print(raw_data_df.info())

# # ymd를 datetime 타입으로 저장
datetime_save_df = raw_data_df.assign(ymd=pd.to_datetime(raw_data_df['ymd'].astype(str)))

print(datetime_save_df.head(20))


Run_time = datetime.timedelta(seconds=(time.time() - start_time))

print(Run_time)