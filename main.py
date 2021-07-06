# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

import pandas as pd

sample = pd.read_csv("20191101.csv",encoding='utf-8')
#sample.loc[sample["고객번호"]==223194406]
print(sample.head())
sample2 = sample.drop_duplicates("고객번호")
print(sample2.info())

sample2.to_csv("20191101-del.csv")

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
