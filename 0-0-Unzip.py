import os
from os.path import join
from zipfile import ZipFile
import shutil

datapath=os.getcwd()
zip_path = join(datapath,"01_Raw_Data")
unzip_path = join(datapath,"01_Raw_Data")
save_path = join(unzip_path,"File")

for i in range(5,17):
    if i <13:
        month = str(i).zfill(2)
        year_month = "2019"+month
    else:
        month = str(i-12).zfill(2)
        year_month = "2020"+month
    zip_name = year_month + ".zip"

    # 월별에 따른 폴더 압축 해제
    with ZipFile(join(zip_path,zip_name),"r") as new_zip :
        new_zip.extractall(unzip_path)

    # 압축풀린 파일을 한곳에 모음
    files = os.listdir(join(unzip_path,year_month))
    for file in files:
        shutil.move(f"{unzip_path}/{year_month}/{file}", save_path)

