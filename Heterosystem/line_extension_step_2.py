# -*- coding: utf-8 -*-
import time
from datetime import datetime, timedelta

import pandas as pd

from Common import df_write_to_csv


# 定义一个函数来将时间字符串转换为秒数的整数类型
def time_to_seconds(time_str):
    time_obj = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S.%f")
    seconds = (time_obj.minute * 60 + time_obj.second) + (time_obj.microsecond / 1000000)
    return int(seconds)


def convert_datetime_to_seconds(dtime):
    return [int(time.mktime(time.strptime(x, '%y-%m-%d %H:%M:%S.%f'))) for x in dtime]


def data_line_extension(in_data_file):
    df = pd.read_csv(in_data_file)

    loop_value = df['f_time'][0]

    res_df = pd.DataFrame(columns=df.columns)

    i_index = old_index = 0
    row_data = df.iloc[0]

    while True:
        if loop_value > df['f_time'].iloc[-1]:
            break

        loop_value += 1
        i_index += 1
        print(f'loop_value: {loop_value}')
        if loop_value in df['f_time'].values:
            print(f'{loop_value} 在数据中')
            old_index += 1

        print(type(row_data))
        res_df.loc[i_index] = df.loc[old_index]
        res_df.loc[i_index, 'f_time'] = loop_value
        print('---' * 50)

    res_df = res_df.drop(columns='UE Time')

    out_file = in_data_file.replace('column_extension.csv', 'final_result.csv')
    df_write_to_csv(res_df, out_file)


if __name__ == '__main__':
    data_file = r'D:\MrData\3月4号_new\NR_MR_Detail_20240311095550_demo_column_extension.csv'
    data_line_extension(data_file)
