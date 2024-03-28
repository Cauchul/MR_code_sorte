# -*- coding: utf-8 -*-
# 读取多个值，以逗号分隔
import os.path

import pandas as pd


def read_csv_get_df(in_df_path):
    if 'csv' in in_df_path:
        # print('csv文件')
        in_df = pd.read_csv(in_df_path, low_memory=False)
    else:
        # print('其他文件')
        in_df = pd.read_excel(in_df_path)
    return in_df


def df_write_to_csv(w_df, w_file):
    w_df.to_csv(w_file, index=False, encoding='UTF-8')


out_flag = False

while True:
    file_list = []
    cnt = 0
    while True:
        file = input("请输入需要合并的csv文件名然后回车（使用绝对路径）,输入out退出整个程序：")

        if '"' in file:
            # 去除字符串两端的引号
            file = file.strip('"')

        if 'out' == file.lower():
            out_flag = True
            break

        if os.path.exists(file):
            cnt += 1

            file_list.append(file)
            if cnt >= 2:
                break
        else:
            print('**' * 50)
            print('输入的csv文件不存在请重新输入，输入out退出整个程序')

    if out_flag:
        break

    df_list = []

    finger_file = ''
    mr_file = ''
    print(f"输入的文件名为：")
    for i_f in file_list:
        print(i_f)  # 使用strip()方法去除值两端的空白字符
        if 'finger' in i_f:

            finger_file = i_f
            tmp_merger_df = read_csv_get_df(i_f)
        elif i_f:
            mr_file = i_f
    print(f'指纹数据为：{finger_file}')
    print(f'MR数据为：{mr_file}')

    print("开始merge数据")
    tmp_merger_df = pd.merge(read_csv_get_df(finger_file), read_csv_get_df(mr_file),
                             left_on="f_time", right_on="f_time", how='left')

    # out_dir = os.path.join(os.path.dirname(finger_file), 'output')
    # out_file = out_dir + os.path.basename(finger_file).replace('.csv', '_mr_data_merge.csv')
    out_file = finger_file.replace('.csv', '_mr_data_merge.csv')
    df_write_to_csv(tmp_merger_df, out_file)
    print(f'输出文件为：{out_file}')
    print('---' * 50)
