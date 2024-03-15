# -*- coding: utf-8 -*-
# 合并多次测试结果
import os
import shutil

import pandas as pd

from Common import get_path_sub_dir, get_all_csv_file, Common, FindFile, find_output_dir, print_with_line_number


def merge_all_data(in_file_list):
    res_path_list = Common.split_path_get_list(in_file_list[0])
    print(res_path_list)
    out_file = f'merge_' + res_path_list[-1]
    print('out_file: ', out_file)
    # 合并数据
    data = pd.concat([pd.read_csv(file) for file in in_file_list])
    # 删除空行
    data = data.dropna(subset=['f_longitude', 'f_latitude'], how='any')
    data.to_csv(os.path.join(folder_path, out_file), index=False)


def merge_data(in_file_list, in_char):
    res_path_list = Common.split_path_get_list(in_file_list[0])
    print(res_path_list)
    out_file = f'merge_{in_char}_' + res_path_list[-1]
    print('out_file: ', out_file)
    # 合并数据
    data = pd.concat([pd.read_csv(file) for file in in_file_list])
    # 删除空行
    data = data.dropna(subset=['f_longitude', 'f_latitude'], how='any')
    data.to_csv(fr'D:\working\data_conv\out_path\{out_file}', index=False)


def get_merge_file_list(in_path, in_char, in_ot_char):
    tmp_csv_files = [os.path.join(in_path, file) for file in os.listdir(in_path) if
                     file.endswith('.csv') and in_char in file and in_ot_char in file]
    return tmp_csv_files


def get_output_dir_csv(in_src_data):
    tmp_res_list = []
    in_output_dir_list = find_output_dir(in_src_data)
    for i_dir in in_output_dir_list:
        in_res_list = Common.list_files_in_directory(i_dir)
        tmp_res_list.extend(in_res_list)

    # 只获取finger文件
    tmp_res_list = [x for x in tmp_res_list if 'finger' in x]
    return tmp_res_list


if __name__ == '__main__':
    folder_path = r'E:\work\MrData\data_place\out\xiaomi_13'
    # 获取当前路径下的所有csv文件
    res_file_list = FindFile.get_cur_dir_all_csv(folder_path)
    # 获取output目录
    # res_file_list = get_output_dir_csv(folder_path)

    # res_file_list = FindFile.find_files_with_string(folder_path, 'set_sid')
    print(res_file_list)

    merge_all_data(res_file_list)
