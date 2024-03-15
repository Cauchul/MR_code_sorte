# -*- coding: utf-8 -*-
# 合并多次测试结果
import os
import shutil

import pandas as pd

from Common import get_path_sub_dir, get_all_csv_file, Common, FindFile, find_output_dir, print_with_line_number
from WalkTour.Ourdoor.teardown.get_csv_file_dir import get_csv_file_path


def merge_all_data(in_file_list, in_out_path):
    in_res_path_list = Common.split_path_get_list(in_file_list[0])
    print_with_line_number(in_res_path_list, __file__)
    out_file = f'merge_' + in_res_path_list[-1]
    print_with_line_number(f'out_file: {out_file}', __file__)
    # 合并数据
    data = pd.concat([pd.read_csv(file) for file in in_file_list])
    # 删除空行
    data = data.dropna(subset=['f_longitude', 'f_latitude'], how='any')
    data.to_csv(os.path.join(in_out_path, out_file), index=False)


def merge_data(in_file_list, in_char):
    in_res_path_list = Common.split_path_get_list(in_file_list[0])
    print_with_line_number(in_res_path_list, __file__)
    out_file = f'merge_{in_char}_' + in_res_path_list[-1]
    print_with_line_number(f'out_file: {out_file}', __file__)
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
    for in_i_dir in in_output_dir_list:
        in_res_list = Common.list_files_in_directory(in_i_dir)
        tmp_res_list.extend(in_res_list)

    # 只获取finger文件
    tmp_res_list = [x for x in tmp_res_list if 'finger' in x]
    return tmp_res_list


def main_merge(in_folder_path, in_cur_path):
    # 获取当前路径下的所有csv文件
    if in_cur_path:
        in_res_file_list = FindFile.get_cur_dir_all_csv(in_folder_path)
    # 获取output目录
    else:
        in_res_file_list = get_output_dir_csv(in_folder_path)

    # print_with_line_number(in_res_file_list, __file__)

    in_4g_file_list = [i_f for i_f in in_res_file_list if '4G' in i_f]
    in_5g_file_list = [i_f for i_f in in_res_file_list if '5G' in i_f]

    # 把list中的文件全部合并
    if in_4g_file_list:
        print_with_line_number(f'合并目录：{in_folder_path} 合并文件数量:{len(in_4g_file_list)}', __file__)
        merge_all_data(in_4g_file_list, in_folder_path)
        print('4G合并完成' + '--' * 50)
    if in_5g_file_list:
        print_with_line_number(f'合并目录：{in_folder_path}  合并文件数量:{len(in_5g_file_list)}', __file__)
        merge_all_data(in_5g_file_list, in_folder_path)
        print('5G合并完成' + '--' * 50)


if __name__ == '__main__':
    folder_path = r'E:\work\MrData\data_place\merge\4G\小米13'

    # 获取csv文件的路径；合并多个目录下文件
    # res_path_list = get_csv_file_path(folder_path)
    # for i_dir in res_path_list:
    #     print_with_line_number(i_dir, __file__)
    #     main_merge(i_dir, in_cur_path=True)

    # 获取一个路径，当前路径
    main_merge(folder_path, in_cur_path=True)

    # 获取一个路径，output目录路径
    # main_merge(folder_path, in_cur_path=False)
