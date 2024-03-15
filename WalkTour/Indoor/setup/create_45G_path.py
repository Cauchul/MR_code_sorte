# -*- coding: utf-8 -*-
# 创建目录结构

import os

from Common import FindFile


def create_dir(in_path, in_list):
    for i_dir in in_list:
        # 拼接子目录的路径
        path = os.path.join(in_path, i_dir)
        # 创建子目录
        os.makedirs(path)


def create_in_all_csv_dir(in_path, in_dir_list):
    # 获取csv文件的路径
    res_csv_path_list = FindFile.get_csv_file_dir_list(in_path)
    # res_csv_path_list = get_data_path_by_char(folder_path)
    # print('res_csv_path_list: ', res_csv_path_list)
    res_csv_path_list = [in_i_string for in_i_string in res_csv_path_list if
                         'output' not in in_i_string and 'unzip' not in in_i_string]

    for i_dir in res_csv_path_list:
        print(i_dir)
        create_dir(i_dir, in_dir_list)


if __name__ == '__main__':
    # dir_list = ['4G', '5G']
    # # 在当前目录下创建三个目录
    # data_path = r'E:\work\MR_Data\1月16号\20240116(1)_new_no_table\20240116'
    # create_dir(data_path, dir_list)

    folder_path = r'E:\work\MrData\data_place\out'
    # 在当前目录创建
    dir_list = ['mate40', 'p40', 'xiaomi_13']
    create_dir(folder_path, dir_list)
    # dir_list = ['1', '2', '3', '4']
    # create_dir(folder_path, dir_list)
    # 在所有的csv目录下创建
    # dir_list = ['4G', '5G']
    # dir_list = ['1', '2', '3']
    # create_in_all_csv_dir(folder_path, dir_list)

    # def create_in_all_csv_dir(in_path):
    #     # 获取csv文件的路径
    #     res_csv_path_list = FindFile.get_csv_file_dir_list(in_path)
    #     # res_csv_path_list = get_data_path_by_char(folder_path)
    #     # print('res_csv_path_list: ', res_csv_path_list)
    #     res_csv_path_list = [in_i_string for in_i_string in res_csv_path_list if
    #                          'output' not in in_i_string and 'unzip' not in in_i_string]
    #
    #     dir_list = ['4G', '5G']
    #     for i_dir in res_csv_path_list:
    #         print(i_dir)
    #         create_dir(i_dir, dir_list)
