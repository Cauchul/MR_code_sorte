# -*- coding: utf-8 -*-
# 创建目录结构

import os

from Common import get_path_sub_dir, print_with_line_number


def create_dir(in_path, in_list):
    for i_dir in in_list:
        # 拼接子目录的路径
        path = os.path.join(in_path, i_dir)
        # 创建子目录
        os.makedirs(path)


def create_cur_path(in_data_path):
    in_dir_list = ['4G', '5G']
    # 在当前目录下创建三个目录
    create_dir(in_data_path, in_dir_list)


def create_sub_path(in_data_path):
    in_res_dir_list = get_path_sub_dir(in_data_path)

    in_dir_list = ['4G', '5G']

    for i_p in in_res_dir_list:
        in_res_sub_path = os.path.join(in_data_path, i_p)
        print_with_line_number(f'当前处理路径: {in_res_sub_path}', __file__)
        create_dir(in_res_sub_path, in_dir_list)


if __name__ == '__main__':
    # dir_list = ['4G', '5G']
    # 在当前目录下创建三个目录
    data_path = r'E:\work\MR_Data\1月18号\20240118_源数据\室内'
    # 当前目录下创建 45G
    # create_cur_path(data_path)
    # 在当前目录的子目录下创建45G目录
    create_sub_path(data_path)

    # res_dir_list = get_path_sub_dir(data_path)
    # res_file_list = []
    #
    # in_dir_list = ['4G', '5G']
    #
    # for i_p in res_dir_list:
    #     sub_path = os.path.join(data_path, i_p)
    #     print_with_line_number(f'当前处理路径: {sub_path}', __file__)
    #     create_dir(sub_path, in_dir_list)
