# -*- coding: utf-8 -*-

import os

from Common import check_path


def create_dir(in_path, in_num):
    """
    :param in_path: 需要创建目录的路径
    :param in_num: 创建目录的个数
    """
    for i in range(1, in_num + 1):
        # 拼接子目录的路径
        path = os.path.join(in_path, str(i))
        # 创建子目录
        check_path(path)
        # os.makedirs(path)


# 获取当前工作目录
current_dir = r'E:\work\MR_Data\2月27号\20240227\5G\WalkTour'


def create_under_all_sub_dir(in_dir, in_dir_num):
    # 遍历当前目录下的所有目录
    for dir_name in os.listdir(in_dir):
        tmp_data_path = os.path.join(in_dir, dir_name)
        if os.path.isdir(tmp_data_path):
            # print(dir_name)
            print(tmp_data_path)
            create_dir(tmp_data_path, in_dir_num)


if __name__ == '__main__':
    # 在当前目录下创建三个目录
    data_path = r'D:\MrData\2月28号\20240228\5G\WalkTour'
    # 当前目录下创建
    # create_dir(data_path, 3)
    # 当前目录下的所有子目录
    create_under_all_sub_dir(data_path, 3)
