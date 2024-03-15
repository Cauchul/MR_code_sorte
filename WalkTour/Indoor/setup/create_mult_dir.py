# -*- coding: utf-8 -*-
# 创建目录结构

import os

from Common import get_data_path_by_char


def create_dir(in_path, in_num):
    """
    :param in_path: 需要创建目录的路径
    :param in_num: 创建目录的个数
    """
    for i in range(1, in_num + 1):
        # 拼接子目录的路径
        path = os.path.join(in_path, str(i))
        # 创建子目录
        os.makedirs(path)


if __name__ == '__main__':
    # 在当前目录下创建三个目录
    data_path = r'D:\MrData\3月5日\5G\20240315测试'
    create_dir(data_path, 4)

    # folder_path = r'E:\work\MR_Data\1月22号\20210122(1)_new_no_table\20210122\岳云伟'
    # res_list = get_data_path_by_char(folder_path)
    #
    # for i_dir in res_list:
    #     print(i_dir)
    #     create_dir(i_dir, 3)
