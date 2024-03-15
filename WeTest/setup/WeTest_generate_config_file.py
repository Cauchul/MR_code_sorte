# -*- coding: utf-8 -*-
# 找到csv所在的目录
import configparser
import os

from Common import get_data_path_by_char, config, print_with_line_number, get_all_csv_file, FindFile


# 室外数据，获取csv所在的目录，list做自动化处理
def outdoor_get_csv_file_dir_list(in_folder_path):
    in_res_list = get_data_path_by_char(in_folder_path, '.csv')
    # list 去重
    in_res_list = list(set(in_res_list))
    return in_res_list


def generate_WeTest_config_file(in_file_list, in_config_out_path, in_data_type='finger'):
    # 添加节和键值对
    section_name = 'WalkTour'
    in_config = config.get_config()

    in_config.set(section_name, 'is_enabled', 'true')
    in_config.set(section_name, 'data_type', in_data_type)
    test_area = 'outdoor'

    for i_f in in_file_list:
        if 'char' in i_f:
            in_config.set(section_name, 'zcy_chart_file', i_f)
            test_area = 'indoor'
            print_with_line_number(f'char文件： {i_f}', __file__)
        elif 'table' in i_f:
            in_config.set(section_name, '45g_table_file', i_f)
            print_with_line_number(f'table文件： {i_f}', __file__)
        else:
            in_config.set(section_name, '45g_test_log', i_f)
            print_with_line_number(f'ue log 文件： {i_f}', __file__)

    in_config.set(section_name, 'test_area', test_area)
    print_with_line_number(f'生成 {section_name} {in_data_type} {test_area} 配置文件', __file__)
    # print('--' * 50)

    with open(os.path.join(in_config_out_path, f'WalkTour_{in_data_type}_config.ini'), 'w', encoding='UTF-8') as f:
        in_config.write(f)


def generate_config(in_csv_path):
    for i_dir in in_csv_path:
        print_with_line_number(f'当前数据路径：{i_dir}', __file__)
        # 获取路径下的所有的csv文件，list
        res_csv_file_list = get_all_csv_file(i_dir)
        # 生成指纹
        generate_WeTest_config_file(res_csv_file_list, i_dir, 'finger')
        print('--' * 50)
        # 生成uemr
        generate_WeTest_config_file(res_csv_file_list, i_dir, 'uemr')
        print('==' * 50)


# 获取所有的csv文件所在的目录
def get_csv_path_list(in_data_path):
    in_res_path_list = FindFile.get_csv_file_dir_list(in_data_path)
    # 去除output和unzip目录；或许需要生成config文件的目录
    res_in_path_list = [in_i_string for in_i_string in in_res_path_list if
                        'output' not in in_i_string and 'unzip' not in in_i_string]
    # 列表排序
    res_in_path_list.sort()
    return res_in_path_list


def get_config_project(in_config_file=r'E:\work\demo\config.ini'):
    res_in_config = configparser.ConfigParser()
    try:
        res_in_config.read(in_config_file, encoding='GBK')
    except UnicodeDecodeError:
        res_in_config.read(in_config_file, encoding='UTF-8')

    return res_in_config


def set_config_info(in_config, in_data_type, in_test_area, in_section_name='WalkTour'):
    in_config.set(in_section_name, 'is_enabled', 'true')
    in_config.set(in_section_name, 'data_type', in_data_type)
    in_config.set(in_section_name, 'test_area', in_test_area)


def set_WeTest_config_file_info(in_folder_path, in_data_type, in_config_file=r'E:\work\demo\no_table_config.ini'):
    in_section_name = 'WeTest'
    in_test_area = 'outdoor'

    in_config = get_config_project(in_config_file)
    # 获取所有的csv路径
    res_list = get_csv_path_list(in_folder_path)

    # print('res_list: ', res_list)
    # 获取目录下的所有的csv文件，准备写配置文件
    for i_path in res_list:
        print_with_line_number(f'当前处理路径：{i_path}', __file__)
        res_csv_file_list = get_all_csv_file(i_path)

        for i_f in res_csv_file_list:
            if 'coordinate_correction' in i_f:
                in_config.set(in_section_name, 'zcy_chart_file', i_f)
                in_test_area = 'indoor'
                print_with_line_number(f'char文件： {i_f}', __file__)
            elif 'chart' in i_f:
                # in_config.set(in_section_name, '45g_table_file', i_f)
                print_with_line_number(f'不写入config文件得chart文件： {i_f}', __file__)
            else:
                in_config.set(in_section_name, '45g_test_log', i_f)
                print_with_line_number(f'ue log 文件： {i_f}', __file__)

        set_config_info(in_config, in_data_type, in_test_area, in_section_name)
        config_out_file = os.path.join(i_path, f'{in_section_name}_{in_test_area}_{in_data_type}_config.ini')
        print_with_line_number(f'生成文件 {config_out_file}', __file__)
        print('---' * 50)
        with open(config_out_file, 'w', encoding='UTF-8') as f:
            in_config.write(f)


if __name__ == '__main__':
    folder_path = r'E:\work\MR_Data\1月24号\20240124(1)_new_no_table\20240124\岳云伟\R荣耀90'

    # 获取config
    # res_config = get_config_project()

    # 生成config文件
    set_WeTest_config_file_info(folder_path, 'finger')
    set_WeTest_config_file_info(folder_path, 'uemr')

    # res_config = get_config_project()
    # # 获取所有的csv路径
    # res_list = get_csv_path_list(folder_path)
    # # 获取目录下的所有的csv文件，准备写配置文件
    # for i_path in res_list:
    #     print_with_line_number(f'当前处理路径：{i_path}', __file__)
    #     res_csv_file_list = get_all_csv_file(i_path)
    #
    #     # if len(res_csv_file_list) > 2:
    #     #     print_with_line_number(f'error, 当前路径：{i_path}，不是WeTest数据，csv文件个数为：{len(res_csv_file_list)}', __file__)
    #     #     exit()
    #     for i_f in res_csv_file_list:
    #         if 'char' in i_f:
    #             res_config.set(section_name, 'zcy_chart_file', i_f)
    #             test_area = 'indoor'
    #             print_with_line_number(f'char文件： {i_f}', __file__)
    #         elif 'table' in i_f:
    #             print_with_line_number(
    #                 f'error, 当前文件：{i_f}，是table文件', __file__)
    #         else:
    #             res_config.set(section_name, '45g_test_log', i_f)
    #             print_with_line_number(f'ue log 文件： {i_f}', __file__)
    #
    #     set_config_info(res_config, data_type, test_area)
    #     print('---' * 50)
    #
    #     with open(os.path.join(i_path, f'{section_name}_{data_type}_config.ini'), 'w', encoding='UTF-8') as f:
    #         res_config.write(f)
