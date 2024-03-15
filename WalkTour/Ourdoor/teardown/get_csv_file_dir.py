# -*- coding: utf-8 -*-
import os

import pandas as pd

from Common import read_csv_get_df, df_write_to_csv, file_add_specified_suffix, print_with_line_number


def between_df(in_data_df, in_range_dict, in_sid_v):
    in_data_df.loc[in_data_df['f_x'].between(in_range_dict['x'][0], in_range_dict['x'][1]) & in_data_df['f_y'].between(
        in_range_dict['y'][0], in_range_dict['y'][1]), 'f_sid'] = in_sid_v


def set_abeam_h1(in_data_file, in_sid):
    in_data_df = read_csv_get_df(in_data_file)

    print('in_sid: ', in_sid)
    in_data_df.loc[(in_data_df['f_y'] > 27.6) | (in_data_df['f_x'] < 11.85), 'f_sid'] = in_sid

    in_data_df.loc[in_data_df['f_y'] < 4.255, 'f_sid'] = in_sid + 2

    in_data_df.loc[
        ~((in_data_df['f_y'] < 4.255) | (in_data_df['f_y'] > 27.6) | (in_data_df['f_x'] < 11.82)), 'f_sid'] = in_sid + 1

    in_res_name = file_add_specified_suffix(in_data_file, 'set_sid')
    print_with_line_number(f'输出文件：{in_res_name}', __file__)

    df_write_to_csv(in_data_df, in_res_name)

    check_df(in_res_name, in_sid + 1)


def check_df(in_data_file, in_check_id):
    in_data_df = read_csv_get_df(in_data_file)
    in_filtered_df = in_data_df[in_data_df['f_sid'] == in_check_id]
    in_res_name = file_add_specified_suffix(in_data_file, f'check_id_{in_check_id}')
    print_with_line_number(f'输出文件：{in_res_name}', __file__)
    df_write_to_csv(in_filtered_df, in_res_name)


def check_in_df(in_data_df, in_check_id, in_data_file):
    in_res_check_name = file_add_specified_suffix(in_data_file, f'check_id_{in_check_id}')
    print_with_line_number(f'输出文件：{in_res_check_name}', __file__)
    in_filtered_df = in_data_df[in_data_df['f_sid'] == in_check_id]
    df_write_to_csv(in_filtered_df, in_res_check_name)


# 获取除output目录外的所有csv文件
def get_all_csv_files(directory):
    csv_files = []
    for root, dirs, i_files in os.walk(directory):
        if "output" in dirs:
            dirs.remove("output")  # 排除名为 "output" 的子目录
        for file in i_files:
            if file.endswith(".csv"):
                # csv_files.append(os.path.join(root, file))
                csv_files.append(root)
    return list(set(csv_files))


def get_csv_file_path(in_src_data):
    tmp_list = []
    in_res_list = get_all_csv_files(in_src_data)

    for in_i_dir in in_res_list:
        # 获取目录下的csv文件
        in_files = os.listdir(in_i_dir)
        for in_i_file in in_files:
            if in_i_file.endswith('.csv'):
                if in_i_dir not in tmp_list:
                    tmp_list.append(in_i_dir)

    return tmp_list


if __name__ == '__main__':
    # data_file = r'D:\MrData\2月27号\20240227\5G\WeTest\RENO8\1\output\Beam_5G_HaiDian_outdoor_WeTest_LOG_DT_UE_0227_finger.csv'
    #
    # df = pd.read_csv(data_file, header=0)
    #
    # if check_blank_line_in_df(df, 'f_longitude'):
    #     print('空行异常')
    # else:
    #     print('正常')
    data_file = r'E:\work\MrData\data_place\merge\4G'
    res = get_csv_file_path(data_file)
    for i in res:
        print(i)
