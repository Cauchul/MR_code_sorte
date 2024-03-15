# -*- coding: utf-8 -*-
import os

from Common import read_csv_get_df, df_write_to_csv, print_with_line_number, Common, find_output_dir


def get_cur_dir_all_csv(in_src_data):
    # tmp_csv_files = [os.path.join(in_src_data, file) for file in os.listdir(in_src_data) if
    #                  file.endswith('.csv') and 'finger' in file]
    tmp_csv_files = [os.path.join(in_src_data, file) for file in os.listdir(in_src_data) if
                     file.endswith('.csv')]
    return tmp_csv_files


def finger_to_UEMR_data(in_data_file):
    print_with_line_number(f'处理文件：{in_data_file}', __file__)

    res_df = read_csv_get_df(in_data_file)

    if 'finger_id' in res_df.columns:
        print_with_line_number('finger 转 UEMR 数据', __file__)

        res_df.rename(columns=lambda x: x.replace('f_', 'u_'), inplace=True)
        res_df = res_df.rename(
            columns={
                'finger_id': 'uemr_id',
            })

        in_out_data_file = in_data_file.replace('finger', 'uemr')

        print_with_line_number(f'输出文件：{in_out_data_file}', __file__)

        df_write_to_csv(res_df, in_out_data_file)


def UEMR_to_finger_data(in_data_file):
    res_df = read_csv_get_df(in_data_file)

    if 'uemr_id' in res_df.columns:
        print_with_line_number('UEMR 转 finger 数据', __file__)

        res_df.rename(columns=lambda x: x.replace('u_', 'f_'), inplace=True)
        res_df = res_df.rename(
            columns={
                'uemr_id': 'finger_id',
            })

        in_out_data_file = in_data_file.replace('uemr', 'finger')

        print_with_line_number(f'输出文件：{in_out_data_file}', __file__)

        df_write_to_csv(res_df, in_out_data_file)


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
    folder_path = r'E:\work\MrData\data_place\out'
    # 获取当前路径下的所有csv文件
    res_file_list = get_cur_dir_all_csv(folder_path)
    # 获取output目录下的finger
    # res_file_list = get_output_dir_csv(folder_path)
    # print(res_file_list)

    for i_f in res_file_list:
        print(i_f)
        finger_to_UEMR_data(i_f)
        # UEMR_to_finger_data(i_f)
        print('---' * 50)
