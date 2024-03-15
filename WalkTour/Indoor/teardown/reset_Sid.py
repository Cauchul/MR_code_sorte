# -*- coding: utf-8 -*-
import os

from Common import find_output_dir, Common, get_file_list_by_char, print_with_line_number, read_csv_get_df, \
    df_write_to_csv


def reset_sid_value(in_file_list):
    cnt = 0
    for i_path in in_file_list:
        cnt += 1
        print_with_line_number(f'当前处理结果文件: {i_path}', __file__)
        res_df = read_csv_get_df(i_path)
        if 'f_sid' in res_df.columns:
            f_sid = cnt
            res_df['f_sid'] = f_sid
            # res_df['f_sid'] = 1
            print_with_line_number(f'当前 f_sid 设置的sid为: {f_sid}', __file__)
        # else:
        #     u_sid = cnt
        #     res_df['u_sid'] = u_sid
        #     # res_df['u_sid'] = 1
        #     print_with_line_number(f'当前 u_sid 设置的sid为: {u_sid}', __file__)
            df_write_to_csv(res_df, i_path)
            print('---' * 50)


def get_cur_dir_all_csv(in_src_data):
    tmp_csv_files = [os.path.join(in_src_data, file) for file in os.listdir(in_src_data) if
                     file.endswith('.csv') and 'finger' in file]
    return tmp_csv_files


# # 处理当前路径下的文件
# data_path = r'E:\work\MR_Data\1月15号\20240115数据\4G'
# set_f_sid_value('4G')
# set_f_sid_value('5G')
# 获取所有output下的所有文件
def get_output_dir_csv(in_src_data):
    tmp_res_list = []
    in_output_dir_list = find_output_dir(in_src_data)
    for i_dir in in_output_dir_list:
        in_res_list = Common.list_files_in_directory(i_dir)
        tmp_res_list.extend(in_res_list)

    # 只获取finger文件
    tmp_res_list = [x for x in tmp_res_list if 'finger' in x]
    return tmp_res_list


# 设置pid要所有需要排序的文件一起设置
if __name__ == '__main__':
    folder_path = r'E:\work\MrData\data_place\merge\0315\xiaomi_13'
    # 获取当前路径下的所有csv文件
    # res_file_list = get_cur_dir_all_csv(folder_path)
    # 获取output目录下的所有的csv文件
    res_file_list = get_output_dir_csv(folder_path)

    print(res_file_list)

    LTE_file_list = [i_f for i_f in res_file_list if '4G' in os.path.basename(i_f)]
    NR_file_list = [i_f for i_f in res_file_list if '5G' in os.path.basename(i_f)]
    print(NR_file_list)
    reset_sid_value(NR_file_list)

    print(LTE_file_list)
    reset_sid_value(LTE_file_list)

    # print(res_file_list)
    # reset_sid_value(res_file_list)
