# -*- coding: utf-8 -*-
# 设置pid
import os

from Common import get_data_path_by_char, print_with_line_number, Common, read_csv_get_df, df_write_to_csv
from WalkTour.Ourdoor.teardown.get_csv_file_dir import get_csv_file_path


# def set_pid(in_char='4G'):
#     # 通过标志查找文件
#     print_with_line_number(f'当前查找文件的标志为：{in_char}', __file__)
#     in_file_list = []
#     in_res_list = get_all_data_path(folder_path, in_char)
#     # 获取每个目录下的所有文件
#     for i_path in in_res_list:
#         print_with_line_number(f'当前处理的路径：{i_path}', __file__)
#         res_file_list = Common.list_files_in_directory(i_path)
#         in_file_list.extend(res_file_list)
#         print_with_line_number(f'返回的文件列表：{res_file_list}', __file__)
#         print('---' * 50)
#     # print_with_line_number(f'需要合并的文件列表为：{in_file_list}', __file__)
#     # 每个文件的pid都重新设置，然后重新写会原来文件
#     for i_f in in_file_list:
#         print_with_line_number(f'修改文件的pid：{i_f}', __file__)
#         res_df = read_csv_get_df(i_f)
#         res_df['f_pid'] = (res_df.index + 1).astype(str)
#         df_write_to_csv(res_df, i_f)
#         # print('---' * 50)


# def get_cur_dir_all_csv(in_src_data):
#     tmp_csv_files = [os.path.join(in_src_data, file) for file in os.listdir(in_src_data) if file.endswith('.csv')]
#     return tmp_csv_files


def reset_pid(in_file_list):
    for i_f in in_file_list:
        print_with_line_number(f'修改文件的pid：{i_f}', __file__)
        res_df = read_csv_get_df(i_f)
        if 'f_pid' in res_df:
            res_df['f_pid'] = (res_df.index + 1).astype(str)
        else:
            res_df['u_pid'] = (res_df.index + 1).astype(str)

        df_write_to_csv(res_df, i_f)


# 找到目录下所有的output目录
def find_output_dir(in_path):
    output_directories = []

    # 遍历根目录及其子目录
    for in_res_folder_name, in_res_sub_folder, in_res_file_name in os.walk(in_path):
        # 检查当前目录是否包含 "output"
        if "output" in in_res_sub_folder:
            output_directories.append(os.path.join(in_res_folder_name, "output"))

    return output_directories


def get_output_dir_csv(in_src_data):
    tmp_res_list = []
    in_output_dir_list = find_output_dir(in_src_data)
    for i_dir in in_output_dir_list:
        in_res_list = Common.list_files_in_directory(i_dir)
        tmp_res_list.extend(in_res_list)
    # 只获取finger文件
    # tmp_res_list = [x for x in tmp_res_list if 'finger' in x]
    return tmp_res_list


def get_cur_dir_all_csv(in_src_data):
    tmp_csv_files = [os.path.join(in_src_data, file) for file in os.listdir(in_src_data) if
                     file.endswith('.csv') and 'finger' in file]
    return tmp_csv_files


def main_set_pid(in_folder_path, in_cur_path):
    # 获取当前路径下的所有csv文件
    if in_cur_path:
        in_res_file_list = get_cur_dir_all_csv(in_folder_path)
    # 获取output目录
    else:
        in_res_file_list = get_output_dir_csv(in_folder_path)
    print(in_res_file_list)
    reset_pid(in_res_file_list)


if __name__ == '__main__':
    folder_path = r'E:\work\MrData\data_place\merge\4G\IQOO7'

    # 获取多个路径
    # res_path_list = get_csv_file_path(folder_path)
    # for i_dir in res_path_list:
    #     # main_set_sid(i_dir, in_cur_path=True)  # 处理当前路径下的所有文件
    #     print(i_dir)
    #     main_set_pid(i_dir, in_cur_path=True)

    # 获取一个路径，当前路径
    main_set_pid(folder_path, in_cur_path=True)

    # 获取一个路径，output目录路径
    # main_set_pid(folder_path, in_cur_path=False)
