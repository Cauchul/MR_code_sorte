# -*- coding: utf-8 -*-
import os

from Common import FindFile, find_output_dir, Common, clear_merge_path, clear_path, print_with_line_number


def list_dir_files(in_dir):
    tmp_list = []
    for root, dirs, files in os.walk(in_dir):
        if in_dir == root:
            for file in files:
                # file_path = os.path.join(root, file)
                # 处理文件的逻辑
                tmp_list.append(root)
    return tmp_list


def get_output_dir_csv(in_src_data):
    tmp_res_list = []
    in_output_dir_list = find_output_dir(in_src_data)
    for in_i_dir in in_output_dir_list:
        in_res_list = list_dir_files(in_i_dir)
        tmp_res_list.extend(in_res_list)

    return list(set(tmp_res_list))


def clear_file(in_data_path, in_char):
    res_zip_file_list = FindFile.find_files_with_string(in_data_path, in_char)

    res_zip_file_list = [x for x in res_zip_file_list if 'unzip' not in x]

    for i_f in res_zip_file_list:
        print(i_f)
        os.remove(i_f)


def clear_output_dir(in_data_path):
    # 找到所有的output目录
    # res_dir_list = get_output_dir_csv(in_data_path)
    res_dir_list = find_output_dir(in_data_path)

    for i_dir in res_dir_list:
        print(i_dir)
        clear_path(i_dir)


def just_clear(in_file_list):
    for in_i_f in in_file_list:
        print(in_i_f)
        os.remove(in_i_f)


if __name__ == '__main__':
    folder_path = r'E:\work\MrData\data_place\merge\0315\mate40'
    # 删除output目录
    # clear_output_dir(data_path)
    # 获取output目录
    res_file_list = get_output_dir_csv(folder_path)
    for i_dir in res_file_list:
        # print(i_dir)
        # print_with_line_number(f'当前合并文件为：{res_f}', __file__)
        clear_file(i_dir, 'finger_hetero_system_merge')
        print('--' * 50)
