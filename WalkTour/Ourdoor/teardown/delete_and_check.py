# -*- coding: utf-8 -*-
import os.path

import pandas as pd

from Common import print_with_line_number, find_output_dir, Common


def check_column(in_f, in_df, in_column_list):
    print_with_line_number('in check_column', __file__)
    res_flag = False
    for i_col in in_column_list:
        # 检测文件
        if i_col in in_df.columns:
            threshold = len(in_df) / 3
            empty_rows_count = in_df[(in_df[i_col].isnull())].shape[0]
            if empty_rows_count > threshold:
                print_with_line_number(f'{i_col} 列，空行数： {empty_rows_count} 总行数： {len(in_df)}', __file__)
                error_records.append({"报错文件": in_f, "错误原因": f'{i_col} 列，空行数： {empty_rows_count} 总行数： {len(in_df)}'})
                res_flag = True
        #
        # if check_blank_line_in_df(in_df, i_col):
        #     print_with_line_number(f'文件 {in_f} 中的 {i_col} 列，空行数异常', __file__)
    return res_flag


def delete_empty_value_column(in_csv_file_list):
    # 读取CSV文件，指定第一行为标题行
    for i_f in in_csv_file_list:
        print_with_line_number(f'当前清理空行文件：{os.path.join(src_data, i_f)}', __file__)
        df = pd.read_csv(os.path.join(src_data, i_f), header=0)

        # 删除第二列（下标为1）中为空的行，保留其他所有列
        if any('f_sinr' in col for col in df.keys()):
            print_with_line_number(f'5G finger 数据', __file__)

            drop_list = ['f_longitude', 'f_latitude', 'f_pci', 'f_freq', 'f_rsrp', 'f_rsrq', 'f_sinr']

            check_column(i_f, df, drop_list)

            df = df.dropna(subset=drop_list, how='any')
        elif any('u_sinr' in col for col in df.keys()):
            print_with_line_number(f'5G UEMR 数据', __file__)
            drop_list = ['u_longitude', 'u_latitude', 'u_pci', 'u_freq', 'u_rsrp', 'u_rsrq', 'u_sinr']
            check_column(i_f, df, drop_list)

            df = df.dropna(subset=drop_list, how='any')
        elif 'f_longitude' in df.columns and 'f_sinr' not in df.columns:
            print_with_line_number(f'4G finger 数据', __file__)

            drop_list = ['f_longitude', 'f_latitude', 'f_pci', 'f_freq', 'f_rsrp', 'f_rsrq']
            check_column(i_f, df, drop_list)

            df = df.dropna(subset=drop_list, how='any')
        elif 'u_longitude' in df.columns and 'u_sinr' not in df.columns:
            print_with_line_number(f'4G UEMR 数据', __file__)

            drop_list = ['u_longitude', 'u_latitude', 'u_pci', 'u_freq', 'u_rsrp', 'u_rsrq']
            check_column(i_f, df, drop_list)

            df = df.dropna(subset=drop_list, how='any')
        print('---' * 50)

        if '_clear_empty' not in i_f:
            i_f = i_f.replace(".csv", "_clear_empty.csv")
        # 将结果写入到新的CSV文件中
        df.to_csv(os.path.join(src_data, i_f), index=False)


def get_cur_dir_all_csv(in_src_data):
    tmp_csv_files = [file for file in os.listdir(in_src_data) if file.endswith('.csv')]
    return tmp_csv_files


def get_output_dir_csv(in_src_data):
    tmp_res_list = []
    in_output_dir_list = find_output_dir(in_src_data)

    for i_dir in in_output_dir_list:
        in_res_list = Common.list_files_in_directory(i_dir)
        tmp_res_list.extend(in_res_list)
    return tmp_res_list


if __name__ == '__main__':
    error_records = []  # 用于存储报错的文件名和报错信息

    src_data = r'D:\MrData\2月27号\20240227'
    # 获取当前目录下的所有的csv文件
    # res_file_list = get_cur_dir_all_csv(src_data)
    # 获取output目录下的所有的csv文件
    res_file_list = get_output_dir_csv(src_data)

    res_file_list = [i_f for i_f in res_file_list if '_clear_empty' not in i_f]

    # for i in res_file_list:
    #     print(i)

    delete_empty_value_column(res_file_list)

    error_df = pd.DataFrame(error_records)
    # 指定要保存到的 CSV 文件路径
    output_csv_file = r"D:\delete_empty_warning_files.csv"
    # 将 DataFrame 写入 CSV 文件，不包含行索引
    error_df.to_csv(output_csv_file, index=False)
