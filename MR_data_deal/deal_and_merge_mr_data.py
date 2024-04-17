# -*- coding: utf-8 -*-
# 读取多个值，以逗号分隔
import datetime
import inspect
import os.path

import pandas as pd


def read_csv_get_df(in_df_path):
    if 'csv' in in_df_path:
        # print('csv文件')
        in_df = pd.read_csv(in_df_path, low_memory=False)
    else:
        # print('其他文件')
        in_df = pd.read_excel(in_df_path)
    return in_df


def df_write_to_csv(w_df, w_file):
    w_df.to_csv(w_file, index=False, encoding='UTF-8')


def print_with_line_number(message, in_file):
    # 获取当前行号
    current_line = inspect.currentframe().f_back.f_lineno
    # 使用 f-string 格式化字符串，包含文件名和行号信息
    print(f"{os.path.basename(in_file)}:{current_line} - {message}")


def convert_datetime_to_seconds(dtime):
    return [round(int(datetime.datetime.strptime(x, "%y-%m-%d %H:%M:%S.%f").timestamp() * 1e3) / 1e3) for x in dtime]


def data_column_extension(in_df, in_group_flag, in_columns_list):
    # 按照列'A'的值进行分组
    data_list = []
    index_list = []
    for i_c, i_group in in_df.groupby(in_group_flag):
        new_data = {}
        cnt = 0
        # 遍历每一行数据
        for i_idx, i_data in i_group.iterrows():
            if cnt > 0:
                # print('i_data', i_data)
                for i_in_c in in_columns_list:
                    new_c = f'{i_in_c}{cnt}'
                    # in_df.loc[i_group.index[0], new_c] = i_data[i_in_c]
                    new_data[new_c] = i_data[i_in_c]
                in_df.drop(i_idx, inplace=True)
            cnt += 1
            if cnt > 8:
                break
        index_list.append(i_group.index[0])
        data_list.append(new_data)
        # print('--' * 50)
    in_df = pd.merge(in_df, pd.DataFrame(data_list, index=index_list), left_index=True, right_index=True)

    return in_df


def data_line_extension(in_df, in_columns_flag):
    new_df = pd.DataFrame(columns=in_df.columns)

    # 根据某一列进行行拓展
    start = in_df[in_columns_flag].iloc[0]
    cn_index = inter_circ_index = 0

    new_df.loc[cn_index] = in_df.iloc[inter_circ_index]
    new_df.loc[cn_index, in_columns_flag] = start

    while True:
        if start >= in_df[in_columns_flag].iloc[-1]:
            # print('start', start)
            break

        cn_index += 1
        start += 1

        if start in in_df[in_columns_flag].values:
            print_with_line_number(f'{start} 在数据中', __file__)
            print('---' * 50)
            inter_circ_index += 1

        new_df.loc[cn_index] = in_df.iloc[inter_circ_index]
        new_df.loc[cn_index, in_columns_flag] = start

    # print('--' * 50)
    return new_df


def lte_stand_df(in_df):
    in_df = in_df.rename(
        columns={
            'ARFCN': 'f_freq_4g_n1',
            'PCI': 'f_pci_4g_n1',
            'RSRP': 'f_rsrp_4g_n1',
            'RSRQ': 'f_rsrq_4g_n1',
        })

    # 列重命名
    i = 0
    while True:
        i += 1
        if f'ARFCN{i}' in in_df.columns:
            in_df = in_df.rename(
                columns={
                    f'ARFCN{i}': f'f_freq_4g_n{i + 1}',
                    f'PCI{i}': f'f_pci_4g_n{i + 1}',
                    f'RSRP{i}': f'f_rsrp_4g_n{i + 1}',
                    f'RSRQ{i}': f'f_rsrq_4g_n{i + 1}',
                })
        else:
            break

    heterogeneous_system_data = ['f_time', 'f_freq_4g_n1', 'f_pci_4g_n1', 'f_rsrp_4g_n1',
                                 'f_rsrq_4g_n1', 'f_freq_4g_n2', 'f_pci_4g_n2', 'f_rsrp_4g_n2',
                                 'f_rsrq_4g_n2', 'f_freq_4g_n3', 'f_pci_4g_n3', 'f_rsrp_4g_n3',
                                 'f_rsrq_4g_n3', 'f_freq_4g_n4', 'f_pci_4g_n4', 'f_rsrp_4g_n4',
                                 'f_rsrq_4g_n4', 'f_freq_4g_n5', 'f_pci_4g_n5', 'f_rsrp_4g_n5',
                                 'f_rsrq_4g_n5', 'f_freq_4g_n6', 'f_pci_4g_n6', 'f_rsrp_4g_n6',
                                 'f_rsrq_4g_n6', 'f_freq_4g_n7', 'f_pci_4g_n7', 'f_rsrp_4g_n7',
                                 'f_rsrq_4g_n7', 'f_freq_4g_n8', 'f_pci_4g_n8', 'f_rsrp_4g_n8',
                                 'f_rsrq_4g_n8']

    in_df = in_df.reindex(columns=heterogeneous_system_data)

    return in_df


def lte_extension(in_data_file):
    print_with_line_number(f'开始处理4G数据', __file__)
    # 数据读取和清理
    df = pd.read_csv(in_data_file, low_memory=False, usecols=['UE Time', 'ARFCN', 'PCI.1', 'RSRP.1', 'RSRQ.1'],
                     encoding='utf-8')

    df.rename(columns={'PCI.1': 'PCI', 'RSRP.1': 'RSRP', 'RSRQ.1': 'RSRQ', }, inplace=True)
    df = df.dropna(subset=['ARFCN', 'PCI', 'RSRP', 'RSRQ'], how='any').reset_index(drop=True)

    df['f_time'] = convert_datetime_to_seconds(df['UE Time'])
    columns_list = ['ARFCN', 'PCI', 'RSRP', 'RSRQ']
    # 列拓展
    df = data_column_extension(df, 'f_time', columns_list)

    # 列拓展
    df = data_line_extension(df, 'f_time')

    # 标准化输出
    df = lte_stand_df(df)
    # 删除列
    # df = df.drop(columns='UE Time')

    return df


def res_csv_file_get_df(in_data_file):
    try:
        in_df = pd.read_csv(in_data_file, low_memory=False, encoding='utf-8')
    except UnicodeDecodeError:
        in_df = pd.read_csv(in_data_file, low_memory=False, encoding='gbk')

    # 将第一行数据作为新的列名，并重新设置索引
    if 'NARFCN' not in in_df.columns and 'EARFCN' not in in_df.columns:
        in_df.columns = in_df.iloc[0]
        in_df = in_df[1:].reset_index(drop=True)

    df_write_to_csv(in_df, in_data_file)


def nr_stand_df(in_df):
    in_df = in_df.rename(
        columns={
            'NARFCN': 'f_freq_5g_n1',
            'PCI': 'f_pci_5g_n1',
            'RSRP': 'f_rsrp_5g_n1',
            'RSRQ': 'f_rsrq_5g_n1',
            'SINR': 'f_sinr_5g_n1',
        })

    # 列重命名
    i = 0
    while True:
        i += 1
        if f'NARFCN{i}' in in_df.columns:
            in_df = in_df.rename(
                columns={
                    f'NARFCN{i}': f'f_freq_5g_n{i + 1}',
                    f'PCI{i}': f'f_pci_5g_n{i + 1}',
                    f'RSRP{i}': f'f_rsrp_5g_n{i + 1}',
                    f'RSRQ{i}': f'f_rsrq_5g_n{i + 1}',
                    f'SINR{i}': f'f_sinr_5g_n{i + 1}',
                })
        else:
            break

    heterogeneous_system_data = ['f_time', 'f_freq_5g_n1', 'f_pci_5g_n1', 'f_rsrp_5g_n1', 'f_sinr_5g_n1',
                                 'f_rsrq_5g_n1', 'f_freq_5g_n2', 'f_pci_5g_n2', 'f_rsrp_5g_n2',
                                 'f_sinr_5g_n2', 'f_rsrq_5g_n2', 'f_freq_5g_n3', 'f_pci_5g_n3',
                                 'f_rsrp_5g_n3', 'f_sinr_5g_n3', 'f_rsrq_5g_n3', 'f_freq_5g_n4',
                                 'f_pci_5g_n4', 'f_rsrp_5g_n4', 'f_sinr_5g_n4', 'f_rsrq_5g_n4',
                                 'f_freq_5g_n5', 'f_pci_5g_n5', 'f_rsrp_5g_n5', 'f_sinr_5g_n5',
                                 'f_rsrq_5g_n5', 'f_freq_5g_n6', 'f_pci_5g_n6', 'f_rsrp_5g_n6',
                                 'f_sinr_5g_n6', 'f_rsrq_5g_n6', 'f_freq_5g_n7', 'f_pci_5g_n7',
                                 'f_rsrp_5g_n7', 'f_sinr_5g_n7', 'f_rsrq_5g_n7', 'f_freq_5g_n8',
                                 'f_pci_5g_n8', 'f_rsrp_5g_n8', 'f_sinr_5g_n8', 'f_rsrq_5g_n8']

    in_df = in_df.reindex(columns=heterogeneous_system_data)

    return in_df


def nr_extension(in_data_file):
    print_with_line_number(f'开始处理5G数据', __file__)
    # 数据读取和清理
    df = pd.read_csv(in_data_file, low_memory=False, usecols=['UE Time', 'NARFCN', 'PCI', 'RSRP', 'RSRQ', 'SINR'],
                     encoding='utf-8')

    df['f_time'] = convert_datetime_to_seconds(df['UE Time'])
    columns_list = ['NARFCN', 'PCI', 'RSRP', 'RSRQ', 'SINR']
    # 列拓展
    print_with_line_number(f'数据列拓展中......', __file__)
    df = data_column_extension(df, 'f_time', columns_list)

    # 行拓展
    print_with_line_number(f'数据行拓展', __file__)
    df = data_line_extension(df, 'f_time')

    # 标准化输出
    df = nr_stand_df(df)

    # 删除列
    # df = df.drop(columns='UE Time')
    return df


def only_lte_stand_df(in_df):
    in_df = in_df.rename(
        columns={
            'EARFCN': 'f_freq_4g_n1',
            'PCI': 'f_pci_4g_n1',
            'RSRP': 'f_rsrp_4g_n1',
            'RSRQ': 'f_rsrq_4g_n1',
        })

    # 列重命名
    i = 0
    while True:
        i += 1
        if f'EARFCN{i}' in in_df.columns:
            in_df = in_df.rename(
                columns={
                    f'EARFCN{i}': f'f_freq_4g_n{i + 1}',
                    f'PCI{i}': f'f_pci_4g_n{i + 1}',
                    f'RSRP{i}': f'f_rsrp_4g_n{i + 1}',
                    f'RSRQ{i}': f'f_rsrq_4g_n{i + 1}',
                })
        else:
            break

    heterogeneous_system_data = ['f_time', 'f_freq_4g_n1', 'f_pci_4g_n1', 'f_rsrp_4g_n1',
                                 'f_rsrq_4g_n1', 'f_freq_4g_n2', 'f_pci_4g_n2', 'f_rsrp_4g_n2',
                                 'f_rsrq_4g_n2', 'f_freq_4g_n3', 'f_pci_4g_n3',
                                 'f_rsrp_4g_n3', 'f_rsrq_4g_n3', 'f_freq_4g_n4',
                                 'f_pci_4g_n4', 'f_rsrp_4g_n4', 'f_rsrq_4g_n4',
                                 'f_freq_4g_n5', 'f_pci_4g_n5', 'f_rsrp_4g_n5',
                                 'f_rsrq_4g_n5', 'f_freq_4g_n6', 'f_pci_4g_n6', 'f_rsrp_4g_n6',
                                 'f_rsrq_4g_n6', 'f_freq_4g_n7', 'f_pci_4g_n7',
                                 'f_rsrp_4g_n7', 'f_rsrq_4g_n7', 'f_freq_4g_n8',
                                 'f_pci_4g_n8', 'f_rsrp_4g_n8', 'f_rsrq_4g_n8']

    in_df = in_df.reindex(columns=heterogeneous_system_data)

    return in_df


def only_lte_extension(in_data_file):
    print_with_line_number(f'只处理lte数据', __file__)
    in_df = pd.read_csv(in_data_file, low_memory=False, usecols=['UE Time', 'EARFCN', 'PCI', 'RSRP', 'RSRQ'])
    in_df = in_df.dropna(subset=['EARFCN', 'PCI', 'RSRP', 'RSRQ'], how='any').reset_index(drop=True)

    in_df['f_time'] = convert_datetime_to_seconds(in_df['UE Time'])
    columns_list = ['EARFCN', 'PCI', 'RSRP', 'RSRQ']
    # 列拓展
    in_df = data_column_extension(in_df, 'f_time', columns_list)

    # 行拓展
    in_df = data_line_extension(in_df, 'f_time')

    # 标准化输出
    in_df = only_lte_stand_df(in_df)

    # 删除列
    # in_df = in_df.drop(columns='UE Time')

    return in_df


def deal_mr_data(in_data_file):
    if '"' in in_data_file:
        # 去除字符串两端的引号
        in_data_file = in_data_file.strip('"')

    # 删除csv的第一行不需要的数据
    res_csv_file_get_df(in_data_file)

    in_res_df = pd.read_csv(in_data_file, low_memory=False, encoding='utf-8')

    if 'NARFCN' in in_res_df.columns:
        in_lte_res_df = lte_extension(in_data_file)
        in_nr_res_df = nr_extension(in_data_file)
        in_tmp_merger_df = pd.merge(in_lte_res_df, in_nr_res_df, left_on="f_time",
                                    right_on="f_time")
    else:
        in_tmp_merger_df = only_lte_extension(in_data_file)

    return in_tmp_merger_df


if __name__ == '__main__':
    out_flag = False

    while True:
        file_list = []
        cnt = 0
        while True:
            file = input("请输入需要合并的csv文件名然后回车（使用绝对路径）,输入out退出整个程序：")

            if '"' in file:
                # 去除字符串两端的引号
                file = file.strip('"')

            if 'out' == file.lower():
                out_flag = True
                break

            if os.path.exists(file):
                cnt += 1

                file_list.append(file)
                if cnt >= 2:
                    break
            else:
                print('**' * 50)
                print('输入的csv文件不存在请重新输入，输入out退出整个程序')

        if out_flag:
            break

        df_list = []

        finger_file = ''
        mr_file = ''
        print(f"输入的文件名为：")
        for i_f in file_list:
            print(i_f)  # 使用strip()方法去除值两端的空白字符
            if 'finger' in i_f or 'uemr' in i_f:
                finger_file = i_f
                tmp_merger_df = read_csv_get_df(i_f)
            elif i_f:
                mr_file = i_f
        print(f'指纹数据为：{finger_file}')
        print(f'MR数据为：{mr_file}')

        # 处理mr数据
        mr_df = deal_mr_data(mr_file)

        finger_df = read_csv_get_df(finger_file)

        if 'u_sinr' in finger_df.columns or 'u_longitude' in finger_df.columns:
            mr_df.rename(columns=lambda x: x.replace('f_', 'u_'), inplace=True)

            print_with_line_number("开始merge数据", __file__)
            tmp_merger_df = pd.merge(finger_df, mr_df,
                                     left_on="u_time", right_on="u_time", how='left')
        else:
            print_with_line_number("开始merge数据", __file__)
            tmp_merger_df = pd.merge(finger_df, mr_df,
                                     left_on="f_time", right_on="f_time", how='left')

        # out_dir = os.path.join(os.path.dirname(finger_file), 'output')
        # out_file = out_dir + os.path.basename(finger_file).replace('.csv', '_mr_data_merge.csv')
        out_file = finger_file.replace('.csv', '_mr_data_merge.csv')
        try:
            df_write_to_csv(tmp_merger_df, out_file)
        except PermissionError as e:
            print_with_line_number(f'写文件报错：{e}', __file__)
        print(f'输出文件为：{out_file}')
        print('---' * 50)
