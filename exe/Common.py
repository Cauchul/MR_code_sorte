# -*- coding: utf-8 -*-
import configparser
import fnmatch
import glob
import inspect
import math
import os
import re
import shutil
import time
import zipfile
from datetime import datetime

import numpy as np
import pandas as pd
import pytz
from matplotlib import pyplot as plt


# from DataPreprocessing import DataPreprocessing
# from GlobalConfig import tmp_res_out_path, f_msisdn_dict

class FindFile:
    @staticmethod
    def find_files_with_string(in_path, in_string):
        matches = []
        for path, dirs, files in os.walk(in_path):
            for i_file in files:
                if fnmatch.fnmatch(i_file, f'*{in_string}*'):
                    matches.append(os.path.join(path, i_file))
        return matches

    # 获取当前目录下的所有csv文件
    @staticmethod
    def get_cur_dir_all_csv(in_src_data):
        tmp_csv_files = [os.path.join(in_src_data, file) for file in os.listdir(in_src_data) if file.endswith('.csv')]
        return tmp_csv_files

    # 获取当前路径下，所有包含csv文件的路径，返回list
    @staticmethod
    def get_csv_file_dir_list(in_folder_path):
        in_res_list = get_data_path_by_char(in_folder_path, '.csv')
        # list 去重
        in_res_list = list(set(in_res_list))
        return in_res_list


class Common:
    @staticmethod
    def split_path_get_list(in_path):
        tmp_path_parts = []
        while True:
            in_path, folder = os.path.split(in_path)
            if folder:
                tmp_path_parts.insert(0, folder)
            else:
                break
        return tmp_path_parts

    # 遍历当前目录下的所有文件，排除目录
    @staticmethod
    def list_files_in_directory(in_dir):
        tmp_list = []
        for root, dirs, files in os.walk(in_dir):
            if in_dir == root:
                for file in files:
                    file_path = os.path.join(root, file)
                    # 处理文件的逻辑
                    tmp_list.append(file_path)
        return tmp_list


def unzip(in_zip_file, in_out_path):
    with zipfile.ZipFile(in_zip_file, 'r') as zip_ref:
        zip_ref.extractall(in_out_path)


def print_with_line_number(message, in_file):
    # 获取当前行号
    current_line = inspect.currentframe().f_back.f_lineno
    # 使用 f-string 格式化字符串，包含文件名和行号信息
    print(f"{os.path.basename(in_file)}:{current_line} - {message}")


def print_error(message):
    current_line = inspect.currentframe().f_back.f_lineno
    # 使用 f-string 格式化字符串，包含文件名和行号信息
    input(f"{os.path.basename(__file__)}:{current_line} - {message}")


# 判断文件list中是否包含某个字符的字符串
def check_char_in_file_list(in_file_list, in_char):
    if any(in_char in s for s in in_file_list):
        return True
    else:
        return False


# 获取含有指定的字符的csv文件list
def get_file_list_by_char(in_path, in_char):
    tmp_csv_files = [os.path.join(in_path, file) for file in os.listdir(in_path) if
                     file.endswith('.csv') and in_char in file]
    return tmp_csv_files


# 删除目录
def clear_path(in_path):
    if os.path.exists(in_path):
        try:
            # os.rmdir(in_path)
            shutil.rmtree(in_path)
            print(f"目录：{in_path}，成功删除")
        except OSError as e:
            print(f"目录：{in_path}，删除失败: {e}")


# 获取包含指定字符串的文件
def get_file_by_string(in_str, in_dir):
    file_list = os.listdir(in_dir)
    for file in file_list:
        if in_str.lower() in file.lower():
            in_file_path = os.path.join(in_dir, file)  # 获取包含指定字符串的文件的完整路径
            return in_file_path
    return


# 获取当前目录下所有的csv文件
def get_all_csv_file(in_path):
    # 使用 glob 获取文件列表
    tmp_file_list = glob.glob(os.path.join(in_path, "*.csv"))
    return tmp_file_list


# 获取整个目录下zip文件存在的所有的子目录路径，也即数据路径
def get_data_path_by_char(in_dir, in_char='zip'):
    tmp_data_path_list = []
    for root, dirs, files in os.walk(in_dir):
        # if root != in_dir:
        for file in files:
            if in_char in file:
                tmp_data_path_list.append(root)
                file_path = os.path.join(root, file)
                # print('file_path: ', file_path)

    return tmp_data_path_list


# 生成输出文件名称
def generate_output_file_name(in_data_path, in_df, in_net_type, in_n_scene, in_test_type):
    tmp_cur_out_path = os.path.join(in_data_path, 'output')
    check_path(tmp_cur_out_path)

    tmp_split_list = split_path_get_list(in_data_path)

    name_d_time = in_df['pc_time'][0].split(' ')[0]
    if '-' in name_d_time:
        name_d_time = name_d_time[name_d_time.find('-'):].replace('-', '')
    else:
        name_d_time = name_d_time[name_d_time.find('/'):].replace('/', '')
    print('name_d_time: ', name_d_time)

    in_msisdn = in_df['f_msisdn'][0]
    n_dev_id = get_dict_key_by_value(f_msisdn_dict, in_msisdn)
    district = in_df['f_district'][0]
    if '海淀' in district:
        n_are = 'HaiDian'
    elif '朝阳' in district:
        n_are = 'CaoYang'
    else:
        n_are = 'DaXin'
    print(district)
    print('type: ', type(district))
    if in_n_scene:
        tmp_out_file_name = f'{in_net_type}_{n_are}_{in_n_scene}_{in_test_type}_LOG_DT_UE_{n_dev_id}_{name_d_time}_{tmp_split_list[-1]}'
        # tmp_out_file_name = f'{in_net_type}_{n_are}_{in_n_scene}_{in_test_type}_LOG_DT_UE_{n_dev_id}_{name_d_time}'
    else:
        tmp_out_file_name = f'{in_net_type}_{n_are}_{in_test_type}_LOG_DT_UE_{n_dev_id}_{name_d_time}_{tmp_split_list[-1]}'
        # tmp_out_file_name = f'{in_net_type}_{n_are}_{in_test_type}_LOG_DT_UE_{n_dev_id}_{name_d_time}'

    tmp_out_file = os.path.join(tmp_res_out_path, tmp_out_file_name)
    print('tmp_out_file: ', tmp_out_file)
    tmp_cur_p_out_file = os.path.join(tmp_cur_out_path, tmp_out_file_name + '.csv')
    return tmp_out_file, tmp_cur_p_out_file


def delete_last_character(in_str, in_chart='_'):
    tmp_res_str = in_str.rsplit(in_chart, 1)[0]
    return tmp_res_str


# 分隔获取最后一个字符
def get_last_character(in_str, in_chart='_'):
    return in_str.rsplit(in_chart, 1)[1]


# 通过字典值查询key
def get_dict_key_by_value(in_dict, in_value):
    for tmp_key, tmp_value in in_dict.items():
        if tmp_value == in_value:
            # print(key)
            return tmp_key
    return


# 生成标准文件
def standard_output_name(in_path, in_net_type, in_name_ue, in_name_d_time):
    tmp_cur_out_path = os.path.join(in_path, 'output')
    check_path(tmp_cur_out_path)

    p_list = split_path_get_list(in_path)
    print('p_list: ', p_list)

    n_scenario = 'Indoor'
    n_area = 'DaXin'
    n_test_type = 'DT'

    file_name = f'{in_net_type}_{n_area}_{n_scenario}_WT_{n_test_type}_{in_name_ue}_{in_name_d_time}_{p_list[-3]}_{p_list[-4]}_{p_list[-2]}_LOG_UE_{p_list[-1]}'
    tmp_out_file = os.path.join(tmp_res_out_path, file_name + '.csv')
    tmp_cur_p_out_file = os.path.join(tmp_cur_out_path, file_name + '.csv')
    print('tmp_out_file: ', tmp_out_file)
    return tmp_out_file, tmp_cur_p_out_file


# 获取zcy数据
def get_zcy_data(in_zcy_file):
    in_zcy_df = read_csv_get_df(in_zcy_file)
    tmp_zcy_df = in_zcy_df[
        ['test_time', 'created_by_ue_time', 'f_x', 'f_y', 'f_longitude', 'f_latitude', 'altitude']]
    if 'direction' in in_zcy_df.columns:
        tmp_zcy_df = in_zcy_df[
            ['test_time', 'created_by_ue_time', 'f_x', 'f_y', 'f_longitude', 'f_latitude', 'direction', 'altitude']]
    return tmp_zcy_df


# 获取wifi数据
def get_wifi_bluetooth_data(in_wifi_bluetooth_file):
    in_wifi_df = read_csv_get_df(in_wifi_bluetooth_file)
    tmp_wifi_df = in_wifi_df.drop(['f_x', 'f_y', 'f_longitude', 'f_latitude', 'f_direction', 'f_altitude'], axis=1)
    return tmp_wifi_df


# zcy合并wifi、蓝牙数据
def get_zcy_merge_wifi_bluetooth_data(in_zcy_file, in_wifi_bluetooth_file):
    # 合并走测仪和wifi数据
    tmp_zcy_df = get_zcy_data(in_zcy_file)
    tmp_wifi_bluetooth_df = get_wifi_bluetooth_data(in_wifi_bluetooth_file)
    # 获取zcy，wifi数据
    tmp_merger_df = pd.merge(tmp_wifi_bluetooth_df, tmp_zcy_df, left_on="f_time", right_on="created_by_ue_time",
                             how='left')
    return tmp_merger_df


# 根据特征获取文件字典
def get_file_dict(in_path, in_file_list):
    tmp_file_dict = {}
    for i_char in in_file_list:
        in_res_file = get_file_by_str(i_char, in_path)
        tmp_file_dict[i_char] = in_res_file
    return tmp_file_dict


# 获取包含指定字符串的文件，区分大小写
def get_file_by_str(in_str, in_dir):
    file_list = os.listdir(in_dir)
    for file in file_list:
        if in_str in file:
            in_file_path = os.path.join(in_dir, file)  # 获取包含指定字符串的文件的完整路径
            return in_file_path
    return


def copy_file(in_src_f, in_des_path):
    shutil.copy2(in_src_f, in_des_path)


def move_file(in_src_f, in_targ_f):
    shutil.move(in_src_f, in_targ_f)


def check_file_exists(in_file):
    return os.path.exists(in_file)


# # 获取包含指定字符串的文件
# def get_file_by_string(in_str, in_dir):
#     file_list = os.listdir(in_dir)
#     for file in file_list:
#         if in_str.lower() in file.lower():
#             in_file_path = os.path.join(in_dir, file)  # 获取包含指定字符串的文件的完整路径
#             return in_file_path
#     return


def read_csv_get_df(in_df_path):
    if 'csv' in in_df_path:
        # print('csv文件')
        in_df = pd.read_csv(in_df_path, low_memory=False)
    else:
        # print('其他文件')
        in_df = pd.read_excel(in_df_path)
    return in_df


# 走测仪数据转经纬度
def data_conversion(in_value, df_data):
    delta_d = df_data.max() - df_data.min()  # 最大最小的差值
    t_x = in_value / delta_d
    n_values = df_data * t_x
    res_dat = n_values - n_values.min()  # 去除负值
    return res_dat


def demo_data_conversion(in_value, df_data):
    # delta_d = df_data.max() - df_data.min()  # 最大最小的差值
    res_dat = in_value - df_data.min() / df_data.max() - df_data.min()
    # t_x = in_value / delta_d
    # n_values = df_data * t_x
    # res_dat = n_values - n_values.min()
    return res_dat


# 获取某个目录下，多个子目录中的中的某个目录的绝对路径
def find_directory(in_directory, in_target_name):
    tmp_res_list = []
    for root, dirs, files in os.walk(in_directory):
        if in_target_name in dirs:
            tmp_res_list.append(os.path.abspath(os.path.join(root, in_target_name)))
    return tmp_res_list


def df_write_to_csv(w_df, w_file):
    w_df.to_csv(w_file, index=False, encoding='UTF-8')


def df_write_to_xlsx(w_df, w_file):
    w_df.to_excel(w_file, index=False)


def clear_merge_path(in_path):
    if os.path.exists(in_path):
        shutil.rmtree(in_path)
        time.sleep(1)


# 获取字符串中，第n次出现某个字符或者字符串的index
def find_nth_occurrence(text, target, n):
    start = text.find(target)
    while start >= 0 and n > 1:
        start = text.find(target, start + len(target))
        n -= 1
    return start


# 切分字符串数字和字母
def get_split_str(in_str):
    res_list = re.findall(r'[a-zA-Z]+|\d+', in_str)
    return res_list


# 给文件名添加后缀
def file_add_specified_suffix(in_file, *in_suffix):
    in_res_file_name, in_res_file_extension = os.path.splitext(in_file)
    # print('in_res_file_name: ', in_res_file_name)
    if f'_{in_suffix[0]}' not in in_res_file_name:
        suffix_str = "_".join(in_suffix)
        print_with_line_number(f'文件添加后缀为: {suffix_str}', __file__)
        in_tmp_file_name = in_res_file_name + f'_{suffix_str}' + in_res_file_extension
    else:
        in_tmp_file_name = in_res_file_name + in_res_file_extension
    return in_tmp_file_name


# 找到目录下所有的output目录
def find_output_dir(in_path):
    output_directories = []

    # 遍历根目录及其子目录
    for in_res_folder_name, in_res_sub_folder, in_res_file_name in os.walk(in_path):
        # 检查当前目录是否包含 "output"
        if "output" in in_res_sub_folder:
            output_directories.append(os.path.join(in_res_folder_name, "output"))

    return output_directories


# 获取当前目录下的所有子目录
def get_path_sub_dir(directory):
    res_sub_dir = [d for d in os.listdir(directory) if os.path.isdir(os.path.join(directory, d))]
    return res_sub_dir


# 获取当前目录下的所有子目录的绝对路径
def get_path_sub_absolute_dir(directory):
    res_sub_dir = [os.path.join(directory, d) for d in os.listdir(directory) if os.path.isdir(os.path.join(directory, d))]
    return res_sub_dir


# # 遍历当前目录下的所有文件，排除目录
# def list_files_in_directory(in_dir):
#     tmp_list = []
#     for root, dirs, files in os.walk(in_dir):
#         if in_dir == root:
#             for file in files:
#                 file_path = os.path.join(root, file)
#                 # 处理文件的逻辑
#                 tmp_list.append(file_path)
#     return tmp_list


# 获取所有列名
def get_csv_total_columns(csv_file):
    in_df = pd.read_csv(csv_file)
    # 获取所有列名
    tmp_res_columns = in_df.columns
    return tmp_res_columns


# 合并csv文件
def merge_csv_file(*args):
    tmp_data = pd.concat([pd.read_csv(file).assign(FileName=os.path.basename(file)) for file in args])
    return tmp_data


def generate_images(in_x1_v, in_y1_v, in_lon, in_lat, in_out_path, in_image_name):
    plt.clf()

    plt.rcParams['axes.unicode_minus'] = False
    plt.rcParams['font.sans-serif'] = 'SimHei'
    # Mac系统字体
    # plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']
    plt.subplot(2, 1, 1)
    plt.plot(in_x1_v, in_y1_v)
    plt.gca().set_aspect('equal', adjustable='box')
    plt.xlabel('X')
    plt.ylabel('Y')
    plt.title('xy_相对位置轨迹图')
    png_file = os.path.join(in_out_path, f'xy_相对位置轨迹图{in_image_name}.png')
    plt.savefig(png_file)

    # 清除图形
    plt.clf()

    # plt.savefig(png_file)
    plt.subplot(2, 1, 2)
    plt.plot(in_lon, in_lat)
    plt.gca().set_aspect('equal', adjustable='box')
    plt.xlabel('lon')
    plt.ylabel('lat')
    plt.title('lonlat_经纬度轨迹图')
    plt.tight_layout()
    # plt.show()
    png_file = os.path.join(in_out_path, f'lonlat_经纬度轨迹图{in_image_name}.png')
    plt.savefig(png_file)


# def split_path_get_list(in_path):
#     tmp_path_parts = []
#     while True:
#         in_path, folder = os.path.split(in_path)
#         if folder:
#             tmp_path_parts.insert(0, folder)
#         else:
#             break
#     return tmp_path_parts


# 检查路径，如果不存在则创建
def check_path(in_path):
    if not os.path.exists(in_path):
        # os.mkdir(in_path)
        os.makedirs(in_path, exist_ok=True)


class DealDf:
    def __int__(self):
        pass

    def delete_duplicate_columns(self, log_df):
        """
        删除重复列
        """
        log_df = log_df.loc[:, ~log_df.columns.duplicated()]
        return log_df

    def delete_second_level_duplicate_data(self, df_dta, g_df):
        """
        # 删除测试log中 秒级重复数据，同秒取第一条。
        """
        df_dta = df_dta.groupby(g_df).first().reset_index()  # 删除测试log中 秒级重复数据，同秒取第一条。
        return df_dta

    def change_to_Shanghai_time_zone(self, deal_df):
        res_df = pd.to_datetime(deal_df, unit='s').dt.tz_localize(pytz.utc).dt.tz_convert(
            pytz.timezone('Asia/Shanghai'))
        return res_df

    def generate_finger_id(self, time_df, msi_df):
        res_id = 'F' + time_df.dt.strftime('%Y%m%d%H') + '_' + msi_df.str[-4:]
        return res_id

    # 获取领区数
    def get_cell_number(self, log_df):
        tmp_num_list = []
        for i in range(len(log_df)):
            tmp_num_list.append(np.count_nonzero(
                log_df[['f_rsrp_n1', 'f_rsrp_n2', 'f_rsrp_n3', 'f_rsrp_n4', 'f_rsrp_n5',
                        'f_rsrp_n6', 'f_rsrp_n7', 'f_rsrp_n8']].isnull().values[i] == False))
        return tmp_num_list

    def add_and_empty_UEMR_data(self, log_df):
        log_df['f_roaming_type'] = ''
        log_df['f_phr'] = ''
        log_df['f_enb_received_power'] = ''
        log_df['f_ta'] = ''
        log_df['f_aoa'] = ''
        return log_df

    def calculate_directions(self, df):
        """
        计算当前位置的方向
        :return: 返回当前数据相对于上一条数据的方向。
        """

        def get_direction(curr_x, curr_y, prev_x, prev_y):
            delta_x = curr_x - prev_x
            delta_y = curr_y - prev_y

            angle = math.atan2(delta_y, delta_x)
            angle_deg = math.degrees(angle)

            if -45 <= angle_deg <= 45:
                return 'N'
            elif 45 < angle_deg <= 135:
                return 'E'
            elif -135 <= angle_deg < -45:
                return 'S'
            else:
                return 'W'

        prev_x = None
        prev_y = None

        directions = []
        for index, row in df.iterrows():
            curr_x = float(row['f_longitude'])
            curr_y = float(row['f_latitude'])

            if prev_x is not None and prev_y is not None:
                direction = get_direction(curr_x, curr_y, prev_x, prev_y)
                directions.append(direction)
            else:
                directions.append('未发生位移')

            prev_x = curr_x
            prev_y = curr_y

        df['f_direction'] = directions
        return df


# 数据读取对象
deal_df_object = DealDf()

Data_4G = ['finger_id', 'f_province', 'f_city', 'f_district', 'f_street',
           'f_building', 'f_floor', 'f_area', 'f_prru_id', 'f_scenario',
           'f_roaming_type', 'f_imsi', 'f_imei', 'f_msisdn', 'f_cell_id',
           'f_enb_id', 'f_time', 'f_longitude', 'f_latitude', 'f_altitude',
           'f_phr', 'f_enb_received_power', 'f_ta', 'f_aoa', 'f_pci',
           'f_freq', 'f_rsrp', 'f_rsrq', 'f_neighbor_cell_number', 'f_freq_n1',
           'f_pci_n1', 'f_freq_n2', 'f_pci_n2', 'f_freq_n3', 'f_pci_n3',
           'f_freq_n4', 'f_pci_n4', 'f_freq_n5', 'f_pci_n5', 'f_freq_n6',
           'f_pci_n6', 'f_freq_n7', 'f_pci_n7', 'f_freq_n8', 'f_pci_n8',
           'f_rsrp_n1', 'f_rsrq_n1', 'f_rsrp_n2', 'f_rsrq_n2', 'f_rsrp_n3',
           'f_rsrq_n3', 'f_rsrp_n4', 'f_rsrq_n4', 'f_rsrp_n5', 'f_rsrq_n5',
           'f_rsrp_n6', 'f_rsrq_n6', 'f_rsrp_n7', 'f_rsrq_n7', 'f_rsrp_n8',
           'f_rsrq_n8', 'f_year', 'f_month', 'f_day', 'pc_time', 'f_x', 'f_y',
           'f_sid', 'f_pid', 'f_direction', 'f_source', 'f_device_brand',
           'f_device_model']

Data_5G = ['finger_id', 'f_province', 'f_city', 'f_district', 'f_street',
           'f_building', 'f_floor', 'f_area', 'f_prru_id', 'f_scenario',
           'f_roaming_type', 'f_imsi', 'f_imei', 'f_msisdn', 'f_cell_id',
           'f_gnb_id', 'f_time', 'f_longitude', 'f_latitude', 'f_altitude',
           'f_phr', 'f_enb_received_power', 'f_ta', 'f_aoa', 'f_pci',
           'f_freq', 'f_rsrp', 'f_rsrq', 'f_sinr', 'f_neighbor_cell_number', 'f_freq_n1',
           'f_pci_n1', 'f_freq_n2', 'f_pci_n2', 'f_freq_n3', 'f_pci_n3',
           'f_freq_n4', 'f_pci_n4', 'f_freq_n5', 'f_pci_n5', 'f_freq_n6',
           'f_pci_n6', 'f_freq_n7', 'f_pci_n7', 'f_freq_n8', 'f_pci_n8',
           'f_rsrp_n1', 'f_rsrq_n1', 'f_sinr_n1', 'f_rsrp_n2', 'f_rsrq_n2', 'f_sinr_n2', 'f_rsrp_n3',
           'f_rsrq_n3', 'f_sinr_n3', 'f_rsrp_n4', 'f_rsrq_n4', 'f_sinr_n4', 'f_rsrp_n5', 'f_rsrq_n5',
           'f_sinr_n5',
           'f_rsrp_n6', 'f_rsrq_n6', 'f_sinr_n6', 'f_rsrp_n7', 'f_rsrq_n7', 'f_sinr_n7', 'f_rsrp_n8',
           'f_rsrq_n8', 'f_sinr_n8', 'f_year', 'f_month', 'f_day', 'pc_time', 'f_x', 'f_y',
           'f_sid', 'f_pid', 'f_direction', 'f_source', 'f_device_brand',
           'f_device_model']

UMER_Data_4G = ['uemr_id', 'u_province', 'u_city', 'u_district', 'u_street',
                'u_building', 'u_floor', 'u_area', 'u_prru_id', 'u_scenario',
                'u_roaming_type', 'u_imsi', 'u_imei', 'u_msisdn', 'u_cell_id',
                'u_enb_id', 'u_time', 'u_longitude', 'u_latitude', 'u_altitude',
                'u_phr', 'u_enb_received_power', 'u_ta', 'u_aoa', 'u_pci', 'u_freq',
                'u_rsrp', 'u_rsrq', 'u_sinr', 'u_neighbor_cell_number', 'u_freq_n1',
                'u_pci_n1', 'u_freq_n2', 'u_pci_n2', 'u_freq_n3', 'u_pci_n3',
                'u_freq_n4', 'u_pci_n4', 'u_freq_n5', 'u_pci_n5', 'u_freq_n6',
                'u_pci_n6', 'u_freq_n7', 'u_pci_n7', 'u_freq_n8', 'u_pci_n8',
                'u_rsrp_n1', 'u_rsrq_n1', 'u_sinr_n1', 'u_rsrp_n2', 'u_rsrq_n2',
                'u_sinr_n2', 'u_rsrp_n3', 'u_rsrq_n3', 'u_sinr_n3', 'u_rsrp_n4',
                'u_rsrq_n4', 'u_sinr_n4', 'u_rsrp_n5', 'u_rsrq_n5', 'u_sinr_n5',
                'u_rsrp_n6', 'u_rsrq_n6', 'u_sinr_n6', 'u_rsrp_n7', 'u_rsrq_n7',
                'u_sinr_n7', 'u_rsrp_n8', 'u_rsrq_n8', 'u_sinr_n8', 'u_year', 'u_month',
                'u_day', 'pc_time', 'u_x', 'u_y', 'u_sid', 'u_pid', 'u_direction',
                'u_source', 'u_device_brand', 'u_device_model']

UEMR_Data_5G = ['uemr_id', 'u_province', 'u_city', 'u_district', 'u_street',
                'u_building', 'u_floor', 'u_area', 'u_prru_id', 'u_scenario',
                'u_roaming_type', 'u_imsi', 'u_imei', 'u_msisdn', 'u_cell_id',
                'u_gnb_id', 'u_time', 'u_longitude', 'u_latitude', 'u_altitude',
                'u_phr', 'u_enb_received_power', 'u_ta', 'u_aoa', 'u_pci', 'u_freq',
                'u_rsrp', 'u_rsrq', 'u_sinr', 'u_neighbor_cell_number', 'u_freq_n1',
                'u_pci_n1', 'u_freq_n2', 'u_pci_n2', 'u_freq_n3', 'u_pci_n3',
                'u_freq_n4', 'u_pci_n4', 'u_freq_n5', 'u_pci_n5', 'u_freq_n6',
                'u_pci_n6', 'u_freq_n7', 'u_pci_n7', 'u_freq_n8', 'u_pci_n8',
                'u_rsrp_n1', 'u_rsrq_n1', 'u_sinr_n1', 'u_rsrp_n2', 'u_rsrq_n2',
                'u_sinr_n2', 'u_rsrp_n3', 'u_rsrq_n3', 'u_sinr_n3', 'u_rsrp_n4',
                'u_rsrq_n4', 'u_sinr_n4', 'u_rsrp_n5', 'u_rsrq_n5', 'u_sinr_n5',
                'u_rsrp_n6', 'u_rsrq_n6', 'u_sinr_n6', 'u_rsrp_n7', 'u_rsrq_n7',
                'u_sinr_n7', 'u_rsrp_n8', 'u_rsrq_n8', 'u_sinr_n8', 'u_year', 'u_month',
                'u_day', 'pc_time', 'u_x', 'u_y', 'u_sid', 'u_pid', 'u_direction',
                'u_source', 'u_device_brand', 'u_device_model']


class DealTime:
    @staticmethod
    def time_str_format(in_time):
        tmp_list = []
        for i_t in in_time:
            in_input_datetime = datetime.strptime(str(i_t), "%Y%m%d%H%M%S%f")
            # 格式化为所需的字符串格式
            tmp_output_str = in_input_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")
            tmp_list.append(tmp_output_str)
        return tmp_list


class DealConfig:
    def __init__(self, in_config_file):
        self.config_file = in_config_file
        self.config = configparser.ConfigParser()
        try:
            self.config.read(self.config_file, encoding='GBK')
        except UnicodeDecodeError:
            self.config.read(self.config_file, encoding='UTF-8')

    def get_config(self):
        return self.config

    def set_config_table(self, in_section_name, in_value):
        self.config.set(in_section_name, '45g_table_file', in_value)

    def set_config_ue_log(self, in_section_name, in_value):
        self.config.set(in_section_name, '45g_test_log', in_value)

    def set_config_char(self, in_section_name, in_value):
        self.config.set(in_section_name, 'zcy_chart_file', in_value)

    def set_config_test_area(self, in_section_name, in_value):
        self.config.set(in_section_name, 'test_area', in_value)

    def set_config_is_enabled(self, in_section_name, in_value):
        self.config.set(in_section_name, 'is_enabled', in_value)

    def set_config_data_type(self, in_section_name, in_value):
        self.config.set(in_section_name, 'data_type', in_value)

    def save_config(self):
        with open(self.config_file, 'w', encoding='UTF-8') as configfile:
            self.config.write(configfile)

    def save_config_in_path(self, in_out_path):
        out_config_file = os.path.join(in_out_path, 'config.ini')
        print_with_line_number(f'生成配置文件为：{out_config_file}', __file__)
        with open(out_config_file, 'w', encoding='UTF-8') as configfile:
            self.config.write(configfile)


config = DealConfig(r'E:\work\mr_dea_data_c2\deal_data_tool\config.ini')


def set_WalkTour_config(in_file_list):
    for i_f in in_file_list:
        if 'char' in i_f:
            config.set_config_char('WalkTour', i_f)
        elif 'table' in i_f:
            config.set_config_table('WalkTour', i_f)
        else:
            config.set_config_ue_log('WalkTour', i_f)

    config.save_config()


def set_WalkTour_config_save(in_file_list, in_out_path):
    print('---------生成WalkTour配置文件-----------')
    for i_f in in_file_list:
        if 'char' in i_f:
            config.set_config_char('WalkTour', i_f)
        elif 'table' in i_f:
            config.set_config_table('WalkTour', i_f)
        else:
            config.set_config_ue_log('WalkTour', i_f)

    config.save_config_in_path(in_out_path)


def set_WeTest_config_save(in_file_list, in_out_path):
    print('---------生成WeTest配置文件-----------')
    for i_f in in_file_list:
        if 'char' in i_f:
            config.set_config_char('WeTest', i_f)
        else:
            config.set_config_ue_log('WeTest', i_f)

    config.save_config_in_path(in_out_path)
