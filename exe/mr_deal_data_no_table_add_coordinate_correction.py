# -*- coding: utf-8 -*-
import configparser
import inspect
import math
import os
import re
import time
import zipfile
from datetime import datetime
from ftplib import FTP
import numpy as np
import pandas as pd
import pytz
from matplotlib import pyplot as plt
from pyproj import Transformer
from sklearn.decomposition import PCA


class Config:
    def __init__(self, in_config_file):
        self.config = configparser.ConfigParser()
        self.config.read(in_config_file, encoding='UTF-8')
        self.data_type = ''
        self._get_data_type()

    def _get_data_type(self):
        test_flag_list = ['true', 'false']
        WalkTour_flag = self.config.get('WalkTour', 'is_enabled')
        WeTest_flag = self.config.get('WeTest', 'is_enabled')
        wifi_bluetooth_flag = self.config.get('WIFI_BlueTooth', 'is_enabled')
        UEMR_flag = self.config.get('DealUEMR', 'is_enabled')
        if 'true' == WalkTour_flag.lower():
            print_with_line_number('WalkTour配置生效，准备处理WalkTour数据')
            self.data_type = 'WalkTour'
        elif 'true' == WeTest_flag.lower():
            print_with_line_number('WeTest配置生效，准备处理WeTest数据')
            self.data_type = 'WeTest'
        elif 'true' == wifi_bluetooth_flag.lower():
            print_with_line_number('WIFI_BlueTooth配置生效，准备处理WIFI_BlueTooth数据')
            self.data_type = 'WIFI_BlueTooth'
        elif 'true' == UEMR_flag.lower():
            print_with_line_number('DealUEMR配置生效，准备处理UEMR数据')
            self.data_type = 'DealUEMR'
        else:
            if WalkTour_flag.lower() not in test_flag_list:
                self.data_type = '配置文件WalkTour，字段is_enabled填写错误，实际填写内容: ' + WalkTour_flag + '，期望内容：true或者false'
            elif WeTest_flag.lower() not in test_flag_list:
                self.data_type = '配置文件WeTest,字段is_enabled填写错误，实际填写内容: ' + WeTest_flag + '，期望内容：true或者false'
            elif wifi_bluetooth_flag.lower() not in test_flag_list:
                self.data_type = '配置文件WIFI_BlueTooth,字段is_enabled填写错误，实际填写内容: ' + wifi_bluetooth_flag + '，期望内容：true或者false'
            elif UEMR_flag.lower() not in test_flag_list:
                self.data_type = '配置文件WIFI_BlueTooth,字段is_enabled填写错误，实际填写内容: ' + UEMR_flag + '，期望内容：true或者false'
            else:
                self.data_type = '配置文件is_enabled字段全是false，缺少true'

    def get_UEMR_file(self):
        try:
            in_res = self.config.get(self.data_type, 'uemr_src_file')
            return in_res
        except configparser.NoOptionError as e:
            print_error(f'获取 {self.data_type} 45g_table_file时报错：{e}')
            exit()

    def get_char_file(self):
        try:
            in_res = self.config.get(self.data_type, 'zcy_chart_file')
            return in_res
        except configparser.NoOptionError as e:
            print_error(f'获取 {self.data_type} zcy_chart_file时报错：{e}')
            # input(f'获取 {self.data_type} zcy_chart_file时报错：{e}')
            exit()

    def get_ue_file(self):
        try:
            in_res = self.config.get(self.data_type, '45g_test_log')
            return in_res
        except configparser.NoOptionError as e:
            print_error(f'获取 {self.data_type} 45g_test_log时报错：{e}')
            exit()

    def get_wifi_bluetooth_file(self):
        try:
            in_res = self.config.get(self.data_type, 'wifi_bluetooth_file')
            return in_res
        except configparser.NoOptionError as e:
            print_error(f'获取 {self.data_type} wifi_bluetooth_file时报错：{e}')
            exit()

    def get_test_area(self):
        try:
            in_res = self.config.get(self.data_type, 'test_area')
            return in_res
        except configparser.NoOptionError as e:
            print_error(f'获取 {self.data_type} test_area时报错：{e}')
            exit()

    def get_lon_value(self):
        try:
            in_res = self.config.get('Coordinates', 'lon_O')
            return float(in_res)
        except configparser.NoOptionError as e:
            print_error(f'获取 Coordinates lon_O时报错：{e}')
            exit()

    def get_lat_value(self):
        try:
            in_res = self.config.get('Coordinates', 'lat_O')
            return float(in_res)
        except configparser.NoOptionError as e:
            print_error(f'获取 Coordinates lat_O时报错：{e}')
            exit()

    def get_len_x_value(self):
        try:
            in_res = self.config.get('Coordinates', 'len_east_x')
            return float(in_res)
        except configparser.NoOptionError as e:
            print_error(f'获取 Coordinates len_east_x时报错：{e}')
            exit()

    def get_len_y_value(self):
        try:
            in_res = self.config.get('Coordinates', 'len_north_y')
            return float(in_res)
        except configparser.NoOptionError as e:
            print_error(f'获取 Coordinates len_north_y时报错：{e}')
            exit()

    def get_device_brand(self):
        try:
            in_res = self.config.get('SceneInfo', 'f_device_brand')
            return in_res
        except configparser.NoOptionError as e:
            print_error(f'获取 SceneInfo f_device_brand时报错：{e}')
            exit()

    def get_device_model(self):
        try:
            in_device_model = self.config.get('SceneInfo', 'f_device_model')
            return in_device_model
        except configparser.NoOptionError as e:
            print_error(f'获取 SceneInfo f_device_model时报错：{e}')
            exit()

    def get_area(self):
        try:
            in_area = self.config.get('SceneInfo', 'f_area')
            return in_area
        except configparser.NoOptionError as e:
            print_error(f'获取 SceneInfo f_area时报错：{e}')
            exit()

    def get_floor(self):
        try:
            in_floor = self.config.get('SceneInfo', 'f_floor')
            return in_floor
        except configparser.NoOptionError as e:
            print_error(f'获取 SceneInfo f_floor时报错：{e}')
            exit()

    def get_scenario(self):
        try:
            in_scenario = self.config.get('SceneInfo', 'f_scenario')
            return in_scenario
        except configparser.NoOptionError as e:
            print_error(f'获取 SceneInfo f_scenario时报错：{e}')
            exit()

    def get_district(self):
        try:
            in_district = self.config.get('SceneInfo', 'f_district')
            return in_district
        except configparser.NoOptionError as e:
            print_error(f'获取 SceneInfo f_district时报错：{e}')
            exit()

    def get_street(self):
        try:
            in_street = self.config.get('SceneInfo', 'f_street')
            return in_street
        except configparser.NoOptionError as e:
            print_error(f'获取 SceneInfo f_street时报错：{e}')
            exit()

    def get_building(self):
        try:
            in_building = self.config.get('SceneInfo', 'f_building')
            return in_building
        except configparser.NoOptionError as e:
            print_error(f'获取 SceneInfo f_building时报错：{e}')
            exit()

    def get_source(self):
        try:
            f_source = self.config.get('SceneInfo', 'f_source')
            return f_source
        except configparser.NoOptionError as e:
            print_error(f'获取 SceneInfo f_source时报错：{e}')
            exit()

    def get_province(self):
        try:
            in_province = self.config.get('SceneInfo', 'f_province')
            return in_province
        except configparser.NoOptionError as e:
            print_error(f'获取 SceneInfo f_province时报错：{e}')
            exit()

    def get_city(self):
        try:
            in_city = self.config.get('SceneInfo', 'f_city')
            return in_city
        except configparser.NoOptionError as e:
            print_error(f'获取 SceneInfo f_city时报错：{e}')
            exit()

    def get_prru_id(self):
        try:
            in_res = self.config.get('SceneInfo', 'f_prru_id')
            return in_res
        except configparser.NoOptionError as e:
            print_error(f'获取 SceneInfo f_prru_id时报错：{e}')
            exit()

    def get_msisdn(self):
        try:
            in_res = self.config.get('SceneInfo', 'f_msisdn')
            return in_res
        except configparser.NoOptionError as e:
            print_error(f'获取 SceneInfo f_msisdn时报错：{e}')
            exit()

    def get_data_type(self):
        try:
            in_building = self.config.get(self.data_type, 'data_type')
            return in_building
        except configparser.NoOptionError as e:
            print_error(f'获取 {self.data_type} data_type时报错：{e}')
            exit()


class Time:
    # 获取当前时间的字符串
    @staticmethod
    def get_now_time_str():
        in_now = datetime.now()  # 获取当前时间
        tmp_year = str(in_now.year)  # 获取当前年份
        tmp_month = str(in_now.month)  # 获取当前月份
        tmp_day = str(in_now.day)  # 获取当前日期
        tmp_hour = str(in_now.hour)  # 获取当前小时
        tmp_minute = str(in_now.minute)  # 获取当前分钟
        tmp_second = str(in_now.second)  # 获取当前秒数

        tmp_time_str = tmp_year + tmp_month + tmp_day + tmp_hour + tmp_minute + tmp_second
        return tmp_time_str

    # 标准化时间
    @staticmethod
    def stand_time(in_df, in_column):
        in_df['tmp'] = pd.to_datetime(in_df[in_column])
        # 格式化 'timestamp' 列为特定的字符串格式
        in_df[in_column] = in_df['tmp'].dt.strftime('%Y-%m-%d %H:%M:%S.%f')
        return in_df

    # 时间戳转日期
    @staticmethod
    def convert_timestamp_to_datetime(timestamp):
        return [datetime.fromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S.%f') for x in timestamp]

    # 秒数转日期
    @staticmethod
    def seconds_to_datetime(in_seconds):
        tmp_datetime_object = datetime.fromtimestamp(in_seconds)
        return str(tmp_datetime_object)

    # df转秒数为日期
    @staticmethod
    def convert_seconds_to_datetime(dtime):
        return [Time.seconds_to_datetime(x) for x in dtime]

    # 转换df中整列数据的时间格式
    @staticmethod
    def convert_time_format(in_time):
        tmp_list = []
        for i_t in in_time:
            in_input_datetime = datetime.strptime(str(i_t), "%Y%m%d%H%M%S%f")
            # 格式化为所需的字符串格式
            tmp_output_str = in_input_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")
            tmp_list.append(tmp_output_str)
        return tmp_list

    # 把秒数转成年月日时间
    @staticmethod
    def convert_seconds_to_datetime_day_year_month(timestamp):
        date_obj = pd.to_datetime(timestamp, unit='s')
        return date_obj.year, date_obj.month, date_obj.day

    @staticmethod
    def convert_datetime_to_seconds(dtime):
        return [int(time.mktime(time.strptime(x, '%Y-%m-%d %H:%M:%S.%f'))) for x in dtime]

    @staticmethod
    def convert_datetime_to_seconds_slash(dtime):
        return [time.mktime(time.strptime(x, '%Y/%m/%d %H:%M:%S.%f')) for x in dtime]


class WeTest:
    @staticmethod
    def merge_ue_zcy_df_convert_format(in_ue_df, in_zcy_df):
        in_ue_df['pc_time'] = Time.convert_time_format(in_ue_df['pc_time'])

        in_ue_df = pd.merge(in_ue_df, in_zcy_df, left_on="pc_time", right_on="test_time", how='left')
        in_ue_df['ts'] = Time.convert_datetime_to_seconds(in_ue_df['pc_time'])
        return in_ue_df

    @staticmethod
    def merge_ue_zcy_df(in_ue_df, in_zcy_df):
        # in_ue_df = pd.merge(in_ue_df, in_zcy_df, left_on="pc_time", right_on="test_time", how='left')
        in_ue_df['create_time'] = Common.get_top_ten_data(in_ue_df['create_time'])
        in_zcy_df['created_by_ue_time'] = Common.get_top_ten_data(in_zcy_df['created_by_ue_time'])

        in_ue_df = pd.merge(in_ue_df, in_zcy_df, left_on="create_time", right_on="created_by_ue_time", how='left')
        # in_ue_df['ts'] = Time.convert_datetime_to_seconds(in_ue_df['test_time'])
        return in_ue_df

    @staticmethod
    def outdoor_get_df(in_config):
        in_ue_file = in_config.get_ue_file()

        if not Common.check_file_exists(in_ue_file):
            print_with_line_number(f'error, 文件：{in_ue_file} 不存在')
            return

        print_with_line_number(f'读取UE文件：{in_ue_file}')
        in_ue_df = Common.read_csv_get_df(in_ue_file)
        if '-' in str(in_ue_df['pc_time'][0]):
            in_ue_df['ts'] = Time.convert_datetime_to_seconds(in_ue_df['pc_time'])
        elif '/' in str(in_ue_df['pc_time'][0]):
            in_ue_df['ts'] = Time.convert_datetime_to_seconds_slash(in_ue_df['pc_time'])
        else:
            in_ue_df['pc_time'] = Time.convert_time_format(in_ue_df['pc_time'])
            in_ue_df['ts'] = Time.convert_datetime_to_seconds(in_ue_df['pc_time'])
        return in_ue_df

    @staticmethod
    def indoor_get_df(in_config, in_deal_zcy_char_csv_file):
        # 获取char数据
        in_char_file = in_config.get_char_file()
        # 获取ue数据
        in_ue_file = in_config.get_ue_file()

        if not Common.check_file_exists(in_ue_file):
            print_with_line_number(f'error, 文件：{in_ue_file} 不存在')
            return
        elif not Common.check_file_exists(in_char_file):
            print_with_line_number(f'error, 文件：{in_char_file} 不存在')
            return
        try:
            print_with_line_number(f'读取char文件：{in_char_file}')
        except UnicodeEncodeError as e:
            print_with_line_number(f'error, 输出char文件名时报错：{e}')
        tmp_zcy_df = Common.read_csv_get_df(in_char_file)

        # 旋转纠偏和水平纠偏
        print_with_line_number('开始旋转纠偏')
        x1, y1 = CommonFunc.cov_x_y_to_real_x_y(tmp_zcy_df['x'], tmp_zcy_df['y'])
        # 水平平移纠偏
        print_with_line_number('开始水平纠偏')
        x1_1, y1_1 = CommonFunc.move_xy_to_real_xy(x1, y1, 800, 600)

        tmp_zcy_df['x'] = x1_1
        tmp_zcy_df['y'] = y1_1

        if 'direction' in tmp_zcy_df.columns:
            print_with_line_number('读取zcy direction数据')
            tmp_zcy_df = tmp_zcy_df[
                ['test_time', 'created_by_ue_time', 'x', 'y', 'longitude', 'latitude', 'direction', 'altitude']]
        else:
            tmp_zcy_df = tmp_zcy_df[
                ['test_time', 'created_by_ue_time', 'x', 'y', 'longitude', 'latitude', 'altitude']]

        in_zcy_df = in_deal_zcy_char_csv_file(tmp_zcy_df)

        in_ue_df = Common.read_csv_get_df(in_ue_file)
        print_with_line_number(f'读取ue文件：{in_ue_file}')
        # if '-' not in str(in_ue_df['pc_time'])[0] and '/' not in str(in_ue_df['pc_time'])[0]:
        index = Common.get_first_valid_value(in_ue_df['pc_time'])
        # print('index: ', index)
        if '-' not in str(in_ue_df['pc_time'][index]) and str(in_ue_df['pc_time'][index]).find("/") == -1:
            # print(in_ue_df['pc_time'][0])
            # print('---' * 50)
            in_ue_df['pc_time'] = Time.convert_time_format(in_ue_df['pc_time'])

        # 设置ts
        if str(in_ue_df['pc_time'][index]).find("/") != -1:
            in_ue_df['ts'] = Time.convert_datetime_to_seconds_slash(in_ue_df['pc_time'])
        else:
            in_ue_df['ts'] = Time.convert_datetime_to_seconds(in_ue_df['pc_time'])
        # 获取前时间前十位
        # in_ue_df['create_time'] = Common.get_top_ten_data(in_ue_df['create_time'])
        in_zcy_df['created_by_ue_time'] = Common.get_top_ten_data(in_zcy_df['created_by_ue_time'])
        # ue中pc_time标准化，秒数转日期
        # in_ue_df['pc_time'] = Time.convert_seconds_to_datetime(in_zcy_df['created_by_ue_time'])
        # 合并ue和char数据
        print_with_line_number(f"test log create_time时间戳为：{in_ue_df['ts'][0]}")
        print_with_line_number(f"char数据 created_by_ue_time时间戳为：{in_zcy_df['created_by_ue_time'][0]}")

        res_zcy_ue_merge_df = pd.merge(in_ue_df, in_zcy_df, left_on="ts", right_on="created_by_ue_time",
                                       how='left')

        # res_zcy_ue_merge_df = WeTest.merge_ue_zcy_df(in_ue_df, in_zcy_df)

        return res_zcy_ue_merge_df

    @staticmethod
    def deal_wetest_5g(log_df_5g):
        # 删除测试log中 秒级重复数据，同秒取第一条。
        # log_df_5g = log_df_5g.groupby(log_df_5g['ts']).first().reset_index()
        # 定义要进行替换的正则表达式模式
        pattern = re.compile(r'NCell(\d)(\d)')
        # 使用正则表达式匹配并替换列名
        log_df_5g.columns = [pattern.sub(lambda x: f'NCell{x.group(1)}', col) for col in log_df_5g.columns]

        cell_cnt = 0
        while True:
            cell_cnt += 1
            if f'nr_neighbor_cell_{cell_cnt}_freq' in log_df_5g.columns:
                log_df_5g = log_df_5g.rename(
                    columns={
                        f'nr_neighbor_cell_{cell_cnt}_freq': f'f_freq_n{cell_cnt}',
                        f'nr_neighbor_cell_{cell_cnt}_pci': f'f_pci_n{cell_cnt}',
                        f'nr_neighbor_cell_{cell_cnt}_ssb_rsrp': f'f_rsrp_n{cell_cnt}',
                        f'nr_neighbor_cell_{cell_cnt}_ssb_rsrq': f'f_rsrq_n{cell_cnt}',
                        f'nr_neighbor_cell_{cell_cnt}_ssb_sinr': f'f_sinr_n{cell_cnt}',
                    })
            else:
                break

        log_df_5g = log_df_5g.rename(
            columns={
                'imsi': 'f_imsi',
                'imei': 'f_imei',
                'nci': 'f_cell_id',
                'ts': 'f_time',
                'nr_serving_cell_pci': 'f_pci',
                'nr_serving_cell_freq': 'f_freq',
                'nr_serving_cell_ssb_rsrp': 'f_rsrp',
                'nr_serving_cell_ssb_rsrq': 'f_rsrq',
                'nr_serving_cell_ssb_sinr': 'f_sinr',
                'altitude': 'f_altitude',
                'direction': 'f_direction',
            })

        if 'f_longitude' not in log_df_5g.columns:
            log_df_5g = log_df_5g.rename(
                columns={
                    'startlocation_longitude': 'f_longitude',
                    'startlocation_latitude': 'f_latitude',
                })

        # 删除重复列
        log_df_5g = Common.delete_duplicate_columns(log_df_5g)

        # 时间转上海时区
        sh_timez = Common.change_time_zone(log_df_5g['f_time'])
        log_df_5g['f_time_1'] = sh_timez
        # 生成finger_id
        finger_id = Common.generate_finger_id(log_df_5g['f_time_1'], log_df_5g['f_msisdn'])
        log_df_5g['finger_id'] = finger_id
        # 获取领区数
        num_list = Common.get_cell_number(log_df_5g)
        log_df_5g['f_neighbor_cell_number'] = num_list
        # 置空 UEMR 数据
        log_df_5g = Common.fill_in_empty_header(log_df_5g)

        log_df_5g['f_gnb_id'] = log_df_5g['f_cell_id'] // 4096
        log_df_5g['f_imsi'] = np.array(log_df_5g['f_imsi'])
        log_df_5g[['f_year', 'f_month', 'f_day']] = log_df_5g['f_time'].apply(
            Time.convert_seconds_to_datetime_day_year_month).to_list()
        # log_df_5g['f_cst_time'] = log_df_5g['pc_time']
        log_df_5g['f_eci'] = log_df_5g['f_cell_id']

        # SID暂时都赋值1
        log_df_5g['f_sid'] = 1
        log_df_5g['f_pid'] = (log_df_5g.index + 1).astype(str)

        log_df_5g = log_df_5g.reindex(columns=OutDataTableFormat.WeTest5G)
        Common.data_filling(log_df_5g, 'f_cell_id')

        log_df_5g = log_df_5g.rename(str.lower, axis='columns')
        return log_df_5g

    @staticmethod
    def deal_wetest_4g(log_df_4g):
        # 删除测试log中 秒级重复数据，同秒取第一条。
        # Common.delete_df_duplicate_data_by_second(log_df_4g, log_df_4g['ts'])

        pattern = re.compile(r'NCell(\d)(\d)')
        # 使用正则表达式匹配并替换列名
        log_df_4g.columns = [pattern.sub(lambda x: f'NCell{x.group(1)}', col) for col in log_df_4g.columns]

        cell_cnt = 0
        while True:
            cell_cnt += 1
            if f'lte_neighbor_cell_{cell_cnt}_freq' in log_df_4g.columns:
                log_df_4g = log_df_4g.rename(
                    columns={
                        f'lte_neighbor_cell_{cell_cnt}_freq': f'f_freq_n{cell_cnt}',
                        f'lte_neighbor_cell_{cell_cnt}_pci': f'f_pci_n{cell_cnt}',
                        f'lte_neighbor_cell_{cell_cnt}_rsrp': f'f_rsrp_n{cell_cnt}',
                        f'lte_neighbor_cell_{cell_cnt}_rsrq': f'f_rsrq_n{cell_cnt}',
                    })
            else:
                break

        # 列重命名
        log_df_4g = log_df_4g.rename(
            columns={
                'imsi': 'f_imsi',
                'imei': 'f_imei',
                'lte_eci': 'f_cell_id',
                'ts': 'f_time',
                'lte_serving_cell_pci': 'f_pci',
                'lte_serving_cell_freq': 'f_freq',
                'lte_serving_cell_rsrp': 'f_rsrp',
                'lte_serving_cell_rsrq': 'f_rsrq',
                'altitude': 'f_altitude',
                'direction': 'f_direction',
            })

        if 'f_longitude' not in log_df_4g.columns:
            log_df_4g = log_df_4g.rename(
                columns={
                    'startlocation_longitude': 'f_longitude',
                    'startlocation_latitude': 'f_latitude',
                })

        # 删除重复列
        log_df_4g = Common.delete_duplicate_columns(log_df_4g)

        # 时间转上海时区
        sh_timez = Common.change_time_zone(log_df_4g['f_time'])
        log_df_4g['f_time_1'] = sh_timez
        # 生成finger_id
        finger_id = Common.generate_finger_id(log_df_4g['f_time_1'], log_df_4g['f_msisdn'])
        log_df_4g['finger_id'] = finger_id
        # 获取领区数
        num_list = Common.get_cell_number(log_df_4g)
        log_df_4g['f_neighbor_cell_number'] = num_list
        # 置空 UEMR 数据
        log_df_4g = Common.fill_in_empty_header(log_df_4g)

        log_df_4g['f_enb_id'] = log_df_4g['f_cell_id'] // 256

        log_df_4g[['f_year', 'f_month', 'f_day']] = log_df_4g['f_time'].apply(
            Time.convert_seconds_to_datetime_day_year_month).to_list()
        log_df_4g['f_eci'] = log_df_4g['f_cell_id']

        # SID暂时都赋值1
        log_df_4g['f_sid'] = ''
        log_df_4g['f_pid'] = (log_df_4g.index + 1).astype(str)
        log_df_4g = log_df_4g.reindex(columns=OutDataTableFormat.WeTest4G)
        Common.data_filling(log_df_4g, 'f_cell_id')

        log_df_4g = log_df_4g.rename(str.lower, axis='columns')
        return log_df_4g


class DealUEMR:
    @staticmethod
    def uemr_merge_char(in_uemr_df, in_deal_data):
        # 获取char数据
        in_char_file = in_deal_data.config.get_char_file()
        print_with_line_number(f'读取char文件：{in_char_file}')
        if not Common.check_file_exists(in_char_file):
            print_error(f'error, char文件：{in_char_file} 不存在')
            exit()
        tmp_zcy_df = Common.read_csv_get_df(in_char_file)
        if 'direction' in tmp_zcy_df.columns:
            # print_with_line_number('读取zcy direction数据')
            tmp_zcy_df = tmp_zcy_df[
                ['test_time', 'created_by_ue_time', 'x', 'y', 'direction', 'altitude']]
            # in_uemr_df['f_direction'] = tmp_zcy_df['direction']
        else:
            tmp_zcy_df = tmp_zcy_df[
                ['test_time', 'created_by_ue_time', 'x', 'y', 'altitude']]

        in_zcy_df = in_deal_data.deal_zcy_char_csv_file(tmp_zcy_df)
        # 获取前时间前十位
        in_zcy_df['created_by_ue_time'] = Common.get_top_ten_data(in_zcy_df['created_by_ue_time'])
        # in_zcy_df['created_by_ue_time'] = in_uemr_df['f_time']

        if 'direction' in in_zcy_df.columns:
            in_zcy_df = in_zcy_df.rename(columns={'direction': 'f_direction', 'altitude': 'f_altitude'})
            in_uemr_df = in_uemr_df.drop(columns=['f_direction'])

        # uemr丢弃走测仪列，合并预处理
        in_uemr_df = in_uemr_df.drop(columns=['f_longitude', 'f_latitude', 'f_altitude', 'f_x', 'f_y'])

        f_time_index = Common.get_first_valid_value(in_uemr_df['f_time'])
        created_by_ue_time_index = Common.get_first_valid_value(in_zcy_df['created_by_ue_time'])

        f_time_v = Time.seconds_to_datetime(in_uemr_df['f_time'][f_time_index])
        created_by_ue_time_v = Time.seconds_to_datetime(in_zcy_df['created_by_ue_time'][created_by_ue_time_index])

        print_with_line_number(f"uemr f_time为：{f_time_v}")
        print_with_line_number(f"走测仪 created_by_ue_time为：{created_by_ue_time_v}")

        res_merge_df = pd.merge(in_uemr_df, in_zcy_df, left_on="f_time", right_on="created_by_ue_time",
                                how='left')

        return res_merge_df

    @staticmethod
    def deal_UEMR_4g(log_df_4g):
        cell_cnt = 0
        while True:
            cell_cnt += 1
            if f'uemr.neighbor_{cell_cnt}_cell_pci' in log_df_4g.columns:
                log_df_4g = log_df_4g.rename(
                    columns={
                        f'uemr.neighbor_{cell_cnt}_freq': f'f_freq_n{cell_cnt}',
                        f'uemr.neighbor_{cell_cnt}_cell_pci': f'f_pci_n{cell_cnt}',
                        f'uemr.neighbor_{cell_cnt}_rsrp': f'f_rsrp_n{cell_cnt}',
                        f'uemr.neighbor_{cell_cnt}_rsrq': f'f_rsrq_n{cell_cnt}',
                    })
            else:
                break

        # 重命名table数据
        log_df_4g = log_df_4g.rename(
            columns={
                'uemr.imsi': 'f_imsi',
                'uemr.imeisv': 'f_imei',
                'uemr.msisdn': 'f_msisdn',
                'uemr.cell_id': 'f_cell_id',
                'uemr.year': 'f_year',
                'uemr.month': 'f_month',
                'uemr.day': 'f_day',
                'uemr.enb_id': 'f_enb_id',
                'uemr.ta': 'f_ta',
                'uemr.aoa': 'f_aoa',
                'uemr.enb_received_power': 'f_enb_received_power',
                'uemr.roaming_type': 'f_roaming_type',
                'uemr.phr': 'f_phr',
                'uemr.location_source': 'f_source',
                'uemr.neighbor_cell_number': 'f_neighbor_cell_number',
                'uemr.serving_freq': 'f_freq',
                'uemr.serving_rsrp': 'f_rsrp',
                'uemr.serving_rsrq': 'f_rsrq',
                'uemr.pci': 'f_pci',
                'uemr.time': 'f_time',
                'uemr.location_latitude': 'f_latitude',
                'uemr.location_longitude': 'f_longitude',
                'uemr.location_altitude': 'f_altitude',
            })

        # 如果f_time列所有数据都相同，则不使用f_time作为时间
        if Common.is_column_all_same(log_df_4g, 'f_time'):
            # print_with_line_number('f_time列所有数据都相同, 不使用f_time作为时间')
            # 使用
            if 'time' in log_df_4g.columns:
                if Common.is_column_all_same(log_df_4g, 'time'):
                    print_with_line_number('当前添加的的time列所有时间戳相同，请在UEMR数据文件中重新设置time列')
                    exit()
                # 标准化time数据
                res_index = Common.get_first_valid_value(log_df_4g['time'])
                if '-' in str(log_df_4g['time'][res_index]):
                    log_df_4g = Time.stand_time(log_df_4g, 'time')
                    log_df_4g['f_time'] = Time.convert_datetime_to_seconds(log_df_4g['time'])
                # exit()
                else:
                    print_with_line_number('time列时间格式不对，请使用时间格式为例如：2023-12-19 14:37:00的设置time列')
                    exit()
                    # log_df_4g['time'] = Time.convert_time_format(log_df_4g['time'])
                    # log_df_4g['f_time'] = Time.convert_datetime_to_seconds(log_df_4g['time'])
            else:
                print_with_line_number(
                    '数据中，uemr.time所有时间戳都相同，且没有找到time列，请设置手动在uemr数据中，额外添加time列，或者修改uemr.time时间戳')
                exit()
            # log_df_4g['f_time'] = Common.get_top_ten_data(log_df_4g['f_time'])
        else:
            # 获取f_time的数字的前十位
            print_with_line_number('使用f_time作为时间')
            log_df_4g['f_time'] = Common.get_top_ten_data(log_df_4g['f_time'])

        log_df_4g['pc_time'] = Time.convert_timestamp_to_datetime(log_df_4g['f_time'])

        # 删除测试log中 秒级重复数据，同秒取第一条。
        # Common.delete_df_duplicate_data_by_second(log_df_4g, log_df_4g['f_time'])

        log_df_4g['f_sid'] = 1
        log_df_4g['f_pid'] = (log_df_4g.index + 1).astype(str)

        finger_id = 'F' + str(log_df_4g['uemr.t'][0]) + '_' + str(log_df_4g['f_msisdn'])[-4:]
        log_df_4g['finger_id'] = finger_id

        # du_column = log_df_4g.columns[log_df_4g.columns.duplicated()]
        # print_with_line_number(f"Duplicate columns: {du_column}")

        log_df_4g = log_df_4g.reindex(columns=OutDataTableFormat.WeTest4G)
        # 计算领区数
        cell_number = Common.get_cell_number(log_df_4g)
        log_df_4g['f_neighbor_cell_number'] = cell_number
        Common.data_filling(log_df_4g, 'f_cell_id')

        log_df_4g = log_df_4g.rename(str.lower, axis='columns')
        return log_df_4g

    @staticmethod
    def deal_UEMR_5g(log_df_5g):
        i = 0
        while True:
            i += 1
            if f'uemr5g.neighbor_{i}_etura_cell_pci' in log_df_5g.columns:
                log_df_5g = log_df_5g.rename(
                    columns={
                        f'uemr5g.neighbor_nr_cell_{i}_freq': f'f_freq_n{i}',
                        f'uemr5g.neighbor_nr_cell_{i}_pci': f'f_pci_n{i}',
                        f'uemr5g.neighbor_nr_cell_{i}_ssb_rsrp': f'f_rsrp_n{i}',
                        f'uemr5g.neighbor_nr_cell_{i}_ssb_rsrq': f'f_rsrq_n{i}',
                        f'uemr5g.neighbor_nr_cell_{i}_ssb_sinr': f'f_sinr_n{i}',
                    })
            else:
                break

        # 重命名table数据
        log_df_5g = log_df_5g.rename(
            columns={
                'uemr5g.imsi': 'f_imsi',
                'uemr5g.pei': 'f_imei',
                'uemr5g.msisdn': 'f_msisdn',
                'uemr5g.cell_id': 'f_cell_id',
                'uemr5g.year': 'f_year',
                'uemr5g.month': 'f_month',
                'uemr5g.day': 'f_day',
                'uemr.enb_id': 'f_enb_id',
                'uemr5g.ta': 'f_ta',
                'uemr5g.aoa': 'f_aoa',
                'uemr5g.enb_received_power': 'f_enb_received_power',
                'uemr5g.roaming_type': 'f_roaming_type',
                'uemr5g.phr': 'f_phr',
                'uemr5g.location_source': 'f_source',
                'uemr5g.neighbor_nr_cell_number': 'f_neighbor_cell_number',
                'uemr5g.create_time': 'f_time',
                'uemr5g.startlocation_latitude': 'f_latitude',
                'uemr5g.startlocation_longitude': 'f_longitude',
                'uemr5g.startlocation_altitude': 'f_altitude',
                'uemr5g.servingcell_1_freq': 'f_freq',
                'uemr5g.servingcell_1_ssb_rsrp': 'f_rsrp',
                'uemr5g.servingcell_1_ssb_rsrq': 'f_rsrq',
                'uemr5g.servingcell_1_pci': 'f_pci',
                'uemr5g.servingcell_1_csi_rs_sinr': 'f_sinr',
            })

        if Common.is_column_all_same(log_df_5g, 'f_time'):
            print_with_line_number('f_time列所有数据都相同, 不使用f_time作为时间')
            # 使用
            if 'time' in log_df_5g.columns:
                if Common.is_column_all_same(log_df_5g, 'time'):
                    print_with_line_number('当前添加的的time列所有时间戳相同，请在UEMR数据文件中重新设置time列')
                    exit()
                # 标准化time数据
                res_index = Common.get_first_valid_value(log_df_5g['time'])
                if '-' in str(log_df_5g['time'][res_index]):
                    log_df_4g = Time.stand_time(log_df_5g, 'time')
                    print_with_line_number('have time')
                    log_df_4g['f_time'] = Time.convert_datetime_to_seconds(log_df_4g['time'])
                # exit()
                else:
                    print_with_line_number('time列时间格式不对，请使用时间格式为例如：2023-12-19 14:37:00的设置time列')
                    exit()
                    # log_df_4g['time'] = Time.convert_time_format(log_df_4g['time'])
                    # log_df_4g['f_time'] = Time.convert_datetime_to_seconds(log_df_4g['time'])
            else:
                print_with_line_number(
                    '数据中，uemr.time所有时间戳都相同，且没有找到time列，请设置手动在uemr数据中，额外添加time列，或者修改uemr.time时间戳')
                exit()
        else:
            # 获取f_time的数字的前十位
            print_with_line_number('使用f_time作为时间')
            log_df_5g['f_time'] = Common.get_top_ten_data(log_df_5g['f_time'])

        log_df_5g['f_sid'] = 1
        log_df_5g['f_pid'] = (log_df_5g.index + 1).astype(str)

        log_df_5g['pc_time'] = Time.convert_timestamp_to_datetime(log_df_5g['f_time'])

        # 删除测试log中 秒级重复数据，同秒取第一条。
        # Common.delete_df_duplicate_data_by_second(log_df_5g, log_df_5g['f_time'])

        # 删除重复行
        log_df_5g = Common.delete_duplicate_columns(log_df_5g)
        # # 设置场景信息
        # log_df_5g = Common.set_scene_data(log_df_5g, in_config)

        finger_id = 'F' + str(log_df_5g['uemr5g.t'][0]) + '_' + log_df_5g['f_msisdn'].str[-4:]
        log_df_5g['finger_id'] = finger_id

        log_df_5g['f_gnb_id'] = log_df_5g['f_cell_id'] // 4096

        log_df_5g = log_df_5g.reindex(columns=OutDataTableFormat.WalkTour5G)
        # 获取领区数
        num_list = Common.get_cell_number(log_df_5g)
        log_df_5g['f_neighbor_cell_number'] = num_list
        Common.data_filling(log_df_5g, 'f_cell_id')

        log_df_5g = log_df_5g.rename(str.lower, axis='columns')
        return log_df_5g


class WalkTour:
    # 获取zcy和wifi联合数据
    @staticmethod
    def indoor_get_df_wifi_bluetooth_zcy(in_config, in_deal_data):
        # 获取zcy，wifi和ue联合数据
        in_ue_file = in_config.get_ue_file()
        in_table_file = in_config.get_table_file()
        in_char_file = in_config.get_char_file()
        in_wifi_bluetooth_file = in_config.get_wifi_bluetooth_file()

        if not Common.check_file_exists(in_ue_file):
            print_with_line_number(f'error, 文件：{in_ue_file} 不存在')
            return
        elif not Common.check_file_exists(in_table_file):
            print_with_line_number(f'error, 文件：{in_table_file} 不存在')
            return
        elif not Common.check_file_exists(in_char_file):
            print_with_line_number(f'error, 文件：{in_char_file} 不存在')
            return
        elif not Common.check_file_exists(in_wifi_bluetooth_file):
            print_with_line_number(f'error, 文件：{in_wifi_bluetooth_file} 不存在')
            return

        zcy_data_path = os.path.join(os.path.dirname(in_ue_file), 'zcy_data')
        Common.check_path(zcy_data_path)

        tmp_zcy_df = Common.read_csv_get_df(in_char_file)

        # 旋转纠偏和水平纠偏
        print_with_line_number('开始旋转纠偏')
        x1, y1 = CommonFunc.cov_x_y_to_real_x_y(tmp_zcy_df['x'], tmp_zcy_df['y'])
        # 水平平移纠偏
        print_with_line_number('开始水平纠偏')
        x1_1, y1_1 = CommonFunc.move_xy_to_real_xy(x1, y1, 800, 600)

        tmp_zcy_df['x'] = x1_1
        tmp_zcy_df['y'] = y1_1

        if 'direction' in tmp_zcy_df.columns:
            print_with_line_number('读取zcy direction数据')
            tmp_zcy_df = tmp_zcy_df[
                ['test_time', 'created_by_ue_time', 'x', 'y', 'longitude', 'latitude', 'direction', 'altitude']]
        else:
            tmp_zcy_df = tmp_zcy_df[
                ['test_time', 'created_by_ue_time', 'x', 'y', 'longitude', 'latitude', 'altitude']]

        tmp_wifi_bluetooth_df = Common.read_csv_get_df(in_wifi_bluetooth_file)

        # 获取秒数的前十位
        tmp_zcy_df['created_by_ue_time'] = Common.get_top_ten_data(tmp_zcy_df['created_by_ue_time'])
        tmp_wifi_bluetooth_df['f_time'] = Common.get_top_ten_data(tmp_wifi_bluetooth_df['f_time'])
        # 获取zcy，wifi 蓝牙合并数据
        tmp_merger_df = pd.merge(tmp_wifi_bluetooth_df, tmp_zcy_df, left_on="f_time", right_on="created_by_ue_time",
                                 how='left')
        tmp_merger_df = in_deal_data.deal_zcy_char_csv_file(tmp_merger_df)

        print_with_line_number(
            f"走测仪原始数据保存为：{os.path.join(zcy_data_path, 'zcy_wifi_bluetooth_merge_data.csv')}")
        try:
            Common.df_write_to_csv(tmp_merger_df, os.path.join(zcy_data_path, 'zcy_wifi_bluetooth_merge_data.csv'))
        except PermissionError as e:
            print_with_line_number(f'写文件报错：{e}')

        # tmp_ue_table_df = WalkTour.get_merge_ue_table_data(in_ue_file, in_table_file)

        in_ue_df = Common.read_csv_get_df(in_ue_file)
        in_table_df = Common.read_csv_get_df(in_table_file)
        try:
            tmp_ue_table_df = pd.merge(in_ue_df, in_table_df, left_on="PC Time", right_on="PCTime", how='left')
        except KeyError as e:
            print_with_line_number(f'type: {type(e)}')
            if 'PCTime' in str(e):
                print_with_line_number(f'table数据中缺少，PCTime列，报错 KeyError：{e}')
            elif 'PC Time' in str(e):
                print_with_line_number(f'test log数据中缺少，PC Time列，报错 KeyError：{e}')
            else:
                print_with_line_number(f'报错 KeyError：{e}')
            return

        # 合并所有数据
        res_merge_df = WalkTour.wifi_bluetooth_merge_ue_zcy_data(tmp_ue_table_df, tmp_merger_df)
        return res_merge_df

    # 室外数据获取merge df
    @staticmethod
    def outdoor_get_df(in_config):
        ue_file = in_config.get_ue_file()

        if not Common.check_file_exists(ue_file):
            print_with_line_number(f'error, 文件：{ue_file} 不存在')
            return

        print_with_line_number(f'读取ue文件：{ue_file}')

        # rea_ue_table_df = WalkTour.get_merge_ue_table_data(ue_file, table_file)
        in_ue_df = Common.read_csv_get_df(ue_file)

        in_ue_df['ts'] = Time.convert_datetime_to_seconds(in_ue_df['PC Time'])
        return in_ue_df

    # 室内数据获取merge df
    @staticmethod
    def indoor_get_df_zcy(in_config, in_deal_zcy_char_csv_file):
        in_ue_file = in_config.get_ue_file()

        # 处理走测仪数据
        in_char_file = in_config.get_char_file()

        if not Common.check_file_exists(in_ue_file):
            print_with_line_number(f'error, 文件：{in_ue_file} 不存在')
            return
        elif not Common.check_file_exists(in_char_file):
            print_with_line_number(f'error, 文件：{in_char_file} 不存在')
            return

        tmp_zcy_df = Common.read_csv_get_df(in_char_file)

        # 旋转纠偏和水平纠偏
        print_with_line_number('开始旋转纠偏')
        x1, y1 = CommonFunc.cov_x_y_to_real_x_y(tmp_zcy_df['x'], tmp_zcy_df['y'])
        # 水平平移纠偏
        print_with_line_number('开始水平纠偏')
        x1_1, y1_1 = CommonFunc.move_xy_to_real_xy(x1, y1, 800, 600)

        tmp_zcy_df['x'] = x1_1
        tmp_zcy_df['y'] = y1_1

        if 'direction' in tmp_zcy_df.columns:
            print_with_line_number('读取zcy direction数据')
            tmp_zcy_df = tmp_zcy_df[
                ['test_time', 'created_by_ue_time', 'x', 'y', 'longitude', 'latitude', 'direction', 'altitude']]
        else:
            tmp_zcy_df = tmp_zcy_df[
                ['test_time', 'created_by_ue_time', 'x', 'y', 'longitude', 'latitude', 'altitude']]
        tmp_zcy_df = in_deal_zcy_char_csv_file(tmp_zcy_df)
        # 获取时间戳前十位
        tmp_zcy_df['created_by_ue_time'] = Common.get_top_ten_data(tmp_zcy_df['created_by_ue_time'])

        in_ue_df = Common.read_csv_get_df(in_ue_file)

        res_zcy_ue_merge_df = WalkTour.merge_ue_zcy_data(in_ue_df, tmp_zcy_df)
        return res_zcy_ue_merge_df

    @staticmethod
    def get_merge_ue_table_data(in_ue_file, in_table_file):
        in_ue_df = Common.read_csv_get_df(in_ue_file)
        in_table_df = Common.read_csv_get_df(in_table_file)
        res_tmp_merge_df = pd.merge(in_ue_df, in_table_df, left_on="PC Time", right_on="PCTime", how='left')
        return res_tmp_merge_df

    # 合并ue和zcy
    @staticmethod
    def wifi_bluetooth_merge_ue_zcy_data(in_ue_df, in_zcy_df):
        in_ue_df['ts'] = Time.convert_datetime_to_seconds(in_ue_df['PC Time'])
        print_with_line_number(f"test log PC Time时间戳为：{in_ue_df['ts'][0]}")
        in_zcy_df['ts'] = in_zcy_df['f_time']
        # in_zcy_df['ts'] = Time.convert_datetime_to_seconds(in_zcy_df['test_time'])
        print_with_line_number(f"走测仪f_time为：{in_zcy_df['ts'][0]}")
        tmp_df = pd.merge(in_ue_df, in_zcy_df)
        return tmp_df

    # 合并ue和zcy
    @staticmethod
    def merge_ue_zcy_data(in_ue_df, in_zcy_df):
        in_ue_df['ts'] = Time.convert_datetime_to_seconds(in_ue_df['PC Time'])
        print_with_line_number(f"test log PC Time时间戳为：{in_ue_df['ts'][0]}")
        in_zcy_df['ts'] = in_zcy_df['created_by_ue_time']
        # in_zcy_df['ts'] = Time.convert_datetime_to_seconds(in_zcy_df['test_time'])
        print_with_line_number(f"走测仪test_time为：{in_zcy_df['ts'][0]}")
        tmp_df = pd.merge(in_ue_df, in_zcy_df, how='left')
        return tmp_df

    @staticmethod
    def deal_5g_df_data(log_df_5g, in_wifi_flag=False):
        # 删除测试log中 秒级重复数据，同秒取第一条
        # log_df_5g = log_df_5g.groupby(log_df_5g['ts']).first().reset_index()  # 删除测试log中 秒级重复数据，同秒取第一条。

        pattern = re.compile(r'NCell(\d)(\d)')
        # 使用正则表达式匹配并替换列名
        log_df_5g.columns = [pattern.sub(lambda x: f'NCell{x.group(1)}', col) for col in log_df_5g.columns]

        i = 0
        while True:
            i += 1
            if f'NCell{i} -Beam NARFCN' in log_df_5g.columns:
                log_df_5g = log_df_5g.rename(
                    columns={
                        f'NCell{i} -Beam NARFCN': f'f_freq_n{i}',
                        f'NCell{i} -Beam PCI': f'f_pci_n{i}',
                        f'NCell{i} -Beam SS-RSRP': f'f_rsrp_n{i}',
                        f'NCell{i} -Beam SS-RSRQ': f'f_rsrq_n{i}',
                        f'NCell{i} -Beam SS-SINR': f'f_sinr_n{i}',
                    })
            else:
                break

        log_df_5g = log_df_5g.rename(
            columns={
                'IMSI': 'f_imsi',
                'IMEI': 'f_imei',
                'NCI': 'f_cell_id',
                'ts': 'f_time',
                'PCell1 -Beam PCI': 'f_pci',
                'PCell1 -Beam NARFCN': 'f_freq',
                'PCell1 -Beam SS-RSRP': 'f_rsrp',
                'PCell1 -Beam SS-RSRQ': 'f_rsrq',
                'PCell1 -Beam SS-SINR': 'f_sinr',
                'PC Time': 'pc_time',
            })

        # 重命名zcy数据
        log_df_5g = log_df_5g.rename(
            columns={
                'altitude': 'f_altitude',
                'direction': 'f_direction',
            })

        if 'f_longitude' not in log_df_5g.columns:
            log_df_5g = log_df_5g.rename(
                columns={
                    'Longitude': 'f_longitude',
                    'Latitude': 'f_latitude',
                })

        # 删除重复行
        log_df_5g = Common.delete_duplicate_columns(log_df_5g)

        # 时间转上海时区
        sh_timez = Common.change_time_zone(log_df_5g['f_time'])
        log_df_5g['f_time_1'] = sh_timez
        # 生成finger_id
        finger_id = Common.generate_finger_id(log_df_5g['f_time_1'], log_df_5g['f_msisdn'])
        log_df_5g['finger_id'] = finger_id
        # 置空 UEMR 数据
        log_df_5g = Common.fill_in_empty_header(log_df_5g)

        log_df_5g['f_imsi'] = np.array(log_df_5g['f_imsi'])
        log_df_5g['f_gnb_id'] = log_df_5g['f_cell_id'] // 4096
        # SID暂时都赋值1
        log_df_5g['f_sid'] = 1
        log_df_5g['f_pid'] = (log_df_5g.index + 1).astype(str)
        log_df_5g[['f_year', 'f_month', 'f_day']] = log_df_5g['f_time'].apply(
            Time.convert_seconds_to_datetime_day_year_month).to_list()
        log_df_5g['f_eci'] = log_df_5g['f_cell_id']

        if in_wifi_flag:
            out_data_table = OutDataTableFormat.WalkTour5G + OutDataTableFormat.WIFI_BlueTooth
        else:
            out_data_table = OutDataTableFormat.WalkTour5G

        log_df_5g = log_df_5g.reindex(columns=out_data_table)
        # 获取领区数
        num_list = Common.get_cell_number(log_df_5g)
        log_df_5g['f_neighbor_cell_number'] = num_list
        Common.data_filling(log_df_5g, 'f_cell_id')

        log_df_5g = log_df_5g.rename(str.lower, axis='columns')
        return log_df_5g

    @staticmethod
    def deal_4g_df_data(log_df_4g, in_wifi_flag=False):
        # 删除测试log中 秒级重复数据，同秒取第一条。
        # Common.delete_df_duplicate_data_by_second(log_df_4g, log_df_4g['ts'])

        pattern = re.compile(r'NCell(\d)(\d)')
        # 使用正则表达式匹配并替换列名
        log_df_4g.columns = [pattern.sub(lambda x: f'NCell{x.group(1)}', col) for col in log_df_4g.columns]

        cell_cnt = 0
        while True:
            cell_cnt += 1
            if f'NCell{cell_cnt} EARFCN' in log_df_4g.columns:
                log_df_4g = log_df_4g.rename(
                    columns={
                        f'NCell{cell_cnt} EARFCN': f'f_freq_n{cell_cnt}',
                        f'NCell{cell_cnt} PCI': f'f_pci_n{cell_cnt}',
                        f'NCell{cell_cnt} RSRP': f'f_rsrp_n{cell_cnt}',
                        f'NCell{cell_cnt} RSRQ': f'f_rsrq_n{cell_cnt}',
                    })
            else:
                break

        # 重命名table数据
        log_df_4g = log_df_4g.rename(
            columns={
                'IMSI': 'f_imsi',  # table
                'IMEI': 'f_imei',  # table
            })
        # 重命名zcy数据
        log_df_4g = log_df_4g.rename(
            columns={
                'altitude': 'f_altitude',
                'direction': 'f_direction',
            })

        if 'f_longitude' not in log_df_4g.columns:
            log_df_4g = log_df_4g.rename(
                columns={
                    'Longitude': 'f_longitude',
                    'Latitude': 'f_latitude',
                })

        # 重命名ue数据
        log_df_4g = log_df_4g.rename(
            columns={
                'PCell ECI': 'f_cell_id',
                'ts': 'f_time',
                'PCell PCI': 'f_pci',
                'PCell EARFCN': 'f_freq',
                'PCell RSRP': 'f_rsrp',
                'PCell RSRQ': 'f_rsrq',
                'PC Time': 'pc_time',
            })

        # 删除重复列
        log_df_4g = Common.delete_duplicate_columns(log_df_4g)

        # 时间转上海时区
        sh_timez = Common.change_time_zone(log_df_4g['f_time'])
        log_df_4g['f_time_1'] = sh_timez
        # 生成finger_id
        finger_id = Common.generate_finger_id(log_df_4g['f_time_1'], log_df_4g['f_msisdn'])
        log_df_4g['finger_id'] = finger_id
        # 置空 UEMR 数据
        log_df_4g = Common.fill_in_empty_header(log_df_4g)

        log_df_4g['f_enb_id'] = log_df_4g['f_cell_id'] // 256
        log_df_4g[['f_year', 'f_month', 'f_day']] = log_df_4g['f_time'].apply(
            Time.convert_seconds_to_datetime_day_year_month).to_list()

        log_df_4g['f_eci'] = log_df_4g['f_cell_id']

        # SID暂时都赋值1
        log_df_4g['f_sid'] = 1
        log_df_4g['f_pid'] = (log_df_4g.index + 1).astype(str)

        if in_wifi_flag:
            out_data_table = OutDataTableFormat.WalkTour4G + OutDataTableFormat.WIFI_BlueTooth
        else:
            out_data_table = OutDataTableFormat.WalkTour4G

        log_df_4g = log_df_4g.reindex(columns=out_data_table)
        # 计算领区数
        cell_number = Common.get_cell_number(log_df_4g)
        log_df_4g['f_neighbor_cell_number'] = cell_number
        Common.data_filling(log_df_4g, 'f_cell_id')

        log_df_4g = log_df_4g.rename(str.lower, axis='columns')
        return log_df_4g


class CommonFunc:
    tf_for_gps_to_utm = Transformer.from_crs(4326, 32750)
    tf_for_utm_to_gps = Transformer.from_crs(32750, 4326)

    @staticmethod
    def transform_gps_to_utm(longitude, latitude):
        utm_x, utm_y = CommonFunc.tf_for_gps_to_utm.transform(latitude, longitude)
        return utm_x, utm_y

    @staticmethod
    def transform_utm_to_gps(x, y):
        longitude, latitude = CommonFunc.tf_for_utm_to_gps.transform(x, y)
        return longitude, latitude

    @staticmethod
    def transform_x_y_list_to_gps(zero_point_lon, zero_point_lat, x_list, y_list):
        """
        转换xy坐标到经纬度
        :param:
            zero_point_lon : 原点经度
            zero_point_lat : 原点维度
            x_list : 待转换的x坐标列表
            y_list : 待转换的y坐标列表
        :return:
            lon_list: 经度
            lat_list: 纬度
        """
        zero_point_utm_x, zero_point_utm_y = CommonFunc.transform_gps_to_utm(zero_point_lon, zero_point_lat)
        cov_len = len(x_list)
        x_list_utm = np.zeros(cov_len, dtype=np.float64)
        y_list_utm = np.zeros(cov_len, dtype=np.float64)
        for i in range(cov_len):
            x_list_utm[i] = zero_point_utm_x + x_list[i]
            y_list_utm[i] = zero_point_utm_y + y_list[i]
        lon_list = np.zeros(cov_len, dtype=np.float64)
        lat_list = np.zeros(cov_len, dtype=np.float64)
        for i in range(cov_len):
            lon_list[i], lat_list[i] = CommonFunc.transform_utm_to_gps(x_list_utm[i], y_list_utm[i])
        return lon_list, lat_list

    @staticmethod
    def find_square_edges(points):
        # 应用PCA
        pca = PCA(n_components=1)  # 我们只需要第一个主成分
        pca.fit(points)

        # 得到第一个主成分方向
        component = pca.components_[0]

        # 计算该方向与x轴正方向之间的夹角
        angle = np.arctan2(component[1], component[0])

        # 旋转反向得到垂直于长边的斜率k
        slope = -1 / np.tan(angle)

        # 计算长边的截距b
        # 取所有点在第一个主成分方向上的投影的平均值来估计中心位置，
        # 然后计算中心点在垂直方向上的截距
        center_projection = pca.transform(pca.mean_.reshape(1, -1))[0][0]
        intercept = pca.mean_[1] - slope * pca.mean_[0]

        return slope, intercept

    @staticmethod
    def LineFit(x, y):
        points = np.zeros((len(x), 2), dtype=np.float64)
        for i in range(len(x)):
            points[i][0] = x[i]
            points[i][1] = y[i]
        k, b = CommonFunc.find_square_edges(points)
        return k, b

    @staticmethod
    def get_vector_degree(vector1, vector2):
        cos_angle = vector1.dot(vector2) / (np.linalg.norm(vector1) * np.linalg.norm(vector2))
        degree = math.degrees(math.acos(cos_angle))
        return degree

    # 顺时针旋转
    @staticmethod
    def S_rotate(angle, in_x, in_y, point_x, point_y):
        in_x = np.array(in_x)
        in_y = np.array(in_y)
        sRotate_x = (in_x - point_x) * math.cos(angle) + (in_y - point_y) * math.sin(angle) + point_x
        sRotate_y = (in_y - point_y) * math.cos(angle) - (in_x - point_x) * math.sin(angle) + point_y
        return sRotate_x, sRotate_y

    # 逆时针旋转
    @staticmethod
    def N_rotate(angle, in_x, in_y, point_x, point_y):
        in_x = np.array(in_x)
        in_y = np.array(in_y)
        sRotate_x = (in_x - point_x) * math.cos(angle) - (in_y - point_y) * math.sin(angle) + point_x
        sRotate_y = (in_x - point_x) * math.sin(angle) + (in_y - point_y) * math.cos(angle) + point_y
        return sRotate_x, sRotate_y

    @staticmethod
    def rotate_point_by_radian(x, y, radian, is_s):
        if is_s:
            x, y = CommonFunc.S_rotate(radian, x, y, 0.0, 0.0)
        else:
            x, y = CommonFunc.N_rotate(radian, x, y, 0.0, 0.0)
        return x, y

    @staticmethod
    def rotate_point_by_angle(x, y, angle, is_s):
        # 将角度转换为弧度
        radian = math.radians(angle)
        return CommonFunc.rotate_point_by_radian(x, y, radian, is_s)

    @staticmethod
    def cov_x_y_to_real_x_y(x_list, y_list):
        # 校正基准向量
        list_base = [0.0, 1.0]
        vector2 = np.array(list_base)
        k, b = CommonFunc.LineFit(x_list, y_list)
        list_to = [1, k]
        vector1 = np.array(list_to)
        degree = CommonFunc.get_vector_degree(vector2, vector1)
        if degree < 90:
            degree = 90 - degree
            ans = CommonFunc.rotate_point_by_angle(x_list, y_list, degree, True)
        elif degree == 90:
            return x_list, y_list
        else:
            degree = degree - 90
            ans = CommonFunc.rotate_point_by_angle(x_list, y_list, degree, False)
        return ans[0], ans[1]

    @staticmethod
    def data_conversion(in_value, df_data):
        delta_d = df_data.max() - df_data.min()
        t_x = in_value / delta_d
        n_values = df_data * t_x
        res_dat = n_values - n_values.min()
        return res_dat

    @staticmethod
    def find_center(points):

        # 检测方向 - 使用PCA来找到数据的主方向
        pca = PCA(n_components=2)  # 我们要检测到二维数据集的两个主成分
        pca.fit(points)

        # 得到旋转后的坐标系中的点的坐标
        transformed_points = pca.transform(points)

        # 在新坐标系中计算中心点
        center_point_transformed = np.mean(transformed_points, axis=0)

        # 将中心点坐标转换回原来的坐标系
        center_point = pca.inverse_transform(center_point_transformed)

        return center_point

    @staticmethod
    def move_xy_to_real_xy(x_list, y_list, center_need_x, center_need_y):
        points = np.zeros((len(x_list), 2), dtype=np.float64)
        for i in range(len(x_list)):
            points[i][0] = x_list[i]
            points[i][1] = y_list[i]
        center_point = CommonFunc.find_center(points)
        # 剩下的就简单了，中心点和中心点需要的位置都知道了，那么就可以直接平移了
        x_out = np.zeros((len(x_list)), dtype=np.float64)
        y_out = np.zeros((len(x_list)), dtype=np.float64)
        x_move = center_need_x - center_point[0]
        y_move = center_need_y - center_point[1]
        for i in range(len(x_list)):
            x_out[i] = x_list[i] + x_move
            y_out[i] = y_list[i] + y_move
        return x_out, y_out


class OutDataTableFormat:
    WalkTour4G = ['finger_id', 'f_province', 'f_city', 'f_district', 'f_street',
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

    WalkTour5G = ['finger_id', 'f_province', 'f_city', 'f_district', 'f_street',
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

    WeTest4G = ['finger_id', 'f_province', 'f_city', 'f_district', 'f_street',
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

    WeTest5G = ['finger_id', 'f_province', 'f_city', 'f_district', 'f_street',
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

    WIFI_BlueTooth = ['f_wifi_name_1', 'f_wifi_mac_1', 'f_wifi_rssi_1',
                      'f_wifi_freq_1', 'f_wifi_name_2', 'f_wifi_mac_2', 'f_wifi_rssi_2',
                      'f_wifi_freq_2', 'f_wifi_name_3', 'f_wifi_mac_3', 'f_wifi_rssi_3',
                      'f_wifi_freq_3', 'f_wifi_name_4',
                      'f_wifi_mac_4', 'f_wifi_rssi_4', 'f_wifi_freq_4', 'f_wifi_name_5',
                      'f_wifi_mac_5', 'f_wifi_rssi_5', 'f_wifi_freq_5', 'f_wifi_name_6',
                      'f_wifi_mac_6', 'f_wifi_rssi_6', 'f_wifi_freq_6', 'f_wifi_name_7',
                      'f_wifi_mac_7', 'f_wifi_rssi_7', 'f_wifi_freq_7', 'f_wifi_name_8',
                      'f_wifi_mac_8', 'f_wifi_rssi_8', 'f_wifi_freq_8', 'f_bluetooth_mac_1',
                      'f_bluetooth_uuid_1', 'f_bluetooth_rssi_1', 'f_bluetooth_mac_2',
                      'f_bluetooth_uuid_2', 'f_bluetooth_rssi_2', 'f_bluetooth_mac_3',
                      'f_bluetooth_uuid_3', 'f_bluetooth_rssi_3', 'f_bluetooth_mac_4',
                      'f_bluetooth_uuid_4', 'f_bluetooth_rssi_4', 'f_bluetooth_mac_5',
                      'f_bluetooth_uuid_5', 'f_bluetooth_rssi_5', 'f_bluetooth_mac_6',
                      'f_bluetooth_uuid_6', 'f_bluetooth_rssi_6', 'f_bluetooth_mac_7',
                      'f_bluetooth_uuid_7', 'f_bluetooth_rssi_7', 'f_bluetooth_mac_8',
                      'f_bluetooth_uuid_8', 'f_bluetooth_rssi_8', ]


class Common:
    # 传入文件list,压缩文件
    @staticmethod
    def zip_file(in_file_list, in_zip_file):
        tmp_zip_file = zipfile.ZipFile(in_zip_file, 'w')
        for i_f in in_file_list:
            tmp_zip_file.write(i_f, os.path.basename(i_f))
        tmp_zip_file.close()

    # 把路径切分成list
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

    # 获取某列第一个非空值的index
    @staticmethod
    def get_first_valid_value(in_df_column):
        tmp_index = in_df_column.first_valid_index()
        return tmp_index

    @staticmethod
    # 判断df某列的数据是否都相同
    def is_column_all_same(in_df, in_column_name):
        return in_df[in_column_name].nunique() == 1

    # 获取int数据的前十位
    @staticmethod
    def get_top_ten_data(in_df):
        # tmp_res = in_df.apply(lambda a: np.int64(str(a)[:10]))
        tmp_res = in_df.apply(lambda a: np.int64(str(a)[:10]) if str(a) and len(str(a)) >= 10 else None)
        return tmp_res

    # 检查df是否为空
    @staticmethod
    def check_df_empty(in_df):
        return in_df.empty

    # 检查时间戳是否正确
    @staticmethod
    def check_valid_timestamp(timestamp):
        try:
            # 尝试将时间戳转换为 datetime 对象
            datetime.fromtimestamp(timestamp)
            return True
        except (OSError, ValueError):
            # 如果发生 ValueError，则时间戳无效
            return False

    # 判断文件存在
    @staticmethod
    def check_file_exists(in_file):
        return os.path.exists(in_file)

    # 检查路径，如果不存在则创建
    @staticmethod
    def check_path(in_path):
        if not os.path.exists(in_path):
            # os.mkdir(in_path)
            try:
                os.makedirs(in_path, exist_ok=True)
            except OSError as e:
                print_with_line_number(f'创建目录报错：{e}')

    # wifi 蓝牙生成输出文件名称
    @staticmethod
    def wifi_bluetooth_generate_output_file_name(in_data_path, in_df, in_n_scene, in_test_type):
        # 时间转换
        name_d_time = str(in_df['test_time'][0]).split(' ')[0]
        if '-' in name_d_time:
            name_d_time = name_d_time[name_d_time.find('-'):].replace('-', '')
        else:
            name_d_time = name_d_time[name_d_time.find('/'):].replace('/', '')
        # print('name_d_time: ', name_d_time)

        district = in_df['f_district'][0]
        if '海淀' in district:
            n_are = 'HaiDian'
        elif '朝阳' in district:
            n_are = 'CaoYang'
        else:
            n_are = 'DaXin'
        # print(district)
        if in_n_scene:
            tmp_out_file_name = f'{n_are}_{in_n_scene}_{in_test_type}_data_{name_d_time}'
            # tmp_out_file_name = f'{in_net_type}_{n_are}_{in_n_scene}_{in_test_type}_LOG_DT_UE_{n_dev_id}_{name_d_time}'
        else:
            tmp_out_file_name = f'{n_are}_{in_test_type}_data_{name_d_time}'
            # tmp_out_file_name = f'{in_net_type}_{n_are}_{in_test_type}_LOG_DT_UE_{n_dev_id}_{name_d_time}'

        tmp_out_file = os.path.join(in_data_path, tmp_out_file_name)
        # print('tmp_out_file: ', tmp_out_file)
        return tmp_out_file

    # 生成输出文件名称
    @staticmethod
    def uemr_generate_output_file_name(in_data_path, in_df, in_net_type, in_test_type):
        name_d_time = in_df['pc_time'][0].split(' ')[0]
        if '-' in name_d_time:
            name_d_time = name_d_time[name_d_time.find('-'):].replace('-', '')
        else:
            name_d_time = name_d_time[name_d_time.find('/'):].replace('/', '')

        district = in_df['f_district'][0]
        if '海淀' in district:
            n_are = 'HaiDian'
        elif '朝阳' in district:
            n_are = 'CaoYang'
        else:
            n_are = 'DaXin'

        tmp_out_file_name = f'{in_net_type}_{n_are}_{in_test_type}_{name_d_time}'

        tmp_out_file = os.path.join(in_data_path, tmp_out_file_name)
        return tmp_out_file

    # 生成输出文件名称
    @staticmethod
    def generate_output_file_name(in_data_path, in_df, in_net_type, in_n_scene, in_test_type):
        name_d_time = in_df['pc_time'][0].split(' ')[0]
        if '-' in name_d_time:
            name_d_time = name_d_time[name_d_time.find('-'):].replace('-', '')
        else:
            name_d_time = name_d_time[name_d_time.find('/'):].replace('/', '')
        # print('name_d_time: ', name_d_time)

        district = in_df['f_district'][0]
        if '海淀' in district:
            n_are = 'HaiDian'
        elif '朝阳' in district:
            n_are = 'CaoYang'
        else:
            n_are = 'DaXin'
        # print(district)
        if in_n_scene:
            tmp_out_file_name = f'{in_net_type}_{n_are}_{in_n_scene}_{in_test_type}_LOG_DT_UE_{name_d_time}'
            # tmp_out_file_name = f'{in_net_type}_{n_are}_{in_n_scene}_{in_test_type}_LOG_DT_UE_{n_dev_id}_{name_d_time}'
        else:
            tmp_out_file_name = f'{in_net_type}_{n_are}_{in_test_type}_LOG_DT_UE_{name_d_time}'
            # tmp_out_file_name = f'{in_net_type}_{n_are}_{in_test_type}_LOG_DT_UE_{n_dev_id}_{name_d_time}'

        tmp_out_file = os.path.join(in_data_path, tmp_out_file_name)
        # print('tmp_out_file: ', tmp_out_file)
        return tmp_out_file

    # UEMR设置场景信息
    @staticmethod
    def uemr_set_scene_data(log_df, in_config):
        log_df['f_device_brand'] = in_config.get_device_brand()
        log_df['f_device_model'] = in_config.get_device_model()
        log_df['f_area'] = in_config.get_area()
        log_df['f_floor'] = in_config.get_floor()
        log_df['f_scenario'] = in_config.get_scenario()
        log_df['f_province'] = in_config.get_province()
        log_df['f_city'] = in_config.get_city()
        log_df['f_district'] = in_config.get_district()
        log_df['f_street'] = in_config.get_street()
        log_df['f_building'] = in_config.get_building()
        log_df['f_prru_id'] = in_config.get_prru_id()
        # log_df['f_source'] = in_config.get_source()
        return log_df

    # 设置场景信息
    @staticmethod
    def set_scene_data(log_df, in_config):
        # 设置场景信息
        log_df['f_device_brand'] = in_config.get_device_brand()
        log_df['f_device_model'] = in_config.get_device_model()
        log_df['f_area'] = in_config.get_area()
        log_df['f_floor'] = in_config.get_floor()
        log_df['f_scenario'] = in_config.get_scenario()
        log_df['f_province'] = in_config.get_province()
        log_df['f_city'] = in_config.get_city()
        log_df['f_district'] = in_config.get_district()
        log_df['f_street'] = in_config.get_street()
        log_df['f_building'] = in_config.get_building()
        log_df['f_prru_id'] = in_config.get_prru_id()
        log_df['f_source'] = in_config.get_source()
        log_df['f_msisdn'] = in_config.get_msisdn()
        return log_df

    # 上填充方式，填充数据
    @staticmethod
    def data_filling(in_df, column_name):
        temp = np.nan
        index = []
        for i in range(len(in_df)):
            value = in_df.loc[i, column_name]  # 获取列每行的值
            if str(value) != 'nan':
                # 临时存储用于填充
                temp = in_df.loc[i, column_name]
            elif str(temp) == 'nan':
                # 将下标存储用于向上填充
                index.append(i)
            else:
                # 将临时变量的值进行填充
                in_df.loc[i, column_name] = temp
            if str(value) != 'nan':
                for k in index:
                    # 避免第一个为空的情况，向上填充
                    in_df.loc[k, column_name] = value
                index.clear()
        return in_df.reset_index(drop=True)

    # 获取领区数
    @staticmethod
    def get_cell_number(log_df):
        tmp_num_list = []
        for i in range(len(log_df)):
            tmp_num_list.append(np.count_nonzero(
                log_df[['f_rsrp_n1', 'f_rsrp_n2', 'f_rsrp_n3', 'f_rsrp_n4', 'f_rsrp_n5',
                        'f_rsrp_n6', 'f_rsrp_n7', 'f_rsrp_n8']].isnull().values[i] == False))
        return tmp_num_list

    # 填充空字符
    @staticmethod
    def fill_in_empty_header(log_df):
        log_df['f_roaming_type'] = ''
        log_df['f_phr'] = ''
        log_df['f_enb_received_power'] = ''
        log_df['f_ta'] = ''
        log_df['f_aoa'] = ''
        return log_df

    # 生成结果数据中得finger_id
    @staticmethod
    def generate_finger_id(time_df, msi_df):
        res_id = 'F' + time_df.dt.strftime('%Y%m%d%H') + '_' + msi_df.str[-4:]
        return res_id

    @staticmethod
    def change_time_zone(deal_df, in_timezone='Asia/Shanghai'):
        res_df = pd.to_datetime(deal_df, unit='s').dt.tz_localize(pytz.utc).dt.tz_convert(
            pytz.timezone(in_timezone))
        return res_df

    @staticmethod
    def delete_duplicate_columns(log_df):
        # 删除重复列
        log_df = log_df.loc[:, ~log_df.columns.duplicated()]
        return log_df

    @staticmethod
    def read_csv_get_df(in_df_path):
        try:
            in_df = pd.read_csv(in_df_path, low_memory=False)
            return in_df
        except FileNotFoundError as e:
            print_error(f'读取文件,报错{e}')
            exit()

    # 走测仪数据转经纬度
    @staticmethod
    def data_conversion(in_value, df_data):
        delta_d = df_data.max() - df_data.min()
        t_x = in_value / delta_d
        n_values = df_data * t_x
        res_dat = n_values - n_values.min()
        return res_dat

    # 走测仪数据生成png图片
    @staticmethod
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

    @staticmethod
    def df_write_to_csv(w_df, w_file):
        w_df.to_csv(w_file, index=False)


class FTPHelper:
    def __init__(self, host='116.6.50.82', username='dingliftp', password='dingliftp2023-2024', port=21):
        self.ftp = FTP()
        self.host = host
        self.username = username
        self.password = password
        self.port = port

    def connect(self):
        self.ftp.connect(self.host, self.port, timeout=5)
        self.ftp.login(self.username, self.password)

    def reconnect(self):
        self.ftp.close()
        self.ftp.connect(self.host, self.port)
        self.ftp.login(self.username, self.password)

    def ftp_upload(self, in_local_file, in_save_dir=r'/var/www/html/walkingindoor_data/MR'):
        self.ftp.cwd(in_save_dir)
        tmp_res_list = Common.split_path_get_list(in_local_file)
        with open(in_local_file, 'rb') as file:
            # 上传文件到远程服务器
            self.ftp.storbinary(f'STOR {tmp_res_list[-1]}', file)


class DealData:
    def __init__(self, in_config):
        self.config = in_config
        self.lon = self.config.get_lon_value()
        self.lat = self.config.get_lat_value()
        self.x = self.config.get_len_x_value()
        self.y = self.config.get_len_y_value()

    def deal_wifi_bluetooth_csv_file(self, in_wifi_bluetooth_file):
        zcy_path = os.path.join(os.path.dirname(in_wifi_bluetooth_file), 'zcy_data')
        Common.check_path(zcy_path)

        wifi_bluetooth_df = Common.read_csv_get_df(in_wifi_bluetooth_file)

        res_x1_values = Common.data_conversion(self.x, wifi_bluetooth_df['f_x'])
        lon = res_x1_values / (111000 * math.cos(self.lon / 180 * math.pi)) + self.lon
        lon = 2 * max(lon) - lon

        res_y1_values = Common.data_conversion(self.y, wifi_bluetooth_df['f_y'])
        lat = res_y1_values / 111000 + self.lat

        wifi_bluetooth_df['f_x'] = res_x1_values
        wifi_bluetooth_df['f_y'] = res_y1_values
        wifi_bluetooth_df['f_longitude'] = lon
        wifi_bluetooth_df['f_latitude'] = lat

        out_f = os.path.basename(in_wifi_bluetooth_file).split(".")[0] + f'_xyToLonLat_WIFI_BlueTooth.csv'
        try:
            Common.df_write_to_csv(wifi_bluetooth_df, os.path.join(zcy_path, out_f))
        except PermissionError as e:
            print_with_line_number(f'写文件报错：{e}')

        Common.generate_images(res_x1_values, res_y1_values, lon, lat, zcy_path, '_wifi_蓝牙')

        # 赛选wifi 蓝牙数据
        res_wifi_bluetooth_df = wifi_bluetooth_df.drop(
            ['f_x', 'f_y', 'f_longitude', 'f_latitude', 'f_direction', 'f_altitude'], axis=1)
        return res_wifi_bluetooth_df

    def deal_zcy_char_csv_file(self, char_df):
        res_x = Common.data_conversion(self.x, char_df['x'])
        res_y = Common.data_conversion(self.y, char_df['y'])

        res_lat, res_lon = CommonFunc.transform_x_y_list_to_gps(self.lon, self.lat, res_x, res_y)
        char_df['f_longitude'] = res_lon
        char_df['f_latitude'] = res_lat
        char_df['f_x'] = res_x
        char_df['f_y'] = res_y

        # 删除列
        columns_to_delete = ['x', 'y']
        res_char_data = char_df.drop(columns_to_delete, axis=1)

        return res_char_data

    def get_wifi_bluetooth_data(self):
        print_with_line_number('获取wifi 蓝牙数据')
        # 获取char数据
        in_char_file = self.config.get_char_file()
        # 获取wifi 蓝牙数据
        in_wifi_bluetooth_file = self.config.get_wifi_bluetooth_file()

        if not Common.check_file_exists(in_char_file):
            print_with_line_number(f'error, 文件：{in_char_file} 不存在')
            return
        elif not Common.check_file_exists(in_wifi_bluetooth_file):
            print_with_line_number(f'error, 文件：{in_wifi_bluetooth_file} 不存在')
            return

        print_with_line_number(f'获取char数据：{in_char_file}')
        # 读取所有的zcy数据
        tmp_zcy_df = Common.read_csv_get_df(in_char_file)

        # 旋转纠偏和水平纠偏
        print_with_line_number('开始旋转纠偏')
        x1, y1 = CommonFunc.cov_x_y_to_real_x_y(tmp_zcy_df['x'], tmp_zcy_df['y'])
        # 水平平移纠偏
        print_with_line_number('开始水平纠偏')
        x1_1, y1_1 = CommonFunc.move_xy_to_real_xy(x1, y1, 800, 600)

        tmp_zcy_df['x'] = x1_1
        tmp_zcy_df['y'] = y1_1

        # 读取char数据中的某些列
        if 'direction' in tmp_zcy_df.columns:
            print_with_line_number('读取zcy direction数据')
            tmp_zcy_df = tmp_zcy_df[
                ['test_time', 'created_by_ue_time', 'x', 'y', 'direction', 'altitude']]
        else:
            tmp_zcy_df = tmp_zcy_df[
                ['test_time', 'created_by_ue_time', 'x', 'y', 'altitude']]

        print_with_line_number(f'获取wifi蓝牙数据：{in_wifi_bluetooth_file}')
        tmp_wifi_bluetooth_df = Common.read_csv_get_df(in_wifi_bluetooth_file)

        tmp_wifi_bluetooth_df = tmp_wifi_bluetooth_df.drop(
            ['f_x', 'f_y', 'f_longitude', 'f_latitude', 'f_direction', 'f_altitude'], axis=1)

        # 获取数据前十位
        tmp_wifi_bluetooth_df['f_time'] = Common.get_top_ten_data(tmp_wifi_bluetooth_df['f_time'])
        tmp_zcy_df['created_by_ue_time'] = Common.get_top_ten_data(tmp_zcy_df['created_by_ue_time'])
        # 获取zcy，wifi 蓝牙合并数据
        tmp_merger_df = pd.merge(tmp_wifi_bluetooth_df, tmp_zcy_df, left_on="f_time", right_on="created_by_ue_time",
                                 how='left')
        tmp_merger_df = self.deal_zcy_char_csv_file(tmp_merger_df)
        return tmp_merger_df

    def deal_wifi_bluetooth(self):
        print_with_line_number('----数据处理开始----')
        # 建立输出目录
        cur_path_output = os.path.join(os.path.dirname(self.config.get_wifi_bluetooth_file()), 'output')
        Common.check_path(cur_path_output)

        # 确定生成文件类型
        data_type = 'finger'
        if 'uemr'.lower() == self.config.get_data_type().lower():
            data_type = 'uemr'
        elif 'finger'.lower() != self.config.get_data_type().lower():
            print_with_line_number(
                f'config.ini文件中，data_type字段填写错误,实际值：{self.config.get_data_type()}，期望值：uemr或者finger')
            return

        # 获取wifi 蓝牙合并数据
        print_with_line_number('----step 1:获取待处理数据----')
        n_scene = 'indoor'
        res_wifi_bluetooth_df = self.get_wifi_bluetooth_data()

        if res_wifi_bluetooth_df is None or Common.check_df_empty(res_wifi_bluetooth_df):
            print_with_line_number(
                'wifi_bluetooth数据的f_time时间戳和char数据的created_by_ue_time时间戳关联不上，合并之后为空')
            return

        # 保存一份处理之前的数据(添加接口)
        cur_path_out_file = os.path.join(cur_path_output, f'wifi_bluetooth_{n_scene}_{data_type}_原始文件.csv')
        print_with_line_number(f'生成原始df数据文件：{cur_path_out_file}')
        try:
            Common.df_write_to_csv(res_wifi_bluetooth_df, cur_path_out_file)
        except PermissionError as e:
            print_with_line_number(f'写文件报错：{e}')

        # 设置场景信息
        print_with_line_number('----step 2:设置场景信息----')
        res_wifi_bluetooth_df = Common.set_scene_data(res_wifi_bluetooth_df, self.config)

        res_wifi_bluetooth_df = res_wifi_bluetooth_df.rename(
            columns={
                'altitude': 'f_altitude',
                'x': 'f_x',
                'y': 'f_y',
                # 'direction': 'f_direction',
            })

        if 'direction' in res_wifi_bluetooth_df.columns:
            res_wifi_bluetooth_df = res_wifi_bluetooth_df.rename(
                columns={
                    'direction': 'f_direction',
                })

        # 生成输出文件名称
        print_with_line_number('----step 3:输出df数据为csv文件----')
        out_file = Common.wifi_bluetooth_generate_output_file_name(cur_path_output, res_wifi_bluetooth_df, n_scene,
                                                                   'wifi_bluetooth')

        if 'uemr'.lower() == data_type:
            res_wifi_bluetooth_df.rename(columns=lambda x: x.replace('f_', 'u_'), inplace=True)
            res_wifi_bluetooth_df = res_wifi_bluetooth_df.rename(
                columns={
                    'finger_id': 'uemr_id',
                })

        print_with_line_number(f'当前处理数据为：wifi_bluetooth {n_scene} {data_type} 数据')
        print_with_line_number(f'输出结果文件为：{out_file}' + f'_{data_type}.csv')

        try:
            Common.df_write_to_csv(res_wifi_bluetooth_df, out_file + f'_{data_type}.csv')
        except PermissionError as e:
            print_with_line_number(f'写文件报错：{e}')

    def deal_walk_tour(self):
        # 处理walktour数据
        print_with_line_number('----开始处理walk_tour数据-----')

        # 创建输出目录
        cur_path_output = os.path.join(os.path.dirname(self.config.get_ue_file()), 'output')
        Common.check_path(cur_path_output)
        print_with_line_number(f'输出目录：{cur_path_output}')

        print_with_line_number('----step 1:获取原始df数据----')
        # 获取室内室外的数据
        if 'indoor'.lower() == self.config.get_test_area().lower():
            n_scene = 'indoor'
            print_with_line_number(f'获取 indoor 数据')
            zcy_ue_merge_df = WalkTour.indoor_get_df_zcy(self.config, self.deal_zcy_char_csv_file)

            if zcy_ue_merge_df is None or Common.check_df_empty(zcy_ue_merge_df):
                print_with_line_number(
                    'wifi_bluetooth数据的created_by_ue_time时间戳和test_log数据的PC Time时间戳关联不上，合并之后为空')
                return
        elif 'outdoor'.lower() == self.config.get_test_area().lower():
            n_scene = 'outdoor'
            print_with_line_number(f'获取 outdoor 数据')
            zcy_ue_merge_df = WalkTour.outdoor_get_df(self.config)
        else:
            print_with_line_number(
                f'config.ini文件中，test_area字段填写错误,实际值：{self.config.get_test_area()}，期望值：indoor或者outdoor')
            return

        # 设置场景信息
        print_with_line_number('----step 2:设置场景信息----')
        zcy_ue_merge_df = Common.set_scene_data(zcy_ue_merge_df, self.config)

        # 处理数据
        net_type = zcy_ue_merge_df['Network Type'][0]
        print_with_line_number(f'当前数据网络类型为：{net_type}')

        print_with_line_number('----step 3:处理df数据----')
        if 'LTE' == net_type:
            net_type = '4G'
            zcy_ue_merge_df = WalkTour.deal_4g_df_data(zcy_ue_merge_df)
        elif 'NR' == net_type or 'ENDC' == net_type:
            net_type = '5G'
            zcy_ue_merge_df = WalkTour.deal_5g_df_data(zcy_ue_merge_df)

        # 生成输出文件名称
        out_file = Common.generate_output_file_name(cur_path_output, zcy_ue_merge_df, net_type, n_scene, 'WT')

        data_type = 'finger'
        if 'uemr'.lower() == self.config.get_data_type().lower():
            data_type = 'uemr'
            zcy_ue_merge_df.rename(columns=lambda x: x.replace('f_', 'u_'), inplace=True)
            zcy_ue_merge_df = zcy_ue_merge_df.rename(
                columns={
                    'finger_id': 'uemr_id',
                })

        print_with_line_number(f'当前处理数据为：WalkTour {net_type} {n_scene} {data_type} 数据')
        print_with_line_number(f'输出结果文件为：{out_file}' + f'_{data_type}.csv')
        try:
            Common.df_write_to_csv(zcy_ue_merge_df, out_file + f'_{data_type}.csv')
        except PermissionError as e:
            print_with_line_number(f'写文件报错：{e}')

    def deal_wetest(self):
        print_with_line_number('----开始处理wetest数据----')

        # 生成输出目录
        cur_path_output = os.path.join(os.path.dirname(self.config.get_ue_file()), 'output')
        Common.check_path(cur_path_output)
        print_with_line_number(f'输出目录：{cur_path_output}')

        # 获取室内数据或者室外数据
        print_with_line_number('----step 1:获取原始df数据----')
        if 'indoor'.lower() == self.config.get_test_area().lower():
            n_scene = 'indoor'
            print_with_line_number(f'获取 indoor 数据')
            res_ue_df = WeTest.indoor_get_df(self.config, self.deal_zcy_char_csv_file)

            if res_ue_df is None or Common.check_df_empty(res_ue_df):
                print_with_line_number(
                    'char数据的created_by_ue_time时间戳和test_log数据的create_time时间戳关联不上，合并之后为空')
                return

        elif 'outdoor'.lower() == self.config.get_test_area().lower():
            n_scene = 'outdoor'
            print_with_line_number(f'获取 outdoor 数据')
            res_ue_df = WeTest.outdoor_get_df(self.config)
        else:
            print_with_line_number(
                f'config.ini文件中，test_area字段填写错误,实际值：{self.config.get_test_area()}，期望值：indoor或者outdoor')
            return

        # 设置场景信息
        print_with_line_number('----step 2:设置场景信息----')
        res_ue_df = Common.set_scene_data(res_ue_df, self.config)

        net_type = ''
        # 处理数据
        print_with_line_number('----step 3:处理df数据----')
        if 'lte_enb_id' in res_ue_df.columns:
            net_type = '4G'
            res_ue_df = WeTest.deal_wetest_4g(res_ue_df)
        elif 'nr_gnb_id' in res_ue_df.columns:
            net_type = '5G'
            res_ue_df = WeTest.deal_wetest_5g(res_ue_df)

        # 生成输出文件名称
        print_with_line_number('----step 4:输出df数据为csv文件----')
        out_file = Common.generate_output_file_name(cur_path_output, res_ue_df, net_type, n_scene, 'WeTest')

        data_type = 'finger'
        if 'uemr'.lower() == self.config.get_data_type().lower():
            data_type = 'uemr'
            res_ue_df.rename(columns=lambda x: x.replace('f_', 'u_'), inplace=True)
            res_ue_df = res_ue_df.rename(
                columns={
                    'finger_id': 'uemr_id',
                })

        print_with_line_number(f'当前处理数据为：WeTest {net_type} {n_scene} {data_type} 数据')
        print_with_line_number(f'输出结果文件为：{out_file}' + f'_{data_type}.csv')
        try:
            Common.df_write_to_csv(res_ue_df, out_file + f'_{data_type}.csv')
        except PermissionError as e:
            print_with_line_number(f'写文件报错：{e}')

    def dealUEMR(self):
        print_with_line_number('----开始处理UEMR数据----')
        # 建立输出目录
        cur_path_output = os.path.join(os.path.dirname(self.config.get_UEMR_file()), 'output')
        Common.check_path(cur_path_output)

        # 确定生成文件类型
        data_type = 'finger'
        if 'uemr'.lower() == self.config.get_data_type().lower():
            data_type = 'uemr'
        elif 'finger'.lower() != self.config.get_data_type().lower():
            print_with_line_number(
                f'config.ini文件中，data_type字段填写错误,实际值：{self.config.get_data_type()}，期望值：uemr或者finger')
            return

        # 获取uemr文件名称
        UEMR_file = self.config.get_UEMR_file()

        if not Common.check_file_exists(UEMR_file):
            print_with_line_number(f'error, 文件：{UEMR_file} 不存在')
            return

        print_with_line_number(f'UEMR_file: {UEMR_file}')

        # 读取UEMR数据
        if UEMR_file.endswith(".xlsx"):
            uemr_df = pd.read_excel(UEMR_file, header=0)
        elif UEMR_file.endswith(".csv"):
            uemr_df = Common.read_csv_get_df(UEMR_file)
        else:
            print_with_line_number(f'UEMR文件格式错误，实际文件：{UEMR_file}, 期望文件为：csv或者xlsx文件')
            return

        # 设置场景信息
        print_with_line_number('----step 2:设置场景信息----')
        uemr_df = Common.uemr_set_scene_data(uemr_df, self.config)

        # 处理UEMR数据
        if 'uemr.imsi' in uemr_df.columns:
            # print_with_line_number('当前处理：4G UEMR 数据')
            net_type = '4G'
            res_df = DealUEMR.deal_UEMR_4g(uemr_df)
        elif 'uemr5g.imsi' in uemr_df.columns:
            # print_with_line_number('当前处理：5G UEMR 数据')
            net_type = '5G'
            res_df = DealUEMR.deal_UEMR_5g(uemr_df)
        else:
            print_with_line_number(
                'UEMR数据中，未找到 uemr.imsi 字段，也未找到 uemr5g.imsi 字段，无法分辨数据为4G还是5G数据')
            return

        if 'finger' == data_type:
            if Common.check_file_exists(self.config.get_char_file()):
                print_with_line_number('---开始合并走测仪数据---')
                res_df = DealUEMR.uemr_merge_char(res_df, self)
                if '5G' == net_type:
                    res_df = res_df.reindex(columns=OutDataTableFormat.WalkTour5G)
                else:
                    res_df = res_df.reindex(columns=OutDataTableFormat.WalkTour4G)
            else:
                print_with_line_number(
                    f'error, char 文件:{self.config.get_char_file()}，不存在，finger数据不合并走测仪数据')

        # 生成输出文件名称
        print_with_line_number('----step 3:输出df数据为csv文件----')
        out_file = Common.uemr_generate_output_file_name(cur_path_output, res_df, net_type, 'UEMR')

        if 'uemr'.lower() == data_type:
            res_df.rename(columns=lambda x: x.replace('f_', 'u_'), inplace=True)
            res_df = res_df.rename(
                columns={
                    'finger_id': 'uemr_id',
                })

        print_with_line_number(f'当前处理数据为：UEMR {net_type} {data_type} 数据')
        print_with_line_number(f'输出结果文件为：{out_file}' + f'_{data_type}.csv')
        try:
            Common.df_write_to_csv(res_df, out_file + f'_{data_type}.csv')
        except PermissionError as e:
            print_with_line_number(f'写文件报错：{e}')

    def deal_wifi_bluetooth_merge_ue(self):
        print_with_line_number('----开始处理walktour合并wifi、蓝牙数据-----')
        cur_path_output = os.path.join(os.path.dirname(self.config.get_ue_file()), 'output')
        Common.check_path(cur_path_output)

        data_type = 'finger'
        if 'uemr'.lower() == self.config.get_data_type().lower():
            data_type = 'uemr'
        elif 'finger'.lower() != self.config.get_data_type().lower():
            print_with_line_number(
                f'config.ini文件中，data_type字段填写错误,实际值：{self.config.get_data_type()}，期望值：uemr或者finger')
            return

        # 获取室内室外的数据
        print_with_line_number('----step 1:获取原始df数据----')
        n_scene = 'indoor'
        res_merge_df = WalkTour.indoor_get_df_wifi_bluetooth_zcy(self.config, self)

        if res_merge_df is None or Common.check_df_empty(res_merge_df):
            print_with_line_number('wifi_bluetooth数据的f_time时间戳和test_log数据的PC Time时间戳关联不上，合并之后为空')
            return

        # 设置场景信息
        print_with_line_number('----step 2:设置场景信息----')
        zcy_ue_merge_df = Common.set_scene_data(res_merge_df, self.config)

        # 处理数据
        net_type = zcy_ue_merge_df['Network Type'][0]
        print_with_line_number(f'当前数据网络类型为：{net_type}')
        print_with_line_number('----step 3:处理df数据----')
        if 'LTE' == net_type:
            net_type = '4G'
            zcy_ue_merge_df = WalkTour.deal_4g_df_data(zcy_ue_merge_df, in_wifi_flag=True)
        elif 'NR' == net_type:
            net_type = '5G'
            zcy_ue_merge_df = WalkTour.deal_5g_df_data(zcy_ue_merge_df, in_wifi_flag=True)

        # 生成输出文件名称
        print_with_line_number('----step 4:输出df数据为csv文件----')
        out_file = Common.generate_output_file_name(cur_path_output, zcy_ue_merge_df, net_type, n_scene,
                                                    'WT_wifi_bluetooth')

        if 'uemr'.lower() == data_type:
            zcy_ue_merge_df.rename(columns=lambda x: x.replace('f_', 'u_'), inplace=True)
            zcy_ue_merge_df = zcy_ue_merge_df.rename(
                columns={
                    'finger_id': 'uemr_id',
                })

        print_with_line_number(f'当前处理数据为：WT_wifi_bluetooth {net_type} {n_scene} {data_type} 数据')
        print_with_line_number(f'输出结果文件为：{out_file}' + f'_{data_type}.csv')
        try:
            Common.df_write_to_csv(zcy_ue_merge_df, out_file + f'_{data_type}.csv')
        except PermissionError as e:
            print_with_line_number(f'写文件报错：{e}')


def print_with_line_number(message):
    # 获取当前行号
    current_line = inspect.currentframe().f_back.f_lineno
    # 使用 f-string 格式化字符串，包含文件名和行号信息
    print(f"{os.path.basename(__file__)}:{current_line} - {message}")


def print_error(message):
    current_line = inspect.currentframe().f_back.f_lineno
    # 使用 f-string 格式化字符串，包含文件名和行号信息
    input(f"{os.path.basename(__file__)}:{current_line} - {message}")


if __name__ == '__main__':
    while True:
        demo_config_file = input("请输入配置文件（使用绝对路径）,或输入out退出: ").strip()

        if 'out' == demo_config_file.lower():
            break

        while True:
            if not os.path.exists(demo_config_file):
                demo_config_file = input("输入的配置文件不存在，请重新输入（使用绝对路径）: ")
            else:
                break

        # 读取配置文件
        config_project = Config(demo_config_file)
        deal_data = DealData(config_project)
        if 'WalkTour' == config_project.data_type:
            deal_data.deal_walk_tour()  # 处理walk tour数据
        elif 'WeTest' == config_project.data_type:
            deal_data.deal_wetest()  # 处理wetest数据
        elif 'WIFI_BlueTooth' == config_project.data_type:
            if Common.check_file_exists(
                    config_project.get_ue_file()):
                print_with_line_number('----处理wifi 蓝牙和 test log数据----')
                deal_data.deal_wifi_bluetooth_merge_ue()  # 处理walk tour 和wifi 蓝牙数据
            else:
                print_with_line_number('----只处理wifi 蓝牙数据----')
                deal_data.deal_wifi_bluetooth()  # 处理 wifi 蓝牙数据
        elif 'DealUEMR' == config_project.data_type:
            deal_data.dealUEMR()  # 处理UEMR数据
        else:
            print_with_line_number(config_project.data_type)
