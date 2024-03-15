# -*- coding: utf-8 -*-

import configparser
import math
import os

from Common import clear_path, get_file_by_string, unzip, copy_file, read_csv_get_df, data_conversion, df_write_to_csv, \
    generate_images, check_path, get_data_path_by_char


def unzip_zcy_zip_file(in_path):
    in_extraction_path = os.path.join(in_path, 'unzip')
    clear_path(in_extraction_path)
    # shutil.rmtree(in_extraction_path)  # 清理输出目录
    # 解压所有的zip
    in_zip_file = get_file_by_string('zip', in_path)
    # print('zip_file: ', in_zip_file)
    # 调用函数解压ZIP文件到当前目录
    unzip(in_zip_file, in_extraction_path)


def copy_zcy_unzip_file(in_path):
    in_unzip_path = os.path.join(in_path, 'unzip')
    in_output_path = os.path.join(in_path, 'output')
    check_path(in_output_path)
    for root, dirs, files in os.walk(in_unzip_path):
        for file in files:
            if '-chart' in file or '_pci_' in file or '_WiFi_BlueTooth' in file:
                file_path = os.path.join(root, file)
                print('file_path: ', file_path)
                if os.path.exists(os.path.join(in_path, file)):
                    os.remove(os.path.join(in_path, file))
                copy_file(file_path, in_output_path)


def copy_char_file(in_path):
    in_unzip_path = os.path.join(in_path, 'unzip')
    # in_output_path = os.path.join(in_path, 'output')
    # check_path(in_output_path)
    for root, dirs, files in os.walk(in_unzip_path):
        for file in files:
            if '-chart' in file or '_pci_' in file:
                file_path = os.path.join(root, file)
                # print('file_path: ', file_path)
                if os.path.exists(os.path.join(in_path, file)):
                    os.remove(os.path.join(in_path, file))
                copy_file(file_path, in_path)


# 读取配置文件
def read_config_file(in_config_path=r'D:\working\merge'):
    # in_con_file = os.path.join(in_config_path, 'config.ini')
    in_con_file = os.path.join(r'D:\working\merge', 'config.ini')
    config = configparser.ConfigParser()
    # config.read(in_config_file, encoding='UTF-8')
    config.read(in_con_file, encoding='GBK')
    in_lon_O = config.get('Coordinates', 'lon_O')
    in_lat_O = config.get('Coordinates', 'lat_O')
    in_len_east_x = config.get('Coordinates', 'len_east_x')
    in_len_north_y = config.get('Coordinates', 'len_north_y')
    return float(in_lon_O), float(in_lat_O), float(in_len_east_x), float(in_len_north_y)


def deal_char(in_path):
    # 获取配置文件信息
    lon_O, lat_O, len_east_x, len_north_y = read_config_file(in_path)
    # 处理char数据
    char_file = get_file_by_string('-chart', in_path)
    print('char_file: ', char_file)
    char_df = read_csv_get_df(char_file)

    res_x1_values = data_conversion(len_east_x, char_df['x'])
    lon = res_x1_values / (111000 * math.cos(lon_O / 180 * math.pi)) + lon_O
    lon = 2 * max(lon) - lon

    res_y1_values = data_conversion(len_north_y, char_df['y'])
    lat = res_y1_values / 111000 + lat_O

    char_df['f_x'] = res_x1_values
    char_df['f_y'] = res_y1_values
    char_df['f_longitude'] = lon
    char_df['f_latitude'] = lat

    # 删除列
    columns_to_delete = ['map_width_pixel', 'map_height_pixel', 'map_width_cm', 'map_height_cm']
    char_data = char_df.drop(columns_to_delete, axis=1)

    out_f = char_file.split(".")[0] + f'_xyToLonLat_ZCY.csv'
    df_write_to_csv(char_data, os.path.join(in_path, out_f))

    generate_images(res_x1_values, res_y1_values, lon, lat, in_path, '_走侧仪')


def deal_wifi_bluetooth(in_path):
    # 获取配置文件信息
    lon_O, lat_O, len_east_x, len_north_y = read_config_file(in_path)
    # 处理char数据
    wifi_bluetooth_file = get_file_by_string('_WiFi_BlueTooth', in_path)
    print('wifi_bluetooth_file: ', wifi_bluetooth_file)
    char_df = read_csv_get_df(wifi_bluetooth_file)

    res_x1_values = data_conversion(len_east_x, char_df['f_x'])
    lon = res_x1_values / (111000 * math.cos(lon_O / 180 * math.pi)) + lon_O
    lon = 2 * max(lon) - lon

    res_y1_values = data_conversion(len_north_y, char_df['f_y'])
    lat = res_y1_values / 111000 + lat_O

    char_df['f_x'] = res_x1_values
    char_df['f_y'] = res_y1_values
    char_df['f_longitude'] = lon
    char_df['f_latitude'] = lat

    # out_f = wifi_bluetooth_file.split(".")[0] + f'_{formatted_date}_xyToLonLat_WIFI_BlueTooth.csv'
    out_f = wifi_bluetooth_file.split(".")[0] + f'_xyToLonLat_WIFI_BlueTooth.csv'
    df_write_to_csv(char_df, os.path.join(in_path, out_f))

    generate_images(res_x1_values, res_y1_values, lon, lat, in_path, '_wifi_蓝牙')


# # 解压，拷贝，然后处理char生成走测仪数据
# def unzip_zcy_data(in_path, in_output_path):
#     unzip_zcy_zip_file(in_path)
#     copy_zcy_unzip_file(in_path, in_output_path)
#     # 处理char数据，生成走测仪数据
#     deal_char(in_path, in_output_path)

# 解压，拷贝，然后处理char生成走测仪数据
def unzip_zcy_data(in_path):
    unzip_zcy_zip_file(in_path)
    in_output_path = os.path.join(in_path, 'output')
    check_path(in_output_path)
    copy_zcy_unzip_file(in_path)
    # 处理char数据，生成走测仪数据
    deal_char(in_path)


# 解压，拷贝，然后处理char生成走测仪数据
def unzip_zcy_wifi_data(in_path):
    unzip_zcy_zip_file(in_path)
    copy_zcy_unzip_file(in_path)
    # 处理char数据，生成走测仪数据
    deal_char(in_path)
    deal_wifi_bluetooth(in_path)


# 解压拷贝char文件
def unzip_copy(in_path):
    unzip_zcy_zip_file(in_path)
    copy_char_file(in_path)


if __name__ == '__main__':
    folder_path = r'D:\MrData\3月4号\下午'
    res_list = get_data_path_by_char(folder_path)
    # print(res_list)
    for i_path in res_list:
        print(i_path)
        unzip_copy(i_path)
