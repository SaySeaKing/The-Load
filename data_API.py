# -*- coding:utf-8 -*-

from __future__ import unicode_literals
import sys
import csv
import openpyxl
import os
import socket, struct
import re
from itertools import islice
import datetime
import copy
import json
reload(sys)
sys.setdefaultencoding('utf-8')

try:
    from _SQL_Util import Sql_Operat
except:
    print "[Error] Package Error"

class data_Util(object):
    def __init__(self):
        pass

    def open_csv(self, file):
        """
        :param file: 文件名
        :return: 以list形式返回csv格式文件的内容
        """
        f = open(file)
        reader = csv.reader(f, delimiter=','.encode('utf-8'))
        data_source = []
        flag = True
        for row in reader:
            if flag:
                flag = False
                continue
            row_container = []
            for cell in row:
                try:
                    cell = int(float(cell))
                    row_container.append(cell)
                    continue
                except:
                    pass
                row_container.append(cell.decode('gbk').encode('utf-8'))
            data_source.append(row_container)
        f.close()
        return data_source

    def open_excel(self, file, sheet):
        """
        :param file: 文件名
        :param sheet: sheet名
        :return: 以list形式返回xlsx文件内容
        """
        wb = openpyxl.load_workbook(file)
        ws = wb.get_sheet_by_name(sheet)
        data_source = []
        for row in list(ws.rows)[1:]:
            temp = [cell.value for cell in row]
            data_source.append(temp)
        wb.close()
        return data_source

    def get_data_from_file(self, dict_file):
        """
        :param dict_file:需要获取数据的文件描述
        eg: dict_file = {
                "file_name" = "123.xlsx",
                "file_type" = "xlsx",
                "file_field" = ["IP", "处理结果", "所属分行"]
            }
        :return: 以list形式的文件内容,和以list形式的文件内容所对应的标题
        """
        title = []
        data_source = []
        title = dict_file["file_field"]
        try:
            if dict_file["file_type"] == "xlsx":
                if type(dict_file["file_name"]) == list:
                    data_source = []
                    if dict_file["file_name"] == []:
                        print "无'%s'数据, 默认数据为空" % dict_file["file_source"]
                    else:
                        for file in dict_file["file_name"]:
                            data_source += self.open_excel(file, dict_file["sheet_name"])
                else:
                    data_source = self.open_excel(dict_file["file_name"], dict_file["sheet_name"])
            elif dict_file["file_type"] == "csv":
                if type(dict_file["file_name"]) == list:
                    data_source = []
                    for file in dict_file["file_name"]:
                        data_source += self.open_excel(file, dict_file["sheet_name"])
                else:
                    data_source = self.open_csv(dict_file["file_name"])
        except:
            print "获取'%s'数据失败, 默认数据为空" % dict_file["file_name"]
            data_source = []
        return title, data_source

    def get_data_from_sql(self, dict_sql):
        """
        :param dict_sql: 从数据库获取数据的sql配置
        eg: dict_sql = {
                "sql_filed" = ["ip", "处理结果"],
                "sql_type" = "mysql",
                "ip" = "0.0.0.0",
                "port" = "3306",
                "db" = "testdb",
                "user" = "test",
                "pwd" = "pwd"
                "sql" = [
                    [select, "select ip, handle_result from table"]
                ]
            }
        :return:
        """
        title = dict_sql["sql_field"]
        data_source = Sql_Operat(dict_sql["sql_type"]).run(dict_sql["ip"], dict_sql["user"], dict_sql["pwd"], dict_sql["port"], dict_sql["db"], dict_sql["sql"])
        return title, data_source

    def mate_single_ac(self, asset, asset_title, field, data):
        field_index = asset_title.index(field)
        for a in asset:
            if a[field_index] == data:
                result = a[:field_index] + a[field_index+1:]
                if len(result) != 1:
                    return tuple(result)
                b = list(copy.deepcopy(a))
                b.remove(a[field_index])
                return b[0]
        return

    def mate_ip_net(self, asset, asset_title, field, data):
        field_index = asset_title.index(field)
        for a in asset:
            if self.ip_in_subnet(data, a[field_index]):
                result = a[:field_index] + a[field_index+1:]
                if len(result) != 1:
                    return tuple(result)
                b = list(copy.deepcopy(a))
                b.remove(a[field_index])
                return b[0]
        return

    def get_Asset(self, asset_all):
        asset = set()
        asset_all = asset_all
        try:
            asset_field_title = asset_all[2]
            wb_asset = openpyxl.load_workbook(asset_all[0])
            ws_asset = wb_asset.get_sheet_by_name(asset_all[1])
            asset_title = [cell.value for cell in list(ws_asset)[0]]
            for sys in list(ws_asset.rows)[1:]:
                temp = []
                for field in asset_field_title:
                    temp.append(sys[asset_title.index(field)].value)
                asset.add(tuple(temp))
            wb_asset.close()
            print u"****%s资产获取完成****" % asset_all[1]
        except IOError, e:
            print e
            print u"[-] 获取%s资产错误" % asset_all[1]
            raise exit()
        return asset, asset_field_title

    def ip_in_subnet(self, ip, subnet):
        try:
            ip = ip.replace(" ", "")
            ip = ip.replace("\r", "")
            ip = ip.replace("\n", "")
            subnet = subnet.replace(" ", "")
            subnet = subnet.replace("\r", "")
            subnet = subnet.replace("\n", "")
            subnet = self.format_subnet(str(subnet))
            subnet_array = subnet.split("/")
            ip = self.format_subnet(ip + "/" + subnet_array[1])
            return ip == subnet
        except:
            return False

    def format_subnet(self, subnet_input):
        if subnet_input.find("/") == -1:
            return subnet_input + "/255.255.255.255"
        else:
            subnet = subnet_input.split("/")
            if len(subnet[1]) < 3:
                mask_num = int(subnet[1])
                last_mask_num = mask_num % 8
                last_mask_str = ""
                for i in range(last_mask_num):
                    last_mask_str += "1"
                if len(last_mask_str) < 8:
                    for i in range(8 - len(last_mask_str)):
                        last_mask_str += "0"
                last_mask_str = str(int(last_mask_str, 2))
                if mask_num / 8 == 0:
                    subnet = subnet[0] + "/" + last_mask_str + "0.0.0"
                elif mask_num / 8 == 1:
                    subnet = subnet[0] + "/255." + last_mask_str + ".0.0"
                elif mask_num / 8 == 2:
                    subnet = subnet[0] + "/255.255." + last_mask_str + ".0"
                elif mask_num / 8 == 3:
                    subnet = subnet[0] + "/255.255.255." + last_mask_str
                elif mask_num / 8 == 4:
                    subnet = subnet[0] + "/255.255.255.255"
                subnet_input = subnet
            subnet_array = subnet_input.split("/")
            subnet_true = socket.inet_ntoa(struct.pack("!I", struct.unpack("!I", socket.inet_aton(subnet_array[0]))[0] &
                                                       struct.unpack("!I", socket.inet_aton(subnet_array[1]))[
                                                           0])) + "/" + subnet_array[1]
            return subnet_true