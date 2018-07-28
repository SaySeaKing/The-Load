# -*- coding:utf-8 -*-

from __future__ import unicode_literals
import sys
from elasticsearch import Elasticsearch, helpers
from itertools import islice
reload(sys)
sys.setdefaultencoding("utf-8")

# helpers.scan()
class ElasticSearchUtil(object):
    def __init__(self, host):
        self.host = host
        self.conn = Elasticsearch([self.host])

    def __del__(self):
        self.close()

    def check(self):
        """
        输出当前系统的ES信息
        :return:
        """
        return self.conn.info()

    def update_settings(self, settings, doc_index=None, cover=True):
        """
        :param settings: settings
        :param doc_index: 待设置的index
        :param cover: 是否将已设置的index进行settings配置更新, 默认更新
        :return: 对全局变量的conn进行设置
        """
        if cover:
            self.conn.indices.put_settings(body=settings, index=doc_index)
        else:
            if not self.conn.indices.exists(index=doc_index):
                self.conn.indices.create(index=doc_index)
                self.conn.indices.put_settings(body=settings, index=doc_index)

    def update_mappings(self, mappings, doc_index=None, doc_type=None, cover=True):
        """
        :param mappings: mappings配置
        :param doc_index:  待设置的index
        :param doc_type: 待设置的type
        :param cover: 是否将已设置的index的type进行mappings配置更新, 默认更新
        :return: 对全局变量的conn进行设置
        """
        if cover:
            self.conn.indices.put_mapping(doc_type=doc_type, body=mappings, index=doc_index, params={"update_all_types": "true"})
        else:
            if not self.conn.indices.exists_type(index=doc_index, doc_type=doc_type):
                self.conn.indices.put_mapping(doc_type=doc_type, body=mappings, index=doc_index, params={"update_all_types": "true"})

    def reindex(self, old, new):
        # 获取基础数据
        old_doc_index = old["doc_index"]
        old_doc_type = old["doc_type"]
        new_doc_index = new["doc_index"]
        new_doc_type = new["doc_type"]
        new_mappings = new["mappings"]
        # 创建alias
        self.conn.indices.put_alias(index=old_doc_index, name="%s_v1" % old_doc_index)
        # 导入新数据
        doc_query = {
            "size": 0,
            "query": {
                "match_all": {}
            }
        }
        result = self.search_DocByQuery(doc_query, doc_index=old_doc_index, doc_type=old_doc_type)
        doc_query["size"] = result["hits"]["total"]
        result = self.search_DocByQuery(doc_query, doc_index=old_doc_index, doc_type=old_doc_type)
        dataList = [x["_source"] for x in result["hits"]["hits"]]
        self.insert_Documents(dataList, doc_index=new_doc_index, doc_type=new_doc_type, mapping=new_mappings)

        # 更新alias
        alias = {
            "actions": [
                {"remove": {"index": old_doc_index, "alias": "%s_v1" % old_doc_index}},
                {"add": {"index": new_doc_index, "alias": "%s_v2" % old_doc_index}},
            ]
        }
        # self.conn.indices.update_aliases

    def insert_Document(self, doc_index, doc_type, doc_body, doc_id=None):
        """
        插入一条数据body到指定的index、指定的type下;可指定Id,若不指定,ES会自动生成
        :param doc_index: 待插入的index值
        :param doc_type: 待插入的type值
        :param doc_body: 待插入的数据 -> dict型
        :param doc_id: 自定义Id值
        :return:
        """
        return self.conn.index(index=doc_index, doc_type=doc_type, body=doc_body, id=None)

    def insert_Documents(self, dataList, doc_index=None, doc_type=None, doc_id=None, mapping=None):
        """
        批量插入接口;
        bulk接口所要求的数据列表结构为:[{{optionType}: {Condition}}, {data}]
        其中optionType可为index、delete、update
        Condition可设置每条数据所对应的index值和type值
        data为具体要插入/更新的单条数据
        :param doc_index: 默认插入的index值
        :param doc_type: 默认插入的type值
        :param dataList: 待插入数据集
        :return:
        """
        settings = {
            "index": {
                "max_result_window": "100000000"
            }
        }
        c = islice(range(len(dataList)), 0, None, 10000)
        try:
            end = c.next()
        except:
            end = None
        while True:
            flag = False
            start = end
            try:
                end = c.next()
            except:
                end = None
                flag = True
            dataList_ = dataList[start:end]
            insertHeadInfoList = [
                {"index": {"_index": doc_index, "_type": doc_type, "_id": doc_id}} for i in range(len(dataList_))
            ]
            doc_body = [dict] * (len(dataList_) * 2)
            doc_body[::2] = insertHeadInfoList
            doc_body[1::2] = dataList_
            try:
                self.update_settings(settings, doc_index=doc_index, cover=False)
                if mapping != None:
                    self.update_mappings(mapping, doc_index=doc_index, doc_type=doc_type, cover=False)
                self.conn.bulk(body=doc_body)
            except Exception, e:
                return str(e)
            if flag:
                break

    def delete_DocById(self, doc_index, doc_type, doc_id):
        """
        删除指定index、type、id对应的数据
        :param doc_index:
        :param doc_type:
        :param doc_id:
        :return:
        """
        return self.conn.delete(index=doc_index, doc_type=doc_type, id=doc_id)

    def delete_DocByQuery(self, doc_body_query, doc_index=None, doc_type=None):
        """
        删除idnex下符合条件query的所有数据
        :param doc_index:
        :param doc_body_query: 满足DSL语法格式
        :param doc_type:
        :return:
        """
        return self.conn.delete_by_query(index=doc_index, body=doc_body_query, doc_type=doc_type)

    def delete_Index(self, doc_index):
        """
        :param doc_index: 需要删除的index
        :return:
        """
        index_exists_flag = self.conn.indices.exists(index=doc_index)
        if index_exists_flag:
            self.conn.indices.delete(index=doc_index)
        else:
            print "%s does not exists!" % doc_index

    def search_DocByQuery(self, doc_body_query, doc_index=None, doc_type=None, params={}):
        """
        查找index下所有符合条件的数据
        :param doc_index:
        :param doc_type:
        :param doc_body_query: 筛选语句,符合DSL语法格式
        :return:
        """
        return self.conn.search(index=doc_index, doc_type=doc_type, body=doc_body_query, params=params)

    def search_DocByScroll(self, doc_body_query, doc_index=None, doc_type=None, scroll='5m'):
        """
        :param doc_body_query: 查询DSL语句
        :param doc_index:
        :param doc_type:
        :param scroll:
        :return:
        """
        return helpers.scan(self.conn, query=doc_body_query, scroll=scroll, index=doc_index, doc_type=doc_type)

    def search_DocById(self, doc_index, doc_type, doc_id):
        """
        获取指定index、type、id对应的数据
        :param doc_index:
        :param doc_type:
        :param doc_id:
        :return:
        """
        return self.conn.get(index=doc_index, doc_type=doc_type, id=doc_id)

    def update_DocById(self, doc_index, doc_type, doc_id, doc_body=None):
        """
        更新指定index、type、id所对应的数据
        :param doc_index:
        :param doc_type:
        :param doc_id:
        :param doc_body: 待更新的值
        :return:
        """
        return self.conn.update(index=doc_index, doc_type=doc_type, id=doc_id, body=doc_body)

    def update_DocByQuery(self, doc_body_query, doc_index=None, doc_type=None, doc_id=None):
        """
        更新指定index、type、id所对应的数据
        :param doc_index:
        :param doc_type:
        :param doc_id:
        :param doc_body_query: 待更新的值
        :return:
        """
        return self.conn.update(index=doc_index, doc_type=doc_type, id=doc_id, body=doc_body_query)

    def close(self):
        if self.conn is not None:
            try:
                self.conn.close()
            except Exception, e:
                pass
            finally:
                self.conn = None