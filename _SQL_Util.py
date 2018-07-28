# -*- coding:utf-8 -*-

import pymssql
import pymysql

class Sql_Operat(object):
    def __init__(self, sql_type):
        self.sql_type = sql_type
        self.retlist = None

    def get_Conn(self, host, user, pwd, port, db):
        try:
            if self.sql_type == "mysql":
                self.conn = pymysql.connect(host=host, user=user, password=pwd, port=port, db=db, charset='utf8')
            if self.sql_type == "mssql":
                self.conn = pymssql.connect(host=host, user=user, password=pwd, database=db, charset='utf8', port=port)
            self.cur = self.conn.cursor()
        except IOError, e:
            print e
            exit()

    def sql_update_Act(self, sql):
        try:
            self.cur.execute(sql)
            self.conn.commit()
        except:
            self.cur.close()
            self.conn.close()
            print "[SQL_update_Error] %s" % sql
            raise exit()

    def sql_insert_Act(self, sql):
        try:
            self.cur.execute(sql)
            self.conn.commit()
        except:
            self.cur.close()
            self.conn.close()
            print "[SQL_Insert_Error] %s" % sql
            raise exit()

    def sql_delete_Act(self, sql):
        try:
            self.cur.execute(sql)
            self.conn.commit()
        except:
            self.cur.close()
            self.conn.close()
            print "[SQL_Delete_Error] %s" % sql
            raise exit()

    def sql_search_Act(self, sql):
        try:
            self.cur.execute(sql)
            self.retlist = self.cur.fetchall()
            self.conn.commit()
        except:
            self.cur.close()
            self.conn.close()
            print "[SQL_Search_Error] %s" % sql
            raise exit()

    def run(self, host, user, pwd, port, db, sql):
        self.get_Conn(host, user, pwd, port, db)
        for s in sql:
            if s[0] == "insert":
                self.sql_insert_Act(s[1])
            if s[0] == "delete":
                self.sql_delete_Act(s[1])
            if s[0] == "update":
                self.sql_update_Act(s[1])
            if s[0] == "search":
                self.sql_search_Act(s[1])
        self.cur.close()
        self.conn.close()
        return self.retlist