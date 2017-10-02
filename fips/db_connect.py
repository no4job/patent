__author__ = 'mdu'
#from itertools import chain
import common_config
import mysql.connector
import datetime
from mysql.connector import Error
import functools
#import configparser
#import os.path


class Database():
    #connection = None
    #cursor = None
    def __init__(self, **kwargs):
        self.host = kwargs.get('HOST')
        self.port = kwargs.get('PORT')
        self.database = kwargs.get('NAME')
        self.user = kwargs.get('USER')
        self.password = kwargs.get('PASSWORD')
        # self.autocommit = True
        self.autocommit = False
        self.connect()

    def connect(self):
        try:
            self.connection = mysql.connector.connect(host=self.host,
                                                      port=self.port,
                                                      database=self.database,
                                                      user=self.user,
                                                      password=self.password,
                                                      autocommit=self.autocommit
                                                      )
            if self.connection.is_connected():
                self.connection_id = self.connection.connection_id
                print("Database connection with ID = %s established." % str(self.connection_id))
                self.cursor = self.connection.cursor()
                #self.cursor_d = self.connection.cursor(dictionary=True)
        except Error as e:
            print(e)
            raise

    def truncate_table(self,table_list):
        for table in table_list:
            sql = "truncate table {}".format(table)
            self.cursor.execute(sql)

    def transaction(self, query_list,sql_strings=True):
        try:
            if sql_strings:
                for query_str in query_list:
                    self.cursor.execute(query_str)
            else:
                for query in query_list:
                    query["func"](**query["data"])
            self.connection.commit()
        except Error as e:
            print ("Transaction failed, rolling back. Error was:")
            print (e.args)
            try:  # empty exception handler in case rollback fails
                self.connection.rollback()
                raise
            except:
                pass
    def query(self, query, params=None,commit = False):
        try:
            self.cursor.execute(query, params)
        except Error as e:
            print(e)
            raise
        finally:
            # if common_config.DEBUG:
            #     print(self.cursor.statement)
            print(self.cursor.statement)
        return self.cursor
    def __enter__(self):
        return self

    def insert_p(self, table, data, commit = False):
        """Insert a record"""
        query = self._serialize_insert(data)
        sql = "INSERT %s (%s) VALUES(%s)" % (table, query[0], query[1])

        #return self.query(sql, data.values()).rowcount
        return self.query(sql, data,commit=commit).rowcount

    def insert(self, **kwargs):
        """Insert a record"""
        if "commit" in kwargs:
            commit= kwargs.get('commit')
        else:
            commit = False
        table=kwargs.get('table')
        data=kwargs.get('data')
        query = self._serialize_insert(data)
        sql = "INSERT %s (%s) VALUES(%s)" % (table, query[0], query[1])
        #return self.query(sql, data.values()).rowcount
        return self.query(sql, data,commit=commit).rowcount

    def _serialize_insert(self, data):
        """Format insert dict values into strings"""
        kv=data.items()
        keys = ",".join( k[0] for k in kv )
        vals = ",".join(["%%(%s)s" % k[0] for k in kv])
        return [keys, vals]

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            #self.conn.rollback()
            pass
        try:
            #self.connection.commit()
            pass
        except Exception:
            #self.connection.rollback()
            pass
        finally:
            self.connection.close()
            if not self.connection.is_connected():
                print("Database connection with ID = %s closed." % str(self.connection_id))


if __name__ == "__main__":
    #ts=datetime.datetime(2015, 6, 11, 19, 29, 50, 974279)
    #data ={"skill_id":"NULL", "name":'name25', "mode":1, "active":2, "creation_time":"NULL"}
    data ={"name":'name25', "mode":1, "active":2}
    #data ={"NULL", "name25", 1, 2, "NULL"}
    with Database(**common_config.DATABASES['default']) as db:
        #print(db.query("select CONNECTION_ID()",None).fetchone())
        table_list = ["patent","patent_f_125","patent_f_126","patent_inid_15","patent_inid_21","patent_inid_22", \
                      "patent_inid_23","patent_inid_31","patent_inid_32","patent_inid_51","patent_inid_56", \
                      "patent_inid_71","patent_inid_73","patent_inid_74"]
        db.query("set foreign_key_checks=0")
        db.truncate_table(table_list)
        db.query("set foreign_key_checks=1")
        query_list =["insert patent(inid_11) values('123')",
                     "insert patent_inid_15(inid_11,inid_15) values('123','456')",
                     "insert patent_inid_15(inid_11,inid_15) values('123','789')"]
        query_list =[
            {"func":db.insert,"data":{"table":"patent","data":{"inid_11":'123'}}},
            {"func":db.insert,"data":{"table":"patent_inid_15","data":{"inid_11":'123',"inid_15":'456'}}},
            {"func":db.insert,"data":{"table":"patent_inid_15","data":{"inid_11":'123',"inid_15":'789'}}}
        ]
        db.transaction(query_list,sql_strings=False)
    exit(0)