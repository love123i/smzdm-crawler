#!/usr/bin/env python
# encoding:utf-8
# db.py
# 2016/9/27  22:02
from pymongo import MongoClient, collection
from pymongo import collection
class DB(object):
    def __init__(self, url='127.0.0.1', port=27017, user=None, password=None, db='test'):
        if user is None or password is None:
            self.conn   =  MongoClient("mongodb://%s" % url, 27017)
        else:
            self.conn = MongoClient("mongodb://%s:%s@%s" % (user, password, url), 27017)
        self.db = self.conn[db]

    # @return {'updatedExisting': True, u'nModified': 0, u'ok': 1, u'n': 1}
    def insert(self, table, filter, data):
        return self.db[table].update_one(filter, {'$set':data}, True).raw_result