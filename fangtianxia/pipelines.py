# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
from fangtianxia.items import NewHouseItem
from fangtianxia.items import ESFHouseItem
from pymysql import cursors
from twisted.enterprise import adbapi

class FangtianxiaPipeline(object):
    """
    异步的方式插入数据
    """
    def __init__(self):
        dbparams = {
            'host': '127.0.0.1',
            'port': 3306,
            'user': 'root',
            'password': '123456',
            'database': 'fangtianxia',
            'charset': 'utf8',
            'cursorclass': cursors.DictCursor
        }
        self.dbpool = adbapi.ConnectionPool('pymysql', **dbparams)
        self.newhousesql = None
        self.esfhousesql = None

    @property
    def newhouse_sql(self):
        if not self.newhousesql:
            self.newhousesql = """
                insert into newhouse(id, province, city, name, price, rooms, area, address, sale, origin_url)
                values(null, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            return self.newhousesql
        return self.newhousesql

    @property
    def esfhouse_sql(self):
        if not self.esfhousesql:
            self.esfhousesql = """
                insert into esfhouse(id, province, city, name, rooms, floor, toward, year, address, price, unit, origin_url)
                values(null, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            return self.esfhousesql
        return self.esfhousesql

    def insert_newhouse(self, cursor, item):
        cursor.execute(self.newhouse_sql, (item['province'], item['city'], item['name'], item['price'], item['rooms'],
                                           item['area'], item['address'], item['sale'], item['origin_url']))

    def insert_esfhouse(self, cursor, item):
        cursor.execute(self.esfhouse_sql, (item['province'], item['city'], item['name'], item['rooms'], item['floor'],
                                           item['toward'], item['year'], item['address'], item['price'], item['unit'],
                                           item['origin_url']))


    def process_item(self, item, spider):
        if isinstance(item, NewHouseItem):
            defer = self.dbpool.runInteraction(self.insert_newhouse, item)
            defer.addErrback(self.handle_error, item, spider)
        if isinstance(item, ESFHouseItem):
            defer = self.dbpool.runInteraction(self.insert_esfhouse, item)
            defer.addErrback(self.handle_error, item, spider)


    def handle_error(self, error, item, spider):
        print('='*10+'error'+'='*10)
        print(error)
        print('='*10+'error'+'='*10)