#!/usr/bin/env python3
import cx_Oracle
import datetime
import logging

class Oracle:
    _connection = None
    def __init__(self, user, password, url, table):
        self.user = user
        self.password = password
        self.url = url
        self.table = table
        self.create_connection(self.user, self.password, self.url)

    @classmethod
    def create_connection(cls, user, password, url):
        cls._connection = cx_Oracle.connect(user, password, url, encoding='UTF-8')


    def insert_filename(self, filename, filehash):
        cursor = self._connection.cursor()
        cursor.execute("insert into admin.shared_files (name, checksum, timestamp) values (:1, :2, :3)", (filename, filehash, datetime.datetime.now()))
        self._connection.commit()
        cursor.close()

    def close(self):
        self._connection.close()

