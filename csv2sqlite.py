#!/usr/bin/env python
# vim:set ts=4 sw=4 et smartindent fileencoding=utf-8 filetype=python:
import argparse
import csv
import os
import pprint
import sqlite3
from datetime import datetime

def create_db(db, keys):
    sql = 'CREATE TABLE IF NOT EXISTS csv'
    columns = ','.join(map(lambda x: '`{0}` TEXT'.format(x), keys))
    sql += '({0})'.format(columns)
    db.execute(sql)

def create_insert_sql(keys):
    sql = 'INSERT INTO csv'
    columns = ','.join(map(lambda x: '`{0}`'.format(x), keys))
    sql += '({0})'.format(columns)
    sql += ' VALUES '
    columns = ','.join(map(lambda x: ':{0}'.format(x), keys))
    sql += '({0})'.format(columns)
    return sql

def convert_sqlite(dbname, fp):
    with sqlite3.connect(dbname) as con:
        cur = con.cursor()
        create_db(cur, fp.fieldnames)
        sql = create_insert_sql(fp.fieldnames)
        for row in fp:
            cur.execute(sql, row)
        con.commit()

ENCODING_LIST = ('utf8', 'sjis', 'euc-jp')
def parse_option():
    parser = argparse.ArgumentParser(description = u'CSVファイルをsqlite3に変換')
    parser.add_argument('--encoding', choices = ENCODING_LIST,
        default = 'utf8', help = u'文字コード')
    parser.add_argument('files', nargs = '+', help = u'CSVファイル')

    return parser.parse_args()

def main():
    option = parse_option()
    encoding = option.encoding
    if encoding == 'sjis':
        encoding = 'ms932'
    for f in option.files:
        with open(f, encoding = encoding, errors = 'backslashreplace', newline = '') as fp:
            reader = csv.DictReader(fp)
            base, ext = os.path.splitext(f)
            fname = base + '.sqlite'
            convert_sqlite(fname, reader)

if __name__ == '__main__':
    main()

