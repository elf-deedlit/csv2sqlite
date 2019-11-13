#!/usr/bin/env python
# vim:set ts=4 sw=4 et smartindent fileencoding=utf-8 filetype=python:
import argparse
import csv
import fileinput
import io
import os
import sys
import sqlite3

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
    parser.add_argument('--output', type = str, help = u'出力ファイル名')
    parser.add_argument('files', nargs = '+', help = u'CSVファイル')

    return parser.parse_args()

def main():
    option = parse_option()
    encoding = option.encoding
    if encoding == 'sjis':
        encoding = 'ms932'

    # 標準入力の場合、unicode変換時のエラーがstrictのままになっている
    # ので、ここで変更してしまう
    sys.stdin = io.TextIOWrapper(sys.stdin.buffer,
            encoding = encoding, errors = 'backslashreplace')

    for file_nr, filename in enumerate(option.files):
        # fileinputは引数にファイル名の配列を渡せるが
        # 最初の行を読み込まないとfilename()とかisstdin()が値を返さない
        # そのため、1ファイルずつ処理する
        with fileinput.FileInput((filename, ),
                openhook = fileinput.hook_encoded(encoding, 'backslashreplace')) as fp:
            if filename == '-':
                if option.output:
                    if file_nr == 0:
                        fname = '{0}.sqlite'.format(option.output)
                    else:
                        base, _ = os.path.splitext(option.output)
                        fname = '{0}.{1}.sqlite'.format(base, file_nr)
                else:
                    fname = '{0}.sqlite'.format(file_nr)
            else:
                base, ext = os.path.splitext(filename)
                fname = base + '.sqlite'
            reader = csv.DictReader(fp)
            convert_sqlite(fname, reader)


if __name__ == '__main__':
    main()

