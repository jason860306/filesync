#!/usr/bin/python
# encoding: utf-8


__file__ = '$'
__author__ = 'szj0306'  # 志杰
__date__ = '6/30/17 12:21 PM'
__license__ = "Public Domain"
__version__ = '$'
__email__ = "jason860306@gmail.com"
# '$'


import MySQLdb
import MySQLdb.cursors

STORE_RESULT_MODE = 0
USE_RESULT_MODE = 1

CURSOR_MODE = 0
DICTCURSOR_MODE = 1
SSCURSOR_MODE = 2
SSDICTCURSOR_MODE = 3

FETCH_ONE = 0
FETCH_MANY = 1
FETCH_ALL = 2

DB_PORT = 3306
CONN_TIMEOUT = 10
CHARSET_UTF8 = "utf8"


class PyMySql:
    """

    """

    def __init__(self, log):
        self.conn = None
        self.log = log

    def __del__(self):
        self.close()
        self.conn = None
        self.log = None

    def connect(self, host, user, passwd, dbname, timeout=CONN_TIMEOUT,
                charset=CHARSET_UTF8, port=DB_PORT):
        """
        建立一个新连接，指定host、port、用户名、密码、默认数据库、超时时间、字符集
        """
        self.conn = MySQLdb.Connect(host=host, port=port, user=user,
                                    passwd=passwd, db=dbname,
                                    connect_timeout=timeout,
                                    charset=charset)
        if self.conn.open is False:
            raise Exception("connect mysql {domain}:{port} failed.".format(
                domain=host, port=port))

    def close(self):
        """
        关闭当前连接
        """
        if self.conn.open:
            self.conn.close()

    def query(self, sqltext, mode=STORE_RESULT_MODE):
        """
        作用：使用connection对象的query方法，并返回一个元组(影响行数(int),结果集(result))
        参数：sqltext：sql语句
             mode=STORE_RESULT_MODE（0）表示返回store_result
             mode=USE_RESULT_MODE（1）  表示返回use_result
        返回：元组(影响行数(int),结果集(result)
        """
        if self.conn is None or self.conn.open is False:
            raise Exception("connect mysql firstly.")
        self.conn.query(sqltext)
        if mode == STORE_RESULT_MODE:
            result = self.conn.store_result()
        elif mode == USE_RESULT_MODE:
            result = self.conn.use_result()
        else:
            raise Exception("mode value is wrong.")
        return self.conn.affected_rows(), result

    def fetch_queryresult(self, result, maxrows=-1, how=0, moreinfo=False):
        """
        参数:result： query后的结果集合
            maxrows：返回的最大行数
            how：    以何种方式存储结果
                     0：tuple
                     1：dictionaries with columnname
                     2：dictionaries with table.columnname)
            moreinfo 表示是否获取更多额外信息（num_fields，num_rows,num_fields）
        返回：元组（数据集，附加信息（当moreinfo=False）或单一数据集（当moreinfo=True）
        """
        if result is None:
            raise Exception("result is None.")
        if maxrows == -1:
            maxrows = result.num_rows()
        dataset = result.fetch_row(maxrows, how)
        if moreinfo is False:
            return dataset
        else:
            num_fields = result.num_fields()
            num_rows = result.num_rows()
            field_flags = result.field_flags()
            info = (num_fields, num_rows, field_flags)
            return dataset, info

    def execute(self, sqltext, args=None, mode=CURSOR_MODE, many=False):
        """
        作用：使用游标（cursor）的execute 执行query
        参数：sqltext： 表示sql语句
             args： sqltext的参数
             mode：以何种方式返回数据集
                CURSOR_MODE = 0 ：store_result , tuple
                DICTCURSOR_MODE = 1 ： store_result , dict
                SSCURSOR_MODE = 2 : use_result , tuple
                SSDICTCURSOR_MODE = 3 : use_result , dict
             many：是否执行多行操作（executemany）
        返回：元组（影响行数（int），游标（Cursor））
        """
        if mode == CURSOR_MODE:
            curclass = MySQLdb.cursors.Cursor
        elif mode == DICTCURSOR_MODE:
            curclass = MySQLdb.cursors.DictCursor
        elif mode == SSCURSOR_MODE:
            curclass = MySQLdb.cursors.SSCursor
        elif mode == SSDICTCURSOR_MODE:
            curclass = MySQLdb.cursors.SSDictCursor
        else:
            raise Exception("mode value is wrong")

        cur = self.conn.cursor(cursorclass=curclass)
        line = 0
        if many is False:
            if args is None:
                line = cur.execute(sqltext)
            else:
                line = cur.execute(sqltext, args)
        else:
            if args is None:
                line = cur.executemany(sqltext)
            else:
                line = cur.executemany(sqltext, args)
        return line, cur

    def fetch_executeresult(self, cursor, mode=FETCH_ONE, rows=1):
        """
        作用：提取cursor获取的数据集
        参数：cursor：游标
             mode：执行提取模式
                   FETCH_ONE: 提取一个
                   FETCH_MANY:提取rows个
                   FETCH_ALL: 提取所有
             rows：提取行数
        返回：fetch数据集
        """
        if cursor is None:
            raise Exception("cursor is None")
        if mode == FETCH_ONE:
            return cursor.fetchone()
        elif mode == FETCH_MANY:
            return cursor.fetchmany(rows)
        elif mode == FETCH_ALL:
            return cursor.fetchall()


def printAuthors(res, mode=0, lines=0):
    """
    格式化输出
    """
    print "*" * 20, " lines: ", lines, " ", "*" * 20
    if mode == 0:
        for author_id, author_last, author_first, country in res:
            print "ID : %s , Author_last : %s , Author_First : %s , Country : %s" \
                  % (author_id, author_last, author_first, country)
    else:
        for item in res:
            print "-----------"
            for key in item.keys():
                print key, " : ", item[key]


if __name__ == "__main__":
    # 测试代码：

    """
    authors 这张表很简单。
    +--------------+-------------+------+-----+---------+----------------+
    | Field        | Type        | Null | Key | Default | Extra          |
    +--------------+-------------+------+-----+---------+----------------+
    | author_id    | int(11)     | NO   | PRI | NULL    | auto_increment |
    | author_last  | varchar(50) | YES  |     | NULL    |                |
    | author_first | varchar(50) | YES  | MUL | NULL    |                |
    | country      | varchar(50) | YES  |     | NULL    |                |
    +--------------+-------------+------+-----+---------+----------------+
    本文主要的所有操作都针对该表。
    """

    import logging as mylog

    mylog.basicConfig(level=mylog.DEBUG, format='%(name)s: %(message)s', )

    # 建立连接
    mysql = PyMySql(mylog)
    mysql.connect(host="localhost", user="root", passwd="peterbbs",
                  dbname="bookstore")
    ""
    # 定义sql语句
    sqltext = "select * from authors order by author_id "
    # 调用query方法,得到result
    lines, res = mysql.query(sqltext, mode=STORE_RESULT_MODE)
    # 提取数据
    data = mysql.fetch_queryresult(res, maxrows=20, how=0, moreinfo=False)
    # 打印
    printAuthors(data, 0, lines)

    # 演示多行插入
    sqltext = "insert into authors (author_last,author_first,country) values (%s,%s,%s)"
    args = [('aaaaaa', 'bbbbbb', 'cccccc'), ('dddddd', 'eeeeee', 'ffffff'),
            ('gggggg', 'hhhhhh', 'iiiiii')]
    lines, cur = mysql.execute(sqltext, args, mode=DICTCURSOR_MODE,
                               many=True)
    print "*" * 20, lines, "行被插入 ", "*" * 20

    sqltext = "select * from authors order by author_id "
    # 调用cursor.execute方法,得到result
    lines, cur = mysql.execute(sqltext, mode=DICTCURSOR_MODE)
    # 提取数据
    data = mysql.fetch_executeresult(cur, mode=FETCH_MANY, rows=20)
    # 打印
    printAuthors(data, 1, lines)

    # 关闭连接
    mysql.close()

    #
    # 测试输出：
    #
    # ** ** ** ** ** ** ** ** ** ** lines: 5 ** ** ** ** ** ** ** ** ** **
    #
    # ID: 1, Author_last: Greene, Author_First: Graham, Country: United
    # Kingdom
    # ID: 4, Author_last: Peter, Author_First: David, Country: China
    # ID: 5, Author_last: mayday, Author_First: Feng, Country: France
    # ID: 6, Author_last: zhang, Author_First: lixin, Country: France
    # ID: 9, Author_last: zhang111, Author_First: lixin, Country: France
    # ** ** ** ** ** ** ** ** ** ** 3
    # 行被插入 ** ** ** ** ** ** ** ** ** **
    # ** ** ** ** ** ** ** ** ** ** lines: 8 ** ** ** ** ** ** ** ** ** **
    # -----------
    # country: United
    # Kingdom
    # author_id: 1
    # author_first: Graham
    # author_last: Greene
    # -----------
    # country: China
    # author_id: 4
    # author_first: David
    # author_last: Peter
    # -----------
    # country: France
    # author_id: 5
    # author_first: Feng
    # author_last: mayday
    # -----------
    # country: France
    # author_id: 6
    # author_first: lixin
    # author_last: zhang
    # -----------
    # country: France
    # author_id: 9
    # author_first: lixin
    # author_last: zhang111
    # -----------
    # country: cccccc
    # author_id: 53
    # author_first: bbbbbb
    # author_last: aaaaaa
    # -----------
    # country: ffffff
    # author_id: 54
    # author_first: eeeeee
    # author_last: dddddd
    # -----------
    # country: iiiiii
    # author_id: 55
    # author_first: hhhhhh
    # author_last: gggggg
