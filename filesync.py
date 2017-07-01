#!/usr/bin/python
# encoding: utf-8


__file__ = '$'
__author__ = 'szj0306'  # 志杰
__date__ = '6/29/17 5:11 PM'
__license__ = "Public Domain"
__version__ = '$'
__email__ = "jason860306@gmail.com"
# '$'


import getopt
import logging as mylog
import os
import sys
import string
import threading
import time
import pymysql
import distdownloader as distdler


MYSQL_DOMAIN = "localhost"
MYSQL_USER = "root"
MYSQL_PASSWD = ""
MYSQL_DB_NAME = "VodSlave"
MYSQL_TBL_FILEINFO = "file_info"
MYSQL_TBL_BUSINES_INFO = "business_info"

VODSLAVE_HOST = "192.168.58.39"
VODSLAVE_PORT = 81

TASK_NUM = 10
TMP_OUTPUT = "/tmp/output"


class FileSync(threading.Thread, object):
    """

    """
    class FileInfo(object):
        """

        """
        def __init__(self):
            self.gcid = ""
            self.businesstype = 0
            self.fileinfo = ""
            self.extfilelst = []

        def parse(self):
            """
            |7463890;4F2D59D73D1AF89602757079B8B9F176;/opt/data/rencoder/EF/5F/EFEF705FFDB4D8E1196CC8104650560F78938A5F|
            :return:
            """
            if len(self.fileinfo) == 0:
                raise Exception("fileinfo is empty")
            for finfo in self.fileinfo.split('|'):
                fields = finfo.split(';')
                fsize = string.atol(fields[0])
                fmd5 = fields[1].upper()
                fpath = fields[2]
                self.extfilelst.append((fsize, fmd5, fpath))

    def __init__(self):
        self.db_host = MYSQL_DOMAIN
        self.user = MYSQL_USER
        self.passwd = MYSQL_PASSWD
        self.dbname = MYSQL_DB_NAME
        self.offset = 0
        self.task_num = TASK_NUM
        self.out_dir = TMP_OUTPUT
        self.all_tbl = {}
        self.cur_tbl = ()
        self.file_list = []
        self.downer = None
        super(FileSync, self).__init__(name="thread for filesync")

    def init(self):
        self.prepare_output()
        if len(self.all_tbl) == 0:
            self.fetch_filetbl_list()
        self.cur_tbl = self.all_tbl.popitem()
        tbl_name = self.cur_tbl[0]
        tbl_item_num = self.cur_tbl[1][0]
        tbl_item_offset = self.cur_tbl[1][1]
        num = min(tbl_item_num-tbl_item_offset, self.task_num)
        self.fetch_file_list(tbl_name, tbl_item_offset, num)

        self.downer = distdler.DistDownloader()

    def prepare_output(self):
        # 1. connect to database
        mysql = self._open_db()

        # 2. query file list from database
        sql = "SELECT FileDir from {tbl}".format(tbl=MYSQL_TBL_BUSINES_INFO)
        lines, result = mysql.query(sql)
        if lines == 0:
            mylog.fatal("no record in table {tbl}".format(
                tbl=MYSQL_TBL_BUSINES_INFO))
            return
        dataset, info = mysql.fetch_queryresult(result, moreinfo=True)
        mylog.debug("%s" % dataset)
        file_dirs = set(dataset)

        # 3. close db connection
        mysql.close()

        # 4. create 2-level directories

    def fetch_filetbl_list(self):
        # 1. connect to database
        mysql = self._open_db()

        # 2. query file list from database
        sql = "SHOW TABLES LIKE {tbl}_%%".format(tbl=MYSQL_TBL_FILEINFO)
        lines, result = mysql.query(sql)
        if lines == 0:
            raise Exception("table {tbl} isn't exist in {db}".format(
                tbl=MYSQL_TBL_FILEINFO, db=self.dbname))
        dataset, info = mysql.fetch_queryresult(result, moreinfo=True)

        # 3. get number of record within each table
        for tbl_name in dataset:
            mylog.debug("%s" % tbl_name)
            sql_cnt = "SELECT COUNT(*) FROM {tbl}".format(tbl=tbl_name)
            lines, result = mysql.query(sql_cnt)
            if lines == 0:
                raise Exception("no record in table {tbl}".format(
                    tbl=tbl_name))
            self.all_tbl[tbl_name] = \
                (string.atol(result[0]), 0, 0)  # capacity, offset, count

        # 4. close db connection
        mysql.close()

    def fetch_file_list(self, tbl_name, offset, num):
        # 1. connect to database
        mysql = self._open_db()

        # 2. query file list from database
        sql = "SELECT Gcid,BusinessType,FileInfo FROM {tbl} " \
              "LIMIT {offset},{n}".format(tbl=MYSQL_TBL_FILEINFO,
                                          offset=offset, n=num)
        lines, result = mysql.query(sql)
        if lines == 0:
            mylog.fatal("no record in table {tbl}".format(
                tbl=MYSQL_TBL_FILEINFO))
            return
        dataset, info = mysql.fetch_queryresult(result, how=1, moreinfo=True)
        for item in dataset:
            finfo = FileSync.FileInfo()
            finfo.gcid = item['Gcid']
            finfo.businesstype = item['BusinessType']
            finfo.fileinfo = item['FileInfo']
            finfo.parse()
            self.file_list.append(finfo)
            mylog.debug("%s" % item)

        self.cur_tbl[1][1] = offset + num

        # 3. close db connection
        mysql.close()

    def run(self):
        while True:
            # 1. download
            for f in self.file_list:
                for extf in f.extfilelst:
                    fname = os.path.basename(extf[2])
                    url_path = "/%s/%s/%s" % (fname[0:2], fname[-3:-1], fname)
                    url = "http://{host}:{port}/{path}".format(
                        host=VODSLAVE_HOST, port=VODSLAVE_PORT, path=url_path)
                    task = distdler.Task(url, extf[1])
                    self.downer.enque_task(task)

            # 2. dispatch download task
            tbl_item_capacity = self.cur_tbl[1][0]
            tbl_item_offset = self.cur_tbl[1][1]
            tbl_item_count = self.cur_tbl[1][2]
            num = min(tbl_item_capacity-tbl_item_offset, self.task_num)
            new_task_num = num - len(self.file_list)
            if new_task_num > 0:
                tbl_name = self.cur_tbl[0]
                self.fetch_file_list(tbl_name, tbl_item_offset, new_task_num)

            # 3. process download result, TODO hasn't process failed task
            tbl_item_count += self.downer.results.qsize()
            if tbl_item_capacity == tbl_item_count:
                self.cur_tbl = self.all_tbl.popitem()
                # 4. download over
                if not self.all_tbl:
                    mylog.debug("sync files over!")
                    break
            self.cur_tbl[1][2] = tbl_item_count

            time.sleep(3)

    def _open_db(self):
        mysql = pymysql.PyMySql()
        # 1. connect to database
        mysql.connect(host=self.db_host, user=self.user,
                      passwd=self.passwd, dbname=MYSQL_DB_NAME)
        return mysql


if __name__ == '__main__':

    HELP_MSG = "\t-h,--help    print this message and quit\n" \
               "\t-d,--daemon  run as a daemon process\n" \
               "\t-a,--addr    address for vodslave's database\n" \
               "\t-p,--port    port for vodslave's database\n" \
               "\t-u,--user    user for vodslave's database\n" \
               "\t-w,--passwd  passwd for vodslave's database\n" \
               "\t-b,--db      db name\n" \
               "\t-c,--tasknum number of task to download concurrently\n" \
               "\t-o,--output  output directory for downloading\n"

    mylog.basicConfig(level=mylog.DEBUG, format='%(name)s: %(message)s',)

    try:
        opts, args = getopt.getopt(
            sys.argv[1:], "hda:p:u:w:b:c:o:", ["help=", "daemon=", "addr=",
                "port=", "user=", "passwd=", "db=", "tasknum=", "output="])
        if len(opts) is 0:
            mylog.error("%s\n%s" % (sys.argv[0], HELP_MSG))
            sys.exit()
    except getopt.GetoptError:
        mylog.critical("%s\n%s" % (sys.argv[0], HELP_MSG))
        sys.exit(-1)

    daemon = False
    fsync = FileSync()
    for opt, arg in opts:
        if opt == '-h':
            mylog.info("%s\n%s" % (sys.argv[0], HELP_MSG))
            sys.exit()
        elif opt in ("-d", "--daemon"):
            daemon = True
        elif opt in ("-a", "--addr"):
            fsync.db_host = arg
        elif opt in ("-p", "--port"):
            fsync.db_port = string.atoi(arg)
        elif opt in ("-u", "--user"):
            fsync.user = arg
        elif opt in ("-b", "--db"):
            fsync.dbname = arg
        elif opt in ("-w", "--passwd"):
            fsync.passwd = arg
        elif opt in ("-c", "--tasknum"):
            fsync.task_num = string.atoi(arg)
        elif opt in ("-o", "--output"):
            fsync.out_dir = arg
        else:
            mylog.error("%s\n%s" % (sys.argv[0], HELP_MSG))
            sys.exit()


    if daemon is True:
        """
        以上代码中main()函数包括了一个永久循环过程：把时间戳写入一个文件。
        运行的时候，建立一个进程，Linux会分配个进程号。然后调用os.fork()
        创建子进程。若pid>0就是自己，自杀。子进程跳过if语句，通过os.setsid()
        成为linux中的独立于终端的进程（不响应sigint，sighup等）。
        第二次os.fork再创建一个子进程，自己自杀。原因是os.setsid()后成为父进程，
        虽然已经不被动响应信号，但访问终端文件时控制权还是会失去。这次创建的进程
        真的是孤魂野鬼的daemon，并且外界对它影响被控制在最小。
        """
        # do the UNIX double-fork magic, see Stevens' "Advanced
        # Programming in the UNIX Environment" for details (ISBN 0201563177)
        try:
            pid = os.fork()
            if pid > 0:
                # exit first parent
                sys.exit(0)
        except OSError, e:
            print >>sys.stderr, "fork #1 failed: %d (%s)" % (e.errno, e.strerror)
            sys.exit(1)

        # decouple from parent environment
        os.chdir("/")
        os.setsid()
        os.umask(0)

        # do second fork
        try:
            pid = os.fork()
            if pid > 0:
                # exit from second parent, print eventual PID before
                print "Daemon PID %d" % pid
                sys.exit(0)
        except OSError, e:
            print >>sys.stderr, "fork #2 failed: %d (%s)" % (e.errno, e.strerror)
            sys.exit(1)

    # start the daemon main loop
    fsync.init()
    fsync.start()
    fsync.join()
