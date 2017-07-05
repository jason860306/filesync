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
import hashlib as hash
import logging as mylog
import os
import signal
import string
import sys
import threading
import time

import creatpath
import distdownloader as distdler
import pymysql

MYSQL_DOMAIN = "localhost"
MYSQL_USER = "root"
MYSQL_PASSWD = ""
MYSQL_DB_NAME = "VodSlave"
MYSQL_TBL_FILEINFO = "file_info"
MYSQL_TBL_BUSINES_INFO = "business_info"

VODSLAVE_HOST = "182.18.58.39"
VODSLAVE_PORT = 81

TASK_NUM = 10
TASK_RETRYNUM = 3
TASK_TIMEOUT = 60 * 60

FILE_DB_NAME = ".file_list.db"


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
            self.downok = False
            self.dispatched = False

        def parse(self):
            """
            |7463890;4F2D59D73D1AF89602757079B8B9F176;/opt/data/rencoder/EF/5F/EFEF705FFDB4D8E1196CC8104650560F78938A5F|
            :return:
            """
            if not self.fileinfo:
                raise Exception("fileinfo is empty")
            for finfo in self.fileinfo.split('|'):
                fields = finfo.split(';')
                if len(fields) != 3:
                    return
                fsize = string.atol(fields[0])
                fmd5 = fields[1].upper()
                fpath = fields[2]
                downover = False
                self.extfilelst.append([fsize, fmd5, fpath, downover])

        def set_down_result(self, fname, result):
            down_res = True
            set_ok = False
            for extf in self.extfilelst:
                if os.path.basename(extf[2]) == fname:
                    extf[3] = result
                    set_ok = True
                if not extf[3]:
                    down_res = False
            self.downok = down_res
            return set_ok

    def __init__(self, logger):
        self.db_host = MYSQL_DOMAIN
        self.user = MYSQL_USER
        self.passwd = MYSQL_PASSWD
        self.dbname = MYSQL_DB_NAME
        self.offset = 0
        self.task_num = TASK_NUM
        self.task_retrynum = TASK_RETRYNUM
        self.task_timeout = TASK_TIMEOUT
        self.all_tbl = {}
        self.cur_tbl = []
        self.file_list = []
        self.downer = distdler.DistDownloader(self.task_num, logger)
        self.logger = logger
        super(FileSync, self).__init__(name="thread for filesync")

    def _init(self):
        self.prepare_output()
        if not self.all_tbl:
            self.fetch_filetbl_list()
        self.cur_tbl = self.all_tbl.popitem()
        tbl_name = self.cur_tbl[0]
        tbl_item_num = self.cur_tbl[1][0]
        tbl_item_offset = self.cur_tbl[1][1]
        num = min(tbl_item_num-tbl_item_offset, self.task_num)
        self.fetch_file_list(tbl_name, tbl_item_offset, num)

        self.downer.start()

    def prepare_output(self):
        # 1. connect to database
        mysql = self._open_db()
        if mysql is None:
            raise StandardError("open db %s faied" % self.dbname)

        # 2. query file list from database
        sql = "SELECT FileDir from {tbl}".format(tbl=MYSQL_TBL_BUSINES_INFO)
        try:
            lines, result = mysql.query(sql)
            if lines == 0:
                raise Exception("no record in table {tbl}".format(
                    tbl=MYSQL_TBL_BUSINES_INFO))
            dataset, info = mysql.fetch_queryresult(result, moreinfo=True)
            file_dirs = set(dataset)
            dir_creator = creatpath.CreatePath()
            for dir in file_dirs:
                # 3. create 2-level directories
                dir_creator(dir[0])
                fdb_name = os.path.join(dir[0], "%s" % FILE_DB_NAME)
                if not os.path.exists(fdb_name):
                    with open(fdb_name, "w+") as fdb:
                        fdb.write("# database of file list in cvs\n")
                        fdb.write("# format: file_name | full_fname | "
                                  "file_size | file_md5 | code\n")
                        os.utime(fdb_name, None)
                self.logger.debug("%s" % dir[0])
        except (StandardError, Exception), err:
            self.logger.fatal("%s" % err)
            raise StandardError("query failed, sql: %s" % sql)

        # 4. close db connection
        mysql.close()

    def fetch_filetbl_list(self):
        # 1. connect to database
        mysql = self._open_db()
        if mysql is None:
            raise StandardError("open db %s faied" % self.dbname)

        # 2. query file list from database
        sql = "SHOW TABLES LIKE '{tbl}_%'".format(tbl=MYSQL_TBL_FILEINFO)
        try:
            lines, result = mysql.query(sql)
            if lines == 0:
                raise Exception("table {tbl} isn't exist in {db}".format(
                    tbl=MYSQL_TBL_FILEINFO, db=self.dbname))
            dataset, info = mysql.fetch_queryresult(result, moreinfo=True)
        except (StandardError, Exception), err:
            self.logger.fatal("%s" % err)
            raise StandardError("query failed, sql: %s" % sql)

        # 3. get number of record within each table
        for tbl_name in dataset:
            tbl = tbl_name[0]
            self.logger.debug("%s" % tbl)
            sql_cnt = "SELECT COUNT(*) FROM {tbl}".format(tbl=tbl)
            try:
                lines, result = mysql.query(sql_cnt)
                if lines == 0:
                    raise Exception("no record in table %s" % tbl)
                data, info = mysql.fetch_queryresult(result, moreinfo=True)
                self.logger.debug("%s" % repr(data))
                # capacity, offset, count
                self.all_tbl[tbl] = [data[0][0], 0, 0]
            except (StandardError, Exception), err:
                self.logger.fatal("%s" % err)
                raise StandardError("query failed, sql: %s" % sql_cnt)

        # 4. close db connection
        mysql.close()

    def fetch_file_list(self, tbl_name, offset, num):
        # 1. connect to database
        mysql = self._open_db()
        if mysql is None:
            raise StandardError("open db %s faied" % self.dbname)

        # 2. query file list from database
        sql = "SELECT Gcid,BusinessType,FileInfo FROM {tbl} " \
              "LIMIT {offset},{n}".format(tbl=tbl_name,
                                          offset=offset, n=num)
        try:
            lines, result = mysql.query(sql)
            if lines == 0:
                raise Exception("no record in table {tbl}".format(
                    tbl=tbl_name))
            dataset, info = mysql.fetch_queryresult(result, how=1,
                                                    moreinfo=True)
            for item in dataset:
                finfo = FileSync.FileInfo()
                finfo.gcid = item['Gcid']
                finfo.businesstype = item['BusinessType']
                finfo.fileinfo = item['FileInfo']
                finfo.parse()
                self.file_list.append(finfo)
                self.logger.debug("%s" % item)

            self.cur_tbl[1][1] = offset + num
        except (StandardError, Exception), err:
            self.logger.fatal("%s" % err)
            raise StandardError("query failed, sql: %s" % sql)

        # 3. close db connection
        mysql.close()

    def run(self):
        self._init()

        while True:
            # 1. download
            for f in self.file_list:
                if f.dispatched:
                    continue
                for extf in f.extfilelst:
                    fsize = extf[0]
                    fmd5 = extf[1]
                    fpath = extf[2]
                    dname = os.path.dirname(fpath)
                    fname = os.path.basename(fpath)
                    if os.path.exists(fpath):
                        with open(fpath) as md5file:
                            new_fmd5 = \
                                hash.md5(md5file.read()).hexdigest().upper()
                            if fmd5 == new_fmd5:
                                self._set_down_result(fname)
                                continue
                    self._add_task(dname, fname, fsize, fmd5)
                f.dispatched = True

            # 2. validate file list, maybe some file have exist
            exist_fcnt = self._validate_file_list()

            # 2. dispatch download task
            tbl_item_capacity = self.cur_tbl[1][0]
            tbl_item_offset = self.cur_tbl[1][1]
            tbl_item_count = self.cur_tbl[1][2] + exist_fcnt
            num = min(tbl_item_capacity-tbl_item_offset, self.task_num)
            num *= 3  # plus url and vrf
            new_task_num = num - self._get_flst_len()  # len(self.file_list)
            new_task_num /= 3
            if new_task_num > 0:
                tbl_name = self.cur_tbl[0]
                self.fetch_file_list(tbl_name, tbl_item_offset, new_task_num)

            # 3. process download result
            down_over_cnt = 0
            res_cnt = self.downer.results.qsize()
            for i in xrange(res_cnt):
                result = self.downer.results.get()
                dname = result[0]
                fname = result[1]
                fsize = result[2]
                fmd5 = result[3]
                retry_cnt = result[4]
                code = result[5]
                info = result[6]
                self.logger.debug(info)
                if code != 0 and retry_cnt < self.task_retrynum:
                    self.logger.debug("%s download failed, try again: %d/%d"
                                      % (fname, retry_cnt, self.task_retrynum))
                    self._add_task(dname, fname, fsize, fmd5, retry_cnt)
                else:
                    if code != 0:
                        self.logger.debug("%s download failed for retrying %d "
                                          "times" % (fname, retry_cnt))
                    # write file db
                    self._write_file_db(dname, fname, fsize, fmd5, code)
                    self._set_down_result(fname)
                    if self._get_down_result(fname):
                        down_over_cnt += 1
                        self.logger.debug("%s download successfully" % fname)
                    self._del_file(fname)
            tbl_item_count += down_over_cnt
            if tbl_item_capacity == tbl_item_count:
                # 4. download over
                if not self.file_list and not self.all_tbl:
                    self.logger.debug("sync files over!")
                    self.downer.quit()
                    break
                self.cur_tbl = self.all_tbl.popitem()
                tbl_item_capacity = self.cur_tbl[1][0]
                tbl_item_offset = self.cur_tbl[1][1]
                tbl_item_count = self.cur_tbl[1][2]
            self.cur_tbl[1][2] = tbl_item_count

            time.sleep(1)

    def join(self, timeout=None):
        try:
            self.downer.join_all()
            super(FileSync, self).join(timeout)
        except BaseException, bex:
            self.logger.fatal("%s" % bex)

    def quit(self):
        self.downer.quit()

    def _open_db(self):
        mysql = pymysql.PyMySql(self.logger)
        # 1. connect to database
        try:
            mysql.connect(host=self.db_host, user=self.user,
                          passwd=self.passwd, dbname=self.dbname)
        except StandardError, operr:
            self.logger.fatal("%s" % operr)
            mysql = None
        return mysql

    def _add_task(self, dname, fname, fsize, fmd5, retry_cnt=0):
        (_, tmpfname) = os.path.split(fname)
        (shotname, _) = os.path.splitext(tmpfname)
        path1 = shotname[0:2]
        path2 = shotname[len(shotname) - 2:len(shotname)]
        url_path = "/%s/%s/%s" % (path1, path2, fname)
        down_url = "http://{host}:{port}{path}".format(
            host=VODSLAVE_HOST, port=VODSLAVE_PORT, path=url_path)
        cmd = "/usr/bin/wget -d -v -c {url} -O {dir}/{fname} 2>&1".format(
            url=down_url, dir=dname, fname=fname)
        task = distdler.Task(dname, fname, fsize, fmd5, cmd,
                             self.task_timeout, retry_cnt)
        self.downer.enque_task(task)

    def _set_down_result(self, fname, result=True):
        for f in self.file_list:
            (_, tmpfname) = os.path.split(fname)
            (shotname, _) = os.path.splitext(tmpfname)
            if shotname == f.gcid:
                f.set_down_result(fname, result)
                break

    def _get_down_result(self, fname):
        for f in self.file_list:
            (_, tmpfname) = os.path.split(fname)
            (shotname, _) = os.path.splitext(tmpfname)
            if shotname == f.gcid:
                return f.downok
        return False

    def _get_flst_len(self):
        fcnt = 0
        for f in self.file_list:
            fcnt += len(f.extfilelst)
        return fcnt

    def _validate_file_list(self):
        exist_file_cnt = 0
        idx_flst = 0
        while True:
            if not self.file_list:
                break
            finfo = self.file_list[idx_flst]
            idx_extflst = 0
            while True:
                if finfo.extfilelst[idx_extflst][3]:
                    del finfo.extfilelst[idx_extflst]
                else:
                    idx_extflst += 1
                if idx_extflst == len(finfo.extfilelst):
                    break
            if idx_extflst == 0:
                del self.file_list[idx_flst]
                exist_file_cnt += 1
            else:
                idx_flst += 1
            if idx_flst == len(self.file_list):
                break
        return exist_file_cnt

    def _del_file(self, fname):
        idx_flst = -1
        for fidx in xrange(len(self.file_list)):
            finfo = self.file_list[fidx]

            idx_del = -1
            for i in xrange(len(finfo.extfilelst)):
                if os.path.basename(finfo.extfilelst[i][2]) == fname:
                    idx_del = i
                    break
            if idx_del != -1:
                del finfo.extfilelst[idx_del]
                if not finfo.extfilelst:
                    idx_flst = fidx
                break
        if idx_flst != -1:
            del self.file_list[idx_flst]

    def _write_file_db(self, dname, fname, fsize, fmd5, code):
        import csv
        csv.register_dialect('fsync', delimiter='|', quoting=csv.QUOTE_NONE)

        head, _ = os.path.split(dname)
        basedir, _ = os.path.split(head)
        fdb_name = os.path.join(basedir, "%s" % FILE_DB_NAME)
        with open(fdb_name, "a+") as fdb:
            csv_writer = csv.writer(fdb, dialect='fsync')
            full_fname = os.path.join(dname, fname)
            csv_writer.writerow((fname, full_fname, fsize, fmd5, code))


if __name__ == '__main__':

    HELP_MSG = "\t-h,--help     print this message and quit\n" \
               "\t-d,--daemon   run as a daemon process\n" \
               "\t-a,--addr     address for vodslave's database\n" \
               "\t-p,--port     port for vodslave's database\n" \
               "\t-u,--user     user for vodslave's database\n" \
               "\t-w,--passwd   passwd for vodslave's database\n" \
               "\t-b,--db       db name\n" \
               "\t-c,--tasknum  number of task to download concurrently\n" \
               "\t-r,--retrynum number of retry when a task is failed\n" \
               "\t-m,--timeout  timeout of a task before download is failed\n"

    base_fname = os.path.basename(sys.argv[0])
    (_, tmpfname) = os.path.split(base_fname)
    (shotname, _) = os.path.splitext(tmpfname)
    mylog.basicConfig(level=mylog.DEBUG, format='%(name)s: %(message)s',
                      filename="%s.log" % shotname)
    logger = mylog.getLogger(shotname)

    try:
        opts, args = getopt.getopt(
            sys.argv[1:], "hda:p:u:w:b:c:r:m:",
            ["help=", "daemon=", "addr=", "port=", "user=", "passwd=",
             "db=", "tasknum=", "retrynum=", "timeout="])
        if not opts:
            logger.error("%s\n%s" % (sys.argv[0], HELP_MSG))
            sys.exit()
    except getopt.GetoptError, err:
        logger.critical("%s" % err)
        logger.info("%s\n%s" % (sys.argv[0], HELP_MSG))
        sys.exit(-1)

    daemon = False
    fsync = FileSync(logger)
    for opt, arg in opts:
        if opt == '-h':
            logger.info("%s\n%s" % (sys.argv[0], HELP_MSG))
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
        elif opt in ("-r", "--retrynum"):
            fsync.task_retrynum = string.atoi(arg)
        elif opt in ("-m", "--timeout"):
            fsync.task_timeout = string.atoi(arg)
        else:
            logger.error("%s\n%s" % (sys.argv[0], HELP_MSG))
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

    try:
        sig_fun = lambda signum, frame: fsync.quit()
        signal.signal(signal.SIGINT, sig_fun)
        signal.signal(signal.SIGQUIT, sig_fun)
        signal.signal(signal.SIGTERM, sig_fun)
        signal.signal(signal.SIGABRT, sig_fun)

        # start the daemon main loop
        fsync.start()
        fsync.join()
    except BaseException, err:
        logger.fatal("%s" % err)
