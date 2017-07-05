#!/usr/bin/python
# encoding: utf-8


__file__ = '$'
__author__ = 'szj0306'  # 志杰
__date__ = '6/30/17 6:57 PM'
__license__ = "Public Domain"
__version__ = '$'
__email__ = "jason860306@gmail.com"
# '$'


import fcntl
import multiprocessing as multiproc
import os
import select
import subprocess as subproc
import sys
import threading
import time


class Consumer(multiproc.Process):
    def __init__(self, task_queue, result_queue, log):
        super(Consumer, self).__init__()
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.log = log

    def run(self):
        proc_name = self.name
        while True:
            next_task = self.task_queue.get()
            if next_task is None:
                # Poison pill means shutdown
                self.log.debug('%s: Exiting' % proc_name)
                self.task_queue.task_done()
                break
            self.log.debug("execute cmd: %s" % next_task.cmd)
            answer = next_task()  # __call__()
            self.task_queue.task_done()
            self.result_queue.put(answer)


class Task(object):
    """
    ret = interact_run('ping 192.168.0.102')
    if ret[0] == 0 and ('timed out' not in ret[1]):
        FREEMV_INTERACT_RESULT = 0
    else:
        FREEMV_INTERACT_RESULT = 1
    """

    def __init__(self, dname, fname, fsize, md5, cmd, timeout, retry_cnt=0):
        self.dname = dname
        self.fname = fname
        self.fsize = fsize
        self.fmd5 = md5
        self.cmd = cmd
        self.timeout = timeout
        self.retry_cnt = retry_cnt
        self.ret_val = None
        self.ret_info = ""

        # log_fname = os.path.join(self.dname, self.fname+".log")
        # mylog.basicConfig(level=mylog.DEBUG, format='%(name)s: %(message)s',
        #                   filename=log_fname)
        # self.logger = mylog.getLogger(self.fname)

    def __call__(self):
        def timeout_trigger((sub_process, ret_info)):
            ret_info = "timeout function trigger\n"
            sub_process.kill()

        pipe = subproc.Popen(self.cmd, shell=True, stdin=subproc.PIPE,
                             stdout=subproc.PIPE, stderr=subproc.PIPE)
        timer = threading.Timer(float(self.timeout), timeout_trigger,
                                args=(pipe, self.ret_info))
        timer.start()

        fl = fcntl.fcntl(pipe.stdout, fcntl.F_GETFL)
        fcntl.fcntl(pipe.stdout, fcntl.F_SETFL, fl | os.O_NONBLOCK)

        try:
            while pipe.poll() is None:
                if select.select([pipe.stdout.fileno()], [], [])[0]:
                    chunk = pipe.stdout.read()
                    # self.logger.debug("%s" % chunk)
                time.sleep(.1)
        except Exception, ex:
            self.ret_info += "%s\n" % ex
        self.ret_info += "command (%s) excute ok!\n" % self.cmd

        timer.cancel()

        self.retry_cnt += 1
        self.ret_val = pipe.returncode
        return self.dname, self.fname, self.fsize, self.fmd5, self.retry_cnt, \
               self.ret_val, self.ret_info

    def __str__(self):
        return '{code}:{info}'.format(code=self.ret_val, info=self.ret_info)


class DistDownloader(object):
    """

    """

    def __init__(self, concurrent_cnt, log):
        # Establish communication queues
        self.consumer_cnt = concurrent_cnt
        self.tasks = multiproc.JoinableQueue()
        self.results = multiproc.Queue()

        self.log = log

    def start(self):
        # Start consumers
        if self.consumer_cnt == 0:
            self.consumer_cnt = multiproc.cpu_count()
        self.log.debug('Creating %d consumers' % self.consumer_cnt)
        consumers = [Consumer(self.tasks, self.results, self.log)
                     for i in xrange(self.consumer_cnt)]
        for consumer in consumers:
            consumer.start()

    def enque_task(self, task):
        # Enqueue jobs
        self.tasks.put(task)

    def join_all(self):
        # Wait for all of the tasks to finish
        self.tasks.join()

    def quit(self):
        for i in xrange(self.consumer_cnt):
            self.enque_task(None)


if __name__ == '__main__':
    # import logging as mylog
    #
    # mylog.basicConfig(level=mylog.DEBUG, format='%(name)s: %(message)s', )
    #
    # downer = DistDownloader(mylog)
    # downer.start()
    #
    # # Enqueue jobs
    # num_jobs = 10
    # for i in range(num_jobs):
    #     downer.enque_task(Task(None, None, None, 'ls', 0))
    #
    # # Add a poison pill for each consumer
    # num_consumers = multiproc.cpu_count()
    # for i in range(num_consumers):
    #     downer.enque_task(None)
    #
    # # Wait for all of the tasks to finish
    # downer.join_all()
    #
    # # Start printing results
    # while num_jobs:
    #     result = downer.results.get()
    #     print ('Result:', result)
    #     num_jobs -= 1

    # # test retrun info from subprocess
    # # take shell command output
    # pattern="2[0-9]%"
    # message="CAPACITY WARNING"
    # # ps = subproc.Popen(
    # #     "sudo /usr/bin/wget -d -v -c http://182.18.58.39:81/E9/4D/E9D09022EE5C349194FB2ACDCCAEC7BD042D9B4D -O /opt/data/rencoder/E9/4D/E9D09022EE5C349194FB2ACDCCAEC7BD042D9B4D",
    # #     shell=True, stdout=subproc.PIPE, stderr=subproc.PIPE)
    # ps = subproc.Popen(
    #     "sudo df -h",
    #     shell=True, stdout=subproc.PIPE, stderr=subproc.PIPE)
    #
    # while True:
    #     line = ps.stdout.readline()
    #     if not line:
    #         break
    #     print "%s" % line
    #
    # ret_val = ps.returncode

    # test retrun info from subprocess asynchronously
    proc = subproc.Popen(
        'sudo /usr/bin/wget -d -v -c http://182.18.58.39:81/E9/4D/E9D09022EE5C349194FB2ACDCCAEC7BD042D9B4D -O /tmp/E9D09022EE5C349194FB2ACDCCAEC7BD042D9B4D 2>&1',
        shell=True, stdin=subproc.PIPE, stdout=subproc.PIPE,
        stderr=subproc.PIPE)

    fl = fcntl.fcntl(proc.stdout, fcntl.F_GETFL)
    fcntl.fcntl(proc.stdout, fcntl.F_SETFL, fl | os.O_NONBLOCK)

    try:
        while proc.poll() is None:
            readx = select.select([proc.stdout.fileno()], [], [])[0]
            if readx:
                chunk = proc.stdout.read()
                sys.stdout.write(chunk)
            time.sleep(.1)
    except Exception, ex:
        print ex
