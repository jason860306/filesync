#!/usr/bin/python
# encoding: utf-8


__file__ = '$'
__author__ = 'szj0306'  # 志杰
__date__ = '6/30/17 6:57 PM'
__license__ = "Public Domain"
__version__ = '$'
__email__ = "jason860306@gmail.com"
# '$'


import multiprocessing as multiproc
import os
import subprocess as subproc
import threading


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
            answer = next_task()  # __call__()
            self.task_queue.task_done()
            self.result_queue.put(answer)
        return


class Task(object):
    """
    ret = interact_run('ping 192.168.0.102')
    if ret[0] == 0 and ('timed out' not in ret[1]):
        FREEMV_INTERACT_RESULT = 0
    else:
        FREEMV_INTERACT_RESULT = 1
    """

    def __init__(self, fname, md5, cmd):
        self.fname = fname
        self.fmd5 = md5
        self.cmd = cmd
        self.ret_val = None
        self.ret_info = None

    def __call__(self, timeout=5):
        def timeout_trigger(sub_process):
            """timeout function trigger"""
            os.system("kill -9" + str(sub_process.pid))
        timeout = float(timeout)
        pipe = subproc.Popen(self.cmd, 0, None, None, subproc.PIPE,
                             subproc.PIPE, shell=True)
        timer = threading.Timer(timeout*60, timeout_trigger, args=(pipe,))
        timer.start()
        pipe.wait()
        timer.cancel()

        self.ret_val = pipe.returncode
        self.ret_info = pipe.stdout.read()
        return self.fname, self.ret_val, self.ret_info

    def __str__(self):
        return '{code}:{info}'.format(code=self.ret_val, info=self.ret_info)


class DistDownloader(object):
    """

    """

    def __init__(self, log):
        # Establish communication queues
        self.consumer_cnt = 0
        self.tasks = multiproc.JoinableQueue()
        self.results = multiproc.Queue()

        self.log = log

    def start(self):
        # Start consumers
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
    import logging as mylog

    mylog.basicConfig(level=mylog.DEBUG, format='%(name)s: %(message)s', )

    downer = DistDownloader(mylog)
    downer.start()

    # Enqueue jobs
    num_jobs = 10
    for i in range(num_jobs):
        downer.enque_task(Task(None, None, None))

    # Add a poison pill for each consumer
    num_consumers = multiproc.cpu_count()
    for i in range(num_consumers):
        downer.enque_task(None)

    # Wait for all of the tasks to finish
    downer.join_all()

    # Start printing results
    while num_jobs:
        result = downer.results.get()
        print ('Result:', result)
        num_jobs -= 1
