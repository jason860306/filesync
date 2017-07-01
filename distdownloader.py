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
import subprocess as subproc
import threading
import os


log = None


class Consumer(multiproc.Process):
    def __init__(self, task_queue, result_queue):
        super(Consumer, self).__init__()
        self.task_queue = task_queue
        self.result_queue = result_queue

    def run(self):
        proc_name = self.name
        while True:
            next_task = self.task_queue.get()
            if next_task is None:
                # Poison pill means shutdown
                print '%s: Exiting' % proc_name
                self.task_queue.task_done()
                break
            print '%s: %s' % (proc_name, next_task)
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
    def __init__(self, url, md5):
        self.url = url
        self.fmd5 = md5
        self.ret_val = None
        self.ret_info = None

    def __call__(self, cmd, timeout=60):
        def timeout_trigger(sub_process):
            """timeout function trigger"""
            os.system("kill -9" + str(sub_process.pid))
        timeout = float(timeout)
        pipe = subproc.Popen(cmd, 0, None, None, subproc.PIPE, subproc.PIPE,
                             shell=True)
        timer = threading.Timer(timeout*60, timeout_trigger, args=(pipe,))
        timer.start()
        pipe.wait()
        timer.cancel()

        self.ret_val = pipe.returncode
        self.ret_info = pipe.stdout.read()
        return self.ret_val, self.ret_info

    def __str__(self):
        return '{code}:{info}'.format(code=self.ret_val, info=self.ret_info)


class DistDownloader(object):
    """

    """
    def __init__(self):
        # Establish communication queues
        self.tasks = multiproc.JoinableQueue()
        self.results = multiproc.Queue()

    def start(self):
        # Start consumers
        num = multiproc.cpu_count()
        print ('Creating %d consumers' % num)
        consumers = [Consumer(self.tasks, self.results)
                     for i in xrange(num)]
        for consumer in consumers:
            consumer.start()

    def enque_task(self, task):
        # Enqueue jobs
        self.tasks.put(task)

    def join_all(self):
        # Wait for all of the tasks to finish
        self.tasks.join()


if __name__ == '__main__':
    downer = DistDownloader()
    downer.start()

    # Enqueue jobs
    num_jobs = 10
    for i in range(num_jobs):
        downer.enque_task(Task(i, i))

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
