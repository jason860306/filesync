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
import time


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
                print ('%s: Exiting' % proc_name)
                self.task_queue.task_done()
                break
            print ('%s: %s' % (proc_name, next_task))
            answer = next_task()  # __call__()
            self.task_queue.task_done()
            self.result_queue.put(answer)
        return


class Task(object):
    """
    from multiprocessing import Process, Pipe

    def f(conn):
        conn.send([42, None, 'hello'])
        conn.close()

    if __name__ == '__main__':
        parent_conn, child_conn = Pipe()
        p = Process(target=f, args=(child_conn,))
        p.start()
        p.join()
        print(parent_conn.recv())   # prints "[42, None, 'hello']"
    """
    def __init__(self, a, b):
        self.a = a
        self.b = b

    def __call__(self):
        time.sleep(0.1)  # pretend to take some time to do the work
        return '%s * %s = %s' % (self.a, self.b, self.a * self.b)

    def __str__(self):
        return '%s * %s' % (self.a, self.b)


class DistDownloader(object):
    """
    
    """
    def __init__(self):
        # Establish communication queues
        self.tasks = multiproc.JoinableQueue()
        self.results = multiproc.Queue()

    def start(self):
        # Start consumers
        num_consumers = multiproc.cpu_count()
        print ('Creating %d consumers' % num_consumers)
        consumers = [Consumer(self.tasks, self.results)
                     for i in xrange(num_consumers)]
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
