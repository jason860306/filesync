#!/usr/bin/python
# encoding: utf-8


__file__ = '$'
__author__ = 'szj0306'  # 志杰
__date__ = '7/4/17 2:09 PM'
__license__ = "Public Domain"
__version__ = '$'
__email__ = "jason860306@gmail.com"
# '$'


import os

PATH_LEVEL = 2
PATH_NUM = 256


class CreatePath(object):
    """

    """

    def __init__(self, level=PATH_LEVEL, dirnum=PATH_NUM):
        self.dir_level = level
        self.dir_num = dirnum

    def __call__(self, path):
        dir_mode = 0755
        for i in xrange(self.dir_num):
            dir1_name = "%02X" % i
            path1 = "%s/%s" % (path, dir1_name)
            if not os.path.exists(path1):
                os.makedirs(path1, dir_mode)
            for j in xrange(self.dir_num):
                dir2_name = "%02X" % j
                path2 = "%s/%s" % (path1, dir2_name)
                if not os.path.exists(path2):
                    os.mkdir(path2, dir_mode)
