# -*- coding: utf-8 -*-
""" """

import os
import logging
import datetime
import uuid
import requests

from common import logger

def check_create_path(path_str, is_dir=False):
    if is_dir:
        _dir = path_str
    else:
        _dir = os.path.dirname(path_str)
    if not os.path.exists(_dir):
        os.makedirs(_dir)
        logging.info('Creating dir: {}'.format(_dir))

def get_unique_name_by_time(file_name):
    suffix = datetime.datetime.now().strftime("%y%m%d_%H%M%S")
    # file_name = os.path.basename(file_name)
    base_name_ = os.path.splitext(file_name)[0]
    ext_name_ = os.path.splitext(file_name)[-1]
    return '{}_{}{}'.format(base_name_,suffix,ext_name_)

def copy_local_path(a, b, copy_base_path=False):
    ''' copy from a to b
    a(str): 
        1. a is dir -> copy dir (copy_base_path=True)
                       copy all files in dir (copy_base_path=False)
        2. a is file -> copy file to b
    b(str):
    copy_base_path(bool)
    '''
    if os.path.isdir(a):
        if not copy_base_path:
            a = '{}/*'.format(a)
    cmd = 'cp -r {} {}'.format(a, b)
    logging.info('Runing cmd: {}'.format(cmd))
    os.system(cmd)

if __name__ == "__main__":
    print(get_unique_name_by_time(''))