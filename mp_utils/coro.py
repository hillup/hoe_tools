from gevent import monkey, pool 
monkey.patch_all()

import gevent 
import time 
import logging

from common import logger

SINCE = 'global_since_time'
DISPLAT_INTERVAL = 100 

def coro_run(items, fn, num_workers, kargs):
    '''
    items: items to be enumerate, element should be the first parameter of fn
    fn (func): working function
    num_workers (int): num of workers
    kargs (dickt): dict of extra args of working function

    Example:

        items = range(1000)
        kargs = {'title': 'hello', 'fp': open('tmp.txt', 'a')}
        num_workers = 100
        
        def f(item, title, fp):
            time.sleep(0.5)
            fp.write('{} {}\n'.format(item, title))
            fp.flush()
        
        coro_run(items, f, num_workers, kargs)
    '''
    pool_size = num_workers
    p = pool.Pool(pool_size)
    global SINCE 
    SINCE = time.time()
    for i, item in enumerate(items):
        p.spawn(fn, item, **kargs)
        if i % DISPLAT_INTERVAL == 0 and i != 0:
            logging.info('Finished processing {} items, qps {:.2f}'.format(i, i / (time.time()-SINCE)))
    p.join()

if __name__ == "__main__":
    items = range(1000)
    kargs = {'title': 'hello', 'fp': open('tmp.txt', 'a')}
    num_workers = 100 

    def f(item, title, fp):
        time.sleep(0.5)
        fp.write("{} {}\n".format(item, title))
        fp.flush()
    coro_run(items, f, num_workers, kargs)
