import os 
import sys 

import logging 
import multiprocessing as mp 
import time 

from common import logger 

MAX_QUEEN_SIZE = 1024
DISPLAY_INTERVAL = 20

def fill_in_q(items, in_q, num_workers):
    '''
    items: input items to be procuessed
    in_q: Input Queue for producer
    num_workers: number of workers
    '''
    for item in items:
        in_q.put(item)
    for _ in range(num_workers):
        in_q.put(None)

def produce(in_q, out_q, producer, args, worker_id):
    '''
    in_q: Input Queue to producer
    out_q: Output Queue from produder
    producer: produde function
    args: dict of args
    worker_id: workder id
    '''
    item = in_q.get()
    all_ct = 0
    fail_ct = 0
    since = time.time()

    while item is not None:
        try:
            all_ct += 1
            ret = producer(item, args)
            out_q.put(ret)
            if all_ct % DISPLAY_INTERVAL == 0:
                last = time.time() - since
                since = time.time()
                logging.info('Worker {:2d}: {} items has been produced, speed={}'.format(worker_id, all_ct, last*1.0/DISPLAY_INTERVAL))
        except Exception:
            fail_ct += 1
            logging.exception('Worker {:2d}: fail to produce {}'.format(worker_id, item))
        finally:
            item = in_q.get()
    out_q.put(None)
    logging.info('Worker {:2d}: Succeed to producing {} items, {} failed'.format(worker_id, all_ct-fail_ct, fail_ct))

def consume(out_q, consumer, args, worker_id, num_workers):
    '''
    out_q: Output Queue from producer
    consumer: consume function
    args: dict of args
    worker_id: worker id
    num_worker: num of workers
    '''
    all_ct = 0
    fail_ct = 0
    while num_workers > 0:
        item = out_q.get()
        if item is None:
            num_workers -= 1
            if num_workers > 0:
                out_q.put(None)
            continue 
        
        try:
            all_ct += 1
            consumer(item, args)
            if all_ct % DISPLAY_INTERVAL == 0:
                logging.info('Worker {:2d}: {} items has been consumed...'.format(worker_id, all_ct))
        except Exception:
            fail_ct += 1
            logging.exception('Worker {:2d}: fail to consume {}'.format(worker_id, item))
    logging.info('Worker {:2d}: Succeed to consuming {} items, {} failed'.format(worker_id, all_ct-fail_ct, fail_ct))

def run_mm(items, producer, consumer, args):
    pass 
    in_q = mp.Queue(maxsize=MAX_QUEEN_SIZE)
    out_q = mp.Queue(maxsize=MAX_QUEEN_SIZE)
    num_workers = args.get('num_workers', 1)
    processes = [mp.Process(target=fill_in_q, args=(items, in_q, num_workers))]
    logging.info("num_workers: {}".format(num_workers))

    for worker_id in range(num_workers):
        processes += [mp.Process(target=produce, args=(in_q, out_q, producer, args, worker_id))]
        processes += [mp.Process(target=consume, args=(out_q, consumer, args, worker_id, num_workers))]

    for proc in processes:
        proc.daemon = True 
        proc.start()
    for proc in processes:
        proc.join()

if __name__ == "__main__":
    import time 
    import json 

    def producer(item, args):
        item['pred'] = '{} fuck'.format(item.get('gid'))
        return item 
    
    def consumer(item, args):
        item = '{}\n'.format(json.dumps(item))
        args['f'].write(item)
        args['f'].flush()
    
    in_items = ({'gid': i} for i in range(10000))
    args = {'num_workers': 10, 'f':open('tmp.json', 'w')}
    run_mm(in_items, producer, consumer, args)