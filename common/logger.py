import logging
import sys
logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format='%(asctime)s [%(levelname)s] %(message)s')

if __name__ == '__main__':
    logging.info('{} test logging info'.format('hello,'))
    logging.debug('{} test logging debug'.format('hello,'))