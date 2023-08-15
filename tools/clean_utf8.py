# -*- coding: utf-8 -*-

import os
import sys

def clean_nul(input_file, output_file):
    fi = open(input_file, 'rb')
    data = fi.read()
    fi.close()
    fo = open(output_file, 'wb')
    fo.write(data.replace(b'\x00', b'').replace('\ufeff'.encode(),b''))
    fo.close()



if __name__ == '__main__':
    clean_nul(input_file='',
             output_file='',
            )