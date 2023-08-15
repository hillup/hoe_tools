import os 
import sys 
import logging 

from common import logger, path_helper
from utils import json_utils, csv_utils, libsvm_utils
from tools import clean_utf8 

def cj_io(input_path, output_path, out_mode='w'):
    assert input_path != output_path, "output_path should be different with input_path"
    if input_path.endswith('csv'):
        logging.info('load csv input file from {}'.format(input_path))
        clean_utf8.clean_nul(input_path,input_path)
        lines = csv_utils.load_csv_list_generator(input_path)
    elif input_path.endswith('tsv'):
        logging.info('load tsv input file from {}'.format(input_path))
        clean_utf8.clean_nul(input_path,input_path)
        lines = csv_utils.load_csv_list_generator(input_path, csv_delimiter='\t')
    elif input_path.endswith('libsvm'):
        lines = libsvm_utils.load_libsvm_list_generator(input_path)
    else:
        logging.info('load json input file from {}'.format(input_path))
        lines = json_utils.load_json_list_generator(input_path)
    path_helper.check_create_path(output_path)
    if output_path.endswith('csv'):
        logging.info('save csv output file to {}'.format(output_path))
        writer = csv_utils.CsvWriter(output_path,mode=out_mode)
    elif output_path.endswith('tsv'):
        logging.info('save tsv output file to {}'.format(output_path))
        writer = csv_utils.CsvWriter(output_path, csv_delimiter='\t', mode=out_mode)
    else:
        logging.info('save json output file to {}'.format(output_path))
        writer = json_utils.JsonWriter(output_path,mode=out_mode)
    return lines, writer

def cj_i(input_path, missing_value=None, num_features=None, gen_label=True):
    if input_path.endswith('csv'):
        clean_utf8.clean_nul(input_path, input_path)
        lines = csv_utils.load_csv_list_generator(input_path)
    elif input_path.endswith('tsv'):
        logging.info('load csv input file from {}'.format(input_path))
        clean_utf8.clean_nul(input_path,input_path)
        lines = csv_utils.load_csv_list_generator(input_path, csv_delimiter='\t')
    elif input_path.endswith('libsvm'):
        lines = libsvm_utils.load_libsvm_list_generator(input_path, missing_value=missing_value, num_features=num_features, gen_label=gen_label)
    else:
        lines = json_utils.load_json_list_generator(input_path)
    return lines 

def cj_o(output_path, mode='w'):
    path_helper.check_create_path(output_path)
    if output_path.endswith('csv'):
        writer = csv_utils.CsvWriter(output_path, csv_delimiter=',', mode=mode)
    elif output_path.endswith('tsv'):
        writer = csv_utils.CsvWriter(output_path, csv_delimiter='\t', mode=mode)
    else:
        writer = json_utils.JsonWriter(output_path,mode=mode)
    return writer
