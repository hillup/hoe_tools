# -*- coding: utf-8

import os
import sys

import fileinput
from contextlib import closing
import json
import logging
from io import open
import glob

from common import logger

def load_json_list_generator(json_file, **kwargs):
  count = 0
  failed = 0
  if '*' in json_file or '[' in json_file:
    input_files = glob.iglob(json_file)
  elif os.path.isdir(json_file):
    json_file = '{}/*'.format(json_file)
  input_files = glob.iglob(json_file)
  with closing(fileinput.input(input_files)) as f:
    for idx, line in enumerate(f):
      try:
        line = line.strip()
        data = json.loads(line)
        yield data
        count += 1
      except Exception as e:
        logging.info('Failed to load json data at line {}: {}'.format(idx, e))
        failed += 1
  logging.info(
      'Loaded {} data from {}, {} failed'.format(count, json_file, failed))


def load_json_list(json_file, **kwargs):
  return [d for d in load_json_list_generator(json_file, **kwargs)]


def dump_json_line(data, encode=False):
  line = json.dumps(data, ensure_ascii=False)
  if encode:
    line = line.encode('utf-8')
  return line

def dump_json_list_file(data_list, output_path, append=False, field_names=[], **kwargs):
  mode = 'a' if append else 'w'
  count = 0
  failed = 0
  with open(output_path, mode, encoding='utf-8') as f:
    for idx, d in enumerate(data_list):
      try:
        if field_names:
          d = {f: d.get(f, "") for f in field_names}
        line = dump_json_line(d)
        f.write(line + '\n')
        count += 1
      except Exception as e:
        logging.error('Failed to write data to file: {}'.format(e))
        failed += 1
  logging.info('{} {} data to {}, {} failed'.format(
      'Appended' if append else 'Wrote', count, output_path, failed))


def load_json_str_recursive(item):
  if isinstance(item, (list, tuple, set)):
    return [load_json_str_recursive(i) for i in item]
  elif isinstance(item, dict):
    return {k: load_json_str_recursive(v) for k, v in item.items()}
  elif isinstance(item, (str, unicode)):
    try:
      item = json.loads(item)
      return load_json_str_recursive(item)
    except:
      return item
  else:
    return item


def load_json_file(json_file):
  with open(json_file) as f:
    conf = json.load(f)
  return conf


def dump_json_file(data, output_path, mode='w', indent=2):
  assert mode in ('a', 'w')
  count = 0
  with open(output_path, mode, encoding='utf-8') as f:
    line = json.dumps(data, ensure_ascii=False, indent=indent)
    f.write(line + '\n')
  logging.info('{} data to {}'.format('Append' if mode == 'a' else 'Wrote',
                                      output_path))


class JsonWriter:
  def __init__(self, file_name, write_byte=False, mode='w'):
    self.f = open(file_name, mode, encoding='utf-8')


  def write_row(self, line_dict):
    line = json.dumps(line_dict, ensure_ascii=False)
    self.f.write(line + '\n')
  
  
  def flush(self):
    self.f.flush()

  def close(self):
    self.f.close()



if __name__ == '__main__':
  lines = load_json_list_generator('')
  