# -*- coding: utf-8

import csv
import logging
from io import open
import sys
from common import logger

def load_csv_list_generator(csv_file, csv_delimiter=',', csv_with_field_names=True, **kwargs):
  f = open(csv_file, 'r', encoding='utf-8')
  reader = csv.reader(f, delimiter=csv_delimiter)
  field_names = []
  if csv_with_field_names:
    field_names = next(reader)
    def generator():
      while True:
        try:
          row = next(reader)
          if field_names:
            data = {k: v for k, v in zip(field_names, row)}
          else:
            data = row
          yield data
        except StopIteration:
          break
        except:
          logging.exception(
              'failed to generate data from csv file: {}'.format(csv_file))
          continue

    return generator()
  else:
    return reader


def dump_csv_list_file(data_list,
                       output_path,
                       field_names=[],
                       csv_with_field_names=True,
                       csv_delimiter=',',
                       mode='w'):
  count = 0
  failed = 0
  if 'w' not in mode:
    csv_with_field_names = False
  with open(output_path, mode, encoding='utf-8') as f:
    writer = csv.writer(f, delimiter=csv_delimiter)
    for data in data_list:
      try:
        if not data:
          raise ValueError('data is empty')
        if count == 0:
          if not field_names:
            if isinstance(data, dict):
              field_names = data.keys()
          if csv_with_field_names and field_names:
            writer.writerow(field_names)
        if isinstance(data, dict):
          data = [data.get(f, '') for f in field_names]
        if not isinstance(data, (list, tuple)):
          raise TypeError(
              'data type {} invalid, expect list or tuple'.format(type(data)))
        if not data:
          raise ValueError('data is empty or none fields is in data')
        writer.writerow(data)
        count += 1
      except:
        logging.exception('failed to write data: {}'.format(data))
        failed += 1
  logging.info('Wrote {} data to file {}, {} failed'.format(
      count, output_path, failed))
  return count, failed, count + failed


def add_csv_args(parser):
  c = parser.add_argument_group('Arguments for csv file IO')
  c.add_argument('--csv_delimiter', type=str, default='\t', help='CSV/TSV column delimiter')
  return c

class CsvWriter:
  def __init__(self, file_name, write_byte=False, csv_delimiter=',', mode='w'):
    self.f = open(file_name, mode, encoding='utf-8')
    self.writer = csv.writer(self.f, delimiter=csv_delimiter)
    self.header_flag = True
    self.field_names = None

  def write_row(self, line_dict):
    if self.header_flag:
      self.field_names = line_dict.keys()
      self.writer.writerow(self.field_names)
      data = [line_dict.get(f, '') for f in self.field_names]
      self.writer.writerow(data)
      self.header_flag = False
      data = [line_dict.get(f, '') for f in self.field_names]
      self.writer.writerow(data)
    else:
      data = [line_dict.get(f, '') for f in self.field_names]
      self.writer.writerow(data)

  def flush(self):
    self.f.flush()
  
  def close(self):
    self.f.close()

