# -*- coding: utf-8 -*-
""" """

def load_libsvm_list_generator(input_path, missing_value=None, num_features=None, gen_label=True):
    with open(input_path, 'r') as f:
        for line in f:
            line = line.strip().split(' ')
            label = None
            if ':' in line[0]:
                feature_line = line
            else:
                feature_line = line[1:]
                label = line[0]
            feature_line = dict(map(lambda x:[int(x.split(':')[0]), x.split(':')[1]], feature_line))
            if gen_label and (label is None):
                raise Exception('There s no label in {}'.format(input_path))
            if gen_label:
                feature_line['label'] = label
            if num_features is not None:
                base_line = dict(zip(range(num_features), [missing_value]*num_features))
                base_line.update(feature_line)
                yield base_line
            else:
                yield feature_line
if __name__ == "__main__":
    pass