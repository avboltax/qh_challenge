#!/usr/bin/python
import argparse
import hashlib
import sqlite3
import json
from itertools import chain

from pandas import read_csv
import pandas

parser = argparse.ArgumentParser()
parser.add_argument('--csv', help="path to csv file")
parser.add_argument('--config', help="path to config file")
parser.add_argument('--name', help="name of sql table")
parser.add_argument('--sample', help="path to where outputted sample should be stored")
args = parser.parse_args()


class Parser(object):
    def __init__(self):
        self.db = '{}{}'.format(str(args.name), '.db')
        self.df = read_csv(str(args.csv))
        # create connection to sqlite3
        self.cnx = sqlite3.connect(self.db)

    def parse_csv(self):
        """ call to perform all operations """
        if args.config:
            transform, sample = self.read_config()
            self.parse_transform(transform)
            self.parse_sample(sample)
        else:
            self.df.to_csv(str(args.sample))
        # then use df.to_sql to write the values to a sql db ()
        self.df.to_sql(str(args.name), self.cnx, if_exists='replace')

    def read_config(self):
        with open(str(args.config), 'rb') as cf:
            data = json.load(cf)
            return data['TRANSFORM'], data['SAMPLE']

    def parse_transform(self, transform):
        """ applies hash and remove operations to dataframe """
        if 'h' in transform.keys():
            hash_cols = self.create_index(transform['h'])
            self.df[self.df.columns[hash_cols]] = self.hash_frame(hash_cols)
        if 'r' in transform.keys():
            drop_cols = self.create_index(transform['r'])
            self.df = self.remove_cols(drop_cols)

    def parse_sample(self, sample):
        """ creates a csv of random and/or specified rows """
        if 'rand' in sample.keys():
            out_df = self.df.sample(frac=float(sample['rand']), axis=0)
        if 'row' in sample.keys():
            out_df = self.df.iloc[self.create_index(sample['row'])]

        out_df.to_csv(str(args.sample))

    def remove_cols(self, drop_cols):
        """ drops specified columns from the dataframe """
        return self.df.drop(self.df.columns[drop_cols], axis=1)

    def hash_frame(self, hash_cols):
        """ hashes elements of specified columns """
        # needs to return a string type for sqlite3
        return (self.df[self.df.columns[hash_cols]].applymap(
                lambda x: hashlib.sha256(x).hexdigest()))

    def create_index(self, index):
        """ takes a list of mixed ranges and expands into index list"""
        # split numeric string on comma and create tuples of ranges and singles
        spans = (num.partition('-')[::2] for num in index.split(','))
        # creates ranges for tuples
        ranges = (range(int(low), int(high) + 1 if high else int(low) + 1)
                  for low, high in spans)
        # builds a list of (non-duplicated) indexes
        return list(chain.from_iterable(ranges))

# execute file parsing class
Parser().parse_csv()
