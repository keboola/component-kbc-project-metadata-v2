import csv
import json

from keboola.component.dao import TableDefinition


class Writer:

    def __init__(self, table_definition: TableDefinition):

        self.tdf = table_definition

    def create_manifest(self):

        template = {
            'incremental': self.tdf.incremental,
            'primary_key': self.tdf.primary_key,
            'columns': self.tdf.columns
        }

        path = self.tdf.full_path + '.manifest'

        with open(path, 'w') as manifest:
            json.dump(template, manifest)

    def __enter__(self):
        self.io = open(self.tdf.full_path, 'a')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.io.close()

    def create_writer(self):
        self.writer = csv.DictWriter(self.io, fieldnames=self.tdf.writer_columns,
                                     restval='', extrasaction='ignore', quotechar='\"', quoting=csv.QUOTE_ALL)

    def write_row(self, row, parent_dict=None):

        if hasattr(self, 'writer') is False:
            self.create_writer()

        save_aside = {}
        for field in self.tdf.json_columns:
            if field not in row:
                continue
            save_aside[field] = json.dumps(row[field])
            del row[field]

        row_f = {**self.flatten_json(x=row), **save_aside}
        _dict_to_write = {}

        for key, value in row_f.items():

            if key in self.tdf.writer_columns:
                _dict_to_write[key] = value

            else:
                continue

        if parent_dict is not None:
            _dict_to_write = {**_dict_to_write, **parent_dict}

        self.writer.writerow(_dict_to_write)

    def write_rows(self, list_to_write, parent_dict=None):

        for row in list_to_write:
            self.write_row(row, parent_dict)

    def flatten_json(self, x, out=None, name=''):
        if out is None:
            out = dict()

        if type(x) is dict:
            for a in x:
                self.flatten_json(x[a], out, name + a + '_')
        else:
            out[name[:-1]] = x

        return out
