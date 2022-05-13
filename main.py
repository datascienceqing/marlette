"""
This is the entrypoint to the program. 'python main.py' will be executed and the 
expected csv file should exist in ../data/destination/ after the execution is complete.
"""
from collections import defaultdict
from src.some_storage_library import SomeStorageLibrary
import os
import csv
from typing import Dict, List
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

SOURCECOLUMNS = 'data/source/SOURCECOLUMNS.txt'
SOURCEDATA = 'data/source/SOURCEDATA.txt'
ENCODING = 'utf-8'
SEPARATOR = '|'
OUTPUT = 'data/stage/joined_output.csv'


class DataParser:
    """A Data Parser that reads columns information from one text file and data from another text file.
        Combine the two file, sort column, and dump to 'data/destination' folder as a csv file"""

    def __init__(self, sourcecolumns: str, sourcedata: str, encoding: str, sep: str, output: str) -> None:
        self.sourcecolumns = sourcecolumns
        self.sourcedata = sourcedata
        self.encoding = encoding
        self.sep = sep
        self.output = output

    @staticmethod
    def read_text(filename: str, encoding: str = 'utf-8') -> List[str]:
        """read one text file into lines"""
        with open(file=filename, encoding=encoding) as f:
            lines = f.readlines()
            if lines is None:
                raise ValueError(f"Can't read {filename}")
        return lines

    def parse_columns(self, filename: str, encoding: str, sep: str) -> Dict[int, str]:
        """parse SourceColumns data and sort by first column"""
        data = dict()
        lines = self.read_text(filename, encoding)
        self.column_count = 0  # get a counter for quality check
        for line in lines:
            l = line.strip('\n').split(sep)
            if l[0] is None:
                raise ValueError(
                    f"First column of {filename} has missing values")
            else:
                data[int(l[0])] = l[1]
            self.column_count += 1
        return dict(sorted(data.items()))

    def parse_data(self, filename: str, encoding: str, sep: str) -> Dict[int, List[str]]:
        """parse text body data"""
        data = defaultdict(list)
        lines = self.read_text(filename, encoding)
        self.row_count = 0.  # get a counter for quality check
        for line in lines:
            cells = line.strip('\n').split(sep)
            for i in range(len(cells)):
                data[i+1].append(cells[i])
            self.row_count += 1
        return data

    def join_two_dicts(self) -> Dict[str, List[str]]:
        """Join the two dictionaries and load them into a list of dictionaries"""
        res = []
        header = self.parse_columns(self.sourcecolumns, self.encoding, self.sep)
        data = self.parse_data(self.sourcedata, self.encoding, self.sep)
        data_length = len(data[1])
        for i in range(data_length):
            d = {}
            for k in header:
                if k != 1:
                    d[header[k]] = data[k][i]
            res.append(d)
        return res

    def load_to_stage(self, res: List[Dict[str, str]]) -> None:
        """Load the list of dictionaries to csv"""
        with open(self.output, 'w') as csvFile:
            writer = csv.DictWriter(csvFile, fieldnames=res[0].keys())
            writer.writeheader()
            writer.writerows(res)
        print("Successfully loaded data into stage folder!! ")


def main(loader: SomeStorageLibrary, data_parser: DataParser) -> None:

    joined_dict = data_parser.join_two_dicts()
    data_parser.load_to_stage(joined_dict)
    stage = DataParser.read_text(OUTPUT)

    # check the dimensions
    assert data_parser.row_count == len(stage) - 1
    assert data_parser.column_count == len(stage[0].split(',')) + 1

    loader.load_csv(OUTPUT)


if __name__ == '__main__':
    """Entrypoint"""
    print('Beginning the ETL process...')

    # create data_parser instance
    data_parser = DataParser(SOURCECOLUMNS, SOURCEDATA,
                             ENCODING, SEPARATOR, OUTPUT)

    # create loader instance
    loader = SomeStorageLibrary()

    main(loader, data_parser)
