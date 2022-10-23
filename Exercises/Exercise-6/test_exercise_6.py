import pytest
import findspark
findspark.init()
from unittest.mock import Mock, MagicMock, patch
import main
from pyspark.sql import SparkSession
from pathlib import Path
from pyspark.sql.types import *
from datetime import date
from chispa.dataframe_comparer import assert_df_equality
    

class Test_read_zipfile_content_to_memory:
    
    @pytest.mark.parametrize(
        'name_list',
        [
            pytest.param('abc.csv'),
            pytest.param('thisistest.avc.csv'),
            pytest.param('abc', marks=pytest.mark.xfail),
            pytest.param('/ab.csv', marks=pytest.mark.xfail)
        ]
    )
    @patch('main.ZipFile')
    def test_match_filename(self, mock_ZipFile, name_list):
        mock_ZipFile.return_value.__enter__.return_value.namelist.return_value = [name_list]
        res = main.read_zipfile_content_to_memory(Path('.'))
        assert res != []

input_csv_data = [
    b'id,started_at,distance\r\n'
    b'1234,2019-10-01 20:06:59,5678\r\n'
    b'1235,2021-10-01 20:06:59,5679',
    b'id,start_time,distance\r\n'
    b'1234,2019-10-01 20:06:59,5678\r\n'
    b'1235,2021-10-01 20:06:59,5679'
]

expected_data = [
    [
        ('1234', '2019-10-01 20:06:59', '5678', date(2019, 10, 1)),
        ('1235', '2021-10-01 20:06:59', '5679', date(2021, 10, 1))
    ],
    [
        ('1234', '2019-10-01 20:06:59', '5678', date(2019, 10, 1)),
        ('1235', '2021-10-01 20:06:59', '5679', date(2021, 10, 1))
    ]
]



class TestReadDataIntoSpark:
    
    @pytest.fixture()
    def zip_file_path(self,):
        return Path("test.zip")
    
    @pytest.fixture()
    def expected_df(self, request, sparksession):
        header = request.param[0]
        schema = StructType([
            StructField('{}'.format(header[0]), StringType()),
            StructField('{}'.format(header[1]), StringType()),
            StructField('{}'.format(header[2]), StringType()),
            StructField('{}'.format(header[3]), DateType())
        ])
        return sparksession.createDataFrame(request.param[1], schema)
    
    @pytest.fixture()
    def zip_file_paths(self,):
        return [Path('./temp')]
    
    @pytest.mark.parametrize(
        'input_csv,expected_df',
        [
            (input_csv_data[0], [['id', 'started_at', 'distance', 'date'], expected_data[0]]),
            (input_csv_data[1], [['id', 'start_time', 'distance', 'date'], expected_data[1]])
        ],
        indirect=['expected_df']
    )
    @patch('main.read_zipfile_content_to_memory', create=True)
    def test_read_data_into_spark(self, mock_read_zipfile, zip_file_paths, sparksession, 
                                   input_csv, expected_df):
        
        mock_read_zipfile.return_value = [input_csv]
        csv_frames = main.read_data_into_spark(zip_file_paths, sparksession)
        assert_df_equality(csv_frames[str(zip_file_paths[0])], expected_df)
        


