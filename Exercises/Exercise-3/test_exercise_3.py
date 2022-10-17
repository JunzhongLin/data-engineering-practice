import pytest, gzip
from tempfile import NamedTemporaryFile

@pytest.fixture(scope="session")
def bucket_name():
    return "test"

@pytest.fixture(scope="session")
def obj_name():
    return "wet.path.gz"

@pytest.fixture(scope="session", autouse=True)
def s3_test_setup(mock_s3_client, bucket_name, obj_name):
    file_text = b"    test.com   "
    mock_s3_client.create_bucket(Bucket=bucket_name)
    with NamedTemporaryFile(mode='w+b', delete=True, suffix=".paths.gz") as tmp:
        gzip_file = gzip.GzipFile(mode='wb' ,fileobj=tmp)
        gzip_file.write(file_text)
        gzip_file.close()
        tmp.seek(0)

        mock_s3_client.upload_file(tmp.name, bucket_name, obj_name)
        
        yield tmp

    
class TestDownload:
    
    @pytest.fixture(scope="class")
    def act_download_obj(self, mock_s3_client, bucket_name, obj_name,):
        from main import downloader
        f_obj = downloader(bucket_s3=bucket_name, file_s3=obj_name)
        return f_obj
    
    def test_is_bucket_created(self, mock_s3_client):
        response = mock_s3_client.list_buckets()
        assert ["test"] == [bucket["Name"] for bucket in response["Buckets"]]
        
    def test_is_not_empty(self, act_download_obj):
        f_obj = act_download_obj
        assert f_obj.getbuffer().nbytes > 0
        
    def test_file_handler_at_start(self, act_download_obj):
        f_obj = act_download_obj
        assert f_obj.tell() == 0
                

class TestExractUrl:
    
    @pytest.fixture
    def act_extract_url(self, s3_test_setup):
        from main import extract_url
        url = extract_url(s3_test_setup)
        return url
    
    def test_is_desired_url(self, act_extract_url):
        url = act_extract_url
        assert url == "test.com"
        
    