import pytest, boto3, os
from moto import mock_s3

@pytest.fixture(scope="session")
def mock_aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"

@pytest.fixture(scope="session")
def mock_s3_client(mock_aws_credentials):
    with mock_s3():
        conn = boto3.client("s3", region_name="us-east-1")
        yield conn


    