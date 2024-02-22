import pytest
from dvc import config
from dvc.config import RemoteNotFoundError
from dvc.fs import LocalFileSystem, get_cloud_fs, get_fs_cls, get_fs_config
from dvc_http import HTTPFileSystem, HTTPSFileSystem
from dvc_s3 import S3FileSystem
from dvc_ssh import SSHFileSystem

# Add docstrings to functions and classes
url_cls_pairs = [
    # Cloud-based file systems
    ("s3://bucket/path", S3FileSystem),
    ("ssh://example.com:/dir/path", SSHFileSystem),
    ("http://example.com/path/to/file", HTTPFileSystem),
    ("https://example.com/path/to/file", HTTPSFileSystem),
    # Local file system
    ("path/to/file", LocalFileSystem),
    ("path\\to\\file", LocalFileSystem),
    ("file", LocalFileSystem),
    ("./file", LocalFileSystem),
    (".\\file", LocalFileSystem),
    ("../file", LocalFileSystem),
    ("..\\file", LocalFileSystem),
    ("unknown://path", LocalFileSystem),
]

try:
    from dvc_hdfs import HDFSFileSystem
    url_cls_pairs += [("hdfs://example.com/dir/path", HDFSFileSystem)]
except ImportError:
    HDFSFileSystem = None

# Use more descriptive names for test functions
@pytest.mark.parametrize("url, expected_cls", url_cls_pairs)
def test_get_fs_cls(url, expected_cls):
    """
    Test if the correct filesystem class is returned for a given URL.
    """
    assert get_fs_cls({"url": url}) == expected_cls

def test_get_fs_config():
    """
    Test the get_fs_config function.
    """
    result = get_fs_config({}, url="ssh://example.com:/dir/path")
    assert result == {"url": "ssh://example.com:/dir/path"}

def test_get_fs_config_error():
    """
    Test get_fs_config when RemoteNotFoundError is raised.
    """
    with pytest.raises(RemoteNotFoundError):
        get_fs_config({"remote": {}}, name="myremote")

def test_remote_url():
    """
    Test remote URL creation from configuration.
    """
    config = {
        "remote": {
            "base": {"url": "http://example.com"},
            "r1": {"url": "remote://base/r1", "user": "user"},
            "r2": {"url": "remote://r1/r2", "password": "123"},
        }
    }
    result = get_fs_config(config, url="remote://r2/foo")
    assert result == {
        "password": "123",
        "user": "user",
        "url": "http://example.com/r1/r2/foo",
    }

def test_get_cloud_fs():
    """
    Test get_cloud_fs function.
    """
    cls, config, path = get_cloud_fs({}, url="ssh://example.com:/dir/path")
    assert cls is SSHFileSystem
    assert config == {"host": "example.com", "verify": False}
    assert path == "/dir/path"
