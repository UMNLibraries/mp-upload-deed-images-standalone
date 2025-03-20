import pytest

from upload import Uploader

def test_standard_raw_ext_filter():
    uploader = Uploader()
    assert uploader.strip_raw_extension('hello.jpg') == 'hello.raw_ext'

def test_numeric_raw_ext_filter():
    uploader = Uploader()
    assert uploader.strip_raw_extension('hello.001') == 'hello.001.raw_ext'

def test_check_already_uploaded():
    uploader = Uploader()

    already_uploaded_keys = [
        uploader.strip_raw_extension('raw/my-workflow/foo_1234.tif'),
        uploader.strip_raw_extension('raw/my-workflow/foo_12345.tif'),
        uploader.strip_raw_extension('raw/my-workflow/foo_1234567.001'),
    ]

    # Things locally that need to be uploaded
    upload_keys = [
        {'s3_path': 'raw/my-workflow/foo_1234.tif'},
        {'s3_path': 'raw/my-workflow/foo_12345.tif'},
        {'s3_path': 'raw/my-workflow/foo_12346.tif'},
        {'s3_path': 'raw/my-workflow/foo_1234567.001'},
    ]
    
    remaining_to_upload = uploader.compare_key_sets(upload_keys, already_uploaded_keys)
    assert remaining_to_upload == [{'s3_path': 'raw/my-workflow/foo_12346.tif'}]