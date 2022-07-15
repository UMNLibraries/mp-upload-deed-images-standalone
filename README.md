# Mapping Prejudice deed images standalone uploader

This python command is designed to upload a large batch of images to S3 in a format that will trigger ingestion into the [racial_covenants_processor](https://github.com/UMNLibraries/racial_covenants_processor) (aka "Deed machine").

Large caches of images suitable for racial covenants research may be stored in an inconvenient location, and it may be cumbersome to move them elsewhere at scale.

The standalone uploader is designed as a lightweight option for two specific scenarios:

1. Deed images stored on a computer where it would be inconvenient to install the full Django deed machine locally due to version or platform constraints
1. Deed images stored on a computer or network where it would be time-consuming to transfer the images to a better developer machine. Connecting to such a machine as a drive and treating the images as local files is also similarly slow at a scale of hundreds of thousands of files.

Before using this uploader script, you should have your s3 bucket set up and the deed machine running elsewhere (see racial_covenants_processor docs).

After this process, the AWS step machine and lambdas will OCR the files, look for racial terms and generate a web-friendly jpeg.

Please note that each of these steps will incur AWS costs.

After this step, you should use deed machine commands to aggregate and ingest the results of this process from s3.

## Requirements
- Python 3 (Have not tried 2)
- boto3
- pandas
- python-slugify

## Usage:
1. Set up a config.py file with settings for AWS/boto (see sample file)
1. Create a workflow config to tell the uploader where to find your raw images. The key (in this example, "WI Milwaukee County") should match exactly your workflow name in the deed machine. (You should be able to paste your config settings from the deed machine here with no issues.)
```
WORKFLOW_SETTINGS = {
    'WI Milwaukee County': {
        'deed_image_glob_root': '/abs/path/to/image/root/',
        'deed_image_glob_remainder': '**/*.tif',
        'deed_image_regex': r'\/(?P<workflow_slug>[A-z\-]+)\/(?P<doc_date_year>\d{4})(?P<doc_date_month>\d{2})(?P<doc_date_day>\d{2})\/(?P<doc_num>[A-Z\d]+)_(?P<doc_type>[A-Z]+)_(?P<page_num>\d+)',
    }
}
```
1. Run `python upload.py --workflow "WI Milwaukee County"`

Optionally, you can use additional flags to control the number of multithreads (`--pool 16`) used or to skip the initial glob scanning of your local files and use a CSV manifest file to guide the upload (`--cache`), which may save time on large uploads that have been interrupted.
