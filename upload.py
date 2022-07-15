import os
import re
import glob
import boto3
import argparse
import pandas as pd
from slugify import slugify
from pathlib import Path, PurePosixPath
from multiprocessing.pool import ThreadPool

from config import WORKFLOW_SETTINGS, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_STORAGE_BUCKET_NAME


class Uploader:
    ''' This is a standalone version of the upload_deed_images.py Django management command in racial_covenants_processor. See README for more details.'''
    raw_storage_class = 'GLACIER_IR'
    args = {}
    session = boto3.Session(
             aws_access_key_id=AWS_ACCESS_KEY_ID,
             aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    s3 = None
    bucket = None

    def __init__(self, *args, **kwargs):
        self.add_arguments()
        self.handle(*args, **kwargs)

    def add_arguments(self):
        parser = argparse.ArgumentParser(description='Uploader options')
        parser.add_argument('-w', '--workflow', type=str,
                            help='Name of Zooniverse workflow to process, e.g. "Ramsey County"')

        parser.add_argument('-c', '--cache', action='store_true',
                            help='Load raw image list from cache')

        parser.add_argument('-p', '--pool', type=int,
                            help='How many threads to use? (Default is 8)')
        self.args = parser.parse_args()
        print(self.args)

    def gather_raw_image_paths(self, workflow_slug, deed_image_glob_root, deed_image_glob_remainder):
        print("Gathering all raw images paths for this workflow ...")
        print(os.path.join(deed_image_glob_root, deed_image_glob_remainder))
        raw_images = glob.glob(os.path.join(deed_image_glob_root, deed_image_glob_remainder), recursive=True)

        img_df = pd.DataFrame(raw_images, columns=['local_path'])
        img_df['remainder'] = img_df['local_path'].apply(lambda x: PurePosixPath(x).relative_to(deed_image_glob_root))
        img_df['filename'] = img_df['local_path'].apply(lambda x: Path(x).name)

        img_df['s3_path'] = img_df['remainder'].apply(
            lambda x: os.path.join('raw', workflow_slug, x)
        )
        return img_df

    def check_already_uploaded(self, workflow_slug, upload_keys):
        print("Checking s3 to see what images have already been uploaded...")
        s3 = self.session.resource('s3')

        key_filter = re.compile(f"raw/{workflow_slug}/.+\.tif")

        matching_keys = [obj.key for obj in self.bucket.objects.filter(
            Prefix=f'raw/{workflow_slug}/'
        ) if re.match(key_filter, obj.key)]

        web_keys_to_check = [key['s3_path'] for key in upload_keys]

        # subtract already uploaded matching_keys from web_keys_to_check
        already_uploaded = set(web_keys_to_check).intersection(matching_keys)
        remaining_to_upload = [
            u for u in upload_keys if u['s3_path'] not in already_uploaded]
        print(
            f"Found {len(already_uploaded)} images already uploaded, {len(remaining_to_upload)} remaining...")

        return remaining_to_upload

    def upload_image(self, key_dict):
        print(f"Uploading {key_dict['s3_path']}")
        self.bucket.upload_file(
            key_dict['local_path'], key_dict['s3_path'], ExtraArgs={
              'StorageClass': self.raw_storage_class
            })

    def handle(self, *args, **kwargs):
        workflow_name = self.args.workflow
        load_from_cache = self.args.cache
        num_threads = self.args.pool if self.args.pool else 8

        if not workflow_name:
            print('Missing workflow name. Please specify with --workflow.')
        else:
            print(workflow_name)
            workflow_config = WORKFLOW_SETTINGS[workflow_name]
            workflow_slug = slugify(workflow_name)

            os.makedirs('data', exist_ok=True)

            if self.args.cache:
                # Read option so you don't have to wait to crawl filesystem again
                try:
                    print("Attempting load from cached image list...")
                    raw_img_df = pd.read_csv(os.path.join(
                        'data', f"{workflow_slug}_raw_images_list.csv"))
                except:
                    print(
                        "Can't read cached file list. Try not using the --cache flag")
                    return False


            else:
                print(
                    "Scanning filesystem for local images using 'deed_image_glob_root', 'deed_image_glob_remainder' setting...")
                raw_img_df = self.gather_raw_image_paths(
                    workflow_slug,
                    workflow_config['deed_image_glob_root'],
                    workflow_config['deed_image_glob_remainder'])

                raw_img_df.to_csv(os.path.join(
                    'data', f"{workflow_slug}_raw_images_list.csv"), index=False)

            upload_keys = raw_img_df[[
                'local_path',
                's3_path'
            ]].to_dict('records')

            self.s3 = self.session.resource('s3')
            self.bucket = self.s3.Bucket(AWS_STORAGE_BUCKET_NAME)

            filtered_upload_keys = self.check_already_uploaded(
                workflow_slug, upload_keys)

            pool = ThreadPool(processes=num_threads)
            pool.map(self.upload_image, filtered_upload_keys)

uploader = Uploader()
