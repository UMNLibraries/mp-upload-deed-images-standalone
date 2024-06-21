import os
import re
import time
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
    min_thread_time = 0

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

        parser.add_argument('-m', '--mintime', type=float,
                            help='What is the minimum time to execute each thread (rate limit) in seconds (Default is 0)')

        parser.add_argument('-d', '--dry', action='store_true',
                            help='Just tell me how many keys are left to upload and exit')
        self.args = parser.parse_args()
        print(self.args)

    def gather_raw_image_paths(self, workflow_slug, deed_image_glob_root, deed_image_glob_remainders):
        print("Gathering all raw images paths for this workflow ...")

        raw_images = []

        for path in deed_image_glob_remainders:
            print(os.path.join(deed_image_glob_root, path))
            raw_images.extend(glob.glob(os.path.join(deed_image_glob_root, path), recursive=True))

        img_df = pd.DataFrame(raw_images, columns=['local_path'])
        img_df['remainder'] = img_df['local_path'].apply(lambda x: PurePosixPath(x).relative_to(deed_image_glob_root))
        img_df['filename'] = img_df['local_path'].apply(lambda x: Path(x).name)

        img_df['s3_path'] = img_df['remainder'].apply(
            lambda x: os.path.join('raw', workflow_slug, x)
        )
        return img_df

    def convert_json_to_raw(self, json_key):
        return re.sub('_SPLITPAGE_\d+', '', json_key).replace('ocr/json/', 'raw/').replace('.json', '.raw_ext')
    
    def strip_raw_extension(self, raw_key):
        return re.sub(r'\.[A-Za-z]{3,4}$', '.raw_ext', raw_key)

    def check_already_uploaded(self, workflow_slug, upload_keys):
        print("Checking s3 to see what images have already been uploaded...")
        s3 = self.session.resource('s3')

        key_filter = re.compile(f"ocr/json/{workflow_slug}/.+\.json")  # OCR JSON results
        # key_filter = re.compile(f"raw/{workflow_slug}/.+\.tif")]
        # OLD: Look for jpgs that have made it all the way through the process
        # key_filter = re.compile(f"web/{workflow_slug}/.+\.jpg")

        matching_keys = [self.convert_json_to_raw(obj.key) for obj in self.bucket.objects.filter(
            Prefix=f'ocr/json/{workflow_slug}/'
        ) if re.match(key_filter, obj.key)]

        print(f"Found {len(matching_keys)} matching keys in bucket")

        web_keys_to_check = [self.strip_raw_extension(key['s3_path']) for key in upload_keys]

        print("Processed matching keys")

        # subtract already uploaded matching_keys from web_keys_to_check
        already_uploaded = set(web_keys_to_check).intersection(matching_keys)
        print("Found intersection")
        remaining_to_upload = [
            u for u in upload_keys if u['s3_path'] not in already_uploaded]
        print(
            f"Found {len(already_uploaded)} images already uploaded, {len(remaining_to_upload)} remaining...")

        return remaining_to_upload

    def upload_image(self, key_dict):
        start_time = time.time()
        print(f"Uploading {key_dict['s3_path']}")
        self.bucket.upload_file(
            key_dict['local_path'], key_dict['s3_path'], ExtraArgs={
              'StorageClass': self.raw_storage_class
            })

        # If necessary, wait before completing
        if self.min_thread_time > 0:
            elapsed = time.time() - start_time
            time_remaining = self.min_thread_time - elapsed
            if time_remaining > 0:
                print(f'Pausing {time_remaining} seconds')
                time.sleep(time_remaining)

    def handle(self, *args, **kwargs):
        workflow_name = self.args.workflow
        load_from_cache = self.args.cache
        num_threads = self.args.pool if self.args.pool else 8
        self.min_thread_time = self.args.mintime if self.args.mintime else 0

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
                    "Scanning filesystem for local images using 'deed_image_glob_root', 'deed_image_glob_remainders' setting...")
                raw_img_df = self.gather_raw_image_paths(
                    workflow_slug,
                    workflow_config['deed_image_glob_root'],
                    workflow_config['deed_image_glob_remainders'])

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

            if self.args.dry:
                # Exit without uploading.
                return True

            pool = ThreadPool(processes=num_threads)
            pool.map(self.upload_image, filtered_upload_keys)

uploader = Uploader()
