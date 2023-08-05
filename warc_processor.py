import os.path
import sys
from cc_processor import parse_files, download_large_file
import argparse

DEFAULT_URL = 'https://data.commoncrawl.org/crawl-data/CC-MAIN-2023-23/segments/1685224643388.45/warc/CC-MAIN-20230527223515-20230528013515-00003.warc.gz'
DOWNLOAD_FILE_PATH = './data/'
DEFAULT_NUM_WORKERS = 4

if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(
        description='Helper script for downloading and parsing common crawl warc files.')
    arg_parser.add_argument('-u', '--url', help='URL of the WARC file.')
    arg_parser.add_argument('-e', '--existing_file_path',
                            help='Location of the existing WARC file to be processed')
    arg_parser.add_argument('-n', '--num_workers', help='Number of workers.')

    args = arg_parser.parse_args()

    url = DEFAULT_URL
    num_workers = DEFAULT_NUM_WORKERS

    if args.url:
        url = args.url
    else:
        print(f'\nUsing default URL: {DEFAULT_URL}')
    if args.num_workers:
        num_workers = int(args.num_workers)

    if args.existing_file_path:
        try:
            if not os.path.isfile(args.existing_file_path):
                raise FileNotFoundError(f"\nThe file {args.existing_file_path} does not exist.\n")
            else:
                parse_files(input_file=args.existing_file_path, num_workers=num_workers)
        except FileNotFoundError as fn_err:
            print(f"\n{fn_err}")
            sys.exit(0)
    else:
        download_large_file(url, num_workers=num_workers)
        parse_files(input_file=DOWNLOAD_FILE_PATH + url[url.rindex('/') + 1:], num_workers=num_workers)
