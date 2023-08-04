from cc_processor import parse_files, download_large_file
import argparse

URL = 'https://data.commoncrawl.org/crawl-data/CC-MAIN-2023-23/segments/1685224643388.45/warc/CC-MAIN-20230527223515-20230528013515-00000.warc.gz'
DOWNLOAD_FILE_PATH = './data/'
OUTPUT_FILE_PATH = './sample-output/'
NUM_WORKERS = 4

if __name__ == '__main__':
    # arg_parser = argparse.ArgumentParser(
    #     description='Helper script for downloading and parsing common crawl warc files.')
    # arg_parser.add_argument('-u', '--url', help='URL of the WARC file')
    # arg_parser.add_argument('-d', '--download_file_path',
    #                         help='Location to the folder where the warc file must be stored after downloading')
    # arg_parser.add_argument('-o', '--output_file_path', help='Location to the folder for the files after processing')
    # arg_parser.add_argument('-n', '--num_workers', help='Number of workers')
    #
    # args = arg_parser.parse_args()
    #
    # if args.url:
    #     URL = args.url
    # if args.download_file_path:
    #     if args.download_file_path[-1] == '/':
    #         DOWNLOAD_FILE_PATH = args.download_file_path + URL[URL.rindex('/') + 1:]
    #     else:
    #         DOWNLOAD_FILE_PATH = args.download_file_path + URL[URL.rindex('/'):]
    #
    # if args.output_file_path:
    #     OUTPUT_FILE_PATH = args.output_file_path
    # if args.num_workers:
    #     NUM_WORKERS = int(args.num_workers)

    # download_large_file()
    parse_files()
