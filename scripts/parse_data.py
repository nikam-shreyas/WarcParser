from resiliparse.parse.html import HTMLTree
from resiliparse.parse.html import traverse_dom, DOMContext, NodeType
from fastwarc.warc import ArchiveIterator, WarcRecordType
from resiliparse.parse.encoding import detect_encoding, bytes_to_str
import asyncio
import concurrent.futures
import functools
from concurrent.futures import ThreadPoolExecutor
import multiprocessing
from cleantext import clean

INPUT_FILE_PATH = 'sample-data/Sample-WARC-file-Small.warc.gz'
OUTPUT_FILE_PATH = 'sample-output/'
# NUM_WORKERS = multiprocessing.cpu_count()
NUM_WORKERS = 4

skip_tags = {'label', 'option', 'address', 'header', 'footer', 'code', 'pre', 'script', 'style', 'legend', 'nav'}


def clean_text(text):
    text = text.replace("@#^*_+-=<>|", "")
    return clean(text,
                 fix_unicode=True,
                 to_ascii=False,  # do not transliterate to closest ASCII representation
                 lower=False,  # maintain text case
                 no_line_breaks=True,  # fully strip line breaks as opposed to only normalizing them
                 no_urls=False,  # replace all URLs with a special token
                 no_emails=True,  # replace all email addresses with a special token
                 no_phone_numbers=True,  # replace all phone numbers with a special token
                 no_numbers=False,  # keep numbers
                 no_digits=False,  # keep digits
                 no_currency_symbols=False,  # keep currency symbols
                 no_punct=False,  # remove punctuations
                 replace_with_url="<URL>",
                 replace_with_email="<EMAIL>",
                 replace_with_phone_number="<PHONE>",
                 )


class Parser:
    current_process = 0

    def __init__(self):
        Parser.current_process += 1
        self.process_id = Parser.current_process
        self.skip_flag = False
        self.skip_tag = None
        self.filestream = open(
            f'{OUTPUT_FILE_PATH}/cleaned_site_{Parser.current_process}.txt', 'w')

    def start_cb(self, ctx: DOMContext):
        if ctx.node.tag in skip_tags:
            self.skip_tag = ctx.node.tag
            self.skip_flag = True

        if self.skip_flag is False:
            if ctx.node.type == NodeType.TEXT and ctx.node.value.strip():
                if ctx.node.parent.tag not in ('audio', 'video'):
                    self.filestream.write(
                        clean_text(ctx.node.value.strip()) + ' ')
            elif ctx.node.tag in ('audio', 'video'):
                self.filestream.write(f' <{ctx.node.tag.upper()}> ')
                for child_node in ctx.node.child_nodes:
                    if child_node.tag == 'source':
                        self.filestream.write(child_node['src'])
            elif ctx.node.tag == 'img':
                self.filestream.write(f' <{ctx.node.tag.upper()}> ')
                try:
                    self.filestream.write(ctx.node['src'])
                except KeyError:
                    pass
                self.filestream.write(f' </{ctx.node.tag.upper()}> ')

    def end_cb(self, ctx: DOMContext):

        if self.skip_flag is True and self.skip_tag == ctx.node.tag:
            self.skip_flag = False
            self.skip_tag = None
        if ctx.node.tag in ('audio', 'video'):
            self.filestream.write(f' </{ctx.node.tag.upper()}> ')

    def __del__(self):
        print(f'Done with file {self.process_id}', end='\r')
        self.filestream.close()


def parse_record(record):
    parser = Parser()
    response = record
    decoded_response = bytes_to_str(response, detect_encoding(response))
    tree = HTMLTree.parse(decoded_response)
    traverse_dom(tree.body, parser.start_cb, parser.end_cb)


def parse(num_files_per_run=5):
    records = []
    stream = open(INPUT_FILE_PATH, 'rb')
    for index, record in enumerate(ArchiveIterator(stream, WarcRecordType.response)):
        if index % num_files_per_run == 0 and records:
            with ThreadPoolExecutor(NUM_WORKERS) as pool:
                pool.map(parse_record, records)
            records = []
        else:
            records.append(record.reader.read())
    if records:
        with ThreadPoolExecutor(NUM_WORKERS) as pool:
            pool.map(parse_record, records)


def parse_files():
    print(f"Starting parsing sites in {INPUT_FILE_PATH}")
    parse()
    print(f"Finished parsing. Output in {OUTPUT_FILE_PATH}")


if __name__ == '__main__':
    parse_files()
