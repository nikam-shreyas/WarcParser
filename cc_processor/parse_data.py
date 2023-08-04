import sys
import os
from resiliparse.parse.html import HTMLTree
from resiliparse.parse.html import traverse_dom, DOMContext, NodeType
from fastwarc.warc import ArchiveIterator, WarcRecordType
from resiliparse.parse.encoding import detect_encoding, bytes_to_str
from concurrent.futures import ThreadPoolExecutor
from cleantext import clean
from .skip_tags import SKIP_TAGS

# File to be processed
INPUT_FILE = './data/sample.warc.gz'

# File location where the output files must be stored
OUTPUT_FILE_PATH = './output'

# Number of workers for processing the files
NUM_WORKERS = 4


def clean_text(text):
    """
    Text processing.

    Arguments:
        text (str): The string to be processed
    """

    # Remove special characters from the text
    text = text.replace("@#^*_+-=<>|", "")

    # Clean using cleantext library
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
    """
    Represents the parser for parsing the WARC files.

    Attributes:
        skip_flag (bool): Flag to denote whether the current tag is to be skipped or not
        skip_tag (str): All the tags under this parent tag are skipped for processing.
                        Example: if skip_tag==<ul>, all the <li> tags will be skipped
        file (file): File to store the processed site.
    """
    current_process = 0

    def __init__(self):
        """
        Initialize the parser.
        """
        Parser.current_process += 1
        self.skip_flag = False
        self.skip_tag = None
        self.file = open(
            f'{OUTPUT_FILE_PATH}/cleaned_site_{Parser.current_process}.txt', 'w')

    def tag_start_callback(self, ctx: DOMContext):
        """
        Callback function when a tag is encountered.

        Arguments:
            ctx (DOMContext): Context of the node in the DOM tree.
        """

        # Check if the node is to be skipped
        if ctx.node.tag in SKIP_TAGS:
            self.skip_tag = ctx.node.tag
            self.skip_flag = True

        # Check if the parent node is skipped
        if self.skip_flag is False:

            # Handle Text elements having non-blank text
            if ctx.node.type == NodeType.TEXT and ctx.node.value.strip():
                if ctx.node.parent.tag not in ('audio', 'video'):
                    self.file.write(
                        clean_text(ctx.node.value.strip()) + ' ')

            # Handle Audio and Video elements
            elif ctx.node.tag in ('audio', 'video'):
                self.file.write(f' <{ctx.node.tag.upper()}> ')
                for child_node in ctx.node.child_nodes:
                    if child_node.tag == 'source':
                        self.file.write(child_node['src'])

            # Handle Image elements
            elif ctx.node.tag == 'img':
                self.file.write(f' <{ctx.node.tag.upper()}> ')
                try:
                    # Skip if the current tag does not have a source url associated with it
                    self.file.write(ctx.node['src'])
                except KeyError:
                    pass
                self.file.write(f' </{ctx.node.tag.upper()}> ')

    def tag_end_callback(self, ctx: DOMContext):
        """
        Callback function for the corresponding end tag.


        Arguments:
        ctx (DOMContext): Context of the node in the DOM tree
        """

        # Reset the flag when the end tag is encountered
        if self.skip_flag is True and self.skip_tag == ctx.node.tag:
            self.skip_flag = False
            self.skip_tag = None

        # Handles Audio and Video end tags
        if ctx.node.tag in ('audio', 'video'):
            self.file.write(f' </{ctx.node.tag.upper()}> ')

    def __del__(self):
        """
        Destructor for the class.
        """
        print(f'Processing files ...', end='\r')


def parse_record(record):
    """
    Parse a particular record from the WARC file.

    Arguments:
    record (WARC Record): the record to be processed.
    """
    # Initialize the parser
    parser = Parser()

    # Convert the content form bytes to string
    decoded_response = bytes_to_str(record, detect_encoding(record))

    # Parse the HTML content
    tree = HTMLTree.parse(decoded_response)

    # Traverse the dom tree associated with the record
    traverse_dom(tree.body, parser.tag_start_callback, parser.tag_end_callback)


def parse_files(input_file=INPUT_FILE,
                output_file_path=OUTPUT_FILE_PATH,
                num_workers=NUM_WORKERS,
                num_files_per_run=5):
    """
    Parse the files in the Website archive.

    Arguments:
    input_file: WARC file to be processed
    output_file_path: Location of the folder to store the output
    num_workers: Number of workers
    num_files_per_run: Number of files to be handled per run
    """
    try:
        # Check if the file exists
        if not os.path.isfile(input_file):
            raise FileNotFoundError(f"The file '{input_file}' does not exist.")

        # Check if the directory exists
        if not os.path.isdir(output_file_path):
            raise NotADirectoryError(f"The directory '{output_file_path}' does not exist.")

    except FileNotFoundError as file_err:
        print(file_err)
        sys.exit(0)

    except NotADirectoryError as dir_err:
        print(dir_err)
        sys.exit(0)

    print(f"\n\nStarting parsing sites in {input_file}")
    records = []
    stream = open(input_file, 'rb')
    for index, record in enumerate(ArchiveIterator(stream, WarcRecordType.response)):

        if index % num_files_per_run == 0 and records:
            with ThreadPoolExecutor(num_workers) as pool:
                pool.map(parse_record, records)
            records = []
        else:
            records.append(record.reader.read())
        if index == 10:
            break
    if records:
        with ThreadPoolExecutor(num_workers) as pool:
            pool.map(parse_record, records)

    print(f"Finished parsing. Output in {output_file_path}")


if __name__ == '__main__':
    parse_files()
