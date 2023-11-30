
# Warc Processor

The WARC Processor project is a Python project that can be used to download WARC files from the Common Crawl website and process them into an interleaved multimodal dataset. The dataset can then be used to train a multimodal language model.  The project is designed to be efficient and scalable, so it can be used to process large WARC files. The project also removes boilerplate content and redundant text from the dataset, which helps to improve the quality of the data.

Read more here: [Project Blog](https://www.nikam-shreyas.com/blogs/project-1-warcparser.html)

## Features
- Streaming and Chunked Processing
- Concurrency and Parallelism
- Memory Management
- Progress monitoring and logging

## Assumptions:

- Among the records in the WARC files, only the 'response' files have been processed. This is because the required information for training the models is primarily found in response records. The processing of 'revisit' and 'resource' records may be considered for future enhancements.
- All hyperlinks leading to other pages have been removed, except for those contained within images, videos, and audios. If preservation of these hyperlinks is desired (e.g., when they are integrated with text and not part of the navigation bar), it is possible to retain them. Simply removing the 'a' tag from the ```skip_tags.py``` file will achieve this.
- Some audio and video files contain multiple links. To accommodate this, all links have been preserved in a comma-separated format. This ensures that the various links associated with multimedia content are captured accurately.
- A distinct and targeted set of special characters has been delineated for preservation throughout data processing. This preservation strategy involves the selective retention of specific special characters, rather than a comprehensive removal. This discerning approach aims to uphold the authenticity, context, and linguistic intricacies of websites across various languages.


## Installation

Install the dependencies:
```bash
pip install -r requirements.txt
```
Once the dependencies are installed, you can run the project by typing the following command into your terminal:
```
python warc_processor.py [OPTIONS]
```
For more information on the options that you can use, please run the following command:
```
python warc_processor.py --help
```

To use the WARC Processor project, you can use the following arguments:

- ```--url```: The URL of the WARC file to download.
- ```--existing_file_path```: The path to the existing WARC file to process.
- ```--num_workers```: The number of workers to use for processing.
For example, to download a WARC file from the Common Crawl website and process it, you would use the following command:

```
python warc_processor.py --url https://data.commoncrawl.org/crawl-data/CC-MAIN-2023-23/segments/1685224643388.45/warc/CC-MAIN-20230527223515-20230528013515-00003.warc.gz
```

To process an existing WARC file, you would use the following command:

```
python warc_processor.py --existing_file_path /path/to/warc_file.warc.gz
```

For more information on the options that you can use, please run the following command:

```
python warc_processor.py --help
```

## Sample Output
The sample output for a few processed websites is provided in the ```sample-output``` directory. 


## Optimizations

- **Asynchronous downloading:** The downloading of WARC files is now done asynchronously using the ```asyncio``` library. This allows the project to download the WARC files in chunks at the same time, which significantly improves the performance of the download. The chunks are then processed to generate a single WARC file. This reduced the download time from ```~20 minutes``` to ```~5 minutes```.
- **Parallel processing:** The processing of WARC records is now done in parallel using multithreading. This allows the project to process multiple WARC records at the same time, which significantly improves the performance of the processing.


## Future improvements

- Use WAT and WET files for further optimization of record processing
- Better processing of inline anchor tags with the text
- Consider using distributed computing frameworks like Apache Spark or Dask to distribute processing across multiple nodes or clusters.
- Batch I/O operations
- Add more integrations

## Acknowledgements

 - [Fastwarc](https://pypi.org/project/FastWARC/)
 - [Resiliparse](https://github.com/chatnoir-eu/chatnoir-resiliparse)
 - [CleanText](https://github.com/jfilter/clean-text)
 - [Common Crawl](https://commoncrawl.org/)
 - [Asyncio](https://docs.python.org/3/library/asyncio.html)
