import asyncio
import concurrent.futures
import functools
import sys
import requests
import os

# URL of the large file to be downloaded
URL = 'https://data.commoncrawl.org/crawl-data/CC-MAIN-2023-23/segments/1685224643388.45/warc/CC-MAIN-20230527223515-20230528013515-00000.warc.gz'

# Output file for the downloaded file
DOWNLOAD_FILE_PATH = './data'

# Number of concurrent workers for downloading chunks
NUM_WORKERS = 4


async def get_remote_file_size(url):
    """
    Fetch the size of the remote file.

    Args:
        url (str): URL of the remote file.

    Returns:
        int: Size of the remote file in bytes.
    """
    response = requests.head(url)
    size = int(response.headers['Content-Length'])
    return size


def download_file_range(url, start_byte, end_byte, output_path, total_file_size):
    """
    Download a specific byte range of the file and save it as a chunk.

    Args:
        url (str): URL of the remote file.
        start_byte (int): Starting byte of the range.
        end_byte (int): Ending byte of the range.
        output_path (str): Path to save the downloaded chunk.
        total_file_size (int): Total size of the remote file.
    """
    headers = {'Range': f'bytes={start_byte}-{end_byte}'}
    response = requests.get(url, headers=headers)

    # Display download progress as a percentage
    downloaded_percentage = (end_byte * 100 / total_file_size)
    print(f'Download progress: {downloaded_percentage:.2f}%', end='\r')

    with open(output_path, 'wb') as chunk_file:
        for part in response.iter_content(1024):
            chunk_file.write(part)


async def download_file_chunks(run, loop, url, download_file_path=DOWNLOAD_FILE_PATH, chunk_size=1000000):
    """
    Download the remote file in smaller chunks and merge them.

    Args:
        run (function): Partial function to run a function using the executor.
        loop (asyncio.AbstractEventLoop): Asyncio event loop.
        url (str): URL of the remote file.
        download_file_path (str): Path to save the downloaded file.
        chunk_size (int): Size of each chunk in bytes.
    """
    total_file_size = await get_remote_file_size(url)
    chunk_ranges = range(0, total_file_size, chunk_size)

    print(f'\n\nBegin downloading file chunks at location: {download_file_path}')

    # Create tasks for downloading chunks in parallel
    tasks = [
        run(
            download_file_range,
            url,
            start,
            start + chunk_size - 1,
            f'{download_file_path}.part{i}',
            total_file_size
        )
        for i, start in enumerate(chunk_ranges)
    ]

    await asyncio.wait(tasks)

    print('All file chunks downloaded successfully')
    print(f'\nMerging data at location: {download_file_path}')

    # Merge the downloaded chunks into the final file
    with open(download_file_path, 'wb') as final_file:
        for i in range(len(chunk_ranges)):
            merge_progress = (i * 100 / len(chunk_ranges))
            print(f'Progress: {merge_progress:.2f}%', end='\r')

            chunk_path = f'{download_file_path}.part{i}'
            with open(chunk_path, 'rb') as chunk_file:
                final_file.write(chunk_file.read())

            # Remove the temporary chunk file
            os.remove(chunk_path)

    print('\nData merging complete.')


def download_large_file(url=URL, num_workers=NUM_WORKERS, download_file_path=DOWNLOAD_FILE_PATH):
    """
    Download a large file from a URL using asynchronous operations and multithreading.

    Args:
        url (str): URL of the remote file.
        num_workers (int): Number of concurrent workers for downloading chunks.
        download_file_path (str): Path to save the downloaded file.
    """
    # Check if the file exists
    try:
        # Check if the directory exists
        if os.path.isdir(download_file_path):
            # Append the file name to the directory
            file_path = os.path.join(download_file_path, url[url.rindex('/') + 1:])
        else:
            raise NotADirectoryError(f"The directory '{download_file_path}' does not exist.")
    except NotADirectoryError as dir_err:
        print(dir_err)
        sys.exit(0)

    # Create a ThreadPoolExecutor for concurrent downloading
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=num_workers)
    loop = asyncio.new_event_loop()
    run = functools.partial(loop.run_in_executor, executor)

    asyncio.set_event_loop(loop)

    try:
        loop.run_until_complete(
            download_file_chunks(run, loop, url, file_path)
        )
    finally:
        loop.close()


# Entry point of the script
if __name__ == "__main__":
    download_large_file()
