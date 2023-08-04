import asyncio
import concurrent.futures
import functools
import requests
import os
import multiprocessing

URL = 'https://data.commoncrawl.org/crawl-data/CC-MAIN-2023-23/segments/1685224643388.45/warc/CC-MAIN-20230527223515-20230528013515-00000.warc.gz'
OUTPUT = './data/sample.warc.gz'
# NUM_WORKERS = multiprocessing.cpu_count()
NUM_WORKERS = 4

async def get_size(url):
    response = requests.head(url)
    size = int(response.headers['Content-Length'])
    return size


def download_range(url, start, end, output, file_size):
    headers = {'Range': f'bytes={start}-{end}'}
    response = requests.get(url, headers=headers)
    print(f'Download progress: {(end * 100 / file_size):.2f}%', end='\r')
    with open(output, 'wb') as f:
        for part in response.iter_content(1024):
            f.write(part)


async def download(run, loop, url, output, chunk_size=1000000):
    file_size = await get_size(url)
    chunks = range(0, file_size, chunk_size)
    print(f'\n\nBegin downloading file chunks at location: {OUTPUT}')

    tasks = [
        run(
            download_range,
            url,
            start,
            start + chunk_size - 1,
            f'{output}.part{i}',
            file_size
        )
        for i, start in enumerate(chunks)
    ]

    await asyncio.wait(tasks)

    print('All file chunks downloaded successfully')
    print(f'\nMerging data at location: {OUTPUT}')

    with open(output, 'wb') as o:
        for i in range(len(chunks)):
            print(f'Progress: {(i * 100 / len(chunks)):.2f}%', end='\r')
            chunk_path = f'{output}.part{i}'

            with open(chunk_path, 'rb') as s:
                o.write(s.read())

            os.remove(chunk_path)
    print('\nData merging complete.')


def download_file(url=URL, num_workers=NUM_WORKERS, output_file_location=OUTPUT):
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=num_workers)
    loop = asyncio.new_event_loop()
    run = functools.partial(loop.run_in_executor, executor)

    asyncio.set_event_loop(loop)

    try:
        loop.run_until_complete(
            download(run, loop, url, output_file_location)
        )
    finally:
        loop.close()


if __name__ == '__main__':
    download_file()
