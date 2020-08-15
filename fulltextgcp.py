from arxiv_public_data import fulltext
from google.cloud import storage

import multiprocessing

max_cpu = 2

storage_client = storage.Client()

if __name__ == '__main__':
    fulltext.convert_directory_parallel(path=storage_client.bucket("gs://nkebucket-arxiv-ai/arxiv-pdf/"), processes=2, timelimit=2*60)
    #
    fulltext.convert_directory(path=storage_client.bucket("gs://nkebucket-arxiv-texts/"), timelimit=2*60)
