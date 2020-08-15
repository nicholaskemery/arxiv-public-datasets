from arxiv_public_data import fulltext

import multiprocessing

max_cpu = multiprocessing.cpu_count()

if __name__ == '__main__':
    fulltext.convert_directory_parallel(path="gs://nkebucket-arxiv-ai/arxiv-pdf/", processes=max_cpu-2, timelimit=2*60)
    #
    fulltext.convert_directory(path="gs://nkebucket-arxiv-texts/", timelimit=2*60)