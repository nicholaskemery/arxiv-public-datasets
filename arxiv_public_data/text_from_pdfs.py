import tempfile
import pdftotext
from google.cloud import storage
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed, wait
from tqdm import tqdm
from itertools import islice

def upload_blob_from_string(bucket, txt, destination_blob_name):

  blob = bucket.blob(destination_blob_name)
  blob.upload_from_string(txt)


def convert(bucket, outputblob, iblob):

    savefile = outputblob + "/" + "".join(str(iblob.name).split("/")[1].split(".")[:-1]) + ".txt"

    with tempfile.NamedTemporaryFile() as tmpfile:
        iblob.download_to_filename(tmpfile.name)
        with open(tmpfile.name, "rb") as f:
            pdf = pdftotext.PDF(f)
        text = "\n\n".join(pdf)
        upload_blob_from_string(bucket, text, savefile)

def convert_safe(bucket, outputblob, iblob):

    try:
        convert(bucket, outputblob, iblob)
    except Exception as e:
        print(e)


def convert_directory(bucketname: str, inputblob: str, outputblob: str, servicekeypath):

    storage_client = storage.Client.from_service_account_json(servicekeypath)
    bucket = storage_client.get_bucket(bucketname)
    blobs = bucket.list_blobs(prefix=inputblob)
    count_errors = 0
    progress_printer = 0

    for blob in blobs:
        try:
            convert(bucket, outputblob, blob)
        except Exception as e:
            count_errors += 1
            print(e)
            continue
        progress_printer += 1
        if progress_printer % 100 == 0:
            print(progress_printer)

    return count_errors


def convert_directory_parallel(bucketname: str, inputblob: str, outputblob: str, workers: int, servicekeypath):

    storage_client = storage.Client.from_service_account_json(servicekeypath)
    bucket = storage_client.get_bucket(bucketname)
    blobs = bucket.list_blobs(prefix=inputblob)

    print("Start!\n")

    jobs = []

    with ThreadPoolExecutor(max_workers=workers) as executor:

        for blob in blobs:
            job = executor.submit(convert_safe, bucket, outputblob, blob)
            jobs.append(job)

        kwargs = {
            'total': len(jobs),
            'unit': 'it',
            'unit_scale': True,
            'leave': True
        }

        for f in tqdm(as_completed(jobs), **kwargs):
            pass

    print("Done!")
    return "Done!"

if __name__ == '__main__':

    # convert_directory_parallel("trial_bucket_name-1", "trial_input_folder", "trial_output_folder",
    #                            workers=4, servicekeypath="papers-and-compute-e93b01db4767.json")

    convert_directory_parallel(bucketname="nkebucket-arxiv-ai",
                               inputblob="arxiv-pdf",
                               outputblob="arxiv-text",
                               workers=4,
                               servicekeypath="/home/niemery/arxiv-public-datasets/arxiv_public_data/nicholaskey.json")
