import tempfile
from functools import partial
import pdftotext
from google.cloud import storage
from multiprocessing import Pool

def upload_blob_from_string(bucket, txt, destination_blob_name):

  blob = bucket.blob(destination_blob_name)
  blob.upload_from_string(txt)


def convert(bucket, outputblob, iblob, storage_client):

    savefile = outputblob + "/" + str(iblob.name).split("/")[1].split(".")[0] + ".txt"

    with tempfile.NamedTemporaryFile() as tmpfile:
        iblob.download_to_filename(tmpfile.name)
        with open(tmpfile.name, "rb") as f:
            pdf = pdftotext.PDF(f)
        text = "\n\n".join(pdf)
        upload_blob_from_string(bucket, text, savefile)

def convert_safe(bucket, outputblob, iblob, storage_client):

    try:
        convert(bucket, outputblob, iblob, storage_client)

    except Exception as e:
        print(e)


def convert_directory(bucketname: str, inputblob: str, outputblob: str, servicekeypath):

    storage_client = storage.Client.from_service_account_json(servicekeypath)
    bucket = storage_client.get_bucket(bucketname)
    blobs = bucket.list_blobs(prefix=inputblob)
    count_errors = 0

    for blob in blobs:
        # print(str(blob.name))
        try:
            convert(bucket, outputblob, blob, storage_client)
        except Exception as e:
            count_errors += 1
            print(e)
            continue

    return count_errors


"""
def convert_directory_parallel(bucketname: str, inputblob: str, outputblob: str, processes: int, servicekeypath):

    print("parallel!")

    storage_client = storage.Client.from_service_account_json(servicekeypath)
    bucket = storage_client.get_bucket(bucketname)
    blobs = bucket.list_blobs(prefix=inputblob)

    pfunc = partial(convert_safe, bucket=bucket,
            inputblob=inputblob, outputblob=outputblob,
            storage_client=storage_client)

    with Pool(processes=processes) as pool:
        pool.map(pfunc, blobs)

    pool.close()
    pool.join()
"""

if __name__ == '__main__':

    result = convert_directory("nkebucket-arxiv-ai", "nkebucket-arxiv-ai/arxiv-pdf", "nkebucket-arxiv-ai/arxiv-text", servicekeypath="/home/niemery/arxiv-public-datasets/arxiv_public_data/key.json")
    print(result, "error(s)")
