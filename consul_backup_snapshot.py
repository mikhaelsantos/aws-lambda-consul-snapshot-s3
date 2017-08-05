#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import itertools
import json
import os
import shutil
import ssl
from datetime import datetime
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import boto3
from botocore.exceptions import NoCredentialsError

FULL_PATH = "/tmp/"
CHUNK_NAME_PREFIX = "snapshot_chunk_"
CHUNK_SIZE = 4000


def split_chunks(file_name, bucket, path, key):
    """
    :param file_name: (str) File name to split
    :param bucket: (str) bucket to where get encryption context
    :param path: (str) path for encryption context
    :return: (dict) with the token
    """

    chunk_dir = datetime.now().strftime("%Y%m%d")
    if os.path.isdir(FULL_PATH + chunk_dir):
        shutil.rmtree(FULL_PATH + chunk_dir)
    os.mkdir(FULL_PATH + chunk_dir)
    client = boto3.client("kms")
    with open(FULL_PATH + file_name, 'rb') as infile:
        for i in itertools.count(0):
            chunk = infile.read(CHUNK_SIZE)
            if not chunk: break
            chunk_name = CHUNK_NAME_PREFIX + str(i)
            response = client.encrypt(
                KeyId=key,
                Plaintext=chunk,
                EncryptionContext={"AppName": "consul-backup",
                                   "BucketPath": os.path.join(bucket, path)}
            )
            with open(FULL_PATH + chunk_dir + "/" + chunk_name, 'wb') as outfile:
                outfile.write(response["CiphertextBlob"])

    return chunk_dir


def get_token(bucket, path):
    """

    :param bucket: (str) Configuration Bucket
    :param path:  (str) Path to encrypted configuration file
    :return: (bytes) Consul Token
    """
    client = boto3.resource("s3")
    client.Bucket(bucket).download_file(path, FULL_PATH + 'secrets.enc')

    client = boto3.client("kms")
    with open(FULL_PATH + "secrets.enc", "rb") as file:
        response = client.decrypt(CiphertextBlob=file.read(),
                                  EncryptionContext={"AppName": "consul-backup",
                                                     "BucketPath": os.path.join(bucket, path)}
                                  )

    return json.loads(response["Plaintext"])


def generate_file_name(prefix="snapshot"):
    """
    Generates file name bases
    :param prefix: (str) Value to prefix the filename. Defaults to snapshot
    :return: (str) File name with the pattern 'prefix_YYYYmmdd'
    """
    now = datetime.now()
    return prefix + "_" + now.strftime("%Y%m%d")


def download_snapshot(url, headers={}):
    """

    :param url: (str) Url to query
    :param headers: (dict) Headers to add to query
    :return: (str) In case of success 'file_name'.
             (NoneType) In case of failure 'None'
    """
    ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    request = Request(url)
    for key in headers:
        request.add_header(key, headers[key])
    file_name = generate_file_name()
    # Download the file from `url` and save it locally under `file_name`:
    try:
        with urlopen(request, context=ssl_context) as response, open(os.path.join(FULL_PATH + generate_file_name()),
                                                                     'wb') as out_file:
            data = response.read()
            out_file.write(data)
    except HTTPError as error:
        print("Snapshot Failed: " + error.reason)
        raise Exception("Backup Failed.")
    except PermissionError as error:
        print("Snapshot Failed: Write " + error.strerror)
        raise Exception("Backup Failed.")
    return file_name


def upload_chunks(chunk_dir, bucket_path, bucket):
    """

    :param file_name: (str) File to upload
    :param bucket: (str) Destination S3 Bucket
    :return: (dict) Response
    """
    client = boto3.client("s3")
    try:
        chunks = os.listdir(FULL_PATH + chunk_dir)
        for chunk in chunks:
            data = open(FULL_PATH + chunk_dir + "/" + chunk, 'rb')
            response = client.put_object(Key=bucket_path + "/" + chunk_dir + "/" + chunk, Body=data, Bucket=bucket)
    except NoCredentialsError:
        print("Upload error: Authentication failed")
        raise Exception("Backup Failed.")
    except FileNotFoundError as error:
        print("Upload error: " + error.strerror)
        raise Exception("Backup Failed.")
    return response


def aws_lambda_handler(*args, **kwargs):
    """

    Main handler for AWS
    """
    config_bucket = os.getenv("CONFIG_BUCKET")
    config_path = os.getenv("CONFIG_PATH")
    backup_bucket = os.getenv("BACKUP_BUCKET")
    backup_path = os.getenv("BACKUP_PATH")
    url = os.getenv("URL")
    key = os.getenv("KEY")

    print("Start execution")
    config = get_token(config_bucket, config_path)
    headers = {"X-Consul-Token": config["token"]}
    print("Download Snapshot")
    file_name = download_snapshot(url, headers)
    chunk_dir = split_chunks(file_name, backup_bucket, backup_path, key)
    print("Upload chunks to s3")
    upload_chunks(chunk_dir, backup_path, backup_bucket)
    print("Execution Successful")


#
# For Local Testing
#

def main():
    split_chunks()


if __name__ == '__main__':
    main()
