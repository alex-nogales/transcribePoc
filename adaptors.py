import re
import pandas as pd
import json
import sys, os
import boto3
import logging
from botocore.exceptions import ClientError
from pytube import YouTube
from urllib.parse import urlparse

def _times(a_string, index):
    """ create internal function that splits time into 'start' and 'end' times
    
    :param a_string: String to split
    :param index: 0 for start 1 for end
    :return: return start or end depending on index
    """  
    
    # split string using re lib
    splits = re.split(r'[, :]', a_string)

    # cast to int each split
    splits = [int(a_split) for index, a_split in enumerate(splits) if index != 4]

    # calculate second start and end
    if index == 0:
        t = splits[0] * 3600 + splits[1] * 60 + splits[2] + splits[3]/1000
    elif index == 1:
        t = splits[4] * 3600 + splits[5] * 60 + splits[6] + splits[7]/1000
    else:
        t = -1

    return t


def youtube2df(filepath, aws_path=True):
    """ Transform YouTube caption format to pandas DataFrame
    
    :param filepath: Path to YouTube caption file
    :param aws_path: Shift to local file or AWS S3 file
    
    :return: pandas DataFrame with columns 'orig_index', 'start', 'end', 'transcript'
    """
    if aws_path:
        # Read the file from AWS S3 bucket
        fpath = urlparse(filepath)
        bucket = fpath.netloc
        key = fpath.path.lstrip('/')
        s3 = boto3.resource('s3')
        obj = s3.Object(bucket, key)
        file = obj.get()['Body'].read().decode('utf8')
    else:
        # Read the file from local filesystem
        with open(filepath) as f:
            file = f.read()

    # Split the file
    lines = file.splitlines()

    # compress into tuples of three, ignoring 4th
    # tuples = [index, time, transcript]
    ## TODO: Fix a bug when there is no 4th data (the blank one)
    tps = [(lines[i], _times(lines[i + 1], 0), _times(lines[i + 1], 1), lines[i + 2]) for i in range(0, len(lines), 4)]

    # convert to dataframe
    _df = pd.DataFrame(tps, columns=['orig_index', 'start', 'end', 'transcript'])

    return _df


# AWS function with filepath to json function
def aws2df(filepath, aws_path=True):
    ''' Transform AWS Transcribe (json) to pandas DataFrame
    
    :param filepath: Path to AWS Transcribe file
    :param aws_path: Shift to local file or AWS S3 file
    
    :return: pandas DataFrame with columns 'start', 'end', 'transcript'
    '''
    if aws_path:
        # Read the file from AWS S3 Bucket
        data = pd.read_json(filepath)
    else:
        # Read the file from local filesystem
        with open(filepath) as f:
            data = json.load(f)
            
    # compress into tuples
    tuples = []
    for value in data['results']['items']:
        if value.get('start_time'):
            tuples.append((value.get('start_time'),
                           value.get('end_time'),
                           value['alternatives'][0].get('content')
                           ))

    # transform to df
    _df = pd.DataFrame(tuples, columns=['start', 'end', 'transcript'])

    return _df

def upload_yt_file(file_name, bucket='awstranscribe-tests', object_name=None):
    """ Upload YouTube file to an S3 bucket
    
    :param file_name: File to upload
    :param bucket: Bucket to upload to, defaults to transcribe job trigger S3
    :param object_name: S3 Object name, will default to key /exampleRecords/youtubeVideos/<file_name>
    :return True if file was uploaded, else False
    """
    # Get file_name name from OS path
    fname = os.path.basename(file_name)
    
    # If object_name was not specified, use fname
    if object_name is None:
        object_name = f'exampleRecords/youtubeVideos/{fname}'
        
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True