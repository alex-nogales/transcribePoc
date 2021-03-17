import re
import pandas as pd
import json
import sys, os
import boto3
import logging
from botocore.exceptions import ClientError
from pytube import YouTube
from urllib.parse import urlparse
import random as rd
import unidecode as uni
import Levenshtein as lv


def _times(a_string, index):
    """ create internal function that splits time into 'start' and 'end' times
    
    :param a_string: String to split
    :param index: 0 for start 1 for end
    :return: None if a_string is empty, else returns start or end depending on index
    """  
    if a_string == '':
        return None
    
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
        fpath = urlparse(filepath)
        bucket = fpath.netloc
        key = fpath.path.lstrip('/')
        s3 = boto3.resource('s3')
        obj = s3.Object(bucket, key)
        file = obj.get()['Body'].read().decode('utf8')
    else:
        # read the file
        with open(filepath) as f:
            file = f.read()

    # split the file
    lines = file.splitlines()
    # compress into tuples of three, ignoring 4th
    # tuples = [index, time, transcript]
    #if _times(lines[i], 0)
    tps = [(lines[i], _times(lines[i + 1], 0), _times(lines[i + 1], 1), lines[i + 2]) for i in range(0, len(lines), 4) 
           if i + 2 < len(lines) and (_times(lines[i + 1], 0) is not None)]
    
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

    return _df.astype({'start':'float', 'end':'float', 'transcript':'string'})

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

### TODO: Move to adaptaros.py and standarice this code.
def get_all_s3_objects(s3, **base_kwargs):
    """ Amplify the limit of AWS results to 1000+
    
    :param s3: Bucket to amplify the result limit
    :return: None
    """
    continuation_token = None
    while True:
        list_kwargs = dict(MaxKeys=1000, **base_kwargs)
        if continuation_token:
            list_kwargs['ContinuationToken'] = continuation_token
        response = s3.list_objects_v2(**list_kwargs)
        yield from response.get('Contents', [])
        if not response.get('IsTruncated'):  # At the end of the list?
            break
        continuation_token = response.get('NextContinuationToken')

def get_folder_list(bucket='awstranscribe-tests', key='transcribeOutputs/Files'):
    """ Get the name of the files inside an AWS S3 Bucket
    
    :param bucket: AWS S3 bucket name
    :param key: directory and name in bucket, defaults to transcribeOutputs/Files
    :return: List with the name of each object in the S3 key
    """
    ###
    #  Get the name of the files in a bucket. While bucket is the AWS S3 Bucket and key is the folder inside that bucket
    # it defaults to transcribeOutputs/Files
    ###
    s3 = boto3.client('s3')
    data_loc = []
    for obj in get_all_s3_objects(s3, Bucket=bucket, Prefix=key):
        names = 's3://{}/{}'.format(bucket, obj['Key'])
        data_loc.append(names)
    return data_loc

def vocabulary_shuffle(vocab_name='IPA_Shuffle', words=10):
    ''' Create a random vocabulary of 'words' number of words from big_ass_dictionary.txt
    
    :param vocab_name: Vocabulary Name, defaults to IPA_Shuffle
    :param words: Number of words from big_ass_dictionary
    '''
    # Declare transcribe client
    client = boto3.client('transcribe')
    
    # Lists vocabularies if vocab_name exists
    response = client.list_vocabularies(
        StateEquals='READY',
        NameContains=vocab_name
        )
    # Create a random vocabulary from big_ass_dictionary.txt
    filepath = 's3://awstranscribe-tests/customVocabIPA/big_ass_dictionary.txt'
    fpath = urlparse(filepath)
    bucket = fpath.netloc
    key = fpath.path.lstrip('/')
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket, key)
    file = obj.get()['Body'].read().decode('utf8')

    file = file.splitlines()

    n = words
    with open(f'/tmp/{vocab_name}.txt', 'w') as f:    
        f.write(file[0])
        f.write("\n")
        for i in range(n):
            f.write(rd.choice(file))
            f.write("\n")
        
    # Upload file to S3
    upload_yt_file(f'/tmp/{vocab_name}.txt', object_name=f'customVocabIPA/{vocab_name}.txt')

    # If vocabulary exists update the vocab, else create a new one
    if response['Vocabularies']:
        vocab = client.update_vocabulary(
            VocabularyName=vocab_name,
            LanguageCode='es-ES',
            VocabularyFileUri=f's3://awstranscribe-tests/customVocabIPA/{vocab_name}.txt'
        )
        return print(f'Updating Vocab: {vocab_name}\n {vocab}')
    else:
        vocab = client.create_vocabulary(
            VocabularyName=vocab_name,
            LanguageCode='es-ES',
            VocabularyFileUri=f's3://awstranscribe-tests/customVocabIPA/{vocab_name}.txt'
        )
        return print(f'Creating Vocab: {vocab_name}\n {vocab}')
    
def neutralize(a_string):
    ''' Neutralize a string, removing unnecesary characters
    
    :param a_string: the string to neutralize
    :return: a_string without unnecesary characters like tildes and HTML annotations
    '''
    a_string = uni.unidecode(a_string)
    a_string = re.sub('[?!@#$.,/]', '', a_string)
    clean = re.compile('<.*?>')
    a_string = re.sub(clean, '', a_string)
    clean = re.compile(".*?\((.*?)\)")
    a_string = re.sub(clean, '', a_string)
    return a_string.lower()

def compress(df_reference, df_to_modify, field='transcript'):
    ''' Compreses separate words into phrase using start and end time notations.
        The df_reference and df_to_modify must have start and end columns
        
    :param df_reference: reference DataFrame that contains the "real transcription"
    :param df_to_modify: DataFrame that needs to be compressed into phrases
    :return: The phrase with space between strings
    '''
    container = []
    for start, end in zip(df_reference['start'], df_reference['end']):
        sub_df = df_to_modify[(df_to_modify['start'] >= start) & (df_to_modify['end'] <= end)]
        
        words = [a_word for a_word in sub_df[field]]
        container.append(' '.join(word for word in words))
        
    return container

def lv_score(a_series, b_series):
    ''' Generate Levenshtein score based on the Levenshtein distance between two strings
    
    :param a_series: A series or string 
    :param b_series: A series or string
    :return: Levenshtein score between two strings
    '''
    metric = 0
    m_list = []
    for a_string, b_string in zip(a_series, b_series):
        a_string = neutralize(a_string)
        b_string = neutralize(b_string)
        if len(a_string) >= len(b_string):
            length = len(a_string)
        else:
            length = len(b_string)
        metric = lv.distance(a_string, b_string) / length
        m_list.append(1 - metric)
        
    return m_list

def average(lst):
    ''' Average of a list
    
    :param lst: A list
    :return: Average of values on the list
    '''
    return sum(lst) / len(lst)


def youtube2aws(url):
    ''' Convert YouYubue videos to mp4, exctracts spanish translate if exists
    
    :param url: Link to the video to uplaod
    :return: Nothing
    '''
    yt = YouTube(url)
    # Get the tittle and replace spaces with underscore
    yt_title = yt.title.replace(' ', '_')
    yt_title = unicodedata.normalize('NFKD', yt_title).encode('ASCII', 'ignore').decode('utf-8')
    yt_title = re.sub('\+', '', yt_title)
    
    # Download the video to tmp folder and save the output name to file_name
    file_name = yt.streams.first().download(output_path="/tmp" ,filename=yt_title)
    
    upload_yt_file(file_name)
    
    if yt.captions.get_by_language_code('es-419'):
        code = 'es-419'
        caption = yt.captions.get_by_language_code('es-419')
        yt_caption = caption.generate_srt_captions()
        yt_title = yt.title.replace(' ', '_')
        yt_title = unicodedata.normalize('NFKD', yt_title).encode('ASCII', 'ignore').decode('utf-8')
        yt_title = re.sub('\+', '', yt_title)
        with open(f'/tmp/{yt_title}_{code}.txt', 'a') as f:
            f.write(yt_caption)
        upload_yt_file(f'/tmp/{yt_title}_{code}.txt', object_name=f'levenshteinTests/ytCaptions/{yt_title}.txt')
    else:
        print('This caption doesn\'t exist for this video: ', yt.title)
        print('You can use one of the following captions: \n', yt.captions.all())
        
    