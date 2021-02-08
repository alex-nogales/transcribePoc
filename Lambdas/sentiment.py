import json
import pandas as pd
from io import StringIO
import boto3
import sys, os
from datetime import date, timedelta, datetime, timezone
import time 
import numpy as np
import random
import tarfile

def get_all_s3_objects(s3, **base_kwargs):
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

def get_folder_list(bucket='socofin-output', key='output-transcribe/FinalTest/'):
    s3 = boto3.client('s3')
    data_loc = []
    for obj in get_all_s3_objects(s3, Bucket=bucket, Prefix=key):
        names = 's3://{}/{}'.format(bucket, obj['Key'])
        data_loc.append(names)
    return data_loc
    
def get_speaker_label(key='output-transcribe/FinalTest/', bucket='socofin-output'):
    data_loc = get_folder_list(key=key, bucket=bucket)
    container, channel = [], []
    for file in data_loc:
        data = pd.read_json(file)
        file_name = os.path.basename(file)
        results = data['results'].get('channel_labels').get('channels')
        for channel in results:
            label = channel.get('channel_label')
            for items in channel.get('items'):
                inner_container = []
                inner_container.append(file_name)
                inner_container.append(items.get('start_time'))
                inner_container.append(items.get('end_time'))
                inner_container.append(items.get('alternatives')[0].get('content'))
                inner_container.append(label)
                container.append(inner_container)
    df = pd.DataFrame(container, columns=['file', 'start_time','end_time','content','channel'])
    
    extra_ch = []
    for file in data_loc:
        file_name = os.path.basename(file)
        if 'ch_0' not in df[df['file']==file]['channel']:
            ch0 = (file_name, 0.1, 1.0, '', 'ch_0')
            extra_ch.append(ch0)
        if 'ch_1' not in df[df['file']==file]['channel']:
            ch1 = (file_name, 0.1, 1.5, '', 'ch_1')
            extra_ch.append(ch1)
    extra_df = pd.DataFrame(extra_ch, columns=['file','start_time','end_time','content','channel'])
    df = df.append(extra_df, ignore_index=True)
    df_final = df.dropna()
    return df_final.reset_index(drop=True)
    
def identify_human(key='output-transcribe/FinalTest/', bucket='socofin-output', speaker_label=None):
    if speaker_label is None:
        df = get_speaker_label(key=key, bucket=bucket)[['start_time', 'end_time', 'content', 'channel', 'file']]
    else:
        df = speaker_label[['start_time', 'end_time','content','channel','file']]
        
    df['seconds'] = (df['end_time'].astype(float) - df['start_time'].astype(float))
    ordered = df.groupby(['file','channel'])['seconds'].agg('sum').to_frame()
    ordered.reset_index(inplace=True)
    ordered = ordered.rename(columns = {'file': 'file', 'channel':'channel'})
    maximum = ordered.groupby(['file'])['seconds'].agg('max').to_frame()
    cruce = pd.merge(ordered, maximum, on='file')
    is_human = (cruce['seconds_x'] != cruce['seconds_y'])
    human = cruce[is_human]
    human = human[['file','channel']]
    return human


def identify_bot(key='output-transcribe/FinalTest/', bucket='socofin-output', speaker_label=None):
    if speaker_label is None:
        df = get_speaker_label(key=key, bucket=bucket)[['start_time', 'end_time', 'content', 'channel', 'file']]
    else:
        df = speaker_label[['start_time', 'end_time','content','channel','file']]
        
    df['seconds'] = (df['end_time'].astype(float) - df['start_time'].astype(float))
    df_new = df[['file','channel','seconds','content','start_time','end_time']]
    ordered = df_new.groupby(['file','channel'])['seconds'].agg('sum').to_frame()
    ordered.reset_index(inplace=True)
    ordered = ordered.rename(columns = {'file': 'file', 'channel':'channel'})
    minimum = ordered.groupby(['file'])['seconds'].agg('min').to_frame()
    cruce = pd.merge(ordered, minimum, on='file')
    is_bot = (cruce['seconds_x'] != cruce['seconds_y'])
    bot = cruce[is_bot]
    bot = bot[['file','channel']]
    return bot


def get_transcript(key='output-transcribe/FinalTest/', bucket='socofin-output', speaker_label=None):
    if speaker_label is None:
        df = get_speaker_label(key=key, bucket=bucket)[['file', 'content', 'start_time']]
    else:
        df = speaker_label[['start_time', 'end_time','content','channel','file']]
        
    df['start_time'] = df['start_time'].astype('float')
    container = []
    
    for file in df['file'].unique():
        subdf = df[df['file']==file]
        subsort = subdf.sort_values(by=['start_time'])
        transcript = ' '.join(subsort['content'])
        container.append((file, transcript))
    return pd.DataFrame(container, columns=['file', 'transcript'])
    
def get_content(key='output-transcribe/FinalTest/', bucket='socofin-output', speaker_label=None):
    if speaker_label is None:
        speaker_labels = get_speaker_label(key=key, bucket=bucket)
    else:
        speaker_labels=speaker_label
        
    final_human = pd.merge(speaker_labels, identify_human(key=key, bucket=bucket, speaker_label=speaker_labels), on=['file','channel'], how='inner')
    final_bot = pd.merge(speaker_labels, identify_bot(key=key, bucket=bucket, speaker_label=speaker_labels), on=['file','channel'], how='inner')
    transcripts = get_transcript(key=key, bucket=bucket, speaker_label=speaker_labels)
    
    final_list = final_human.file.unique()
    final_list_bot = final_bot.file.unique()
    container, container_bot = [], []
    for file in final_list:
        df_temp = final_human[final_human['file'] == file]
        frase = ' '.join(df_temp['content'])
        container.append([file, frase])

    for file in final_list_bot:
        df_temp = final_bot[final_bot['file'] == file]
        frase = ' '.join(df_temp['content'])
        container_bot.append([file, frase])

    df = pd.DataFrame(container, columns=['file','frase_human'])
    # sort df because it can cause problems with extra channels added to human
    df.sort_values(by='file', inplace=True)
    df.reset_index(inplace=True, drop=True)
    
    df2 = pd.DataFrame(container_bot, columns=['file', 'frase_bot'])
    df2.sort_values(by='file', inplace=True)
    df2.reset_index(inplace=True, drop=True)

    transcripts.sort_values(by='file', inplace=True)
    transcripts.reset_index(inplace=True, drop=True)

    df['frase_bot'] = df2['frase_bot']
    df['transcript'] = transcripts['transcript']
    df['bot_file'] = df2['file']
    df['tr_file'] = transcripts['file']
    return df



def add_custom_job(content_df,
                    bucket='socofin-output',
                    key='output-comprehend/temp.csv',
                    doc_arn='arn:aws:comprehend:us-east-1:661346392611:document-classifier/SocofinTest21', 
                    data_arn= 'arn:aws:iam::661346392611:role/ComprehendExperimentBucketAccessRole'):

    client = boto3.client('comprehend')
    region = boto3.session.Session().region_name
    account_id = boto3.client('sts').get_caller_identity().get('Account')

    # save content_df transcripts into a temp.csv file
    csv_buffer = StringIO()
    content_df['transcript'].to_csv(csv_buffer, index=False, header=False)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket, key).put(Body=csv_buffer.getvalue())

 
    # create job
    response = None
    job_id = ''
    test_object_name_s3uri = f's3://{bucket}/{key}'
    test_output_s3uri = f's3://{bucket}/output-comprehend/output/'
    response = client.start_document_classification_job(JobName= '%x' % random.getrandbits(32), 
                                                        DocumentClassifierArn=doc_arn, 
                                                        DataAccessRoleArn=data_arn,
                                                        InputDataConfig={'InputFormat': 'ONE_DOC_PER_LINE',
                                                                         'S3Uri': test_object_name_s3uri},
                                                        OutputDataConfig={'S3Uri': test_output_s3uri},
                                                       )
    job_id = response['JobId']

    return 'Job Id: ' + job_id

    
    
def lambda_handler(event, context):
    csv_buffer = StringIO()
    bucket='socofin-output'
    
    ##Set yesterday
    yesterday = date.today() - timedelta(days=1)
    yesterday = yesterday.strftime('%Y%m%d') ## <--- Use yesterday to set up a folder cambio a Normal
    #yesterday = '20210108'
    t0 = time.time()
    
    speaker_label = get_speaker_label(key=f'output-transcribe/{yesterday}')
    speaker_label.to_csv(csv_buffer)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket, 'output-comprehend/speaker_tmp.csv').put(Body=csv_buffer.getvalue())
    
    df = get_content(key=f'output-transcribe/{yesterday}', speaker_label=speaker_label)
    compare = [a != b for a,b in zip(df['bot_file'], df['tr_file'])]
    print("compare: ", np.any(compare))
    t1= time.time()
    print("get_content time: ", t1 - t0)
    add_custom_job(df)
