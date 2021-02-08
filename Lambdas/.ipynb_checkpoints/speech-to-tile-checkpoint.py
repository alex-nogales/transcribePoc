import json
import pandas as pd
from io import StringIO
import boto3
import sys, os
from datetime import date, timedelta


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

def get_data_loc(date, bucket='socofin-output'):
    prefix=f'output-transcribe/{date}/'
    s3 = boto3.client('s3')
    data_loc = []
    for file in get_all_s3_objects(s3, Bucket=bucket, Prefix=prefix):
        names = 's3://{}/{}'.format(bucket, file['Key'])
        data_loc.append(names)

    return data_loc

def process_day(date):
    data_loc = get_data_loc(date)
    container = []
    for file in data_loc:
        data = pd.read_json(file)
        my_name = os.path.basename(file)
        items = data['results'].get('items')
        for item in items:
            if item['type'] != 'punctuation':
                values = item['alternatives'][0]
                word = (values['confidence'], values['content'], item['start_time'], item['end_time'], my_name)
        container.append(word)
    # create dataframes from both sources and join them
    df = pd.DataFrame(container, columns=['confidence', 'content', 'start_time', 'end_time', 'file'])
    df.sort_values(by=['confidence'], ascending=True, inplace=True)
    return df 

def to_file(date):
    csv_buffer = StringIO()
    data_frame = process_day(date)
    data_frame.to_csv(csv_buffer, decimal=',', sep='|', encoding='utf-8')
    s3_resource = boto3.resource('s3')
    s3_resource.Object('socofin-output', f'output-sagemaker/TranscribeReports/output_day_{date}.csv').put(Body=csv_buffer.getvalue())
    
    return f'Saved as file: output-sagemaker/TranscribeReports/output_day_{date}.csv'


def lambda_handler(event, context):
    yesterday = date.today() - timedelta(days=1)
    yesterday = yesterday.strftime('%Y%m%d')
    to_file(yesterday)
    #to_file('20201210')

