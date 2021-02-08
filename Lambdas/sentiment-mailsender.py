## PART 2
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
from urllib.parse import unquote_plus
import uuid

s3_client = boto3.client('s3')

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
        speaker_labels = speaker_label
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
    
def add_sentiment(content_df):
    comprehend = boto3.client(service_name='comprehend', region_name='us-east-1')
    sentiments = []
    neutralscores = []
    negativescores = []
    positivescores = []
    
    for text in content_df['frase_human']:
        if text == ' ' or text == '':
            sentiments.append('Neutral')
            neutralscores.append(0)
            negativescores.append(0)
            positivescores.append(0)
            continue
        analysis = comprehend.detect_sentiment(Text=text, LanguageCode='es')
        # save in results
        subresult = analysis['SentimentScore']
        max_score = 0
        max_sentiment = ''
        for key in subresult:
            if key == 'Neutral':
                neutralscores.append(subresult[key])
            elif key == 'Negative':
                negativescores.append(subresult[key])
            elif key == 'Positive':
                positivescores.append(subresult[key])
            
            if subresult[key] > max_score:
                max_score = subresult[key]
                max_sentiment = key
        sentiments.append(max_sentiment)
            
    content_df['sentiment'] = sentiments
    content_df['PositiveScore'] = positivescores
    content_df['NegativeScore'] = negativescores
    content_df['NeutralScore'] = neutralscores
    return
    
def to_file(label, subdf, yesterday):
    csv_buffer = StringIO()
    data_frame = subdf
    data_frame.to_csv(csv_buffer, decimal='.', sep=',', encoding='utf-8', index=False, header=None)
    s3_resource = boto3.resource('s3')
    s3_resource.Object('socofin-output', f'output-comprehend/Mails/{label}_{yesterday}.csv').put(Body=csv_buffer.getvalue()) ## CHANGE temp_Mail for Mails
    
    return f'Saved as file: output-comprehend/Mails/{label}_{yesterday}.csv'

def send_mail(log_group, log_stream, alarmTag, alarmMsg):
    logs = boto3.client('logs')
    timestamp = int(round(time.time() * 1000))
    token = logs.describe_log_streams(logGroupName=log_group)
    response = logs.put_log_events(
        logGroupName=log_group,
        logStreamName=log_stream,
        logEvents=[
            {
                'timestamp': timestamp,
                'message': f'[{alarmTag}] {alarmMsg}'
                }
            ],
            sequenceToken=token['logStreams'][0]['uploadSequenceToken']
        )

def lambda_handler(event, context):
    yesterday = date.today() - timedelta(days=1)
    yesterday = yesterday.strftime('%Y%m%d') ## <--- Use yesterday to set up a folder normal

    t0 = time.time()
    speaker_label = pd.read_csv('s3://socofin-output/output-comprehend/speaker_tmp.csv')
    speaker_label.drop('Unnamed: 0', inplace=True, axis=1)
    speaker_label.fillna(' ', inplace=True)
    speaker_label['content'] = speaker_label['content'].astype(str)
    df = get_content(key=f'output-transcribe/{yesterday}', speaker_label=speaker_label)
    t1= time.time()
    
    print("get_content: ", t1 - t0)
    compare = [a != b for a,b in zip(df['bot_file'], df['tr_file'])]
    print("compare: ", np.any(compare))
    
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])
        file_name = key.split("/")[-1]
        download_path = f'/tmp/{file_name}'
        s3_client.download_file(bucket,key,download_path)
    tar = tarfile.open(download_path, "r:gz")
    for member in tar.getmembers():
        f = tar.extractfile(member)
        if f is not None:
            result = [json.loads(jline) for jline in f.read().splitlines()]
    t2 = time.time()
    print(f'gzip path: {bucket} / {key}')
    print(f"Time_read: ", t2 -t1)
    add_sentiment(df)
    t3 = time.time()
    print("Sentiment: ", t3 - t2)
    
    
    weights = {'ROBOTEVASION': 1, 'RECORDER': 1, 'ROBOTFAILSDETECTION': 1, 'PROFANITY' : 1, 'WORK': 1,'DISEASE': 1,'NOROBOT': 1,'PROMISE': 1,'WRONGNUM': 1,'NONAME': 1, 'OK': 1}
    cont_label = []
    cont_confidence = []
    cont_score = []
    
    t4 = time.time()
    print("Df: ", len(df))
    print("Result: ", len(result))
    insults = pd.read_csv('s3://socofin-input/archivoPlano/TranscribeDiccionarios/INSULTOS.csv', header=None)
    for index, response in enumerate(result):
        if df['frase_human'].iloc[index] == ' ':
            cont_label.append('ROBOTEVASION')
            cont_confidence.append('HIGHCONFIDENCE')
            cont_score.append(1.0)
            continue
        found_word = ''
        for word in insults[0]:
            if word in df['frase_human'].iloc[index]:
                found_word = word
                break
        if found_word:
            cont_label.append('PROFANITY')
            cont_confidence.append('HIGHCONFIDENCE')
            cont_score.append(1.0)
            continue
        label1 = response['Classes'][0]['Name']
        label2 = response['Classes'][1]['Name']
        label3 = response['Classes'][2]['Name']
        score1 = response['Classes'][0]['Score'] * weights[label1]
        score2 = response['Classes'][1]['Score'] * weights[label2]
        score3 = response['Classes'][2]['Score'] * weights[label3]
        if score1 < 0.9 and score2 < 0.9 and score3 < 0.9:
            label = 'OK'
            score = max(score1, score2, score3)
            confidence = 'LOWCONFIDENCE'
        elif score1 > score2 and score1 > score3:
            label = label1
        elif score2 > score1 and score2 > score3:
            label = label2
        else:
            label = label3
        
        score = max(score1,score2,score3)
        if score<0.95:
            cont_confidence.append('LOWCONFIDENCE')
        else:
            cont_confidence.append('HIGHCONFIDENCE')
            
        cont_score.append(score)
        cont_label.append(label)
    t5 = time.time()
    print("Labels: ", t5 - t4)
    print("Entrando a sendmail")
    t6 = time.time()
    df['label'] = cont_label
    df['confidence'] = cont_confidence
    df['score'] = cont_score
    val_final = df.copy()
    val_final.drop('frase_bot', inplace=True, axis=1)
    labels = ['ROBOTEVASION', 'RECORDER', 'ROBOTFAILSDETECTION', 'PROFANITY', 'WORK','DISEASE','NOROBOT','PROMISE','WRONGNUM','NONAME', 'OK']
    for label in labels:
        subdf = val_final[val_final['label'] == label].copy()
        if not subdf.empty:
            container_rut, container_phone, container_callid, container_val_date, container_val_time = [], [], [], [], []
            for val in subdf['file']:
                val_split = str(val).split("_")
                container_rut.append(val_split[0])
                container_phone.append(val_split[1])
                container_callid.append(val_split[2])
                container_val_date.append(val_split[3])
                container_val_time.append(val_split[4])
            subdf['rut'] = container_rut
            subdf['phone'] = container_phone
            subdf['callid'] = container_callid
            subdf['date'] = container_val_date
            subdf['time'] = container_val_time
            to_file(label, subdf.sort_values(by='score', ascending=False), yesterday)
            subdf.drop('file', inplace=True, axis=1)
            subdf.drop('label', inplace=True, axis=1)
            subdf.drop('frase_human', inplace=True, axis=1)
            subdf.drop('transcript', inplace=True, axis=1)
            subdf.drop('tr_file', inplace=True, axis=1)
            subdf.drop('bot_file', inplace=True, axis=1)
            n = 1500
            list_df = [subdf[i:i+n] for i in range(0,subdf.shape[0],n)]
            for adf in list_df:
                send_mail('/aws/lambda/mail-sender','mvp-sendmail', label, adf.sort_values(by=['score'], ascending=False).to_string()) ## Flase
    t7 = time.time()
    print("Ending: ", t7-t6)