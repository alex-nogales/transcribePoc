import json
import sys, os
from datetime import datetime
from pip._internal import main
from urllib.parse import unquote_plus
import re
import unicodedata
# Update BOTO3 version to latest
main(['install', 'boto3', '--target', '/tmp'])
sys.path.insert(0, '/tmp/')
import boto3

lastReqId = 0

def name_formater(a_string):
    ''' removes / and _ from string
    :param a_string: a string to be formated
    :return: returns the string formated in lower case
    '''
    
    a_string = re.sub('/_. ', '_', a_string)
    return a_string.lower()
    
def create_transcribe_job(input_bucket, input_key, output_bucket):
    ''' Create the transcribe job without IPA custom vocabulary
    
    :param input_bucket: Bucket from where the audio file is located
    :param input_key: Key of audio file
    :param output_bucket: Bucket of the output file (.json formatted)
    :return: Nothing
    '''
    
    # Generate the variables used in the rest of the code
    client = boto3.client('transcribe')
    now_timestamp = datetime.now().timestamp()
    path = 'levenshteinTests/RAW/'
    file_name = os.path.basename(input_key)
    job_name = f'{name_formater(file_name)}-{now_timestamp}-RAW' 
    output_name = f'{path}{file_name}-{now_timestamp}-RAW.json'
    job_uri = f's3://{input_bucket}/{input_key}'
    
    # Create the transcirption job with specific settings
    client.start_transcription_job(
        TranscriptionJobName=job_name,
        LanguageCode='es-ES',
        Media={
            'MediaFileUri': job_uri
        },
        OutputBucketName=output_bucket,
        OutputKey=output_name,
        Settings={
            'ChannelIdentification': False
        },
        )    
        

def lambda_handler(event, context):
    ''' AWS Lambda Handler
    
    '''
    global lastReqId
    
    if lastReqId == context.aws_request_id:
        return True
    else:
        lastReqId = context.aws_request_id
        
    message = json.loads(event['Records'][0]['Sns']['Message'])
    bucket = message['Records'][0]['s3']['bucket']['name']
    key = message['Records'][0]['s3']['object']['key']
    endpoint = f's3://{bucket}/'
    bucket_out = 'awstranscribe-tests'
 
    create_transcribe_job(bucket, key, bucket_out)
    