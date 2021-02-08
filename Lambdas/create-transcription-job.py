from urllib.parse import unquote_plus
from hashlib import sha256
import time
from datetime import datetime
import sys
from pip._internal import main

#Actualiza Version de Boto3
main(['install', 'boto3', '--target', '/tmp/'])
sys.path.insert(0,'/tmp/')
import boto3

lastReqId = 0

def createTranscribeJob(input_bucket, input_bucket_key, output_bucket_name):

    transcribe = boto3.client('transcribe')

    key = input_bucket_key
    #job_name = "mvp-socofin-" + datetime.now().strftime("%Y%m%d%H%M%S%f")
    #job_name = "mvp-socofin-" + sha256(input_bucket_key.encode('utf-8')).hexdigest()
    job_name = key[17:-4]
    job_name = 'DA'+job_name.replace('/',"_").replace('.',"_").replace(' ',"_");
    print("job_name: "+job_name)
    #[a-zA-Z0-9-_.!*'()/]{1,1024}$; Value '/20210118/2020-12-15 12-13-59.056959051918'


    output_name = "output-transcribe" + key[17:-4].replace('.',"_").replace(' ',"_") + ".json"
    job_uri = "s3://" + input_bucket + "/" + key
    output_bucket = output_bucket_name
    
    print("output_name --- ", output_name)
    
    settings={
            'ChannelIdentification': True,
            'VocabularyName': 'mvp-socofin-voc'
        }
    
    transcribe.start_transcription_job(
        TranscriptionJobName=job_name,
        Media={'MediaFileUri': job_uri},
        LanguageCode='es-ES',
        Settings=settings,
        OutputBucketName=output_bucket,
        OutputKey=output_name
    )
    while True:
        status = transcribe.get_transcription_job(
            TranscriptionJobName=job_name)
        if status['TranscriptionJob']['TranscriptionJobStatus'] in ['COMPLETED', 'FAILED']:
            if status['TranscriptionJob']['TranscriptionJobStatus'] == 'COMPLETED':
                #transcribe.delete_transcription_job(
                #    TranscriptionJobName=job_name
                #)
                print("Se ha eliminado el job: ", job_name,
                      " La Transcripcion se encuentra en : ", output_bucket, " con la key: ", output_name)
                
                s3 = boto3.resource('s3')
                s3.Object(input_bucket,"procesados"+input_bucket_key[17:]).copy_from(CopySource=input_bucket +"/"+ input_bucket_key)
                #s3.Object(input_bucket,input_bucket_key).delete()
                print("Se ha movido el archivo a la carpeta procesados.")
            break
        print("No se encuentra listo aun...")
        time.sleep(5)
    print(status)

def lambda_handler(event, context):
    global lastReqId
    
    if lastReqId == context.aws_request_id:
        return True
    else:
        lastReqId = context.aws_request_id
        
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])
        endpoint = "s3://" + bucket + "/"
        bucket_out = "socofin-output"
        
        createTranscribeJob(bucket, key, bucket_out)
