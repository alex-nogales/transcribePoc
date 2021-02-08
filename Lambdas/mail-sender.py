import base64
import boto3
import gzip
import json
import logging
import os
from itertools import zip_longest
import pandas as pd
from datetime import date, timedelta

from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def logpayload(event):
    logger.setLevel(logging.DEBUG)
    logger.debug(event['awslogs']['data'])
    compressed_payload = base64.b64decode(event['awslogs']['data'])
    uncompressed_payload = gzip.decompress(compressed_payload)
    log_payload = json.loads(uncompressed_payload)
    return log_payload


def error_details(payload):
    error_msg = ""
    log_events = payload['logEvents']
    logger.debug(payload)
    loggroup = payload['logGroup']
    logstream = payload['logStream']
    lambda_func_name = loggroup.split('/')
    logger.debug(f'LogGroup: {loggroup}')
    logger.debug(f'Logstream: {logstream}')
    logger.debug(f'Function name: {lambda_func_name[3]}')
    logger.debug(log_events)
    for log_event in log_events:
        error_msg += log_event['message']
    logger.debug('Message: %s' % error_msg.split("\n"))
    return loggroup, logstream, error_msg, lambda_func_name

def get_sns(error_msg):
    labels_robotica = ['ROBOTEVASION', 'RECORDER', 'ROBOTFAILSDETECTION','EARLYHANG','NOROBOT','NONAME', 'OK']
    labels_comercial = ['DISEASE','WORK','PROMISE']
    labels_calidad = ['WRONGNUM','PROFANITY']
    for label in labels_calidad:
        if label in error_msg:
            return 'arnCalidad'
            
    for label in labels_comercial:
        if label in error_msg:
            return 'arnComercial'
            
    return 'arnRobotica'
    
    
    #Robotica y canales: ROBOTEVASION, ROBOTFAILSDETECTION, RECORDER, NONAME, NOROBOT, EARLYHANG, PROMISE
    #Area comercial: DISEASE, WORK
    #Calidad: WRONGNUM, PROFANITY
    
def publish_message(loggroup, logstream, error_msg, lambda_func_name):
    snsARN = get_sns(error_msg)
    yesterday = date.today() - timedelta(days=1)
    
        
    sns_arn = os.environ[snsARN]  # SNS NEEDS TO BE CHANGED TO VARIABLE snsARN normal
    snsclient = boto3.client('sns')
    try:
        z = error_msg.split("\n")
        tag = error_msg[error_msg.find("[")+1:error_msg.find("]")]
        header = z[0].split(" ")
        header = [i for i in header if i != '' ]
        #label = header.pop(0)
        body = z[1:]
        body = [i.split(" ") for i in body]
        #body = [[i] for j in body for i in j if i != '' ]
        body = [list(filter(None, lst)) for lst in body]
        print("Header: ", header)
        print("Body: ", body)
        df = pd.DataFrame(body, columns=header)
        df.drop(df.columns[0], axis=1, inplace=True)
        yesterday = yesterday.strftime('%Y%m%d')
        subject_title = f'Analisis Speech Analytics {yesterday}: {tag} '
        message = ""
        message += f"\nAnalisis Speech Analytics {yesterday}: {tag}" + "\n\n"
        message += "##########################################################\n"
        message += "# LogGroup Name:- " + str(loggroup) + "\n"
        message += "# LogStream:- " + str(logstream) + "\n"
        message += "# Log Message:- " + "\n"
        #message += "# \t\t" + str(error_msg.split("\n")) + "\n"
        message += df.to_string(index=False) + "\n"
        message += "##########################################################\n"

        
        # Sending the notification...
        snsclient.publish(
            TargetArn=sns_arn,
            Subject=subject_title,
            Message=message
        )
    except ClientError as e:
        logger.error("An error occured: %s" % e)


def lambda_handler(event, context):
    pload = logpayload(event)
    lgroup, lstream, errmessage, lambdaname = error_details(pload)
    publish_message(lgroup, lstream, errmessage, lambdaname)
