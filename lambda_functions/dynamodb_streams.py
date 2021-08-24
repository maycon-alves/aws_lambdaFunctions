import json
import boto3
import datetime

tableName = "name"
s3 = boto3.client('s3')
today = datetime.datetime.now().strftime("%Y-%m-%d")

def lambda_handler(event, context):    
    try:
        for record in event['Records']:
            if record['eventName'] == 'INSERT':
                handle_insert_case(record)
            elif record['eventName'] == 'MODIFY':
                handle_modify_case(record)
            elif record['eventName'] == 'REMOVE':
                handle_remove_case(record)
        return {
        'statusCode': 200,
        'body': json.dumps('ALL STEPS COMPLETED WITH SUCESS!!!!!')
        }  
    except Exception as e:
        print(e)
        return "Oops! We had a little problem!"
    
def handle_insert_case(record):
    newRegister = record['dynamodb']['NewImage']
    newId = record['dynamodb']['Keys']['codigoUsuario']['S']
    bucketName = "name"
    handle_write_in_bucket(bucketName, 'INSERT', newRegister, newId)

def handle_modify_case(record):
    newRegister = {
        'newImage' : record['dynamodb']['NewImage'], 
        'oldImage' : record['dynamodb']['OldImage']
    }    
    newId = record['dynamodb']['Keys']['codigoUsuario']['S']
    bucketName = "name"
    handle_write_in_bucket(bucketName, 'MODIFY', newRegister, newId)

def handle_remove_case(record):
    newRegister = record['dynamodb']['OldImage']
    newId = record['dynamodb']['Keys']['codigoUsuario']['S']
    bucketName = "name"
    handle_write_in_bucket(bucketName, 'REMOVE', newRegister, newId)

def handle_write_in_bucket(nameBucket, operation, bodyRegister, Id):
    folder = f'extracao-diaria/{today}/{tableName}/{operation}/'
    fileName = f"usuario-{Id}"
    uploadByteStream = bytes(json.dumps(bodyRegister).encode('UTF-8'))
    s3.put_object(Bucket=nameBucket, Key=folder + fileName, Body=uploadByteStream)