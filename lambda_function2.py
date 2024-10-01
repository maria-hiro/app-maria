import json
import boto3
from botocore.exceptions import ClientError
import datetime

s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
sns_client = boto3.client('sns')

TABLE_NAME = 'ArquivoS3'

SNS_TOPIC_ARN = 'arn:aws:sns:us-east-1:724772050288:notificacao_app'

def lambda_handler(event, context):
    """
    Função Lambda para processar uploads de arquivos no S3 e armazenar metadados no DynamoDB.
    :param event: Evento gerado pelo S3 (upload de arquivo).
    :param context: Contexto de execução.
    :return: Mensagem de sucesso ou erro.
    """
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    nome_arquivo = event['Records'][0]['s3']['object']['key']
    
    try:
        response = s3_client.head_object(Bucket=bucket_name, Key=nome_arquivo)
        tam_arquivo = response['ContentLength']
        data_upload = datetime.datetime.now().isoformat()
        
        table = dynamodb.Table('ArquivoS3')
        
        table.put_item(
            Item={
                'nome_arquivo': nome_arquivo,
                'data_upload': data_upload,
                'tam_arquivo': tam_arquivo
            }
        )
        
        mensagem = (f"O arquivo '{nome_arquivo}' foi carregado no bucket S3'{bucket_name}'.\n"
                    f"Tamanho: {tam_arquivo} bytes\nData de upload: {data_upload}")
                    
        sns = sns_client.publish(
            TopicArn='arn:aws:sns:us-east-1:724772050288:notificacao_app',
            Message=mensagem,
            Subject=f"Novo Upload: {nome_arquivo}"
        )
            
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Metadados do arquivo {nome_arquivo} armazenados com sucesso no DynamoDB!',
                'nome_arquivo': nome_arquivo,
                'tam_arquivo': tam_arquivo,
                'data_upload': data_upload
            })
        }
    
    except ClientError as e:
        print(f"Erro ao processar arquivo {nome_arquivo} do bucket {bucket_name}: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Erro ao processar o arquivo',
                'error': str(e)
            })
        }
