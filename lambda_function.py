import json
import boto3
from botocore.exceptions import ClientError
import datetime

# Inicializando os clientes do S3 e DynamoDB
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

# Nome da tabela DynamoDB
TABLE_NAME = 'ArquivoS3'

def lambda_handler(event, context):
    """
    Função Lambda para processar uploads de arquivos no S3 e armazenar metadados no DynamoDB.
    :param event: Evento gerado pelo S3 (upload de arquivo).
    :param context: Contexto de execução.
    :return: Mensagem de sucesso ou erro.
    """
    # Obtendo o nome do bucket e o nome do arquivo a partir do evento
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    nome_arquivo = event['Records'][0]['s3']['object']['key']
    
    try:
        # Obtendo os metadados do arquivo (como tamanho)
        response = s3_client.head_object(Bucket=bucket_name, Key=nome_arquivo)
        tam_arquivo = response['ContentLength']
        data_upload = datetime.datetime.now().isoformat()
        
        # Referenciando a tabela no DynamoDB
        table = dynamodb.Table(TABLE_NAME)
        
        # Inserindo os metadados no DynamoDB
        table.put_item(
            Item={
                'nome_arquivo': nome_arquivo,
                'data_upload': data_upload,
                'tam_arquivo': tam_arquivo
            }
        )
        
        # Resposta de sucesso
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
        # Tratamento de erro
        print(f"Erro ao processar arquivo {nome_arquivo} do bucket {bucket_name}: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Erro ao processar o arquivo',
                'error': str(e)
            })
        }