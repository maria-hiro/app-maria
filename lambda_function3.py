import json
import boto3
from botocore.exceptions import ClientError
import csv
import datetime
from io import StringIO
from boto3.dynamodb.conditions import Attr

dynamodb_resource = boto3.resource('dynamodb')
dynamodb_client = boto3.client('dynamodb')
sns_client = boto3.client('sns')
s3_client = boto3.client('s3')

# Variáveis de ambiente
TABLE_NAME = "Arquivo-S3"
SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:724772050288:notificacao_rel"
BUCKET_NAME = "rel-semanal-app"

def gerar_relatorio(event, context):
    """
    Função Lambda para gerar relatórios semanais dos metadados armazenados no DynamoDB.
    Gera um CSV contendo os metadados dos arquivos carregados no bucket S3 e envia via SNS um e-mail com o arquivo.
    """
    
    # Captura a data atual e calcula a data de uma semana atrás
    data_atual = datetime.datetime.now()
    data_semanal_inicio = (data_atual - datetime.timedelta(days=7)).isoformat()
    
    try:
        # Carrega os itens da tabela DynamoDB
        table = dynamodb_resource.Table(TABLE_NAME)
        response = table.scan(
            TableName = "ArquivoS3",
            FilterExpression=Attr('data_upload').gte(data_semanal_inicio)
        )
        
        # Filtra os itens carregados na última semana
        arquivos_semana = response['Items']
        
        # Se não houver arquivos carregados na última semana
        if not arquivos_semana:
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Nenhum arquivo carregado na última semana.'
                })
            }
        
        # Criação do arquivo CSV
        csv_buffer = StringIO()
        csv_writer = csv.writer(csv_buffer)
        csv_writer.writerow(['Nome do Arquivo', 'Data de Upload', 'Tamanho do Arquivo (bytes)'])
        
        for arquivo in arquivos_semana:
            csv_writer.writerow([arquivo['nome_arquivo'], arquivo['data_upload'], arquivo['tam_arquivo']])
        
        # Nome do arquivo CSV personalizado
        nome_relatorio = f"Rel-Semanal_{data_atual.strftime('%d-%b-%y')}.csv"
        
        # Verificar o conteúdo do CSV
        print("Conteúdo do CSV:")
        print(csv_buffer.getvalue())
        
        # Armazenar o CSV no bucket S3
        try:
            s3_response = s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=nome_relatorio,
                Body=csv_buffer.getvalue(),
                ContentType='text/csv'
            )
            print(f"Resposta do S3: {s3_response}")
            if s3_response['ResponseMetadata']['HTTPStatusCode'] == 200:
                print(f"Arquivo {nome_relatorio} salvo com sucesso no S3.")
            else:
                print(f"Falha ao salvar o arquivo no S3. Resposta: {s3_response}")
        except ClientError as e:
            print(f"Erro ao salvar o arquivo no S3: {e}")
        
        # Gerar o link do arquivo CSV no S3
        link_arquivo = f"https://{BUCKET_NAME}.s3.amazonaws.com/{nome_relatorio}"
        
        # Enviar o relatório via SNS
        enviar_relatorio_via_sns(link_arquivo, nome_relatorio)
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Relatório semanal gerado e enviado via SNS com sucesso!',
                'link_arquivo': link_arquivo
            })
        }
    except ClientError as e:
        print(f"Erro ao gerar relatório: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Erro ao gerar o relatório',
                'error': str(e)
            })
        }
def enviar_relatorio_via_sns(link_arquivo, nome_relatorio):
    """
    Envia o link do relatório CSV por SNS para os assinantes do tópico.
    """
    try:
        mensagem = (f"Relatório Semanal de Arquivos S3 - {nome_relatorio}\n"
                    f"O relatório foi gerado e pode ser acessado no link abaixo:\n"
                    f"{link_arquivo}")
        # Envio da mensagem ao tópico SNS
        sns_response = sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=mensagem,
            Subject=f"Relatório Semanal de Arquivos S3 - {nome_relatorio}"
        )
        # Verifica se a mensagem foi enviada com sucesso
        if sns_response['ResponseMetadata']['HTTPStatusCode'] == 200:
            print(f"Relatório enviado com sucesso: {sns_response}")
        else:
            print(f"Falha ao enviar relatório. Resposta SNS: {sns_response}")
    except ClientError as e:
        print(f"Erro ao enviar relatório via SNS: {e}")
    except Exception as ex:
        print(f"Erro inesperado: {ex}")
