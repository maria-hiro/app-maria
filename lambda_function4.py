import json
import boto3
from datetime import datetime, timedelta
from botocore.exceptions import ClientError

# Inicializa os clientes Bedrock e SNS
bedrock_client = boto3.client("bedrock-runtime", region_name="us-east-1")
sns_client = boto3.client("sns", region_name="us-east-1")
s3_client = boto3.client("s3", region_name="us-east-1")

# Obtém o ARN do tópico SNS a partir de uma variável de ambiente
SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:724772050288:notificacao_bedrock"

def lambda_handler(event, context):
    for record in event['Records']:
        try:
            # Lê o corpo da mensagem da fila SQS
            message_body = json.loads(record['body'])
            
            # Extrai os detalhes do arquivo S3 da mensagem personalizada
            bucket_name = message_body['bucket_name']
            object_key = message_body['object_key']

            print(f"Processando arquivo '{object_key}' no bucket '{bucket_name}'.")

            # Lê o conteúdo do arquivo JSON no S3
            response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
            file_content = response['Body'].read().decode('utf-8')  # Converte para string
            
            # Extrai o nome do arquivo para ser usado no título do email
            nome_arquivo = object_key.split('/')[-1]  # Apenas o nome do arquivo, sem o caminho
            
            # Formata o prompt para enviar ao modelo
            prompt_text = f"""Você é um assistente empresarial e está analisando arquivos txt.
                                Dado o arquivo:

                                {file_content}

                                Siga as seguintes instruções:
                                    - identifique somente a data, a organização e a localização;
                                    - traga insights relevantes sobre as informações do arquivo para que o CEO possa fazer uma análise boa nos pontos;
                                    - informe sobre qual arquivo está sendo analisado;

                                Após a coleta das informações, escreva um relatório com nome Maria Clara H. e atenciosamente para ser enviado para o CEO da empresa central.
                                """
            
            # Estrutura do pedido para invocar o modelo com o texto do prompt
            native_request = {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 1000,
                "temperature": 0,
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": prompt_text
                            }
                        ]
                    }
                ]
            }

            # Converte o pedido para JSON
            request = json.dumps(native_request)

            # Invoca o modelo
            response = bedrock_client.invoke_model(modelId="anthropic.claude-3-haiku-20240307-v1:0", body=request)

            # Decodifica a resposta
            model_response = json.loads(response["body"].read().decode('utf-8'))

            # Extrai a resposta do modelo
            if 'content' in model_response:
                response_text = model_response['content'][0]['text']
                
                # Obtém a data e hora no fuso horário de São Paulo (Brasil) - Ajuste de 3 horas
                current_datetime_utc = datetime.utcnow()  # Pega a hora em UTC
                current_datetime_brazil = current_datetime_utc - timedelta(hours=3)  # Ajusta para o fuso horário de São Paulo (GMT-3)
                
                # Formata a data e hora
                formatted_datetime = current_datetime_brazil.strftime("%d/%m/%Y %H:%M:%S")
                
                # Assunto do email (nome do arquivo e data/hora no formato brasileiro)
                email_subject = f"Resumo Mensal Diretoria - Arquivo: {nome_arquivo} - {formatted_datetime}"
                
                # Publica a mensagem no SNS
                sns_client.publish(
                    TopicArn=SNS_TOPIC_ARN,
                    Message=response_text,
                    Subject=email_subject
                )
                print("Mensagem enviada com sucesso para o tópico SNS.")
            else:
                print("A resposta do modelo não contém 'content'.")

        except ClientError as e:
            print(f"Erro de cliente ao invocar o modelo ou enviar mensagem: {e}")
        except Exception as e:
            print(f"Erro ao processar o arquivo, invocar o modelo ou enviar mensagem: {e}")

    return {
        'statusCode': 200,
        'body': json.dumps('Processamento concluído com sucesso!')
    }
