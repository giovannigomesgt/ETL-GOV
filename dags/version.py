from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
import os
import requests
from zipfile import ZipFile
import re
from bs4 import BeautifulSoup
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime, timedelta
from airflow.utils.trigger_rule import TriggerRule
from os import walk
from airflow.models import Variable
from pytz import timezone
from urllib.parse import urljoin

import botocore.exceptions


from airflow.providers.amazon.aws.hooks.s3 import S3Hook

## 256240406578-datalake-
## 256240406578-datalake-

# Variables

ENVIRONMENT = Variable.get("ENVIRONMENT")
OBJECT = 'Cnaes'
BUCKET_NAME = f'256240406578-datalake-{ENVIRONMENT}-raw'
PREFIX_OBJECT = f'dados_publicos_cnpj/{OBJECT}/'
RF_URL = 'https://dadosabertos.rfb.gov.br/CNPJ/'

# Cria um objeto S3Hook
s3_hook = S3Hook(aws_conn_id='aws_default')

def getLinks():
    url = 'http://200.152.38.155/CNPJ/'
    soup = BeautifulSoup(requests.get(url).content, 'html.parser')
    links = [urljoin(url, link.get('href'))
             for link in soup.find_all('a', href=re.compile('\.zip$'))]
    for link in links:
        print(link)
    return links
   
def getobject(task_instance):
    links = task_instance.xcom_pull(task_ids='Get_Links')
    filescnaes = [lista for lista in links if OBJECT.lower() in lista.lower()]
    return filescnaes

#### VERSIONAMENTO ####
### VERIFICA VERSÃO DO ARQUIVO NO S3
def getVersionS3():
    try:
        keys = s3_hook.list_keys(BUCKET_NAME, prefix=PREFIX_OBJECT)
        if len(keys) > 1:
            firstObject = keys[0]
            response = s3_hook.get_key(firstObject, BUCKET_NAME)

            #Imprime a data da última modificação
            last_modified = response.last_modified.replace(tzinfo=None)
            print(f'last_modified do arquivo {last_modified}')
            # CONVERTENDO datetime.datetime para str
            print(type(last_modified.strftime("%d/%m/%Y %H:%M:%S")))
            return last_modified.strftime("%d/%m/%Y %H:%M:%S")
        else:
            print(type('01/01/2000 00:00:00'))
            return '01/01/2000 00:00:00'
    
    except Exception as e:
        if 'AccessDenied' in str(e):
            # tratamento de exceção específico para permissões insuficientes
            print(f'Não foi possível acessar o bucket: {BUCKET_NAME} de {ENVIRONMENT} devido a permissões insuficientes')
            return None

        elif '404' in str(e):
            print('Arquivo não encontrado')
            return None
        else:
            return None

### VERIFICA VERSÃO DO ARQUIVO NO SITE DO GOV
def get_rf_versions():
    page = requests.get(RF_URL)
    soup = BeautifulSoup(page.content, 'html.parser')
    table = soup.find('table')
    for row in table.find_all('tr'):
        cells = row.find_all('td')
        if cells:
            file_name = cells[1].text.replace(' ', '')
            if '.zip' in file_name.lower() and OBJECT.lower() in file_name.lower():
                data_hora_str = re.sub(r'[^\d]+', '', cells[2].text)  # Remove caracteres não numéricos
                
                data_hora = datetime.strptime(data_hora_str, '%Y%m%d%H%M%S')
                print(data_hora.strftime("%d/%m/%Y %H:%M:%S")) ## <class 'datetime.datetime'>
                
                #print(type(date_obj.strftime("%d/%m/%Y %H:%M:%S")))
                return (data_hora.strftime("%d/%m/%Y %H:%M:%S"))   
    
    return None

### COMPARA VERSÕES
def versionamento(task_instance):
    rf_version = task_instance.xcom_pull(task_ids=f'Checking_{OBJECT}_Version_on_web')
    s3_version = task_instance.xcom_pull(task_ids=f'Checking_{OBJECT}_Version_on_s3')

    date_format = '%d/%m/%Y %H:%M:%S'

    rf_version_date_obj = datetime.strptime(rf_version, date_format)
    s3_version_date_obj = datetime.strptime(s3_version, date_format)


    if rf_version > s3_version:
        print('DADOS DO GOV ESTÃO MAIS ATUALIZADOS!')
        print('')
        print(f'VERSÃO NO SITE OFICIAL {rf_version_date_obj.strftime("%d/%m/%Y %H:%M:%S")}')
        print(f'VERSÃO NO S3 {s3_version_date_obj.strftime("%d/%m/%Y %H:%M:%S")}')
        return 'Downloading'
    else:
        print(f'VERSÃO NO SITE OFICIAL {rf_version_date_obj.strftime("%d/%m/%Y %H:%M:%S")}')
        print(f'VERSÃO NO S3 {s3_version_date_obj.strftime("%d/%m/%Y %H:%M:%S")}')
        return 'Version_OK'
      
def download(task_instance):
    file_urls = task_instance.xcom_pull(task_ids='Find_Cnaes')
    for file_url in file_urls:
        response = requests.head(file_url)
        file_name = os.path.basename(file_url)
        folder_name = os.path.splitext(file_name)[0]

        endereco = re.sub(r'\d+', '', folder_name)
        os.makedirs(endereco, exist_ok=True)

        checkfiles = os.listdir(endereco)
        if f"{folder_name}.zip" in checkfiles or f"{folder_name}.CSV" in checkfiles:
            print('Arquivo já existe')
        else:
            response = requests.get(file_url, stream=True)
            print('*' * 100)
            print(f"Downloading {file_name}")
            print('*' * 100)
            with open(f'{endereco}/{file_name}', "wb") as f:
                for chunk in response.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)
            print('*' * 100)
            print('Download Concluido!')
            print('*' * 100)
    return f'{endereco}/{file_name}'
    
def skip():
    print('*'*50)
    print('A versão do arquivo está ok')
    print('*'*50)
    return 'fim'

def extract(task_instance):
    # RECEBE O NOME DA PASTA
    folder = task_instance.xcom_pull(task_ids='Find_Cnaes')[0].split('/')[-1].replace('.zip','')
    #nomedoarquivo = folder.split('/')[-1].replace('.zip','')
    folder = re.sub(u'[0123456789]', '',folder)
    print('*'*150)
    print('Iniciando Extração')
    # VERIFICA A EXISTÊNCIA DA PASTA
    print('*'*150)
    if os.path.exists(folder):
        checkfile = os.listdir(folder)
        for file in checkfile:
            if '.zip' in file:
                filename = f'{folder}/{file}'
                print(filename)
                # Abre o arquivo ZIP
                with ZipFile(filename, "r") as z:
                    # Imprime o nome de cada arquivo dentro do arquivo ZIP
                    for compressed in z.namelist():
                        compressedfile = compressed
                    z.extractall(folder)
                    print(f'{filename} extraído')
                os.remove(filename)
                print(f'{filename} excluído')
                nomedoarquivo = filename.split('/')[-1].replace('.zip','')
            #print(f'caminho_do_arquivo {folder}')
            #print(f'Caminho do novo arquivo {nomedoarquivo}')     
                try:
                    # Renomeando Arquivo
                    caminho_do_arquivo = f'{folder}/{compressedfile}'
                    novo_nome = f'{folder}/{nomedoarquivo}.CSV'
                    os.rename(caminho_do_arquivo, novo_nome)
                    print(f'Arquivo {compressedfile} renomeado para {nomedoarquivo}.CSV')
                except:
                    print('arquivo já Existe')
    else:
        print('*'*50)
        print('Pasta Não encontrada')
        print('*'*50)
    print('*'*50)
    return 'fim'
    #return 'Upload_to_S3'


with DAG(f'testeVersionamento_{OBJECT}', start_date=datetime(2022,12,16), schedule_interval=None, catchup= False, tags=['VERSIONAMENTO','S3']) as dag:
    
    fim =  DummyOperator(task_id = "fim", trigger_rule=TriggerRule.NONE_FAILED)
        
    taskgetLinks = PythonOperator(
        task_id = 'Get_Links',
        python_callable = getLinks
    )

    taskdownloadcnaes = PythonOperator(
        task_id = f'Find_{OBJECT}',
        python_callable = getobject
    )

    taskversionings3 = PythonOperator(
        task_id = f'Checking_{OBJECT}_Version_on_s3',
        python_callable = getVersionS3
    )
    #Checking CNAE Version_ on S3
    taskversioningrf = PythonOperator(
        task_id = f'Checking_{OBJECT}_Version_on_web',
        python_callable = get_rf_versions
    )

    taskversion = BranchPythonOperator(
        task_id = 'Checking_Objects_Version',
        python_callable = versionamento,
        trigger_rule=TriggerRule.NONE_FAILED
    )
    
    ###### PARA FAZER A VERIFICAÇÃO É NECESSÁRIO USAR UM MÉTODO BranchPythonOperator #######


    taskdownload = PythonOperator(
        task_id = 'Downloading',
        python_callable = download
    )

    taskskip = BranchPythonOperator(
    task_id = 'Version_OK',
    python_callable = skip
    )

    taskextract = BranchPythonOperator(
        task_id='Extracting',
        python_callable=extract,
        #trigger_rule = TriggerRule.ALL_DONE,
        trigger_rule = TriggerRule.NONE_FAILED
    )


# ORQUESTRAÇÃO

taskgetLinks >> taskdownloadcnaes
taskdownloadcnaes >> [taskversionings3, taskversioningrf]
[taskversionings3, taskversioningrf] >> taskversion
taskversion >> [taskdownload, taskskip]
[taskdownload, taskskip] >> taskextract
taskextract >> fim


