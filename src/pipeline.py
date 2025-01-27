import requests
import os
from google.cloud.sql.connector import Connector, IPTypes
import pytds
import sqlalchemy
from sqlalchemy import Table, MetaData, Column, String, Float

def extract_dados_bitcoin():
    url = "https://api.coinbase.com/v2/prices/spot"

    response = requests.get(url)
    dados = response.json()
    return dados

def transforma_dados_bitcoin(dados):
    valor = dados['data']['amount']
    criptomoeda = dados['data']['base']
    moeda = dados['data']['currency']
    
    dados_transformados = {
        "valor" : valor,
        "criptomoeda": criptomoeda,
        "moeda": moeda
    }
    return dados_transformados

def load_dados_bitcoin():
    instance_connection_name = os.getenv("INSTANCE_CONNECTION_NAME")  # e.g. 'project:region:instance'
    db_user = os.getenv("DB_USER")  # e.g. 'my-db-user'
    db_pass = os.getenv("DB_PASSWORD")  # e.g. 'my-db-password'
    db_name = os.getenv("DB_NAME")  # e.g. 'my-database'
    
    print(db_name)
    
    ip_type = IPTypes.PUBLIC

    connector = Connector(ip_type)

    def getconn():
        conn = connector.connect(
            instance_connection_name,
            "pytds",
            user=db_user,
            password=db_pass,
            db=db_name
        )
        return conn

    # Use 'creator' to pass the connection factory to SQLAlchemy
    engine = sqlalchemy.create_engine(
        "mssql+pytds://",
        creator=getconn
    )
    return engine

def criar_tabela(engine):
    metadata = MetaData()

    tabela = Table(
        'CriptoMoeda', metadata,
        Column('valor', Float, nullable=False),
        Column('criptomoeda', String(50), nullable=False),
        Column('moeda', String(10), nullable=False),
    )

    # Verificando se a tabela já existe e criando-a, caso necessário
    metadata.create_all(engine)

def inserir_dados_no_banco(dados_tratados):
    engine = load_dados_bitcoin()
    
    # Criar tabela, se não existir
    criar_tabela(engine)

    metadata = MetaData()
    tabela = Table('CriptoMoeda', metadata, autoload_with=engine)

    # Inserindo os dados transformados
    with engine.connect() as connection:
        insert_stmt = tabela.insert().values(
            valor=dados_tratados['valor'],
            criptomoeda=dados_tratados['criptomoeda'],
            moeda=dados_tratados['moeda']
        )
        try:
            connection.execute(insert_stmt)
            print("Dados inseridos com sucesso!")
        except Exception as e:
            print(f"Erro ao inserir dados: {e}")

if __name__ == "__main__":
    # Extração dos dados
    dados_json = extract_dados_bitcoin()
    dados_tratados = transforma_dados_bitcoin(dados_json)
    print(dados_tratados)
    # Enviar os dados para o banco
    inserir_dados_no_banco(dados_tratados)