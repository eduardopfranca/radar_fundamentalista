import pandas as pd
from datetime import datetime, timedelta
import time
import requests

URL_BASE = 'https://api.fintz.com.br'
api_key = 'varos_4a8d8a338da7bea2fb78540c73b0bfa7'

headers = { 'X-API-Key': api_key}

# endpoint = URL_BASE + '/bolsa/b3/avista/busca'
# res = requests.get(endpoint, headers=headers, params=params)
# print(res.json())

class Fintz:
    
    @staticmethod
    def precos_arquivo(self):
        params = {}

        endpoint = URL_BASE + '/bolsa/b3/avista/cotacoes/historico/arquivos'
        response = requests.get(endpoint, headers=self.headers, params=params)
        print(response.json())

    
    @staticmethod
    def contabilidade_arquivo(self, item, periodo):
        params = { 'item': item, 'tipoPeriodo': periodo }

        endpoint = URL_BASE + '/bolsa/b3/avista/itens-contabeis/point-in-time/arquivos'
        response = requests.get(endpoint, headers=self.headers, params=params)
        print(response.json())


    @staticmethod
    def indicadores_arquivo(self, indicador):
        params = { 'indicador': indicador }

        endpoint = URL_BASE + '/bolsa/b3/avista/indicadores/point-in-time/arquivos'
        response = requests.get(endpoint, headers=self.headers, params=params)
        print(response.json())


    @staticmethod
    def instantiate_arquivo(self, arquivo, path=''):
        if path[-6:] == 'parquet':
            caminho_completo = path
        else:
            caminho_completo = f'{path}\{arquivo}'

        df = pd.read_parquet(caminho_completo)

        return df