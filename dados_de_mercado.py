import pandas as pd
from pprint import pprint
import time
from dotenv import load_dotenv
import os
import requests
from datetime import datetime, timedelta
import matplotlib.ticker as ticker
import matplotlib.dates as mdates
import matplotlib.pyplot as plt

load_dotenv()

api_key_ = os.getenv("DADOS_DE_MERCADO")

class Dados_de_mercado:

    get = ''
    url_base = f"https://api.dadosdemercado.com.br/v1/"
    api_key = api_key_

    all = []

    dic_yf_indices_setoriais = {'ibov': '^BVSP',
                     'iagro': "AGFS.SA",
                     'imob': pd.NA,
                     'ifnc': 'IFNC.SA',
                     'icon': pd.NA,
                     'iee': '^IEE',
                     'imat': 'IMAT.SA',
                     'indx': pd.NA,
                     'util': pd.NA,
                     'smll': pd.NA,
                     'midlarge': pd.NA,
                     'idiv': 'DIVO.SA',
                     'all': pd.NA}

    hoje = datetime.now()
    hoje_str = hoje.strftime('%Y-%m-%d')
    ontem = datetime.now() - timedelta(days=1)
    ontem_str = ontem.strftime('%Y-%m-%d')
    anteontem = datetime.now() - timedelta(days=2)
    anteontem_str = anteontem.strftime('%Y-%m-%d')
    anteanteontem = datetime.now() - timedelta(days=3)
    anteanteontem_str = anteanteontem.strftime('%Y-%m-%d')

    def __init__(self):
        self.all.append(self)

    
    def indices_setoriais(self):
        headers = {'Authorization': f'Bearer {self.api_key}'}
        params = {}
        get = '/indexes'
        response = requests.get(str(Dados_de_mercado.url_base+get), headers=headers, params=params)
        json = response.json()
        indices_setoriais = []
        for item in json:
            indices_setoriais.append(item['ticker'])
        return indices_setoriais
    
    def dicionario_yf_indices(self):
        return self.dic_yf_indices_setoriais

    def fluxo(self, grafico=False, path=''):
        get = '/investors'
        headers = {'Authorization': f'Bearer {self.api_key}'}
        params = {}
        response = requests.get(str(Dados_de_mercado.url_base+get), headers=headers, params=params)
        json = response.json()
        
        fluxo = pd.DataFrame(json)
        fluxo = fluxo.set_index('date')
        fluxo = fluxo[['financial_institutions', 'foreigners', 'individuals', 'institutional', 'other']]
        fluxo_invertido = fluxo.iloc[::-1]
        fluxo_invertido['instituicao_financeira'] = fluxo_invertido['financial_institutions'].cumsum()
        fluxo_invertido['estrangeiro'] = fluxo_invertido['foreigners'].cumsum()
        fluxo_invertido['institucional'] = fluxo_invertido['institutional'].cumsum()
        fluxo_invertido['pessoa_fisica'] = fluxo_invertido['individuals'].cumsum()
        fluxo_invertido['outros'] = fluxo_invertido['other'].cumsum()
        fluxo_saldo = fluxo_invertido[['estrangeiro', 'institucional', 'pessoa_fisica', 'instituicao_financeira', "outros"]]

        if grafico:
           
            fig,ax = plt.subplots()

            ax.set_title('FLUXO DE INVESTIMENTO', color='k', fontsize=20, fontweight='bold')
            ax.set_xlabel('Data', color='k', fontsize=12)
            ax.set_ylabel('Valor em milhões R$', color='k', fontsize=12)

            ax.xaxis.set_major_locator(mdates.DayLocator(interval=120))

            ax.grid(True)

            ax.plot(fluxo_saldo.index, fluxo_saldo.estrangeiro/1000000, color='b',label='Estrangeiro')
            ax.plot(fluxo_saldo.index, fluxo_saldo.institucional/1000000, color='r', label='Institucional')
            ax.plot(fluxo_saldo.index, fluxo_saldo.pessoa_fisica/1000000, color='y', label='Pessoa Física')

            ax.axhline(y=0, color='black', linewidth=2)

            ax.legend()

            plt.savefig(f'{path}fluxo.jpeg')

        return fluxo_saldo
    
    def codigo_cvm(self, estatal=False):
        get = '/companies'
        headers = {'Authorization': f'Bearer {self.api_key}'}
        params = {}
        response = requests.get(str(Dados_de_mercado.url_base+get), headers=headers, params=params)
        json = response.json()

        df_list = []
        for empresa in json:    
            b3_issuer_code = empresa['b3_issuer_code']
            cvm_code = empresa['cvm_code']
            name = empresa["name"]
            e_estatal = empresa['is_state_owned']
           
            df = pd.DataFrame({
                'nome': name,
                'codigo_cvm': cvm_code,
                'codigo_b3': b3_issuer_code
            }, index=[0])
            if estatal:
                df['estatal'] = e_estatal
            df_list.append(df)

        codigo_cvm = pd.concat(df_list, ignore_index=True)

        return codigo_cvm
    


    @staticmethod
    def composicao_indices_setoriais(path='composicao_indices_setoriais.xlsx'):
        ibov = pd.read_excel(path, sheet_name='ibov')
        ibov['Part. (%)'] = ibov['Part. (%)']/1000
        ibov = ibov[['Código', 'Part. (%)']]

        iagro = pd.read_excel(path, sheet_name='iagro')
        iagro['Part. (%)'] = iagro['Part. (%)']/1000
        iagro = iagro[['Código', 'Part. (%)']]

        imob = pd.read_excel(path, sheet_name='imob')
        imob['Part. (%)'] = imob['Part. (%)']/1000
        imob = imob[['Código', 'Part. (%)']]

        ifnc = pd.read_excel(path, sheet_name='ifnc')
        ifnc['Part. (%)'] = ifnc['Part. (%)']/1000
        ifnc = ifnc[['Código', 'Part. (%)']]

        icon = pd.read_excel(path, sheet_name='icon')
        icon['Part. (%)'] = icon['Part. (%)']/1000
        icon = icon[['Código', 'Part. (%)']]

        iee = pd.read_excel(path, sheet_name='iee')
        iee['Part. (%)'] = iee['Part. (%)']/1000
        iee = iee[['Código', 'Part. (%)']]

        imat = pd.read_excel(path, sheet_name='imat')
        imat['Part. (%)'] = imat['Part. (%)']/1000
        imat = imat[['Código', 'Part. (%)']]
    
        indx = pd.read_excel(path, sheet_name='indx')
        indx['Part. (%)'] = indx['Part. (%)']/1000
        indx = indx[['Código', 'Part. (%)']]

        util = pd.read_excel(path, sheet_name='util')
        util['Part. (%)'] = util['Part. (%)']/1000
        util = util[['Código', 'Part. (%)']]

        midlarge = pd.read_excel(path, sheet_name='midlarge')
        midlarge['Part. (%)'] = midlarge['Part. (%)']/1000
        midlarge = midlarge[['Código', 'Part. (%)']]

        smll = pd.read_excel(path, sheet_name='smll')
        smll['Part. (%)'] = smll['Part. (%)']/1000
        smll = smll[['Código', 'Part. (%)']]

        idiv = pd.read_excel(path, sheet_name='idiv')
        idiv['Part. (%)'] = idiv['Part. (%)']/1000
        idiv = idiv[['Código', 'Part. (%)']]

        all = pd.read_excel(path, sheet_name='all')
        all['Part. (%)'] = all['Part. (%)']/1000
        all = all[['Código', 'Part. (%)']]

        indices_setoriais = {'ibov': ibov,
                     'iagro': iagro,
                     'imob': imob,
                     'ifnc': ifnc,
                     'icon': icon,
                     'iee': iee,
                     'imat': imat,
                     'indx': indx,
                     'util': util,
                     'smll': smll,
                     'midlarge': midlarge,
                     'idiv': idiv,
                     'all': all}

        
        return indices_setoriais
    

    def ativos(self):
        get = '/tickers'
        headers = {'Authorization': f'Bearer {self.api_key}'}
        params = {}
        response = requests.get(str(Dados_de_mercado.url_base+get), headers=headers, params=params)
        json = response.json()

        ticker_list = []
        name_list = []
        issuer_code_list = []

        for empresa in json:
            if empresa['type'] in ['stock', 'unit']:
                ticker = empresa['ticker']
                name = empresa['name']
                issuer_code = empresa['issuer_code']
                ticker_list.append(ticker)
                name_list.append(name)
                issuer_code_list.append(issuer_code)
            else:
                pass

        ativos = pd.DataFrame()
        ativos['nome'] = name_list
        ativos['ticker'] = ticker_list
        ativos['codigo_negociacao'] = issuer_code_list

        return ativos


    def balanco(self, codigo=4170, tipo='', data=''):

        get = f'/companies/{codigo}/balances'
        headers = {'Authorization': f'Bearer {self.api_key}'}
        
        url = f'https://api.dadosdemercado.com.br/v1/{get}'
        if tipo=='' and data=='':
            response = requests.get(url=url, headers=headers)
        elif tipo=='':
            response = requests.get(url=url, headers=headers, params={'reference_date': f'{data}'})
        elif data=='':
            response = requests.get(url=url, headers=headers, params={'statement_type': tipo})
        else:
            response = requests.get(url=url, headers=headers, params={'reference_date': f'{data}','statement_type': tipo})
        json = response.json()
        balanco = pd.DataFrame(json)

        return balanco
    

    def resultado(self, codigo=4170, tipo='', info='year'):

        get = f'/companies/{codigo}/incomes'
        headers = {'Authorization': f'Bearer {self.api_key}'}
        params={
                'statement_type': tipo,
                'period_type': f'{info}'
                }
        
        url = f'https://api.dadosdemercado.com.br/v1/{get}'

        if tipo=='':
            response = requests.get(url=url, headers=headers, params={'period_type': info})
        else:
            response = requests.get(url=url, headers=headers, params={'period_type': info,'statement_type': tipo})

        json = response.json()
        resultado = pd.DataFrame(json)

        return resultado
    
    def dividendos(self, codigo=4170, data=''):
        get = f'/companies/{codigo}/dividends'
        headers = {'Authorization': f'Bearer {self.api_key}'}

        url = f'https://api.dadosdemercado.com.br/v1/{get}'

        if data == '':
            response = requests.get(url=url, headers=headers)
        else:
            response = requests.get(url=url, headers=headers, params={'date_from': data})

        json = response.json()
        dividendos = pd.DataFrame(json)

        return dividendos
    
    def num_acoes(self, codigo=4170):
        get = f'/companies/{codigo}/shares'
        headers = {'Authorization': f'Bearer {self.api_key}'}

        url = f'https://api.dadosdemercado.com.br/v1/{get}'
        response = requests.get(url, headers=headers)
        json = response.json()
        num_acoes = pd.DataFrame(json, index=[0])
        return num_acoes
    
    def cotacoes(self, data_in='2010-01-01', data_end='', ticker='VALE3'):
        if data_end == '':
            data_end = Dados_de_mercado.hoje_str

        get = f'/tickers/{ticker}/quotes'
        headers = {'Authorization': f'Bearer {self.api_key}'}
        params = {'period_init': data_in,
                  'period_end': data_end}
        response = requests.get(url=f'https://api.dadosdemercado.com.br/v1{get}', headers=headers, params=params)
        json = response.json()
        cotacoes = pd.DataFrame(json)

        return cotacoes
    
    def indicadores_financeiros(self, codigo=4170, tipo = ['con', 'ind', 'con*', 'ind*'], periodo='year'):
        get = f'/companies/{codigo}/ratios'
        headers = {'Authorization': f'Bearer {self.api_key}'}
        url = f'https://api.dadosdemercado.com.br/v1/{get}'
        params = {'statement_type': tipo,
                  'period_type': periodo}
        
        response = requests.get(url, headers=headers, params=params)

        json = response.json()
        indicadores = pd.DataFrame(json)
        try:
            return indicadores
        except:
            return response
    
    def indicadores_de_mercado(self, codigo=4170, tipo = ['con', 'ind', 'con*', 'ind*'], period_init='2010-01-01', period_end='data'):
        get = f'/companies/{codigo}/market_ratios'
        headers = {'Authorization': f'Bearer {self.api_key}'}
        url = f'https://api.dadosdemercado.com.br/v1/{get}'
        if period_end == 'data':
            period_end = self.hoje_str
        else:
            pass

        params = {'statement_type': tipo,
                  'period_init': period_init,
                  'period_end': period_end
                  }
        response = requests.get(url=url, headers=headers, params=params)
        json = response.json()
        indicadores = pd.DataFrame(json)
        try:
            return indicadores
        except:
            return response