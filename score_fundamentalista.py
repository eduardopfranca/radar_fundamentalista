import pandas as pd
import numpy as np
import os
from itertools import product
from datetime import datetime, timedelta
import yfinance as yf

class score_fundamentalista():

    caminho_dados = r'C:\Users\eduar\dev\base_dados_br'
    camihno_pasta_principal = os.getcwd()

    principais = ['RRRP3', 'ABEV3','AZUL4','B3SA3','BBSE3','BBDC3','BBDC4','BBAS3','BRKM5','BRFS3','BPAC11','CMIG4','CIEL3','COGN3',
    'CPLE6','CSAN3','CMIN3','CVCB3','CYRE3','ELET3','EMBR3','EQTL3','GGBR4','GOAU4','NTCO3','HAPV3','IRBR3','ITSA4','ITUB4','JBSS3',
    'KLBN11','RENT3','LREN3','LWSA3','MGLU3','MRFG3','BEEF3','MRVE3','PCAR3','PETR4','PRIO3','PETZ3','SBSP3','SANB11','CSNA3','SLCE3',
    'SUZB3','TAEE11','VIVT3','TIMS3','UGPA3','USIM5','VALE3','WEGE3','YDUQ3']

    radar = ['CMIG4', 'CMIG3', 'TAEE11', 'TAEE3', 'TAEE4', 'CPLE6', 'CPLE3', 'CSMG3', 'SAPR11', 'SAPR3', 'SAPR4', 'SBSP3', 'BRSR3', 
    'BRSR5', 'BRSR6', 'ITSA3', 'ITSA4', 'SANB11', 'SANB3', 'SANB4', 'BBAS3', 'BBDC4', 'BBDC3', 'ITUB3', 'ITUB4', 'BBSE3', 'PSSA3', 
    'WIZC3', 'UGPA3', 'ENAT3', 'VIVT3', 'TIMS3']

    ibov = ['RRRP3', 'ALOS3', 'ALPA4', 'ABEV3', 'ARZZ3', 'ASAI3', 'AZUL4', 'B3SA3', 'BBSE3', 'BBDC3', 'BBDC4', 'BRAP4', 'BBAS3', 'BRKM5', 
    'BRFS3', 'BPAC11', 'CRFB3', 'CCRO3', 'CMIG4', 'CIEL3', 'COGN3', 'CPLE6', 'CSAN3', 'CPFE3', 'CMIN3', 'CVCB3', 'CYRE3', 'DXCO3', 
    'ELET3', 'ELET6', 'EMBR3', 'ENGI11', 'ENEV3', 'EGIE3', 'EQTL3', 'EZTC3', 'FLRY3', 'GGBR4', 'GOAU4', 'NTCO3', 'SOMA3', 'HAPV3', 
    'HYPE3', 'IGTI11', 'IRBR3', 'ITSA4', 'ITUB4', 'JBSS3', 'KLBN11', 'RENT3', 'LREN3', 'LWSA3', 'MGLU3', 'MRFG3', 'BEEF3', 'MRVE3', 
    'MULT3', 'PCAR3', 'PETR3', 'PETR4', 'RECV3', 'PRIO3', 'PETZ3', 'RADL3', 'RAIZ4', 'RDOR3', 'RAIL3', 'SBSP3', 'SANB11', 'SMTO3', 
    'CSNA3', 'SLCE3', 'SUZB3', 'TAEE11', 'VIVT3', 'TIMS3', 'TOTS3', 'TRPL4', 'UGPA3', 'USIM5', 'VALE3', 'VAMO3', 'VBBR3', 'VIVA3', 
    'WEGE3', 'YDUQ3']

    todos_ativos = []
    todos_ativos.extend(principais)
    todos_ativos.extend(radar)
    todos_ativos.extend(ibov)

    empresas_splitadas_ano = [('BBAS3', 2)]

    ano_atual = datetime.now().year
    cinco_anos = range(ano_atual, ano_atual-4, -1)
    cinco_anos_str = [str(ano) for ano in cinco_anos]

    def __init__(self, filtro_empresas=todos_ativos):

        self.empresas = filtro_empresas
        self.carregar_cotacoes()
        anos_disponiveis = self.cotacoes.ano.unique()
        combinacoes = list(product(filtro_empresas, anos_disponiveis))
        df_empresas = pd.DataFrame(combinacoes, columns=['ticker', 'ano'])
        
        self.df_empresas = df_empresas
        self.anos = anos_disponiveis

    def carregar_cotacoes(self):
        cotacoes = pd.read_parquet(f'{self.caminho_dados}\\cotacoes.parquet')
        cotacoes['data'] = pd.to_datetime(cotacoes['data'])
        cotacoes = cotacoes[cotacoes['ticker'].isin(self.empresas)]
        cotacoes['ano'] = cotacoes['data'].dt.year

        self.cotacoes = cotacoes

    def carregar_lpa(self):
        lpa = pd.read_parquet(f'{self.caminho_dados}\\lpa.parquet')
        lpa['data'] = pd.to_datetime(lpa['data'])
        lpa = lpa[lpa['ticker'].isin(self.todos_ativos)]
        lpa = lpa[['ticker', 'data', 'valor']]

        self.lpa = lpa

    def carregar_vpa(self):
        vpa = pd.read_parquet(f'{self.caminho_dados}\\vpa.parquet')
        vpa['data'] = pd.to_datetime(vpa['data'])
        vpa = vpa[vpa['ticker'].isin(self.todos_ativos)]
        vpa = vpa[['ticker', 'data', 'valor']]

        self.vpa = vpa

    def carregar_dy(self):
        df = pd.read_parquet(f'{self.caminho_dados}\\DividendYield.parquet')
        df['data'] = pd.to_datetime(df['data'])
        df = df[df['ticker'].isin(self.todos_ativos)]
        df = df[['ticker', 'data', 'valor']]

        self.dy = df

    def calcular_dividendos(self):
        cotacoes = self.cotacoes.copy()
        dy = self.dy.copy()

        cotacoes = cotacoes[['ticker', 'data', 'preco_fechamento_ajustado']]

        cotacoes['id'] = cotacoes['ticker'] + "_" + cotacoes['data'].astype(str)
        dy['id'] = dy['ticker'] + "_" + dy['data'].astype(str)

        dividendos = pd.merge(cotacoes, dy, on='id', how='inner')
        dividendos = dividendos[['ticker_x', 'data_x', 'preco_fechamento_ajustado', 'valor']]
        dividendos.columns = ['ticker', 'data', 'cotacao', 'dy']
        dividendos['valor'] = dividendos['cotacao'] * dividendos['dy']
        dividendos = dividendos[['ticker', 'data', 'valor']]

        self.dividendos = dividendos

    def carregar_roe(self):
        df = pd.read_parquet(f'{self.caminho_dados}\\ROE.parquet')
        df['data'] = pd.to_datetime(df['data'])
        df = df[df['ticker'].isin(self.todos_ativos)]
        df = df[['ticker', 'data', 'valor']]

        self.roe = df

    def carregar_roic(self):
        df = pd.read_parquet(f'{self.caminho_dados}\\ROIC.parquet')
        df['data'] = pd.to_datetime(df['data'])
        df = df[df['ticker'].isin(self.todos_ativos)]
        df = df[['ticker', 'data', 'valor']]

        self.roic = df

    def carregar_ebit_dl(self):
        df = pd.read_parquet(f'{self.caminho_dados}\\ebit_dl.parquet')
        df['data'] = pd.to_datetime(df['data'])
        df = df[df['ticker'].isin(self.todos_ativos)]
        df = df[['ticker', 'data', 'valor']]

        self.ebit_dl = df

    def carregar_arquivos_score_tek(self, ano_inicio=2020, ano_final=2024):

        os.chdir(self.caminho_dados)

        self.precos = self.cotacoes
        
        self.ebit = pd.read_parquet('EBIT.parquet')
        self.ebit['data'] = pd.to_datetime(self.ebit['data'])
        self.ebit['ano'] = self.ebit['data'].dt.year
        self.ebit = self.ebit[(self.ebit['ano'] >= ano_inicio) & (self.ebit['ano'] <= ano_final)]
        self.ebit = self.ebit[self.ebit['ticker'].isin(self.empresas)]
        self.ebit = self.ebit[['ticker', 'data', 'ano', 'valor']]

        self.receita_liquida = pd.read_parquet('ReceitaLiquida.parquet')
        self.receita_liquida['data'] = pd.to_datetime(self.receita_liquida['data'])
        self.receita_liquida['ano'] = self.receita_liquida['data'].dt.year
        self.receita_liquida = self.receita_liquida[(self.receita_liquida['ano'] >= ano_inicio) & (self.receita_liquida['ano'] <= ano_final)]
        self.receita_liquida = self.receita_liquida[self.receita_liquida['ticker'].isin(self.empresas)]
        self.receita_liquida = self.receita_liquida[['ticker', 'data', 'ano', 'valor']]

        self.custos = pd.read_parquet('Custos.parquet')
        self.custos['data'] = pd.to_datetime(self.custos['data'])
        self.custos['ano'] = self.custos['data'].dt.year
        self.custos = self.custos[(self.custos['ano'] >= ano_inicio) & (self.custos['ano'] <= ano_final)]
        self.custos = self.custos[self.custos['ticker'].isin(self.empresas)]
        self.custos = self.custos[['ticker', 'data', 'ano', 'valor']]

        self.lucro_liquido = pd.read_parquet('LucroLiquido.parquet')
        self.lucro_liquido['data'] = pd.to_datetime(self.lucro_liquido['data'])
        self.lucro_liquido['ano'] = self.lucro_liquido['data'].dt.year
        self.lucro_liquido = self.lucro_liquido[(self.lucro_liquido['ano'] >= ano_inicio) & (self.lucro_liquido['ano'] <= ano_final)]
        self.lucro_liquido = self.lucro_liquido[self.lucro_liquido['ticker'].isin(self.empresas)]
        self.lucro_liquido = self.lucro_liquido[['ticker', 'data', 'ano', 'valor']]

        # Carregar e processar o arquivo AtivoCirculante
        self.ativo_circulante = pd.read_parquet('AtivoCirculante.parquet')
        self.ativo_circulante['data'] = pd.to_datetime(self.ativo_circulante['data'])
        self.ativo_circulante['ano'] = self.ativo_circulante['data'].dt.year
        self.ativo_circulante = self.ativo_circulante[(self.ativo_circulante['ano'] >= ano_inicio) & (self.ativo_circulante['ano'] <= ano_final)]
        self.ativo_circulante = self.ativo_circulante[self.ativo_circulante['ticker'].isin(self.empresas)]
        self.ativo_circulante = self.ativo_circulante[['ticker', 'data', 'ano', 'valor']]

        # Carregar e processar o arquivo AtivoNaoCirculante
        self.ativo_nao_circulante = pd.read_parquet('AtivoNaoCirculante.parquet')
        self.ativo_nao_circulante['data'] = pd.to_datetime(self.ativo_nao_circulante['data'])
        self.ativo_nao_circulante['ano'] = self.ativo_nao_circulante['data'].dt.year
        self.ativo_nao_circulante = self.ativo_nao_circulante[(self.ativo_nao_circulante['ano'] >= ano_inicio) & (self.ativo_nao_circulante['ano'] <= ano_final)]
        self.ativo_nao_circulante = self.ativo_nao_circulante[self.ativo_nao_circulante['ticker'].isin(self.empresas)]
        self.ativo_nao_circulante = self.ativo_nao_circulante[['ticker', 'data', 'ano', 'valor']]

        # Carregar e processar o arquivo AtivoTotal
        self.ativo_total = pd.read_parquet('AtivoTotal.parquet')
        self.ativo_total['data'] = pd.to_datetime(self.ativo_total['data'])
        self.ativo_total['ano'] = self.ativo_total['data'].dt.year
        self.ativo_total = self.ativo_total[(self.ativo_total['ano'] >= ano_inicio) & (self.ativo_total['ano'] <= ano_final)]
        self.ativo_total = self.ativo_total[self.ativo_total['ticker'].isin(self.empresas)]
        self.ativo_total = self.ativo_total[['ticker', 'data', 'ano', 'valor']]

        # Carregar e processar o arquivo CaixaEquivalentes
        self.caixa = pd.read_parquet('CaixaEquivalentes.parquet')
        self.caixa['data'] = pd.to_datetime(self.caixa['data'])
        self.caixa['ano'] = self.caixa['data'].dt.year
        self.caixa = self.caixa[(self.caixa['ano'] >= ano_inicio) & (self.caixa['ano'] <= ano_final)]
        self.caixa = self.caixa[self.caixa['ticker'].isin(self.empresas)]
        self.caixa = self.caixa[['ticker', 'data', 'ano', 'valor']]

        # Carregar e processar o arquivo DividaBruta
        self.divida_bruta = pd.read_parquet('DividaBruta.parquet')
        self.divida_bruta['data'] = pd.to_datetime(self.divida_bruta['data'])
        self.divida_bruta['ano'] = self.divida_bruta['data'].dt.year
        self.divida_bruta = self.divida_bruta[(self.divida_bruta['ano'] >= ano_inicio) & (self.divida_bruta['ano'] <= ano_final)]
        self.divida_bruta = self.divida_bruta[self.divida_bruta['ticker'].isin(self.empresas)]
        self.divida_bruta = self.divida_bruta[['ticker', 'data', 'ano', 'valor']]

        self.divida_liquida = pd.read_parquet('DividaLiquida.parquet')
        self.divida_liquida['data'] = pd.to_datetime(self.divida_liquida['data'])
        self.divida_liquida['ano'] = self.divida_liquida['data'].dt.year
        self.divida_liquida = self.divida_liquida[(self.divida_liquida['ano'] >= ano_inicio) & (self.divida_liquida['ano'] <= ano_final)]
        self.divida_liquida = self.divida_liquida[self.divida_liquida['ticker'].isin(self.empresas)]
        self.divida_liquida = self.divida_liquida[['ticker', 'data', 'ano', 'valor']]

        # Carregar e processar o arquivo EBITDA
        self.ebitda = pd.read_parquet('EBITDA.parquet')
        self.ebitda['data'] = pd.to_datetime(self.ebitda['data'])
        self.ebitda['ano'] = self.ebitda['data'].dt.year
        self.ebitda = self.ebitda[(self.ebitda['ano'] >= ano_inicio) & (self.ebitda['ano'] <= ano_final)]
        self.ebitda = self.ebitda[self.ebitda['ticker'].isin(self.empresas)]
        self.ebitda = self.ebitda[['ticker', 'data', 'ano', 'valor']]

        # Carregar e processar o arquivo PassivoCirculante
        self.passivo_circulante = pd.read_parquet('PassivoCirculante.parquet')
        self.passivo_circulante['data'] = pd.to_datetime(self.passivo_circulante['data'])
        self.passivo_circulante['ano'] = self.passivo_circulante['data'].dt.year
        self.passivo_circulante = self.passivo_circulante[(self.passivo_circulante['ano'] >= ano_inicio) & (self.passivo_circulante['ano'] <= ano_final)]
        self.passivo_circulante = self.passivo_circulante[self.passivo_circulante['ticker'].isin(self.empresas)]
        self.passivo_circulante = self.passivo_circulante[['ticker', 'data', 'ano', 'valor']]

        # Carregar e processar o arquivo PassivoNaoCirculante
        self.passivo_nao_circulante = pd.read_parquet('PassivoNaoCirculante.parquet')
        self.passivo_nao_circulante['data'] = pd.to_datetime(self.passivo_nao_circulante['data'])
        self.passivo_nao_circulante['ano'] = self.passivo_nao_circulante['data'].dt.year
        self.passivo_nao_circulante = self.passivo_nao_circulante[(self.passivo_nao_circulante['ano'] >= ano_inicio) & (self.passivo_nao_circulante['ano'] <= ano_final)]
        self.passivo_nao_circulante = self.passivo_nao_circulante[self.passivo_nao_circulante['ticker'].isin(self.empresas)]
        self.passivo_nao_circulante = self.passivo_nao_circulante[['ticker', 'data', 'ano', 'valor']]

        # Carregar e processar o arquivo PassivoTotal
        self.passivo_total = pd.read_parquet('PassivoTotal.parquet')
        self.passivo_total['data'] = pd.to_datetime(self.passivo_total['data'])
        self.passivo_total['ano'] = self.passivo_total['data'].dt.year
        self.passivo_total = self.passivo_total[(self.passivo_total['ano'] >= ano_inicio) & (self.passivo_total['ano'] <= ano_final)]
        self.passivo_total = self.passivo_total[self.passivo_total['ticker'].isin(self.empresas)]
        self.passivo_total = self.passivo_total[['ticker', 'data', 'ano', 'valor']]

        # Carregar e processar o arquivo PatrimonioLiquido
        self.patrimonio_liquido = pd.read_parquet('PatrimonioLiquido.parquet')
        self.patrimonio_liquido['data'] = pd.to_datetime(self.patrimonio_liquido['data'])
        self.patrimonio_liquido['ano'] = self.patrimonio_liquido['data'].dt.year
        self.patrimonio_liquido = self.patrimonio_liquido[(self.patrimonio_liquido['ano'] >= ano_inicio) & (self.patrimonio_liquido['ano'] <= ano_final)]
        self.patrimonio_liquido = self.patrimonio_liquido[self.patrimonio_liquido['ticker'].isin(self.empresas)]
        self.patrimonio_liquido = self.patrimonio_liquido[['ticker', 'data', 'ano', 'valor']]

        # Carregar e processar o arquivo ROE
        self.roe = pd.read_parquet('ROE.parquet')
        self.roe['data'] = pd.to_datetime(self.roe['data'])
        self.roe['ano'] = self.roe['data'].dt.year
        self.roe = self.roe[(self.roe['ano'] >= ano_inicio) & (self.roe['ano'] <= ano_final)]
        self.roe = self.roe[self.roe['ticker'].isin(self.empresas)]
        self.roe = self.roe[['ticker', 'data', 'ano', 'valor']]

        # Carregar e processar o arquivo P_L
        self.p_l = pd.read_parquet('P_L.parquet')
        self.p_l['data'] = pd.to_datetime(self.p_l['data'])
        self.p_l['ano'] = self.p_l['data'].dt.year
        self.p_l = self.p_l[(self.p_l['ano'] >= ano_inicio) & (self.p_l['ano'] <= ano_final)]
        self.p_l = self.p_l[self.p_l['ticker'].isin(self.empresas)]
        self.p_l = self.p_l[['ticker', 'data', 'ano', 'valor']]

        # Carregar e processar o arquivo P_VP
        self.p_vp = pd.read_parquet('P_VP.parquet')
        self.p_vp['data'] = pd.to_datetime(self.p_vp['data'])
        self.p_vp['ano'] = self.p_vp['data'].dt.year
        self.p_vp = self.p_vp[(self.p_vp['ano'] >= ano_inicio) & (self.p_vp['ano'] <= ano_final)]
        self.p_vp = self.p_vp[self.p_vp['ticker'].isin(self.empresas)]
        self.p_vp = self.p_vp[['ticker', 'data', 'ano', 'valor']]

        # Carregar e processar o arquivo DividendYield
        self.dy = pd.read_parquet('DividendYield.parquet')
        self.dy['data'] = pd.to_datetime(self.dy['data'])
        self.dy['ano'] = self.dy['data'].dt.year
        self.dy = self.dy[(self.dy['ano'] >= ano_inicio) & (self.dy['ano'] <= ano_final)]
        self.dy = self.dy[self.dy['ticker'].isin(self.empresas)]
        self.dy = self.dy[['ticker', 'data', 'ano', 'valor']]

        # Carregar e processar o arquivo ROIC
        self.roic = pd.read_parquet('ROIC.parquet')
        self.roic['data'] = pd.to_datetime(self.roic['data'])
        self.roic['ano'] = self.roic['data'].dt.year
        self.roic = self.roic[(self.roic['ano'] >= ano_inicio) & (self.roic['ano'] <= ano_final)]
        self.roic = self.roic[self.roic['ticker'].isin(self.empresas)]
        self.roic = self.roic[['ticker', 'data', 'ano', 'valor']]

        # Carregar e processar o arquivo VPA
        self.vpa = pd.read_parquet('VPA.parquet')
        self.vpa['data'] = pd.to_datetime(self.vpa['data'])
        self.vpa['ano'] = self.vpa['data'].dt.year
        self.vpa = self.vpa[(self.vpa['ano'] >= ano_inicio) & (self.vpa['ano'] <= ano_final)]
        self.vpa = self.vpa[self.vpa['ticker'].isin(self.empresas)]
        self.vpa = self.vpa[['ticker', 'data', 'ano', 'valor']]

        # Carregar e processar o arquivo LPA
        self.lpa = pd.read_parquet('LPA.parquet')
        self.lpa['data'] = pd.to_datetime(self.lpa['data'])
        self.lpa['ano'] = self.lpa['data'].dt.year
        self.lpa = self.lpa[(self.lpa['ano'] >= ano_inicio) & (self.lpa['ano'] <= ano_final)]
        self.lpa = self.lpa[self.lpa['ticker'].isin(self.empresas)]
        self.lpa = self.lpa

        # Carregar e processar o arquivo DividaBruta_PatrimonioLiquido
        self.divida_bruta_patrimonio_liquido = pd.read_parquet('DividaBruta_PatrimonioLiquido.parquet')
        self.divida_bruta_patrimonio_liquido['data'] = pd.to_datetime(self.divida_bruta_patrimonio_liquido['data'])
        self.divida_bruta_patrimonio_liquido['ano'] = self.divida_bruta_patrimonio_liquido['data'].dt.year
        self.divida_bruta_patrimonio_liquido = self.divida_bruta_patrimonio_liquido[(self.divida_bruta_patrimonio_liquido['ano'] >= ano_inicio) & (self.divida_bruta_patrimonio_liquido['ano'] <= ano_final)]
        self.divida_bruta_patrimonio_liquido = self.divida_bruta_patrimonio_liquido[self.divida_bruta_patrimonio_liquido['ticker'].isin(self.empresas)]
        self.divida_bruta_patrimonio_liquido = self.divida_bruta_patrimonio_liquido[['ticker', 'data', 'ano', 'valor']]

        # Carregar e processar o arquivo DividaLiquida_PatrimonioLiquido
        self.divida_liquida_patrimonio_liquido = pd.read_parquet('DividaLiquida_PatrimonioLiquido.parquet')
        self.divida_liquida_patrimonio_liquido['data'] = pd.to_datetime(self.divida_liquida_patrimonio_liquido['data'])
        self.divida_liquida_patrimonio_liquido['ano'] = self.divida_liquida_patrimonio_liquido['data'].dt.year
        self.divida_liquida_patrimonio_liquido = self.divida_liquida_patrimonio_liquido[(self.divida_liquida_patrimonio_liquido['ano'] >= ano_inicio) & (self.divida_liquida_patrimonio_liquido['ano'] <= ano_final)]
        self.divida_liquida_patrimonio_liquido = self.divida_liquida_patrimonio_liquido[self.divida_liquida_patrimonio_liquido['ticker'].isin(self.empresas)]
        self.divida_liquida_patrimonio_liquido = self.divida_liquida_patrimonio_liquido[['ticker', 'data', 'ano', 'valor']]

        # Carregar e processar o arquivo DividaLiquida_EBIT
        self.divida_liquida_ebit = pd.read_parquet('DividaLiquida_EBIT.parquet')
        self.divida_liquida_ebit['data'] = pd.to_datetime(self.divida_liquida_ebit['data'])
        self.divida_liquida_ebit['ano'] = self.divida_liquida_ebit['data'].dt.year
        self.divida_liquida_ebit = self.divida_liquida_ebit[(self.divida_liquida_ebit['ano'] >= ano_inicio) & (self.divida_liquida_ebit['ano'] <= ano_final)]
        self.divida_liquida_ebit = self.divida_liquida_ebit[self.divida_liquida_ebit['ticker'].isin(self.empresas)]
        self.divida_liquida_ebit = self.divida_liquida_ebit[['ticker', 'data', 'ano', 'valor']]

        # Carregar e processar o arquivo DividaLiquida_EBITDA
        self.divida_liquida_ebitda = pd.read_parquet('DividaLiquida_EBITDA.parquet')
        self.divida_liquida_ebitda['data'] = pd.to_datetime(self.divida_liquida_ebitda['data'])
        self.divida_liquida_ebitda['ano'] = self.divida_liquida_ebitda['data'].dt.year
        self.divida_liquida_ebitda = self.divida_liquida_ebitda[(self.divida_liquida_ebitda['ano'] >= ano_inicio) & (self.divida_liquida_ebitda['ano'] <= ano_final)]
        self.divida_liquida_ebitda = self.divida_liquida_ebitda[self.divida_liquida_ebitda['ticker'].isin(self.empresas)]
        self.divida_liquida_ebitda = self.divida_liquida_ebitda[['ticker', 'data', 'ano', 'valor']]

        # Carregar e processar o arquivo ValorDeMercado
        self.valor_de_mercado = pd.read_parquet('ValorDeMercado.parquet')
        self.valor_de_mercado['data'] = pd.to_datetime(self.valor_de_mercado['data'])
        self.valor_de_mercado['ano'] = self.valor_de_mercado['data'].dt.year
        self.valor_de_mercado = self.valor_de_mercado[(self.valor_de_mercado['ano'] >= ano_inicio) & (self.valor_de_mercado['ano'] <= ano_final)]
        self.valor_de_mercado = self.valor_de_mercado[self.valor_de_mercado['ticker'].isin(self.empresas)]
        self.valor_de_mercado = self.valor_de_mercado[['ticker', 'data', 'ano', 'valor']]

        # Carregar e processar o arquivo EV
        self.ev = pd.read_parquet('EV.parquet')
        self.ev['data'] = pd.to_datetime(self.ev['data'])
        self.ev['ano'] = self.ev['data'].dt.year
        self.ev = self.ev[(self.ev['ano'] >= ano_inicio) & (self.ev['ano'] <= ano_final)]
        self.ev = self.ev[self.ev['ticker'].isin(self.empresas)]
        self.ev = self.ev

        os.chdir(self.camihno_pasta_principal)

    def cotacoes_anuais(self, modificar_df_empresas=True):
        cotacoes = self.cotacoes.copy()
        cotacoes_anuais = pd.DataFrame()
        cotacoes_anuais['open'] = cotacoes.groupby(['ano', 'ticker'])['preco_maximo_ajustado'].first()
        cotacoes_anuais['low'] = cotacoes.groupby(['ano', 'ticker'])['preco_maximo_ajustado'].min()
        cotacoes_anuais['high'] = cotacoes.groupby(['ano', 'ticker'])['preco_maximo_ajustado'].max()
        cotacoes_anuais['close'] = cotacoes.groupby(['ano', 'ticker'])['preco_maximo_ajustado'].last()
        cotacoes_anuais = cotacoes_anuais.reset_index()
        
        self.cotacoes_anuais = cotacoes_anuais

        if modificar_df_empresas:
            df_empresas = self.df_empresas.copy()
            df_empresas['id'] = df_empresas['ticker'] + df_empresas['ano'].astype(str)

            cotacoes_anuais['id'] = cotacoes_anuais['ticker'] + cotacoes_anuais['ano'].astype(str)

            df_empresas = pd.merge(df_empresas,cotacoes_anuais, on='id', how='inner')
            df_empresas = df_empresas[['ticker_x', 'ano_x', 'open', 'low', 'high', 'close']]
            df_empresas.columns = ['ticker', 'ano', 'open', 'low', 'high', 'close']
            
            self.df_empresas = df_empresas

    def indicadores_radar_anuais(self):
        df_empresas = self.df_empresas.copy()
        df_empresas['id'] = df_empresas['ticker'] + df_empresas['ano'].astype(str)

        indicadores = [self.lpa, self.vpa, self.dividendos, self.roe, self.roic, self.ebit_dl]
        indicadores_nomes = ['lpa', 'vpa', 'dividendos', 'roe', 'roic', 'ebit_dl']

        for i,indicador in enumerate(indicadores):
            df_indicador = indicador.copy()
            df_indicador['ano'] = df_indicador['data'].dt.year
            df_indicador = df_indicador.groupby(['ano','ticker'])['valor'].last()
            df_indicador = df_indicador.reset_index()
            df_indicador['id'] = df_indicador['ticker'] + df_indicador['ano'].astype(str)            

            df_empresas = pd.merge(df_empresas, df_indicador, on='id', how='left')
            df_empresas = df_empresas.drop(columns=['ticker_y', 'ano_y'])
            df_empresas.rename(columns={
                'valor':indicadores_nomes[i],
                'ticker_x':'ticker',
                'ano_x':'ano'
                }, inplace=True)

        df_empresas = df_empresas.drop_duplicates()
        df_empresas = df_empresas.drop(columns=['id'])

        self.df_empresas = df_empresas

    def filtrar_datas(self, ano_inicio=2014, ano_fim=2024):
        self.df_empresas = self.df_empresas[self.df_empresas['ano']>=ano_inicio]
        self.df_empresas = self.df_empresas[self.df_empresas['ano']<=ano_fim]

    def precos_teto_por_indicadores(self, shift=False):
        # shift=True usa dados do ano anterior para calcular os preços
        df_precos_teto = self.df_empresas.copy()
        self.shift = shift

        if shift:

            df_precos_teto['lpa_anterior'] = df_precos_teto['lpa'].shift(1)
            df_precos_teto['vpa_anterior'] = df_precos_teto['vpa'].shift(1)
            df_precos_teto['dividendos_anterior'] = df_precos_teto['dividendos'].shift(1)

            df_precos_teto['pl_low'] = df_precos_teto['low'] / df_precos_teto['lpa_anterior']
            df_precos_teto['pl_high'] = df_precos_teto['high'] / df_precos_teto['lpa_anterior']

            df_precos_teto['pvp_low'] = df_precos_teto['low'] / df_precos_teto['vpa_anterior']
            df_precos_teto['pvp_high'] = df_precos_teto['high'] / df_precos_teto['vpa_anterior']

            df_precos_teto['dy_low'] = df_precos_teto['dividendos_anterior'] / df_precos_teto['low']
            df_precos_teto['dy_high'] = df_precos_teto['dividendos_anterior'] / df_precos_teto['high']

        else:

            df_precos_teto['pl_low'] = df_precos_teto['low']/df_precos_teto['lpa']
            df_precos_teto['pl_high'] = df_precos_teto['high']/df_precos_teto['lpa']

            df_precos_teto['pvp_low'] = df_precos_teto['low']/df_precos_teto['vpa']
            df_precos_teto['pvp_high'] = df_precos_teto['high']/df_precos_teto['vpa']

            df_precos_teto['dy_low'] = df_precos_teto['dividendos']/df_precos_teto['low']
            df_precos_teto['dy_high'] = df_precos_teto['dividendos']/df_precos_teto['high']

        df_precos_teto = df_precos_teto[['ticker', 'ano', 'pl_low', 'pl_high', 'pvp_low', 'pvp_high', 'dy_low', 'dy_high']]

        self.df_precos_teto_por_indicadores = df_precos_teto

    def filtrando_precos_teto(self):
        df_precos_teto = self.df_precos_teto_por_indicadores.copy()

        df_precos_teto_por_empresa = df_precos_teto.groupby('ticker').median()
        df_precos_teto_por_empresa = df_precos_teto_por_empresa.drop(columns=['ano'])
        
        self.df_precos_teto_por_empresa = df_precos_teto_por_empresa

    def pegando_cotacoes_atuais(self):
        empresas = self.empresas.copy()
        tickers_yf = [empresa + '.SA' for empresa in empresas]
        data_ano_passado = datetime.now() - timedelta(days=365)

        cotacoes_atuais = yf.download(tickers_yf, data_ano_passado, datetime.now())['Close']

        cotacoes_52_weeks = cotacoes_atuais.melt()
        cotacoes_52_weeks.columns = ['ticker', 'close']
        cotacoes_52_weeks = pd.merge((cotacoes_52_weeks.groupby('ticker').min()), (cotacoes_52_weeks.groupby('ticker').max()),
                                     left_index=True, right_index=True, how='inner')
        cotacoes_52_weeks.columns = ['low', 'high']
        cotacoes_52_weeks.index = cotacoes_52_weeks.index.str.replace('.SA', '')

        cotacoes_atuais = cotacoes_atuais.iloc[-1]
        cotacoes_atuais.index = cotacoes_atuais.index.str.replace('.SA', '')
        
        for dado in self.empresas_splitadas_ano:
            cotacoes_atuais.loc[dado[0]] = cotacoes_atuais.loc[dado[0]] * dado[1]
            cotacoes_52_weeks.loc[dado[0]] = cotacoes_52_weeks.loc[dado[0]] * dado[1]

        self.cotacoes_atuais = cotacoes_atuais
        self.cotacoes_52_weeks = cotacoes_52_weeks

    def criando_df_pontuacao(self):
        df_pontuacao = self.cotacoes_atuais.copy()
        df_pontuacao = df_pontuacao.to_frame()
        df_pontuacao.columns = ['preco']

        df_indicadores = self.df_empresas
        df_indicadores = df_indicadores.groupby('ticker').last()
        df_indicadores = df_indicadores[['ano', 'lpa', 'vpa', 'dividendos', 'roe', 'roic', 'ebit_dl']]

        df_pontuacao = pd.merge(df_pontuacao, df_indicadores,left_index=True, right_index=True, how='left')

        self.df_pontuacao = df_pontuacao
    
    def pontuando_cotacoes(self, n_quartis=4):
        preco_teto = self.df_precos_teto_por_empresa.copy()
        df_pontuacao = self.df_pontuacao.copy()

        df_pontuacao['pl']=df_pontuacao['preco']/df_pontuacao['lpa']
        df_pontuacao['pvp']=df_pontuacao['preco']/df_pontuacao['vpa']
        df_pontuacao['dy']=df_pontuacao['dividendos']/df_pontuacao['preco']

        df_pontuacao = pd.merge(df_pontuacao, preco_teto,left_index=True, right_index=True, how='left')

        df_pontuacao['score_pl'] = pd.NA
        df_pontuacao.loc[df_pontuacao['pl'] <=0, 'score_pl'] = 999.99
        df_pontuacao.loc[df_pontuacao['score_pl'].isna(), 'score_pl'] = (
            (df_pontuacao.loc[df_pontuacao['score_pl'].isna()]['pl']-df_pontuacao.loc[df_pontuacao['score_pl'].isna()]['pl_low'])/
            (df_pontuacao.loc[df_pontuacao['score_pl'].isna()]['pl_high']-df_pontuacao.loc[df_pontuacao['score_pl'].isna()]['pl_low']))
        df_pontuacao.loc[df_pontuacao['pl_high'] <1, 'score_pl'] = df_pontuacao['score_pl'].median() * 1.5
        df_pontuacao = self.separar_por_quartis(df=df_pontuacao, nome_coluna_dividir='score_pl', nome_coluna_resultado='pl_quartis', n_quartis=n_quartis)

        df_pontuacao['score_pvp'] = pd.NA
        df_pontuacao.loc[df_pontuacao['pvp'] <=0, 'score_pvp'] = 999.99
        df_pontuacao.loc[df_pontuacao['score_pvp'].isna(), 'score_pvp'] = (
            (df_pontuacao.loc[df_pontuacao['score_pvp'].isna()]['pvp']-df_pontuacao.loc[df_pontuacao['score_pvp'].isna()]['pvp_low'])/
            (df_pontuacao.loc[df_pontuacao['score_pvp'].isna()]['pvp_high']-df_pontuacao.loc[df_pontuacao['score_pvp'].isna()]['pvp_low']))
        df_pontuacao.loc[df_pontuacao['pvp_high'] <0.5, 'score_pvp'] = df_pontuacao['score_pvp'].median() * 1.5
        df_pontuacao = self.separar_por_quartis(df=df_pontuacao, nome_coluna_dividir='score_pvp', nome_coluna_resultado='pvp_quartis', n_quartis=n_quartis)

        df_pontuacao['score_dy'] = pd.NA
        df_pontuacao.loc[df_pontuacao['dy'] <=0, 'score_dy'] = 999.99
        df_pontuacao.loc[df_pontuacao['score_dy'].isna(), 'score_dy'] = (
            (abs(df_pontuacao.loc[df_pontuacao['score_dy'].isna()]['dy']-df_pontuacao.loc[df_pontuacao['score_dy'].isna()]['dy_high']))/
            (abs(df_pontuacao.loc[df_pontuacao['score_dy'].isna()]['dy_low']-df_pontuacao.loc[df_pontuacao['score_dy'].isna()]['dy_high'])))
        maior_valor_dy = df_pontuacao['score_dy'].max()
        df_pontuacao['score_dy'] = maior_valor_dy - df_pontuacao['score_dy']
        df_pontuacao = self.separar_por_quartis(df=df_pontuacao, nome_coluna_dividir='score_dy', nome_coluna_resultado='dy_quartis', n_quartis=n_quartis)

        df_pontuacao = df_pontuacao[['preco','score_pl', 'pl_quartis', 
                                     'score_pvp', 'pvp_quartis',
                                     'score_dy', 'dy_quartis']]

        self.df_pontuacao = pd.merge(self.df_pontuacao,df_pontuacao, left_index=True, right_index=True, how='left')
        self.df_pontuacao = self.df_pontuacao.drop(columns=['preco_y'])
        self.df_pontuacao.rename(columns={'preco_x':'preco'}, inplace=True)

    def pontuando_outros_indicadores(self, n_quartis=4):
        df_pontuacao = self.df_pontuacao.copy()
        indicadores = ['roe', 'roic', 'ebit_dl']
        df_pontuacao = df_pontuacao[indicadores]

        min_roic = min([df_pontuacao['roic'].mean(),df_pontuacao['roic'].median()])
        df_pontuacao.loc[df_pontuacao['roic'].isna(), 'roic'] = min_roic
        min_ebit = min([df_pontuacao['ebit_dl'].mean(),df_pontuacao['ebit_dl'].median()])
        df_pontuacao.loc[df_pontuacao['ebit_dl'].isna(), 'ebit_dl'] = min_ebit

        for indicador in indicadores:
            df_pontuacao = self.separar_por_quartis(df=df_pontuacao, nome_coluna_dividir=indicador, 
                                        nome_coluna_resultado=f"{indicador}_quartis", n_quartis=n_quartis, ascending=False)

        self.df_pontuacao = pd.merge(self.df_pontuacao,df_pontuacao, left_index=True, right_index=True, how='left')
        self.df_pontuacao = self.df_pontuacao.drop(columns=['roe_y', 'roic_y', 'ebit_dl_y'])
        self.df_pontuacao.rename(columns={
                                        'roe_x':'roe',
                                        'roic_x':'roic',
                                        'ebit_dl_x':'ebit_dl',
                                        }, inplace=True)
        
    def score_caixa(self):
        caixa = self.caixa.copy()

        score_caixa = caixa.groupby('ticker')['valor'].last()
        score_caixa = score_caixa.to_frame()

        caixa = caixa[caixa['ano']<2024]
        score_caixa['anos_1'] = caixa.groupby('ticker')['valor'].last()

        caixa = caixa[caixa['ano']<2023]
        score_caixa['anos_2'] = caixa.groupby('ticker')['valor'].last()

        caixa = caixa[caixa['ano']<2022]
        score_caixa['anos_3'] = caixa.groupby('ticker')['valor'].last()

        caixa = caixa[caixa['ano']<2021]
        score_caixa['anos_4'] = caixa.groupby('ticker')['valor'].last()

        score_caixa['media'] = score_caixa[['anos_1','anos_2','anos_3','anos_4']].median(axis=1)
        score_caixa['score'] = (score_caixa['valor'] / score_caixa['media'] + score_caixa['valor'] / score_caixa['anos_1'] + 
                                score_caixa['valor'] / score_caixa['anos_4'])/3
        score_caixa = self.separar_por_quartis(df=score_caixa, nome_coluna_dividir='score', nome_coluna_resultado='quartis', 
                                               n_quartis=10, ascending=False)
        
        score_caixa = score_caixa[['score', 'quartis']]
        score_caixa.columns = ['score_caixa', 'quartil_caixa']
        self.score = score_caixa

    def score_divida_longa(self):
        dados = self.passivo_nao_circulante.copy()

        score = dados.groupby('ticker')['valor'].last()
        score = score.to_frame()

        anos = range(self.ano_atual, self.ano_atual-4, -1)
        anos_str = [str(ano) for ano in anos]
        for ano in anos:
            dados = dados[dados['ano']<ano]
            score[f'{ano}'] = dados.groupby('ticker')['valor'].last()

        score['media'] = score[anos_str].apply(lambda row: row.median(), axis=1)
        score['score'] = (score['valor'] / score['media'] + score['valor'] / score[anos_str[1]] + 
                                score['valor'] / score[anos_str[-1]])/3
        score = self.separar_por_quartis(df=score, nome_coluna_dividir='score', nome_coluna_resultado='quartis', 
                                               n_quartis=10, ascending=True)

        score = score[['score', 'quartis']]
        score.columns = ['scores_divida_lp', 'quartil_divida_lp']
        
        score = pd.merge(self.score, score, left_index=True, right_index=True, how='left')
        score.fillna(score.mean(), inplace=True)

        self.score = score

    def score_num_acoes(self, nome_atributo = 'num_acoes'):
        dados = self.patrimonio_liquido.copy()
        dados = pd.merge(dados, self.vpa, on='ticker', how='left')
        dados['valor'] = dados['valor_x']/dados['valor_y']
        dados = dados[['ticker', 'ano_x', 'valor']]
        dados.rename(columns={'ano_x': 'ano'}, inplace=True)
        
        score = dados.groupby('ticker')['valor'].last()
        score = score.to_frame()

        for ano in self.cinco_anos:
            dados = dados[dados['ano']<ano]
            score[f'{ano}'] = dados.groupby('ticker')['valor'].last()

        score['media'] = score[self.cinco_anos_str].apply(lambda row: row.median(), axis=1)
        score['score'] = (score['valor'] / score['media'] + score['valor'] / score[self.cinco_anos_str[1]] + 
                                score['valor'] / score[self.cinco_anos_str[-1]])/3
        score = self.separar_por_quartis(df=score, nome_coluna_dividir='score', nome_coluna_resultado='quartis', 
                                               n_quartis=10, ascending=True)
        
        score = score[['score', 'quartis']]
        score.columns = [f'scores_{nome_atributo}', f'quartil_{nome_atributo}']
        
        score = pd.merge(self.score, score, left_index=True, right_index=True, how='left')
        score.fillna(score.mean(), inplace=True)

        self.score = score

    def score_db_pl(self, nome_atributo = 'db_pl'):
        dados = self.divida_bruta_patrimonio_liquido.copy()

        score = dados.groupby('ticker')['valor'].last()
        score = score.to_frame()

        anos = range(self.ano_atual, self.ano_atual-4, -1)
        anos_str = [str(ano) for ano in anos]
        for ano in anos:
            dados = dados[dados['ano']<ano]
            score[f'{ano}'] = dados.groupby('ticker')['valor'].last()

        score['media'] = score[anos_str].apply(lambda row: row.median(), axis=1)
        score['score'] = (score['valor'] / score['media'] + score['valor'] / score[anos_str[1]] + 
                                score['valor'] / score[anos_str[-1]])/3
        score = self.separar_por_quartis(df=score, nome_coluna_dividir='score', nome_coluna_resultado='quartis', 
                                               n_quartis=10, ascending=False)

        score = score[['score', 'quartis']]
        score.columns = [f'scores_{nome_atributo}', f'quartil_{nome_atributo}']
        
        score = pd.merge(self.score, score, left_index=True, right_index=True, how='left')
        score.fillna(score.median(), inplace=True)

        self.score = score

    def score_lucro_liquido(self, nome_atributo = 'lucro_liq'):
        dados = self.lucro_liquido.copy()

        score = dados.groupby('ticker')['valor'].last()
        score = score.to_frame()

        anos = range(self.ano_atual, self.ano_atual-4, -1)
        anos_str = [str(ano) for ano in anos]
        for ano in anos:
            dados = dados[dados['ano']<ano]
            score[f'{ano}'] = dados.groupby('ticker')['valor'].last()

        score['media'] = score[anos_str].apply(lambda row: row.median(), axis=1)
        score['score'] = (score['valor'] / score['media'] + score['valor'] / score[anos_str[1]] + 
                                score['valor'] / score[anos_str[-1]])/3
        score = self.separar_por_quartis(df=score, nome_coluna_dividir='score', nome_coluna_resultado='quartis', 
                                               n_quartis=10, ascending=False)

        score = score[['score', 'quartis']]
        score.columns = [f'scores_{nome_atributo}', f'quartil_{nome_atributo}']
        
        score = pd.merge(self.score, score, left_index=True, right_index=True, how='left')
        score.fillna(score.median(), inplace=True)

        self.score = score

    def score_custos(self, nome_atributo = 'custos'):
        dados = self.custos.copy()

        score = dados.groupby('ticker')['valor'].last()
        score = score.to_frame()

        anos = range(self.ano_atual, self.ano_atual-4, -1)
        anos_str = [str(ano) for ano in anos]
        for ano in anos:
            dados = dados[dados['ano']<ano]
            score[f'{ano}'] = dados.groupby('ticker')['valor'].last()

        score['media'] = score[anos_str].apply(lambda row: row.median(), axis=1)
        score['score'] = (score['valor'] / score['media'] + score['valor'] / score[anos_str[1]] + 
                                score['valor'] / score[anos_str[-1]])/3
        score = self.separar_por_quartis(df=score, nome_coluna_dividir='score', nome_coluna_resultado='quartis', 
                                               n_quartis=10, ascending=True)

        score = score[['score', 'quartis']]
        score.columns = [f'scores_{nome_atributo}', f'quartil_{nome_atributo}']
        
        score = pd.merge(self.score, score, left_index=True, right_index=True, how='left')
        score.fillna(score.median(), inplace=True)

        self.score = score

    def score_dividendos(self, nome_atributo = 'dividendos'):
        dados = self.dy.copy()
        dados['id'] = dados['ticker'] + dados['data'].astype(str)
        cotacoes = self.cotacoes.copy()
        cotacoes = cotacoes[['data', 'ticker', 'preco_fechamento_ajustado']]
        cotacoes['ano'] = cotacoes['data'].dt.year
        cotacoes['id'] = cotacoes['ticker'] + cotacoes['data'].astype(str)

        dados = pd.merge(dados, cotacoes, on='id', how='left')
        dados = dados.dropna()
        dados = dados[['ticker_x', 'data_x', 'ano_x', 'valor', 'preco_fechamento_ajustado']]
        dados.columns = ['ticker', 'data', 'ano', 'valor', 'preco']

        dados['dividendo'] = dados['preco'] * dados['valor']
        dados = dados[['ticker', 'data', 'ano', 'dividendo']]
        dados.columns = ['ticker', 'data', 'ano', 'valor']

        score = dados.groupby('ticker')['valor'].last()
        score = score.to_frame()

        anos = range(self.ano_atual, self.ano_atual-4, -1)
        anos_str = [str(ano) for ano in anos]
        for ano in anos:
            dados = dados[dados['ano']<ano]
            score[f'{ano}'] = dados.groupby('ticker')['valor'].last()

        score['media'] = score[anos_str].apply(lambda row: row.median(), axis=1)
        score['score'] = (score['valor'] / score['media'] + score['valor'] / score[anos_str[1]] + 
                                score['valor'] / score[anos_str[-1]])/3
        score = self.separar_por_quartis(df=score, nome_coluna_dividir='score', nome_coluna_resultado='quartis', 
                                               n_quartis=10, ascending=False)

        score = score[['score', 'quartis']]
        score.columns = [f'scores_{nome_atributo}', f'quartil_{nome_atributo}']
        
        score = pd.merge(self.score, score, left_index=True, right_index=True, how='left')
        score.fillna(score.median(), inplace=True)

        self.score = score

    def score_divida_liq_pl(self, nome_atributo = 'dl_pl', ascending=True):
        dados = self.divida_liquida_patrimonio_liquido.copy()
        
        score = dados.groupby('ticker')['valor'].last()
        score = score.to_frame()

        anos = range(self.ano_atual, self.ano_atual-4, -1)
        anos_str = [str(ano) for ano in anos]
        for ano in anos:
            dados = dados[dados['ano']<ano]
            score[f'{ano}'] = dados.groupby('ticker')['valor'].last()

        score['media'] = score[anos_str].apply(lambda row: row.median(), axis=1)
        score['score'] = (score['valor'] / score['media'] + score['valor'] / score[anos_str[1]] + 
                                score['valor'] / score[anos_str[-1]])/3
        score = self.separar_por_quartis(df=score, nome_coluna_dividir='score', nome_coluna_resultado='quartis', 
                                               n_quartis=10, ascending=ascending)

        score = score[['score', 'quartis']]
        score.columns = [f'scores_{nome_atributo}', f'quartil_{nome_atributo}']
        
        score = pd.merge(self.score, score, left_index=True, right_index=True, how='left')
        score.fillna(score.median(), inplace=True)

        self.score = score

    def score_divida_liq_ebitda(self, nome_atributo = 'dl_ebitda', ascending=True):
        dados = self.divida_liquida_ebitda.copy()
        
        score = dados.groupby('ticker')['valor'].last()
        score = score.to_frame()

        anos = range(self.ano_atual, self.ano_atual-4, -1)
        anos_str = [str(ano) for ano in anos]
        for ano in anos:
            dados = dados[dados['ano']<ano]
            score[f'{ano}'] = dados.groupby('ticker')['valor'].last()

        score['media'] = score[anos_str].apply(lambda row: row.median(), axis=1)
        score['score'] = (score['valor'] / score['media'] + score['valor'] / score[anos_str[1]] + 
                                score['valor'] / score[anos_str[-1]])/3
        score = self.separar_por_quartis(df=score, nome_coluna_dividir='score', nome_coluna_resultado='quartis', 
                                               n_quartis=10, ascending=ascending)

        score = score[['score', 'quartis']]
        score.columns = [f'scores_{nome_atributo}', f'quartil_{nome_atributo}']
        
        score = pd.merge(self.score, score, left_index=True, right_index=True, how='left')
        score.fillna(score.median(), inplace=True)

        self.score = score

    def somar_quartis_tek(self):
        dados = self.score.copy()

        dados['soma_quartis'] = 0
        contador = 0
        for coluna in dados.columns:
            if coluna [:7] == 'quartil':
                contador += 1
                dados[coluna] = np.where(dados[coluna] == 0, 5, dados[coluna])
                dados['soma_quartis'] += dados[coluna]

        pior_nota_possivel = contador * 10
        melhor_nota_possivel = contador * 1
        range_notas = pior_nota_possivel - melhor_nota_possivel

        dados['score'] = dados['soma_quartis']/range_notas
        dados = dados.sort_values('score')

        self.score = dados
        self.resultado_final = dados[['score']]

    def rodar_score_tek_completo(self, salvar_df=True):
        self.carregar_cotacoes()
        print('cotações carregadas')
        self.carregar_arquivos_score_tek()
        print('indicadores carregados')
        self.score_caixa()
        print('análise caixa concluída')
        self.score_divida_longa()
        print('análise divida_longa concluída')
        self.score_num_acoes()
        print('análise número de ações concluída')
        self.score_db_pl()
        print('análise dívida bruta/patrimonio liquido concluída')
        self.score_lucro_liquido()
        print('análise lucro liquido concluída')
        self.score_custos()
        print('análise custos concluída')
        self.score_dividendos()
        print('análise dividendos concluída')
        self.score_divida_liq_pl()
        print('análise divida liquida/patrimonio liquido concluída')
        self.score_divida_liq_ebitda()
        print('análise divida liquida/ebtida concluída')
        self.somar_quartis_tek()
        print('análise concluída')

        if salvar_df:
            self.salvar_df(df=self.resultado_final, nome='score_tek')
            print('arquivo salvo na pasta')

    def filtrando_melhores_empresas(self):
        df_empresas_filtrado = self.df_pontuacao.copy()

        df_empresas_filtrado['pontuação'] = (
            (df_empresas_filtrado['pl_quartis'] + df_empresas_filtrado['pvp_quartis'] + df_empresas_filtrado['dy_quartis']) * 2 +
            (df_empresas_filtrado['roe_quartis'] + df_empresas_filtrado['roic_quartis'] + df_empresas_filtrado['ebit_dl_quartis']))
        
        range_maximo_pontuacao = 90 - 12 #pontuação máxima possível menos a mínima

        df_empresas_filtrado['score'] = df_empresas_filtrado['pontuação']/range_maximo_pontuacao
        df_empresas_filtrado.sort_values('score', inplace=True)

        df_empresas_filtrado = df_empresas_filtrado[['preco','lpa','vpa','dividendos','roe','roic','ebit_dl','score']]
        self.df_empresas_filtrado = df_empresas_filtrado
        
    def usando_multiplicadores(self):
        df = self.df_empresas_filtrado.copy()
        df = self.adicionar_setor(df=df)
        df = self.ponderando_por_setor(df=df)
        df = self.adicionar_e_estatal(df=df)
        df = self.ponderando_estatais(df=df)
        
        df = df[['preco','setor', 'estatal', 'lpa', 'vpa', 'dividendos', 'roe', 'roic', 'ebit_dl', 'score']]
        df = df.sort_values('score')

        self.df_empresas_filtrado = df
        
    def valor_teto_indicadores(self, indice_seguranca=0.25):
        df = self.df_precos_teto_por_indicadores.copy()
        
        df = df.groupby('ticker').median()
        df['pl_range'] = df['pl_high'] - df['pl_low']
        df['teto_pl'] = (df['pl_range'] * indice_seguranca) + df['pl_low']

        df['pvp_range'] = df['pvp_high'] - df['pvp_low']
        df['teto_pvp'] = (df['pvp_range'] * indice_seguranca) + df['pvp_low']

        df['dy_range'] = df['dy_low'] - df['dy_high']
        df['teto_dy'] = (df['dy_range'] * indice_seguranca) + df['dy_high']

        df = df[['teto_pl', 'teto_pvp', 'teto_dy']]

        self.valor_teto_indicadores = df

    def calculando_preco_teto(self):
        df = self.valor_teto_indicadores
        df = pd.merge(self.df_empresas_filtrado, df, left_index=True, right_index=True, how='left')

        cotacoes = self.cotacoes_52_weeks
        df = pd.merge(df,cotacoes, left_index=True, right_index=True, how='left')

        df['preco_lpa'] = df['teto_pl'] * df['lpa']
        df['preco_lpa'] = np.where(df['preco_lpa'] < 0, 0, df['preco_lpa'])
        df['preco_vpa'] = df['teto_pvp'] * df['vpa']
        df['preco_vpa'] = np.where(df['preco_vpa'] < 0, 0, df['preco_vpa'])
        df['preco_dy'] = df['dividendos']/df['teto_dy']
        df['preco_dy'] = np.where(df['preco_dy'] < 0, 0, df['preco_dy'])

        df['preco_teto'] = np.minimum.reduce([df['preco_lpa'], df['preco_vpa'], df['preco_dy']])
        df['preco_medio'] = df[['preco_lpa', 'preco_vpa', 'preco_dy']].mean(axis=1)

        df['preco_teto'] = np.where(df['preco_teto'] < df['low'], df['preco_medio'], df['preco_teto'])
        
        df = self.ponderando_estatais(df, coluna_multiplicar='preco_teto', invertido=True)
        df = self.ponderando_por_setor(df, coluna_multiplicar='preco_teto', invertido=True)

        df['cotacao_media_52w'] = (df['low']+df['high'])/2
        df['preco_teto'] = np.where(df['preco_teto'] > df['high'], df['cotacao_media_52w'], df['preco_teto'])
        df['preco_teto'] = np.where(df['preco_teto'] < df['low'], df['low']*0.8, df['preco_teto'])

        df=df[['setor', 'estatal', 'score', 'dividendos', 'roe', 'roic', 'low', 'high', 'preco', 'preco_teto']]
        df.rename(columns={'low':'low_52w', 'high':'high_52w'}, inplace=True)

        colunas_preco = ['low_52w', 'high_52w', 'preco', 'preco_teto']
        for dado in self.empresas_splitadas_ano:
            for coluna in colunas_preco:
                df.loc[dado[0], coluna] /= dado[1]

        df = df.sort_values('score')

        self.preco_teto = df

    def salvar_df_pasta(self, caminho=r'C:\\Users\\eduar\\dev\\github', nome='preco_teto'):
        df = self.preco_teto

        df.to_csv(f'{caminho}\\{nome}.csv')

    def rodar_radar_completo(self, n_quartis=10, salvar_df=True):
        self.carregar_lpa()
        self.carregar_vpa()
        self.carregar_dy()
        self.calcular_dividendos()
        self.carregar_roe()
        self.carregar_roic()
        self.carregar_ebit_dl()
        self.cotacoes_anuais()
        self.indicadores_radar_anuais()
        self.filtrar_datas()
        self.precos_teto_por_indicadores()
        self.filtrando_precos_teto()
        self.pegando_cotacoes_atuais()
        self.criando_df_pontuacao()
        self.pontuando_cotacoes(n_quartis=n_quartis)
        self.pontuando_outros_indicadores(n_quartis=n_quartis)
        self.filtrando_melhores_empresas()
        self.usando_multiplicadores()
        self.valor_teto_indicadores()
        self.calculando_preco_teto()
        if salvar_df:
            self.salvar_df_pasta()

    @staticmethod
    def salvar_df(df, nome, caminho=r'C:\\Users\\eduar\\dev\\github'):
        df.to_csv(f'{caminho}\\{nome}')
    
    @staticmethod
    def adicionar_setor(df):
        setores = {
        'energia': ['CMIG4', 'CMIG3', 'TAEE11', 'TAEE3', 'TAEE4', 'CPLE6', 'CPLE3'],
        'saneamento': ['CSMG3', 'SAPR11', 'SAPR3', 'SAPR4', 'SBSP3'],
        'banco': ['BRSR3', 'BRSR5', 'BRSR6', 'ITSA3', 'ITSA4', 'SANB11', 'SANB3', 'SANB4', 'BBAS3', 'BBDC4', 'BBDC3', 'ITUB3', 'ITUB4'],
        'seguro': ['BBSE3', 'PSSA3', 'WIZC3'],
        'combustivel': ['UGPA3', 'ENAT3'],
        'telecom': ['VIVT3', 'TIMS3']
        }

        setores_list = []
        for ticker in df.index:
            for k, v in setores.items():
                if ticker in v:
                    setores_list.append(k)

        df['setor'] = setores_list
        return df
            
    @staticmethod
    def ponderando_por_setor(df, coluna_multiplicar='score', invertido=False):
        setores = {
        'energia': 0.8,
        'saneamento': 0.5,
        'banco': 0.8,
        'seguro': 1,
        'combustivel': 1,
        'telecom': 1
        }

        novo_score = []
        for ticker in df.index:
            for k, v in setores.items():
                if invertido:
                    v = 2 - v
                if df.at[ticker,'setor'] == k:
                    novo_score.append(df.at[ticker,coluna_multiplicar]*v)

        df[coluna_multiplicar] = novo_score
        return df
    
    @staticmethod
    def adicionar_e_estatal(df):
        estatais = pd.read_csv('estatais.csv')
        estatais = estatais.set_index('ticker')

        df = pd.merge(df, estatais,left_index=True, right_index=True, how='left')
        
        return df
    
    @staticmethod
    def ponderando_estatais(df, multiplicador=1.25, coluna_multiplicar='score', invertido=False):
        if invertido:
            multiplicador = 2-multiplicador
        df.loc[df['estatal'], coluna_multiplicar] *= multiplicador
        return df

    @staticmethod
    def separar_por_quartis(df, nome_coluna_dividir, nome_coluna_resultado, n_quartis=4, ascending=True):
        df.fillna(df.median(), inplace=True)

        tamanho_tranches = 100/n_quartis
        quartis = []
        dividir_por = 0
        for _ in range(n_quartis):
            dividir_por += tamanho_tranches
            quartis.append(np.percentile(df[nome_coluna_dividir], dividir_por))

        if not ascending:
            quartis = quartis[::-1]  # Invertendo a ordem dos quartis

        labels = range(1, n_quartis+1, 1)

        conditions = []
        for _, q in enumerate(quartis):
            if ascending:
                condition = df[nome_coluna_dividir] <= q
            else:
                condition = df[nome_coluna_dividir] >= q
            conditions.append(condition)

        df[nome_coluna_resultado] = np.select(conditions, labels)

        return df
            



if __name__ == '__main__':

    print()
    print('.')

    # radar = score_fundamentalista(filtro_empresas=score_fundamentalista().radar)
    # radar.rodar_radar_completo()
    # print(radar.preco_teto)

    score_tek = score_fundamentalista()
    score_tek.rodar_score_tek_completo()
    print(score_tek.resultado_final)

    # teste = score_fundamentalista(filtro_empresas=score_fundamentalista().radar)
    # teste.carregar_cotacoes()
    # teste.carregar_arquivos_score_tek()
    # teste.score_caixa()
    # teste.score_divida_longa()
    # teste.score_num_acoes()
    # teste.score_db_pl()
    # teste.score_lucro_liquido()
    # teste.score_custos()
    # teste.score_dividendos()
    # teste.score_divida_liq_pl()
    # teste.score_divida_liq_ebitda()
    # teste.somar_quartis_tek()

    # print(teste.ebit)

    # teste = score_fundamentalista(filtro_empresas=score_fundamentalista().radar)

    # teste.carregar_lpa()
    # teste.carregar_vpa()
    # teste.carregar_dy()
    # teste.calcular_dividendos()
    # teste.carregar_roe()
    # teste.carregar_roic()
    # teste.carregar_ebit_dl()
    # teste.cotacoes_anuais()
    # teste.indicadores_radar_anuais()
    # teste.filtrar_datas()
    # teste.precos_teto_por_indicadores()
    # teste.filtrando_precos_teto()
    # teste.pegando_cotacoes_atuais()
    # teste.criando_df_pontuacao()
    # teste.pontuando_cotacoes(n_quartis=10)
    # teste.pontuando_outros_indicadores(n_quartis=10)
    # teste.filtrando_melhores_empresas()
    # teste.usando_multiplicadores()
    # teste.valor_teto_indicadores()
    # teste.calculando_preco_teto()
    # teste.salvar_df_pasta()

    # print(teste.df_empresas)

    # df = teste.df_empresas
    # df = df[df['ticker'].isin(['TAEE11', 'TAEE3', 'TAEE4'])]

    # print(df.sort_values('ano'))