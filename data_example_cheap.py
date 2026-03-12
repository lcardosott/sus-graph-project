import os
import itertools
import pandas as pd
from ftplib import FTP
from pyreaddbc import dbc2dbf
from dbfread import DBF

def main():
    servidor = 'ftp.datasus.gov.br'
    caminho_pasta = 'dissemin/publicos/SIASUS/200801_/Dados'
    nome_arquivo_dbc = 'PASP2502a.dbc'
    nome_arquivo_dbf = 'PASP2502a.dbf'
    nome_arquivo_saida = 'amostra_PASP2502a.csv'
    
    # 1. Download do arquivo do FTP
    if not os.path.exists(nome_arquivo_dbc):
        print(f"Baixando {nome_arquivo_dbc} do FTP...")
        try:
            ftp = FTP(servidor)
            ftp.login()
            ftp.cwd(caminho_pasta)
            with open(nome_arquivo_dbc, 'wb') as f:
                ftp.retrbinary(f'RETR {nome_arquivo_dbc}', f.write)
            ftp.quit()
            print("Download concluído!\n")
        except Exception as e:
            print(f"Erro no FTP: {e}")
            return
    else:
        print(f"[{nome_arquivo_dbc}] já existe. Pulando download.\n")

    # 2. Conversão (DBC para DBF)
    print("Convertendo formato proprietário do DATASUS (DBC) para DBF...")
    try:
        dbc2dbf(nome_arquivo_dbc, nome_arquivo_dbf)
    except Exception as e:
        print(f"Erro na conversão: {e}")
        return

    # 3. Leitura Otimizada (100 linhas)
    print("Extraindo as 100 primeiras linhas para amostra...\n")
    try:
        tabela_dbf = DBF(nome_arquivo_dbf, load=False, encoding='latin1')
        amostra_100_linhas = list(itertools.islice(tabela_dbf, 100))
        df = pd.DataFrame(amostra_100_linhas)
    except Exception as e:
        print(f"Erro ao ler o DBF: {e}")
        return

    # 4. SALVAR O ARQUIVO NO MELHOR FORMATO PARA EXPLORAÇÃO VISUAL
    print("Salvando os dados no disco...")
    try:
        # utf-8-sig garante que o Excel entenda os acentos corretamente
        # sep=';' é o padrão brasileiro de planilhas
        df.to_csv(nome_arquivo_saida, index=False, sep=';', encoding='utf-8-sig')
        print(f"✅ SUCESSO! Amostra salva no arquivo: {nome_arquivo_saida}")
    except Exception as e:
        print(f"Erro ao salvar o arquivo: {e}")
        return

    # 5. Log final no terminal
    print("\n" + "="*50)
    print("📊 RAIOS-X DOS DADOS (SIASUS - AMOSTRA) 📊")
    print("="*50)
    print(f"-> Total de colunas encontradas: {df.shape[1]}")
    print(f"-> O arquivo {nome_arquivo_saida} está pronto na sua pasta para ser aberto no Excel/LibreOffice.")
    print("="*50 + "\n")

if __name__ == "__main__":
    main()