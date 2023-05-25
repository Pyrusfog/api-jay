import pandas as pd
import sys
import re
import time
from sqlalchemy.pool import NullPool
from sqlalchemy import *
# from IPython.display import display
from datetime import datetime, date, timedelta
from typing import List
import os, os.path
import smtplib
from const import destinatarios, assunto, corpo, remetente, senha, valores_colunas_att,col_names_front
from utils import att_col, rename_col,concat_col_names


# Contar Segundos
inicio = time.perf_counter()
negative_day = 60
engine2 = create_engine("mssql+pymssql://sa:Pyrusfog16%40@localhost:1433/master", poolclass=NullPool)
conn = engine2.connect()
yesterday = date.today() - timedelta(days=negative_day)
yesterday = str(yesterday)


# model = '82PFC'
# menor_data = str(yesterday) + ' 00:00:00.000'
# maior_data =  str(yesterday) + ' 23:59:59.000'
# menor_data = '2022-10-31 00:00:00.000'
# maior_data = '2022-10-31 23:59:59.000'
# maior_data = '2022-11-01 01:59:59.000'



def enviar_email():
    # Configurações do servidor SMTP
    servidor_smtp = 'smtp.gmail.com'
    porta_smtp = 587

    # Monta a mensagem de email
    mensagem = f'Subject: {assunto}\n\n{corpo}'

    query = "SELECT EMAIL FROM _USER"
    df = pd.read_sql(text(query), con=conn)
    lista_emails = df["EMAIL"].tolist()
    print(lista_emails)

    # Imprime os dados do DataFrame retornado pela consulta
    # Inicializa uma conexão SMTP
    with smtplib.SMTP(servidor_smtp, porta_smtp) as servidor:
        # Habilita a conexão segura
        servidor.starttls()

        # Realiza a autenticação no servidor SMTP
        servidor.login(remetente, senha)

        # Envia os emails para cada destinatário
        for destinatario in destinatarios:
            servidor.sendmail(remetente, destinatario, mensagem)

    print('Emails enviados com sucesso.')
    conn.invalidate()
    engine2.dispose()
    conn.rollback()





def get_history(date_history):
    query = f"SELECT * FROM CAB_HISTORY WHERE CAB_DATE = '{date_history}'"
    df = pd.read_sql(text(query), con=conn)
    # Imprime os dados do DataFrame retornado pela consulta
    conn.invalidate()
    engine2.dispose()
    conn.rollback()
    return df


def history_cab(flag_cab_collumn, bool_cab, date, folder_path="", vec_ftp=""):
    data_hora_atual = datetime.now()
    data_hora_atual = data_hora_atual.strftime('%Y-%m-%d %H:%M:%S.%f')

    print(data_hora_atual)
    table_name = 'master.dbo.CAB_HISTORY'
    print(type(date))
    data = date


    if flag_cab_collumn == 1:
        # Verifica se a linha com a data especificada já existe na tabela
        exists_query = f"SELECT CASE WHEN EXISTS (SELECT 1 FROM {table_name} WHERE CAB_DATE = '{data}') THEN 1 ELSE 0 END;"
        # exists_query = f"SELECT EXISTS(SELECT 1 FROM {table_name} WHERE data = '{data}')"
        exists = conn.execute(text(exists_query)).scalar()
        nova_coluna1 = bool_cab

        if not exists:
            # Cria um novo DataFrame com a nova linha
            nova_linha = pd.DataFrame({
                'CAB_DATE': [data],
                'CAB_DB_INSERT': [nova_coluna1],
                'CAB_FTP_SEND': [None],
                'CAB_CREATE': [None],
                'CAB_CREATE_FILES': [None],
                'CAB_CREATE_HOUR': [None]
            })
            # Adiciona a nova linha à tabela
            nova_linha.to_sql("CAB_HISTORY", con=engine2, index=False, if_exists='append')
            print("entrou")
        else:
            # Atualiza a linha existente com os novos valores
            print("entrou")
            update_query = f"UPDATE master.dbo.CAB_HISTORY SET CAB_DB_INSERT='{nova_coluna1}' WHERE CAB_DATE='{data}'"
            print(update_query)
            print(conn.execute(text(update_query)))
            print(conn.commit())

    if flag_cab_collumn == 2:
        # Verifica se a linha com a data especificada já existe na tabela
        # ftp_send_list_ok = ', '.join(vec_ftp)
        print(vec_ftp)
        exists_query = f"SELECT CASE WHEN EXISTS (SELECT 1 FROM {table_name} WHERE CAB_DATE = '{data}') THEN 1 ELSE 0 END;"
        # exists_query = f"SELECT EXISTS(SELECT 1 FROM {table_name} WHERE DATE = '{data}')"
        exists = conn.execute(text(exists_query)).scalar()
        nova_coluna2 = bool_cab
        if not exists:
            # Cria um novo DataFrame com a nova linha
            nova_linha = pd.DataFrame({
                'CAB_DATE': [data],
                'CAB_DB_INSERT': [None],
                'CAB_FTP_SEND': [nova_coluna2],
                'CAB_CREATE': [None],
                'CAB_CREATE_FILES': [None],
                'CAB_CREATE_HOUR': [None],
                'CAB_FTP_HOUR': [datetime.now()],
                'CAB_FTP_SEND_FILES': [vec_ftp]
            })
            # Adiciona a nova linha à tabela
            print(nova_linha)
            nova_linha.to_sql("CAB_HISTORY", con=engine2, index=False, if_exists='append')
            conn.commit()
            conn.invalidate()
            engine2.dispose()

        else:
            # Atualiza a linha existente com os novos valores
            print(type(data),data)
            update_query  = f"UPDATE master.dbo.CAB_HISTORY SET CAB_FTP_SEND='{nova_coluna2}', CAB_FTP_HOUR='{datetime.now()}', CAB_FTP_SEND_FILES='{vec_ftp}' WHERE CAB_DATE= '{data}'"
            print(conn.execute(text(update_query)))
            conn.commit()
            conn.invalidate()
            engine2.dispose()
            # print(conn.commit())


    if flag_cab_collumn == 3:
        # Verifica se a linha com a data especificada já existe na tabela
        print(folder_path)
        files_cab_list = os.listdir(folder_path)
        files_cab_list = ', '.join(files_cab_list)
        print(files_cab_list)
        # exists_query = f"SELECT EXISTS(SELECT 1 FROM {table_name} WHERE DATE = '{data}')"
        exists_query = f"SELECT CASE WHEN EXISTS (SELECT 1 FROM {table_name} WHERE CAB_DATE = '{data}') THEN 1 ELSE 0 END;"
        exists = conn.execute(text(exists_query)).scalar()
        print(exists)
        nova_coluna3 = bool_cab
        #NO WINDOWS ALTERAR DATA DE CAB_DATE PARA DATA DO DIA
        if not exists:
            # Cria um novo DataFrame com a nova linha
            nova_linha = pd.DataFrame({
                'CAB_DATE': [data],
                'CAB_DB_INSERT': [None],
                'CAB_FTP_SEND': [None],
                'CAB_CREATE': [nova_coluna3],
                'CAB_CREATE_FILES': [files_cab_list],
                'CAB_CREATE_HOUR': [datetime.now()]
            })
            # Adiciona a nova linha à tabela
            print("adicionou")
            nova_linha.to_sql("CAB_HISTORY", con=engine2, index=False, if_exists='append')
        else:
            # Atualiza a linha existente com os novos valores
            print("atualizou")
            update_query = f"UPDATE master.dbo.CAB_HISTORY SET CAB_CREATE='{nova_coluna3}', CAB_CREATE_FILES='{files_cab_list}', CAB_CREATE_HOUR='{datetime.now()}' WHERE CAB_DATE='{data}'"
            print(update_query)
            print(conn.execute(text(update_query)))
            print(conn.commit())


    if flag_cab_collumn == 4:
        # Verifica se a linha com a data especificada já existe na tabela
        # exists_query = f"SELECT EXISTS(SELECT 1 FROM {table_name} WHERE DATE = '{data}')"
        exists_query = f"SELECT CASE WHEN EXISTS (SELECT 1 FROM {table_name} WHERE CAB_DATE = '{data}') THEN 1 ELSE 0 END;"
        exists = conn.execute(text(exists_query)).scalar()
        print(exists)
        nova_coluna3 = bool_cab

        if not exists:
            # Cria um novo DataFrame com a nova linha
            nova_linha = pd.DataFrame({
                'CAB_DATE': [data],
                'CAB_DB_INSERT': [nova_coluna3],
                'CAB_FTP_SEND': [nova_coluna3],
                'CAB_CREATE': [nova_coluna3],
                'CAB_CREATE_FILES': [nova_coluna3],
                'CAB_CREATE_HOUR': [None]
            })
            # Adiciona a nova linha à tabela
            print("adicionou")
            nova_linha.to_sql("CAB_HISTORY", con=engine2, index=False, if_exists='append')
        else:
            # Atualiza a linha existente com os novos valores
            print("treessss" + data)
            print("atualizou")
            update_query = f"UPDATE master.dbo.CAB_HISTORY SET CAB_DB_INSERT='{nova_coluna3}',CAB_FTP_SEND='{nova_coluna3}',CAB_CREATE='{nova_coluna3}' WHERE CAB_DATE='{data}'"
            print(update_query)
            print(conn.execute(text(update_query)))
            print(conn.commit())


def format_file(Cab, date="", list_col_db=""):
    if date == "":
        yesterday = date.today() - timedelta(days=1)
        yesterday = yesterday.strftime('%Y%m%d')
        data = str(yesterday)
        data_folder = date.today() - timedelta(days=1)
        data_folder = data_folder.strftime('%Y-%m-%d')
        data_folder = str(data_folder)
        print(data)
        print(data_folder)
    # yesterday =  date.today() - timedelta(days=1)
    # data_dttype = date.strftime('%Y%m%d')
    # data_folder =  data_dttype
    else:
        data_vec = re.findall(r"(^\d{4})\/(0?[1-9]|1[012])\/(0?[1-9]|[12][0-9]|3[01])$", date)
        print(data_vec)
        data = "{}{}{}".format(data_vec[0][0], data_vec[0][1], data_vec[0][2])
        data_folder = "{}-{}-{}".format(data_vec[0][0], data_vec[0][1], data_vec[0][2])
        # data_folder = date.strftime('%Y-%m-%d')
        # data_folder = str(data_folder)
    print(data_folder)
    newpath = '/home/franciscosilva/Documents/test_files_cab/' + data_folder
    if not os.path.exists(newpath):
        os.makedirs(newpath)
    # data = datetime.today().strftime('%Y%m%d')]
    # PPID_DB = list_col_db["PPID"][0]
    # UNIQUE_DB = list_col_db["Unique_Comp_Name"][0]
    # str_db = [PPID_DB, UNIQUE_DB]

    col = concat_col_names(col_names_front,list_col_db)
    print(col)
    name_file = 'FOXCONN_JUNDIAI_CAB_' + data + "_FILE01.csv"
    name_filezip = '/home/franciscosilva/Documents/test_files_cab/' + data_folder + "/" + 'FOXCONN_JUNDIAI_CAB_' + data + "_FILE01.zip"
    name_file2 = 'FOXCONN_JUNDIAI_CAB_' + data + "_FILE02.csv"
    name_file2zip = '/home/franciscosilva/Documents/test_files_cab/' + data_folder + "/" + 'FOXCONN_JUNDIAI_CAB_' + data + "_FILE02.zip"
    name_file3 = 'FOXCONN_JUNDIAI_CAB_' + data + "_FILE03.csv"
    name_file3zip = '/home/franciscosilva/Documents/test_files_cab/' + data_folder + "/" + 'FOXCONN_JUNDIAI_CAB_' + data + "_FILE03.zip"
    name_file4 = 'FOXCONN_JUNDIAI_CAB_' + data + "_FILE04.csv"
    name_file4zip = '/home/franciscosilva/Documents/test_files_cab/' + data_folder + "/" + 'FOXCONN_JUNDIAI_CAB_' + data + "_FILE04.zip"
    name_file5 = 'FOXCONN_JUNDIAI_CAB_' + data + "_FILE05.csv"
    name_file5zip = '/home/franciscosilva/Documents/test_files_cab/' + data_folder + "/" + 'FOXCONN_JUNDIAI_CAB_' + data + "_FILE05.zip"
    name_file6 = 'FOXCONN_JUNDIAI_CAB_' + data + "_FILE06.csv"
    name_file6zip = '/home/franciscosilva/Documents/test_files_cab/' + data_folder + "/" + 'FOXCONN_JUNDIAI_CAB_' + data + "_FILE06.zip"
    name_file7 = 'FOXCONN_JUNDIAI_CAB_' + data + "_FILE07.csv"
    name_file7zip = '/home/franciscosilva/Documents/test_files_cab/' + data_folder + "/" + 'FOXCONN_JUNDIAI_CAB_' + data + "_FILE07.zip"
    name_file8 = 'FOXCONN_JUNDIAI_CAB_' + data + "_FILE08.csv"
    name_file8zip = '/home/franciscosilva/Documents/test_files_cab/' + data_folder + "/" + 'FOXCONN_JUNDIAI_CAB_' + data + "_FILE08.zip"
    name_file9 = 'FOXCONN_JUNDIAI_CAB_' + data + "_FILE09.csv"
    name_file9zip = '/home/franciscosilva/Documents/test_files_cab/' + data_folder + "/" + 'FOXCONN_JUNDIAI_CAB_' + data + "_FILE09.zip"
    name_file10 = 'FOXCONN_JUNDIAI_CAB_' + data + "_FILE010.csv"
    name_file10zip = '/home/franciscosilva/Documents/test_files_cab/' + data_folder + "/" + 'FOXCONN_JUNDIAI_CAB_' + data + "_FILE010.zip"
    name_file11 = 'FOXCONN_JUNDIAI_CAB_' + data + "_FILE11.csv"
    name_file11zip = '/home/franciscosilva/Documents/test_files_cab/' + data_folder + "/" + 'FOXCONN_JUNDIAI_CAB_' + data + "_FILE11.zip"
    name_file12 = 'FOXCONN_JUNDIAI_CAB_' + data + "_FILE12.csv"
    name_file12zip = '/home/franciscosilva/Documents/test_files_cab/' + data_folder + "/" + 'FOXCONN_JUNDIAI_CAB_' + data + "_FILE12.zip"
    try:
        Cab[(Cab["IN_STATION_TIME"] <= data + ' 01:59:59.000')].to_csv(name_filezip, columns=col, index=False,
                                                                       compression={'method': 'zip',
                                                                                    'archive_name': name_file})
        Cab[(Cab["IN_STATION_TIME"] > data + ' 01:59:59.000') & (
                Cab["IN_STATION_TIME"] <= data + ' 03:59:59.000')].to_csv(name_file2zip, columns=col, index=False,
                                                                          compression={'method': 'zip',
                                                                                       'archive_name': name_file2})
        Cab[(Cab["IN_STATION_TIME"] > data + ' 03:59:59.000') & (
                Cab["IN_STATION_TIME"] <= data + ' 05:59:59.000')].to_csv(name_file3zip, columns=col, index=False,
                                                                          compression={'method': 'zip',
                                                                                       'archive_name': name_file3})
        Cab[(Cab["IN_STATION_TIME"] > data + ' 05:59:59.000') & (
                Cab["IN_STATION_TIME"] <= data + ' 07:59:59.000')].to_csv(name_file4zip, columns=col, index=False,
                                                                          compression={'method': 'zip',
                                                                                       'archive_name': name_file4})
        Cab[(Cab["IN_STATION_TIME"] > data + ' 07:59:59.000') & (
                Cab["IN_STATION_TIME"] <= data + ' 09:59:59.000')].to_csv(name_file5zip, columns=col, index=False,
                                                                          compression={'method': 'zip',
                                                                                       'archive_name': name_file5})
        Cab[(Cab["IN_STATION_TIME"] > data + ' 09:59:59.000') & (
                Cab["IN_STATION_TIME"] <= data + ' 11:59:59.000')].to_csv(name_file6zip, columns=col, index=False,
                                                                          compression={'method': 'zip',
                                                                                       'archive_name': name_file6})
        Cab[(Cab["IN_STATION_TIME"] > data + ' 11:59:59.000') & (
                Cab["IN_STATION_TIME"] <= data + ' 13:59:59.000')].to_csv(name_file7zip, columns=col, index=False,
                                                                          compression={'method': 'zip',
                                                                                       'archive_name': name_file7})
        Cab[(Cab["IN_STATION_TIME"] > data + ' 13:59:59.000') & (
                Cab["IN_STATION_TIME"] <= data + ' 15:59:59.000')].to_csv(name_file8zip, columns=col, index=False,
                                                                          compression={'method': 'zip',
                                                                                       'archive_name': name_file8})
        Cab[(Cab["IN_STATION_TIME"] > data + ' 15:59:59.000') & (
                Cab["IN_STATION_TIME"] <= data + ' 17:59:59.000')].to_csv(name_file9zip, columns=col, index=False,
                                                                          compression={'method': 'zip',
                                                                                       'archive_name': name_file9})
        Cab[(Cab["IN_STATION_TIME"] > data + ' 17:59:59.000') & (
                Cab["IN_STATION_TIME"] <= data + ' 19:59:59.000')].to_csv(name_file10zip, columns=col, index=False,
                                                                          compression={'method': 'zip',
                                                                                       'archive_name': name_file10})
        Cab[(Cab["IN_STATION_TIME"] > data + ' 19:59:59.000') & (
                Cab["IN_STATION_TIME"] <= data + ' 21:59:59.000')].to_csv(name_file11zip, columns=col, index=False,
                                                                          compression={'method': 'zip',
                                                                                       'archive_name': name_file11})
        Cab[(Cab["IN_STATION_TIME"] > data + ' 21:59:59.000') & (
                Cab["IN_STATION_TIME"] <= data + ' 23:59:59.000')].to_csv(name_file12zip, columns=col, index=False,
                                                                          compression={'method': 'zip',
                                                                                       'archive_name': name_file12})
        folder = '/home/franciscosilva/Documents/test_files_cab/' + data_folder + "/"
        date = date + " 00:00:00.000"

        for root, _, files in os.walk(folder):
            for f in files:
                fullpath = os.path.join(root, f)
                try:
                    if os.path.getsize(fullpath) < 10 * 1024:  # set file size in kb
                        print(fullpath)
                        os.remove(fullpath)
                except WindowsError:
                    print("entrou")
                    print("Error" + fullpath)
        history_cab(3, "GERADO COM SUCESSO", date, newpath)

    except FileExistsError:
        date = date + " 00:00:00.000"
        history_cab(3, "FALHA NA GERACAO DO ARQUIVO",date ,newpath)
        print("Error ao salvar arquivo")

def run_cab(model, menor_data, maior_data):
    query_cab = "SELECT R_SN_DETAIL_T_VIEW.SERIAL_NUMBER," \
                "R_SMT_LOG_T.HH_PN,R_SMT_LOG_T.KEY_PART_NO," \
                "BOM.TXT_LOCATION," \
                "R_SN_DETAIL_T_VIEW.MODEL_NAME," \
                "R_COMPONENT_TYPE_T.MAT_NO," \
                "R_COMPONENT_TYPE_T.MAT_TYPE," \
                "R_SN_DETAIL_T_VIEW.VERSION_CODE ," \
                "R_SN_DETAIL_T_VIEW.MO_NUMBER," \
                "R_SMT_LOG_T.VENDOR,R_SMT_LOG_T.LOT_NO," \
                "R_SMT_LOG_T.DATE_CODE," \
                "R_SMT_LOG_T.REF_DESC_LIST," \
                "R_SMT_LOG_T.PKG_ID," \
                "R_SN_DETAIL_T_VIEW.IN_STATION_TIME," \
                "CASE WHEN R_SMT_LOG_T.REF_DESC_LIST = '[]' THEN BOM.TXT_LOCATION ELSE R_SMT_LOG_T.REF_DESC_LIST END AS REF_DESC_LIST_NEW " \
                "FROM R_SN_DETAIL_T_VIEW " \
                "INNER JOIN R_SMT_LOG_T ON R_SN_DETAIL_T_VIEW.LINE_NAME = R_SMT_LOG_T.LINE_NAME " \
                "   AND R_SMT_LOG_T.PRODUCT_NO = R_SN_DETAIL_T_VIEW.MODEL_NAME " \
                "   AND R_SN_DETAIL_T_VIEW.IN_STATION_TIME BETWEEN R_SMT_LOG_T.WORK_TIME AND R_SMT_LOG_T.END_TIME " \
                "INNER JOIN (SELECT BOM_ALT.TXT_LOCATION, BOM_ITEM.TXT_HH_PART_NUMBER AS HH_PN_BOM FROM R_BBM_BOM_ALT_ITEM_T BOM_ALT " \
                "   INNER JOIN C_BBM_ITEM_T BOM_ITEM ON BOM_ALT.NUM_ITEM_ID = BOM_ITEM.ID " \
                "   WHERE NUM_ITEM_ID IN (SELECT ID FROM C_BBM_ITEM_T WHERE NUM_BOM_INFO_ID = (SELECT  TOP 1 ID FROM C_BBM_BOM_INFO_T WHERE  TXT_BOARD_MODEL = '{}' ORDER BY ID DESC))" \
                "   UNION " \
                "   SELECT BOM_ALT.TXT_LOCATION, BOM_ITEM.TXT_HH_PART_NUMBER AS HH_PN_BOM FROM R_BBM_BOM_ITEM_T BOM_ALT " \
                "   INNER JOIN C_BBM_ITEM_T BOM_ITEM ON BOM_ALT.NUM_ITEM_ID = BOM_ITEM.ID " \
                "   WHERE NUM_ITEM_ID IN (SELECT ID FROM C_BBM_ITEM_T " \
                "WHERE NUM_BOM_INFO_ID = (SELECT  TOP 1 ID FROM C_BBM_BOM_INFO_T WHERE  TXT_BOARD_MODEL = '{}' ORDER BY ID DESC))) AS BOM " \
                "ON BOM.HH_PN_BOM = R_SMT_LOG_T.KEY_PART_NO  " \
                "LEFT JOIN R_COMPONENT_TYPE_T ON R_SMT_LOG_T.KEY_PART_NO = R_COMPONENT_TYPE_T.MAT_NO " \
                "WHERE R_SN_DETAIL_T_VIEW.IN_STATION_TIME BETWEEN '{}' AND '{}' AND GROUP_NAME ='REFLOW VI2'" \
                "AND R_SMT_LOG_T.PRODUCT_NO = '{}'" \
                "AND R_SN_DETAIL_T_VIEW.MODEL_NAME = '{}'".format(model, model, menor_data, maior_data, model, model)

    cab = pd.read_sql(text(query_cab), engine2.connect())
    return cab


def insert_db(cab_file, menor_data, maior_data):
    # engineyolo = create_engine("mssql+pymssql://yolo.sys:Y01o!@#123@10.8.162.80:1433/yolo", poolclass=NullPool)
    # engineyolo = create_engine("mssql+pymssql://sa:Pyrusfog16%40@localhost:1433/master", poolclass=NullPool)
    # table_orcl = pd.read_sql_query("SELECT top 100 * FROM master.dbo.R_SN_DETAIL_T_VIEW", engineyolo)
    query_pegar_model = "SELECT TOP 1 IN_STATION_TIME FROM master.dbo.CAB_LOG  " \
                        "WHERE IN_STATION_TIME BETWEEN '{}' AND '{}'".format(menor_data, maior_data)
    cab = pd.read_sql(text(query_pegar_model), engine2.connect())
    print(len(cab))
    print("LINHAAAAAAAS " + str(len(cab)))
    if len(cab) == 0:
        print("entrou banco com sucesso")
        cab_file.to_sql(con=engine2, name="CAB_LOG", index=False, if_exists="append")
        history_cab(1, "CAB INSERIDO NO BANCO", menor_data)
        return "Cab inserido com sucesso"
    else:
        print("entrou ja inseriu")
        history_cab(1, "CAB INSERIDO NO BANCO", menor_data)
        return "O cab ja foi inserido anteriormente no banco "
    # print(cab_file.to_sql(con=engine2, name="CAB_LOG", index=False, if_exists="append"))
    # engine2.dispose()



def get_cab(menor_data, maior_data, data="", flag="0"):
    try:
        cab_true_final = pd.DataFrame()

        att_col(valores_colunas_att,conn)

        query_pegar_model = "SELECT DISTINCT MODEL_NAME  FROM master.dbo.R_SN_DETAIL_T_VIEW  " \
                            "WHERE IN_STATION_TIME BETWEEN '{}' AND '{}' AND GROUP_NAME = 'REFLOW VI2'".format(
            menor_data,
            maior_data)
        # enviar_email()


        modelo_table = pd.read_sql(text(query_pegar_model), engine2.connect())
        modelo_col = modelo_table["MODEL_NAME"]
        # print(modelo_col)
        for modelo_placa in modelo_col:
            print(modelo_placa)
            cab_part = run_cab(modelo_placa, menor_data, maior_data)
            cab_true_final = pd.concat([cab_true_final, cab_part], ignore_index=True)

        # cab_true_final['KEY_PART_NO'] = cab_true_final['KEY_PART_NO'] + " #"+cab_true_final.groupby(['SERIAL_NUMBER','KEY_PART_NO']).cumcount().astype(str).replace('0','')

        # raise Exception("lalalalala")
        cab_true_final,list_col_db,cols_for_db_cab = rename_col(cab_true_final, engine2, col_names_front)
        # cab_true_final = cab_true_final.sort_values(by=['SERIAL_NUMBER'])
        # cab_true_final["Supplier_Name"] = "Foxconn"
        # cab_true_final["Commodity_Type"] = "PCBA"
        # cab_true_final["Comp_Supplier_Name"] = cab_true_final["VENDOR"]
        # cab_true_final["Comp_Supplier_ID"] = cab_true_final["VENDOR"]
        # cab_true_final["Comp_Supplier_Factory_name"] = cab_true_final["VENDOR"]
        # cab_true_final["Comp_Supplier_SN"] = ""
        # cab_true_final["CAB_ATTR_03"] = ""
        # cab_true_final["Comp_Supplier_Mfg_Dt"] = ""
        # cab_true_final["CAB_ATTR_02"] = cab_true_final["KEY_PART_NO"]
        # cab_true_final["CAB_ATTR_04"] = cab_true_final["KEY_PART_NO"]
        # cab_true_final.rename(columns={'SERIAL_NUMBER': 'PPID'}, inplace=True)
        # cab_true_final.rename(columns={'KEY_PART_NO': 'Unique_Comp_Name'}, inplace=True)
        # cab_true_final.rename(columns={'VERSION_CODE': 'Revision'}, inplace=True)
        # cab_true_final.rename(columns={'MO_NUMBER': 'WO#'}, inplace=True)
        # cab_true_final.rename(columns={'Supplier_Name': 'Supplier_Name'}, inplace=True)
        # cab_true_final.rename(columns={'Commodity_Type': 'Commodity_Type'}, inplace=True)
        # cab_true_final.rename(columns={'MAT_TYPE': 'Component_Type'}, inplace=True)
        # cab_true_final.rename(columns={'Comp_Supplier_Name': 'Comp_Supplier_Name'}, inplace=True)
        # cab_true_final.rename(columns={'Comp_Supplier_ID': 'Comp_Supplier_ID'}, inplace=True)
        # cab_true_final.rename(columns={'Comp_Supplier_SN': 'Comp_Supplier_SN'}, inplace=True)
        # cab_true_final.rename(columns={'HH_PN': 'Comp_Supplier_PN'}, inplace=True)
        # cab_true_final.rename(columns={'LOT_NO': 'Comp_Supplier_Lot_Code'}, inplace=True)
        # cab_true_final.rename(columns={'Comp_Supplier_Mfg_Dt': 'Comp_Supplier_Mfg_Dt'}, inplace=True)
        # cab_true_final.rename(columns={'Comp_Supplier_Factory_name': 'Comp_Supplier_Factory_name'}, inplace=True)
        # cab_true_final.rename(columns={'DATE_CODE': 'CAB_ATTR_01'}, inplace=True)
        # cab_true_final.rename(columns={'CAB_ATTR_02': 'CAB_ATTR_02'}, inplace=True)
        # cab_true_final.rename(columns={'CAB_ATTR_03': 'CAB_ATTR_03'}, inplace=True)
        # cab_true_final.rename(columns={'CAB_ATTR_04': 'CAB_ATTR_04'}, inplace=True)
        # cab_true_final.rename(columns={'REF_DESC_LIST_NEW': 'CAB_ATTR_05'}, inplace=True)
        #
        # cab_true_final["CAB_ATTR_05"] = [','.join(re.findall(r'[A-Z]+\w+', str(x))) for x in
        #                                  cab_true_final['CAB_ATTR_05']]
        # # dropar pkg_id duplicadas
        # cab_true_final = cab_true_final.drop_duplicates(subset=["PPID", "PKG_ID", "CAB_ATTR_05"], keep='first')
        #
        # cab_true_final['Unique_Comp_Name'] = cab_true_final['Unique_Comp_Name'] + " #" + cab_true_final.groupby(
        #     ['PPID', 'Unique_Comp_Name']).cumcount().astype(str).replace('0', '')
        # # cab_true_final["Unique_Comp_Name"] = [re.sub(r'\s\S*\s(.*)', r' \1#', str(x)) for x in
        # #                                       cab_true_final['Unique_Comp_Name']]
        # # cab_true_final["Unique_Comp_Name"] = [re.sub(r'\s(\D)', '', str(x)) for x in cab_true_final['Unique_Comp_Name']]
        # cab_true_final["Unique_Comp_Name"] = [re.sub(r'(\s\#$)', '', str(x)) for x in
        #                                       cab_true_final['Unique_Comp_Name']]
        # cab_true_final["CAB_ATTR_05"] = [re.sub(',', ';', str(x)) for x in cab_true_final['CAB_ATTR_05']]
        #
        # # cab_true_final.drop_duplicates()
        # cab_true_final['PPID'] = [str(x)[:-3] for x in cab_true_final['PPID']]
        # cab_true_final = optimize(cab_true_final)
        # cab_true_final = pos_processing(cab_true_final)
        # print(cab_true_final.info())
        # cab_true_final = cab_true_final[
        #     ["PPID", "IN_STATION_TIME", "Unique_Comp_Name", "Revision", "WO#", "Supplier_Name", "Commodity_Type",
        #      'Component_Type',
        #      "Comp_Supplier_Name", "Comp_Supplier_ID", "Comp_Supplier_SN", "Comp_Supplier_PN", "Comp_Supplier_Lot_Code",
        #      "Comp_Supplier_Mfg_Dt", "Comp_Supplier_Factory_name", "CAB_ATTR_01", "CAB_ATTR_02", "CAB_ATTR_03",
        #      "CAB_ATTR_04", "CAB_ATTR_05"]]
        # print(cab_true_final.head())



        # cab_true_final.drop_duplicates(subset=["PPID", "CAB_ATTR_05"], keep='first',inplace=True)
        # cab_true_final['PPID'] = cab_true_final['PPID'] + cab_true_final['Unique_Comp_Name']
        # cab_true_final = pos_processing(cab_true_final)
        if flag == "0":
            format_file(cab_true_final, data, list_col_db)
            insert_db(cab_true_final, menor_data, maior_data)
        conn.invalidate()
        engine2.dispose()
        conn.rollback()

        return "O CAB FOI GERADO COM SUCESSO", cab_true_final
    except Exception as erro:
        history_cab(4, "ERRO NA GERACAO DO ARQUIVO", menor_data)
        conn.invalidate()
        engine2.dispose()
        conn.rollback()
        return "Ocorreu um erro ao gerar o arquivo CAB  :" + str(erro), None

# test = get_cab(menor_data, maior_data)

# display(test)


# fim = time.perf_counter()
# total = round(fim - inicio,2)
# # print(CAB_TRUE_FINAL.dtypes)
# print("\n")
# print("Tempo: " + str(total))
