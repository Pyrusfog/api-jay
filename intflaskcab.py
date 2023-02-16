import pandas as pd
import sys
import re
import datetime
import time
from sqlalchemy.pool import NullPool
from sqlalchemy import *
# from IPython.display import display
from datetime import datetime, date,timedelta
from typing import List
import os, os.path
import traceback
import concurrent.futures
import multiprocessing
from itertools import product
import numpy as np

from itertools import product

# Contar Segundos
inicio = time.perf_counter()
negative_day = 60
engine2 = create_engine("mssql+pymssql://sa:Pyrusfog16%40@localhost:1433/master", poolclass=NullPool)
conn = engine2.connect()
yesterday =  date.today() - timedelta(days=negative_day)
yesterday = str(yesterday)

# model = '82PFC'
# menor_data = str(yesterday) + ' 00:00:00.000'
# maior_data =  str(yesterday) + ' 23:59:59.000'
# menor_data = '2022-10-31 00:00:00.000'
# maior_data = '2022-10-31 23:59:59.000'
# maior_data = '2022-11-01 01:59:59.000'



def optimize_floats(df: pd.DataFrame) -> pd.DataFrame:
    floats = df.select_dtypes(include=['float64']).columns.tolist()
    df[floats] = df[floats].apply(pd.to_numeric, downcast='float')
    return df


def optimize_ints(df: pd.DataFrame) -> pd.DataFrame:
    ints = df.select_dtypes(include=['int64']).columns.tolist()
    df[ints] = df[ints].apply(pd.to_numeric, downcast='integer')
    return df


def optimize_objects(df: pd.DataFrame, datetime_features: List[str]) -> pd.DataFrame:
    for col in df.select_dtypes(include=['object']):
        if col not in datetime_features:
            if not (type(df[col][0])==list):
                num_unique_values = len(df[col].unique())
                num_total_values = len(df[col])
                if float(num_unique_values) / num_total_values < 0.5:
                    df[col] = df[col].astype('category')
        else:
            df[col] = pd.to_datetime(df[col])
    return df



def optimize(df: pd.DataFrame, datetime_features: List[str] = []):
    return optimize_floats(optimize_ints(optimize_objects(df, datetime_features)))


def format_file(Cab, date=""):
    if date=="":
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
        data = "{}{}{}".format(data_vec[0][0],data_vec[0][1],data_vec[0][2])
        data_folder = "{}-{}-{}".format(data_vec[0][0],data_vec[0][1],data_vec[0][2])
        # data_folder = date.strftime('%Y-%m-%d')
        # data_folder = str(data_folder)
    print(data_folder)
    newpath = '/home/franciscosilva/Documents/test_files_cab/' + data_folder
    if not os.path.exists(newpath):
        os.makedirs(newpath)
    # data = datetime.today().strftime('%Y%m%d')]
    col = ["PPID","Unique_Comp_Name", "Revision", "WO#", "Supplier_Name", "Commodity_Type", 'Component_Type',
         "Comp_Supplier_Name", "Comp_Supplier_ID", "Comp_Supplier_SN", "Comp_Supplier_PN", "Comp_Supplier_Lot_Code",
         "Comp_Supplier_Mfg_Dt", "Comp_Supplier_Factory_name", "CAB_ATTR_01", "CAB_ATTR_02", "CAB_ATTR_03",
         "CAB_ATTR_04","CAB_ATTR_05"]

    name_file = 'FOXCONN_JUNDIAI_CAB_' + data + "_FILE01.csv"
    name_filezip = '/home/franciscosilva/Documents/test_files_cab/'+ data_folder + "/" +'FOXCONN_JUNDIAI_CAB_' + data + "_FILE01.zip"
    name_file2 = 'FOXCONN_JUNDIAI_CAB_' + data + "_FILE02.csv"
    name_file2zip = '/home/franciscosilva/Documents/test_files_cab/'+ data_folder + "/" + 'FOXCONN_JUNDIAI_CAB_' + data + "_FILE02.zip"
    name_file3 = 'FOXCONN_JUNDIAI_CAB_' + data + "_FILE03.csv"
    name_file3zip = '/home/franciscosilva/Documents/test_files_cab/'+ data_folder + "/" + 'FOXCONN_JUNDIAI_CAB_' + data + "_FILE03.zip"
    name_file4 = 'FOXCONN_JUNDIAI_CAB_' + data + "_FILE04.csv"
    name_file4zip = '/home/franciscosilva/Documents/test_files_cab/'+ data_folder + "/" + 'FOXCONN_JUNDIAI_CAB_' + data + "_FILE04.zip"
    name_file5 = 'FOXCONN_JUNDIAI_CAB_' + data + "_FILE05.csv"
    name_file5zip = '/home/franciscosilva/Documents/test_files_cab/'+ data_folder + "/" + 'FOXCONN_JUNDIAI_CAB_' + data + "_FILE05.zip"
    name_file6 = 'FOXCONN_JUNDIAI_CAB_' + data + "_FILE06.csv"
    name_file6zip = '/home/franciscosilva/Documents/test_files_cab/'+ data_folder + "/" + 'FOXCONN_JUNDIAI_CAB_' + data + "_FILE06.zip"
    name_file7 = 'FOXCONN_JUNDIAI_CAB_' + data + "_FILE07.csv"
    name_file7zip = '/home/franciscosilva/Documents/test_files_cab/'+ data_folder + "/" + 'FOXCONN_JUNDIAI_CAB_' + data + "_FILE07.zip"
    name_file8 = 'FOXCONN_JUNDIAI_CAB_' + data + "_FILE08.csv"
    name_file8zip = '/home/franciscosilva/Documents/test_files_cab/'+ data_folder + "/" + 'FOXCONN_JUNDIAI_CAB_' + data + "_FILE08.zip"
    name_file9 = 'FOXCONN_JUNDIAI_CAB_' + data + "_FILE09.csv"
    name_file9zip = '/home/franciscosilva/Documents/test_files_cab/'+ data_folder + "/" + 'FOXCONN_JUNDIAI_CAB_' + data + "_FILE09.zip"
    name_file10 = 'FOXCONN_JUNDIAI_CAB_' + data + "_FILE010.csv"
    name_file10zip = '/home/franciscosilva/Documents/test_files_cab/'+ data_folder + "/" + 'FOXCONN_JUNDIAI_CAB_' + data + "_FILE010.zip"
    name_file11 = 'FOXCONN_JUNDIAI_CAB_' + data + "_FILE11.csv"
    name_file11zip = '/home/franciscosilva/Documents/test_files_cab/'+ data_folder + "/" + 'FOXCONN_JUNDIAI_CAB_' + data + "_FILE11.zip"
    name_file12 = 'FOXCONN_JUNDIAI_CAB_' + data + "_FILE12.csv"
    name_file12zip = '/home/franciscosilva/Documents/test_files_cab/'+ data_folder + "/" + 'FOXCONN_JUNDIAI_CAB_' + data + "_FILE12.zip"
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
        folder = '/home/franciscosilva/Documents/test_files_cab/'+ data_folder + "/"
        for root, _, files in os.walk(folder):
            for f in files:
                fullpath = os.path.join(root, f)
                try:
                    if os.path.getsize(fullpath) < 10 * 1024:   #set file size in kb
                        print (fullpath)
                        os.remove(fullpath)
                except WindowsError:
                    print ("Error" + fullpath)

    except FileExistsError:
        print("Error ao salvar arquivo")




def pos_processing(cab):
    cab["CAB_ATTR_05"] = cab["CAB_ATTR_05"].str.split(';')
    cab = cab.explode('CAB_ATTR_05')
    #cab_true_final = cab_true_final.drop_duplicates(subset=["PPID", "PKG_ID", "CAB_ATTR_05"], keep='first')
    cab.drop_duplicates(subset=["PPID", "CAB_ATTR_05"], keep='first',inplace=True)
    cab['CAB_ATTR_05'] = cab.groupby(["PPID", "PKG_ID"])['CAB_ATTR_05'].transform(lambda x: ';'.join(x))
    cab.drop_duplicates(subset=["PPID", "CAB_ATTR_05"], keep='first',inplace=True)
    return cab


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

    cab = pd.read_sql(query_cab, engine2.connect())
    return cab

def insert_db(cab_file,menor_data,maior_data):
        # engineyolo = create_engine("mssql+pymssql://yolo.sys:Y01o!@#123@10.8.162.80:1433/yolo", poolclass=NullPool)
        # engineyolo = create_engine("mssql+pymssql://sa:Pyrusfog16%40@localhost:1433/master", poolclass=NullPool)
        # table_orcl = pd.read_sql_query("SELECT top 100 * FROM master.dbo.R_SN_DETAIL_T_VIEW", engineyolo)
        query_pegar_model = "SELECT TOP 1 IN_STATION_TIME FROM master.dbo.CAB_LOG  " \
                            "WHERE IN_STATION_TIME BETWEEN '{}' AND '{}'".format(menor_data, maior_data)
        cab = pd.read_sql(query_pegar_model, engine2.connect())
        print(len(cab))
        print("LINHAAAAAAAS " + str(len(cab)))
        if len(cab) == 0:
            cab_file.to_sql(con=engine2, name="CAB_LOG", index=False, if_exists="append")
            return "Cab inserido com sucesso"
        else:
            return "O cab ja foi inserido anteriormente no banco "
        # print(cab_file.to_sql(con=engine2, name="CAB_LOG", index=False, if_exists="append"))
        # engine2.dispose()

def get_cab(menor_data, maior_data,data="",flag="0"):

    try:
        cab_true_final = pd.DataFrame()

        query_pegar_model = "SELECT DISTINCT MODEL_NAME  FROM master.dbo.R_SN_DETAIL_T_VIEW  " \
                            "WHERE IN_STATION_TIME BETWEEN '{}' AND '{}' AND GROUP_NAME = 'REFLOW VI2'".format(menor_data,
                                                                                                               maior_data)

        modelo_table = pd.read_sql(query_pegar_model, engine2.connect())
        modelo_col = modelo_table["MODEL_NAME"]
        # print(modelo_col)
        for modelo_placa in modelo_col:
            print(modelo_placa)
            cab_part = run_cab(modelo_placa, menor_data, maior_data)
            cab_true_final = pd.concat([cab_true_final, cab_part], ignore_index=True)

        # cab_true_final['KEY_PART_NO'] = cab_true_final['KEY_PART_NO'] + " #"+cab_true_final.groupby(['SERIAL_NUMBER','KEY_PART_NO']).cumcount().astype(str).replace('0','')


        cab_true_final = cab_true_final.sort_values(by=['SERIAL_NUMBER'])
        cab_true_final["Supplier_Name"] = "Foxconn"
        cab_true_final["Commodity_Type"] = "PCBA"
        cab_true_final["Comp_Supplier_Name"] = cab_true_final["VENDOR"]
        cab_true_final["Comp_Supplier_ID"] = cab_true_final["VENDOR"]
        cab_true_final["Comp_Supplier_Factory_name"] = cab_true_final["VENDOR"]
        cab_true_final["Comp_Supplier_SN"] = ""
        cab_true_final["CAB_ATTR_03"] = ""
        cab_true_final["Comp_Supplier_Mfg_Dt"] = ""
        cab_true_final["CAB_ATTR_02"] = cab_true_final["KEY_PART_NO"]
        cab_true_final["CAB_ATTR_04"] = cab_true_final["KEY_PART_NO"]
        cab_true_final = cab_true_final.rename(columns={'SERIAL_NUMBER': 'PPID'})
        cab_true_final = cab_true_final.rename(columns={'MAT_TYPE': 'Component_Type'})
        cab_true_final = cab_true_final.rename(columns={'KEY_PART_NO': 'Unique_Comp_Name'})
        cab_true_final = cab_true_final.rename(columns={'HH_PN': 'Comp_Supplier_PN'})
        cab_true_final = cab_true_final.rename(columns={'VERSION_CODE': 'Revision'})
        cab_true_final = cab_true_final.rename(columns={'MO_NUMBER': 'WO#'})
        cab_true_final = cab_true_final.rename(columns={'LOT_NO': 'Comp_Supplier_Lot_Code'})
        cab_true_final = cab_true_final.rename(columns={'HH_PN': 'Comp_Supplier_PN'})
        cab_true_final = cab_true_final.rename(columns={'DATE_CODE': 'CAB_ATTR_01'})
        cab_true_final = cab_true_final.rename(columns={'REF_DESC_LIST_NEW': 'CAB_ATTR_05'})



        cab_true_final["CAB_ATTR_05"] = [','.join(re.findall(r'[A-Z]+\w+', str(x))) for x in
                                         cab_true_final['CAB_ATTR_05']]
        # dropar pkg_id duplicadas
        cab_true_final = cab_true_final.drop_duplicates(subset=["PPID", "PKG_ID", "CAB_ATTR_05"], keep='first')

        cab_true_final['Unique_Comp_Name'] = cab_true_final['Unique_Comp_Name'] + " #" + cab_true_final.groupby(['PPID', 'Unique_Comp_Name']).cumcount().astype(str).replace('0', '')
        # cab_true_final["Unique_Comp_Name"] = [re.sub(r'\s\S*\s(.*)', r' \1#', str(x)) for x in
        #                                       cab_true_final['Unique_Comp_Name']]
        # cab_true_final["Unique_Comp_Name"] = [re.sub(r'\s(\D)', '', str(x)) for x in cab_true_final['Unique_Comp_Name']]
        cab_true_final["Unique_Comp_Name"] = [re.sub(r'(\s\#$)', '', str(x)) for x in cab_true_final['Unique_Comp_Name']]
        cab_true_final["CAB_ATTR_05"] = [re.sub(',', ';', str(x)) for x in cab_true_final['CAB_ATTR_05']]

        # cab_true_final.drop_duplicates()
        cab_true_final['PPID'] = [str(x)[:-3] for x in cab_true_final['PPID']]
        cab_true_final = optimize(cab_true_final)
        cab_true_final = pos_processing(cab_true_final)
        print(cab_true_final.info())
        cab_true_final = cab_true_final[
            ["PPID","IN_STATION_TIME","Unique_Comp_Name", "Revision", "WO#", "Supplier_Name", "Commodity_Type", 'Component_Type',
             "Comp_Supplier_Name", "Comp_Supplier_ID", "Comp_Supplier_SN", "Comp_Supplier_PN", "Comp_Supplier_Lot_Code",
             "Comp_Supplier_Mfg_Dt", "Comp_Supplier_Factory_name", "CAB_ATTR_01", "CAB_ATTR_02", "CAB_ATTR_03",
             "CAB_ATTR_04","CAB_ATTR_05"]]
        print(cab_true_final.head())
        #cab_true_final.drop_duplicates(subset=["PPID", "CAB_ATTR_05"], keep='first',inplace=True)
        # cab_true_final['PPID'] = cab_true_final['PPID'] + cab_true_final['Unique_Comp_Name']
        #cab_true_final = pos_processing(cab_true_final)
        if flag == "0":
            format_file(cab_true_final, data)
            insert_db(cab_true_final, menor_data, maior_data)
        conn.invalidate()
        engine2.dispose()

        return  "O CAB FOI GERADO COM SUCESSO", cab_true_final
    except Exception as erro:
        return "Ocorreu um erro ao gerar o arquivo CAB  :" + str(erro), None


# test = get_cab(menor_data, maior_data)

# display(test)


# fim = time.perf_counter()
# total = round(fim - inicio,2)
# # print(CAB_TRUE_FINAL.dtypes)
# print("\n")
# print("Tempo: " + str(total))


