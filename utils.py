from sqlalchemy import *
import pandas as pd
import re
from typing import List
from const import fix_col_names,valores_colunas_att


def coluna_existe(tabela, coluna, conexao):
    inspector = inspect(conexao)
    colunas = inspector.get_columns(tabela)
    print(coluna in [col["name"] for col in colunas])
    return coluna in [col["name"] for col in colunas]

def obter_colunas_tabela(tabela, conexao):
    inspector = inspect(conexao)
    colunas = inspector.get_columns(tabela)
    return [col["name"] for col in colunas]


def apagar_colunas_excedentes(valores, conexao):
    tabela = 'CAB_COL'

    # Obter todas as colunas existentes na tabela
    colunas_existentes = obter_colunas_tabela(tabela, conexao)

    # Obter as colunas de valores.items()
    colunas_valores = list(valores.keys())
    print(colunas_valores)

    # Identificar as colunas que devem ser removidas
    # colunas_remover = [coluna for coluna in colunas_existentes if coluna not in colunas_valores]
    colunas_remover = [coluna for coluna in colunas_existentes if coluna not in colunas_valores and coluna not in fix_col_names]

    # Gerar e executar a consulta de remoção das colunas
    for coluna in colunas_remover:
        consulta_remover = "ALTER TABLE {} DROP COLUMN {}".format(tabela, coluna)
        conexao.execute(text(consulta_remover))

    # Confirmar a transação
    conexao.commit()
    return colunas_remover
def adicionar_col_excedentes_dataframe(valores, cab_dataframe, conexao):
    tabela = 'CAB_COL'

    # Obter todas as colunas existentes na tabela
    colunas_existentes = obter_colunas_tabela(tabela, conexao)

    # Obter as colunas de valores.items()
    colunas_valores = list(valores.keys())
    print(colunas_valores)

    # Identificar as colunas que devem ser removidas
    # colunas_remover = [coluna for coluna in colunas_existentes if coluna not in colunas_valores]
    # Adicionar colunas ao DataFrame
    for coluna, valor in valores.items():
        if coluna not in fix_col_names:
            if isinstance(valor, list):
                if valor[1] in cab_dataframe.columns and valor[2] in cab_dataframe.columns:
                    # Concatenar as duas colunas
                    # colunas_concat = cab_dataframe[valor[1]].astype(str) + cab_dataframe[valor[2]].astype(str)
                    cab_dataframe[valor[0]] = cab_dataframe[valor[1]].map(str) + ' ' + cab_dataframe[valor[2]].map(str)
                    # cab_dataframe[coluna] = colunas_concat
            else:
                cab_dataframe[coluna] = valor

    # Confirmar a transação
    conexao.commit()
    return cab_dataframe
# def att_col(valores,conexao):
#     # Construir a consulta UPDATE
#     consulta = "UPDATE CAB_COL SET "
#     colunas_valores = []
#     valores_lista = []
#     for coluna, valor in valores.items():
#         colunas_valores.append("{} = '{}'".format(coluna, valor))
#         valores_lista.append((valor,))
#         coluna_existe('CAB_COL', coluna, conexao)
#         # print(valores_lista)
#
#     consulta += ', '.join(colunas_valores)
#     print(consulta)
#     # Executar a consulta
#     conexao.execute(text(consulta))
#
#     # Confirmar a transação
#     conexao.commit()

def att_col(valores, conexao):
    colunas_valores = []
    valores_lista = []
    apagar_colunas_excedentes(valores, conexao)
    for coluna, valor in valores.items():
        # Verificar se a coluna existe no banco
        if not coluna_existe('CAB_COL', coluna, conexao):
            # A coluna não existe, então adicioná-la usando ALTER TABLE
            consulta_alter = "ALTER TABLE CAB_COL ADD {} VARCHAR(255)"
            conexao.execute(text(consulta_alter.format(coluna)))

        # Verificar se é necessário concatenar as colunas
        if isinstance(valor, list):
            # Concatenar as duas colunas
            colunas_valores.append("{} = '{}'".format(coluna, valor[0]))
        else:
            colunas_valores.append("{} = '{}'".format(coluna, valor))

    consulta = "UPDATE CAB_COL SET "
    consulta += ', '.join(colunas_valores)
    print(consulta)

    # Executar a consulta
    conexao.execute(text(consulta))

    # Confirmar a transação
    conexao.commit()

def concat_col_names(col_names_front, list_col_db):
    vec = []
    print(col_names_front)
    for coluna in col_names_front:
        vec.append(list_col_db[coluna][0])
        print(coluna)
        # print(valores_lista)
    return vec


def rename_col(cab_true_final, engine2, col_names_front):
    query_cab = "SELECT * FROM CAB_COL"

    cab = pd.read_sql(text(query_cab), engine2.connect())
    list_col_db = cab.to_dict()
    print(list_col_db["PPID"][0])
    PPID = list_col_db["PPID"][0]
    colunas_cab_automatic  = concat_col_names(col_names_front, list_col_db)
    print(colunas_cab_automatic)

    cab_true_final.sort_values(by=['SERIAL_NUMBER'], inplace=True)
    cab_true_final["Supplier_Name"] = "Foxconn"
    cab_true_final["Commodity_Type"] = "PCBA"
    cab_true_final["Comp_Supplier_Name"] = cab_true_final["VENDOR"]
    # cab_true_final = cab_true_final.assign(Comp_Supplier_Name=cab_true_final["VENDOR"])
    cab_true_final["Comp_Supplier_ID"] = cab_true_final["VENDOR"]
    cab_true_final["Comp_Supplier_Factory_name"] = cab_true_final["VENDOR"]
    cab_true_final["Comp_Supplier_SN"] = ""
    cab_true_final["CAB_ATTR_03"] = ""
    cab_true_final["Comp_Supplier_Mfg_Dt"] = ""
    cab_true_final["CAB_ATTR_02"] = cab_true_final["KEY_PART_NO"]
    cab_true_final["CAB_ATTR_04"] = cab_true_final["KEY_PART_NO"]

    # cab_true_final.rename(columns={'SERIAL_NUMBER': PPID}, inplace=True)
    cab_true_final.rename(columns={'SERIAL_NUMBER': list_col_db["PPID"][0]}, inplace=True)
    cab_true_final.rename(columns={'KEY_PART_NO': list_col_db["Unique_Comp_Name"][0]}, inplace=True)
    cab_true_final.rename(columns={'VERSION_CODE': list_col_db["Revision"][0]}, inplace=True)
    cab_true_final.rename(columns={'MO_NUMBER': list_col_db["WO#"][0]}, inplace=True)
    cab_true_final.rename(columns={'Supplier_Name': list_col_db["Supplier_Name"][0]}, inplace=True)
    cab_true_final.rename(columns={'Commodity_Type': list_col_db["Commodity_Type"][0]}, inplace=True)
    cab_true_final.rename(columns={'MAT_TYPE': list_col_db["Component_Type"][0]}, inplace=True)
    cab_true_final.rename(columns={'Comp_Supplier_Name': list_col_db["Comp_Supplier_Name"][0]}, inplace=True)
    cab_true_final.rename(columns={'Comp_Supplier_ID': list_col_db["Comp_Supplier_ID"][0]}, inplace=True)
    cab_true_final.rename(columns={'Comp_Supplier_SN': list_col_db["Comp_Supplier_SN"][0]} ,inplace=True)
    cab_true_final.rename(columns={'HH_PN': list_col_db["Comp_Supplier_PN"][0]}, inplace=True)
    cab_true_final.rename(columns={'LOT_NO': list_col_db["Comp_Supplier_Lot_Code"][0]}, inplace=True)
    cab_true_final.rename(columns={'Comp_Supplier_Mfg_Dt': list_col_db["Comp_Supplier_Mfg_Dt"][0]}, inplace=True)
    cab_true_final.rename(columns={'Comp_Supplier_Factory_name': list_col_db["Comp_Supplier_Factory_name"][0]}, inplace=True)
    cab_true_final.rename(columns={'DATE_CODE': list_col_db["CAB_ATTR_01"][0]}, inplace=True)
    cab_true_final.rename(columns={'CAB_ATTR_02': list_col_db["CAB_ATTR_02"][0]}, inplace=True)
    cab_true_final.rename(columns={'CAB_ATTR_03': list_col_db["CAB_ATTR_03"][0]}, inplace=True)
    cab_true_final.rename(columns={'CAB_ATTR_04': list_col_db["CAB_ATTR_04"][0]}, inplace=True)
    cab_true_final.rename(columns={'REF_DESC_LIST_NEW': list_col_db["CAB_ATTR_05"][0]}, inplace=True)
    cab_true_final = adicionar_col_excedentes_dataframe(valores_colunas_att,cab_true_final,engine2.connect())

    cab_true_final[list_col_db["CAB_ATTR_05"][0]] = [','.join(re.findall(r'[A-Z]+\w+', str(x))) for x in
                                     cab_true_final[list_col_db["CAB_ATTR_05"][0]]]
    cab_true_final.drop_duplicates(subset=[list_col_db["PPID"][0], "PKG_ID", list_col_db["CAB_ATTR_05"][0]], keep='first',inplace=True)
    cab_true_final[list_col_db["Unique_Comp_Name"][0]] = cab_true_final[list_col_db["Unique_Comp_Name"][0]] + " #" + cab_true_final.groupby(
        [list_col_db["PPID"][0], list_col_db["Unique_Comp_Name"][0]]).cumcount().astype(str).replace('0', '')

    cab_true_final[list_col_db["Unique_Comp_Name"][0]] = [re.sub(r'(\s\#$)', '', str(x)) for x in
                                          cab_true_final[list_col_db["Unique_Comp_Name"][0]]]
    cab_true_final[list_col_db["CAB_ATTR_05"][0]] = [re.sub(',', ';', str(x)) for x in cab_true_final[list_col_db["CAB_ATTR_05"][0]]]

    # cab_true_final.drop_duplicates()
    cab_true_final[list_col_db["PPID"][0]] = [str(x)[:-3] for x in cab_true_final[list_col_db["PPID"][0]]]
    cab_true_final = optimize(cab_true_final)
    cab_true_final = pos_processing(cab_true_final, list_col_db)
    print(cab_true_final.info())

    colunas_cab_automatic_db = colunas_cab_automatic + ["IN_STATION_TIME"]
    print(colunas_cab_automatic_db)
    cab_true_final = cab_true_final[colunas_cab_automatic_db]
    print("lalalalalal")
    print(cab_true_final)


    print(cab_true_final.head())
    return cab_true_final,list_col_db, colunas_cab_automatic_db

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
            if not (type(df[col][0]) == list):
                num_unique_values = len(df[col].unique())
                num_total_values = len(df[col])
                if float(num_unique_values) / num_total_values < 0.5:
                    df[col] = df[col].astype('category')
        else:
            df[col] = pd.to_datetime(df[col])
    return df


def optimize(df: pd.DataFrame, datetime_features: List[str] = []):
    return optimize_floats(optimize_ints(optimize_objects(df, datetime_features)))

def pos_processing(cab,list_col_db):
    cab[list_col_db["CAB_ATTR_05"][0]] = cab[list_col_db["CAB_ATTR_05"][0]].str.split(';')
    cab = cab.explode(list_col_db["CAB_ATTR_05"][0])
    # cab_true_final = cab_true_final.drop_duplicates(subset=["PPID", "PKG_ID", "CAB_ATTR_05"], keep='first')
    cab.drop_duplicates(subset=["{}".format(list_col_db["PPID"][0]), list_col_db["CAB_ATTR_05"][0]], keep='first', inplace=True)
    cab[list_col_db["CAB_ATTR_05"][0]] = cab.groupby([list_col_db["PPID"][0], "PKG_ID"])[list_col_db["CAB_ATTR_05"][0]].transform(lambda x: ';'.join(x))
    cab.drop_duplicates(subset=["{}".format(list_col_db["PPID"][0]), list_col_db["CAB_ATTR_05"][0]], keep='first', inplace=True)
    return cab