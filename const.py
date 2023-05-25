
col_names_front = ["PPID","Unique_Comp_Name", "Revision", "WO#", "Supplier_Name", "Commodity_Type", 'Component_Type',
         "Comp_Supplier_Name", "Comp_Supplier_ID", "Comp_Supplier_SN", "Comp_Supplier_PN", "Comp_Supplier_Lot_Code",
         "Comp_Supplier_Mfg_Dt", "Comp_Supplier_Factory_name", "CAB_ATTR_01", "CAB_ATTR_02", "CAB_ATTR_03",
         "CAB_ATTR_04","CAB_ATTR_05","new_col"]

fix_col_names = ["PPID","Unique_Comp_Name", "Revision", "WO#", "Supplier_Name", "Commodity_Type", 'Component_Type',
         "Comp_Supplier_Name", "Comp_Supplier_ID", "Comp_Supplier_SN", "Comp_Supplier_PN", "Comp_Supplier_Lot_Code",
         "Comp_Supplier_Mfg_Dt", "Comp_Supplier_Factory_name", "CAB_ATTR_01", "CAB_ATTR_02", "CAB_ATTR_03",
         "CAB_ATTR_04","CAB_ATTR_05"]

destinatarios = ['x@gmail.com', 'mig.x@gmail.com']
assunto = 'Testeeeeeeeeee FInal de HOJE'
corpo = 'e o ultimo pessoal eu juro'
remetente = 'edenilson.filho1998@gmail.com'
senha = 'x'


# PPID_DB = list_col_db["PPID"][0]

valores_colunas_att = {
    "PPID": "PPID_VALOR",
    "Unique_Comp_Name": "valor_Unique_Comp_Name",
    "Revision": "valor_Revision",
    "WO#": "valor_WO",
    "Supplier_Name": "valor_Supplier_Name",
    "Commodity_Type": "valor_Commodity_Type",
    "Component_Type": "valor_Component_Type",
    "Comp_Supplier_Name": "valor_Comp_Supplier_Name",
    "Comp_Supplier_ID": "valor_Comp_Supplier_ID",
    "Comp_Supplier_SN": "valor_Comp_Supplier_SN",
    "Comp_Supplier_PN": "valor_Comp_Supplier_PNjojoj",
    "Comp_Supplier_Lot_Code": "valor_Comp_Supplier_Lot_Code",
    "Comp_Supplier_Mfg_Dt": "valor_Comp_Supplier_Mfg_Dt",
    "Comp_Supplier_Factory_name": "BLABLA",
    "CAB_ATTR_01": "CAB_ATTR_01_asd",
    "CAB_ATTR_02": "CAB_ATTR_02_dddd",
    "CAB_ATTR_03": "CAB_ATTR_03_aass",
    "CAB_ATTR_04": "CAB_ATTR_04asdas",
    "CAB_ATTR_05": ["Nome_COll_concat","PPID_VALOR", "PPID_VALOR"],
    "new_col": ["concat","PPID_VALOR", "valor_Revision"]
}