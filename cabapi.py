from datetime import datetime
import re
import io
from flask import *
from intflaskcab import get_cab,insert_db,get_history
from ftpflask import send_ftp
from datetime import timedelta
import zipfile
import os
from flask_mail import Mail,Message

app = Flask(__name__)
mail = Mail(app)

@app.route('/date-cab-rerun', methods=['GET'])
def init_cab_with_date():
    data = request.args.get('data_inicio')
    data_vec = re.findall(r"(^\d{4})\/(0?[1-9]|1[012])\/(0?[1-9]|[12][0-9]|3[01])$", data)
    print(data_vec[0][1])
    menor_data = data + " 00:00:00.000"
    maior_data = data + " 23:59:59.000"
    print(menor_data)
    print(maior_data)
    menssage ,_ = get_cab(menor_data, maior_data, data)
    return menssage

@app.route('/rerun-ftp', methods=['GET'])
def ftp_rerun():
    data = request.args.get('data')
    data_vec = re.findall(r"(^\d{4})\/(0?[1-9]|1[012])\/(0?[1-9]|[12][0-9]|3[01])$", data)
    data_folder = "{}-{}-{}".format(data_vec[0][0], data_vec[0][1], data_vec[0][2])
    print(data_vec[0][1])

    send_ftp(data_folder)

    return "CAB ENVIADO COM SUCESSO"

@app.route('/db-cab-rerun', methods=['GET'])
def db_cab_rerun():
    data = request.args.get('data')
    data_vec = re.findall(r"(^\d{4})\/(0?[1-9]|1[012])\/(0?[1-9]|[12][0-9]|3[01])$", data)
    print(data_vec[0][1])
    menor_data = data + " 00:00:00.000"
    maior_data = data + " 23:59:59.000"
    print(menor_data)
    print(maior_data)
    _, cab_df = get_cab(menor_data, maior_data, data,flag="1")
    return insert_db(cab_df, menor_data, maior_data)

@app.route('/history-cab', methods=['GET'])
def history_cab():
    try:
        jsons=[]
        data_inicio = datetime.strptime(request.args.get('data_inicio'), '%Y/%m/%d')
        data_fim = datetime.strptime(request.args.get('data_final'), '%Y/%m/%d')
        print(data_inicio,data_fim)


        # Percorre as datas entre as datas inicial e final
        for n in range(int((data_fim - data_inicio).days) + 1):
            data_format = (data_inicio + timedelta(n)).strftime('%Y-%m-%d')
            print(data_format)
            folder_path = '/home/franciscosilva/Documents/test_files_cab/' + data_format

            if os.path.exists(folder_path) and os.listdir(folder_path):
                folder = "Exist"
            if not os.path.exists(folder_path) or not os.listdir(folder_path):
                folder = "Not exist"

            history_df = get_history(data_format)
            # print(history_df['CAB_CREATE'].iloc[0])
            if len(history_df) > 0:
                data = {
                        "data": history_df['CAB_DATE'].iloc[0].to_pydatetime().strftime('%Y-%m-%d'),
                        "CAB_DB_INSERT": history_df['CAB_DB_INSERT'].iloc[0],
                        "CAB_FTP_SEND": history_df['CAB_FTP_SEND'].iloc[0],
                        "CAB_CREATE": history_df['CAB_CREATE'].iloc[0],
                        "CAB_CREATE_FILES": history_df['CAB_CREATE_FILES'].iloc[0],
                        "CAB_CREATE_HOUR": history_df['CAB_CREATE_HOUR'].iloc[0],
                        "CAB_FTP_HOUR": history_df['CAB_FTP_HOUR'].iloc[0],
                        "FOLDER_EXIST":folder
                }
                jsons.append(data)
        return jsonify(jsons)
    except ValueError:
        return jsonify({"message": "Data em formato incorreto"})
    except Exception as erro:
        return jsonify({"message": "Essa data nao existe no banco: "+ str(erro)})
@app.route('/download')
def download():
    # pasta que contém os arquivos a serem adicionados ao ZIP
    data = request.args.get('data')
    data = re.sub(r'/', '-', data)
    folder_path = '/home/franciscosilva/Documents/test_files_cab/' + data

    if not os.path.exists(folder_path):
        return jsonify({"message": "Diretorio nao encontrado"})

    # cria um arquivo ZIP em memória
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED, False) as zip_file:
        # adiciona todos os arquivos da pasta ao ZIP
        for file_name in os.listdir(folder_path):
            file_path = os.path.join(folder_path, file_name)
            if os.path.isfile(file_path):
                zip_file.write(file_path, file_name)

    # move o ponteiro para o início do arquivo ZIP
    zip_buffer.seek(0)

    # envia o arquivo ZIP como um anexo para download
    send_file(zip_buffer, download_name=data + '.zip', as_attachment=True)
    return




if __name__ == '__main__':
    app.run(host='0.0.0.0', port=150)
