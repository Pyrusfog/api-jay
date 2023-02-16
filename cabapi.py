from datetime import datetime
import re
from flask import *
from intflaskcab import get_cab,insert_db
from ftpflask import send_ftp
from datetime import date, timedelta
app = Flask(__name__)

@app.route('/date-cab-rerun', methods=['GET'])
def init_cab_with_date():
    data = request.args.get('data')
    data_vec = re.findall(r"(^\d{4})\/(0?[1-9]|1[012])\/(0?[1-9]|[12][0-9]|3[01])$", data)
    print(data_vec[0][1])
    menor_data = data + " 00:00:00.000"
    maior_data = data + " 23:59:59.000"
    print(menor_data)
    print(maior_data)
    menssage ,_ = get_cab(menor_data, maior_data, data)
    return menssage

@app.route('/rerun-ftp', methods=['GET'])
def ftp_rerun_4534353543():
    data = request.args.get('data')
    data_vec = re.findall(r"(^\d{4})\/(0?[1-9]|1[012])\/(0?[1-9]|[12][0-9]|3[01])$", data)
    data_folder = "{}-{}-{}".format(data_vec[0][0], data_vec[0][1], data_vec[0][2])
    print(data_vec[0][1])
    send_ftp(data_folder)
    return "fdffdgdhhffgfhjjjjjddddjjjjjjjjjjjjjjjjjjjjjjjgh"
@app.route('/db-cab-rerun', methods=['GET'])
def db_cab_rerun():
    data = request.args.get('data')
    data_vec = re.findall(r"(^\d{4})\/(0?[1-9]|1[012])\/(0?[1-9]|[12][0-9]|3[01])$", data)
    print(data_vec[0][1])
    menor_data = data + " 00:00:00.000"
    maior_data = data + " 23:59:59.000"
    print(menor_data)
    print(maior_data)
    _,cab_df = get_cab(menor_data, maior_data, data,flag="1")
    return insert_db(cab_df,menor_data,maior_data)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=150)
