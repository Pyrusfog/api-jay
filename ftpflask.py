import pysftp
from urllib.parse import urlparse
import os
import time
from datetime import datetime, date, timedelta
from intflaskcab import history_cab


class Sftp:
    def __init__(self, port=22):
        """Constructor Method"""
        # Set connection object to None (initial value)
        self.connection = None
        self.hostname = "10.10.252.69"
        self.username = "francisco"
        self.password = "Ipe@2023"
        self.port = port

    def connect(self):
        """Connects to the sftp server and returns the sftp connection object"""

        try:

            # Get the sftp connection object
            self.connection = pysftp.Connection(
                host=self.hostname,
                username=self.username,
                password=self.password,
                port=self.port,
            )
        except Exception as err:
            raise Exception(err)
        finally:
            print(f"Connected to {self.hostname} as {self.username}.")

    def disconnect(self):
        """Closes the sftp connection"""
        self.connection.close()
        print(f"Disconnected from host {self.hostname}")

    def listdir(self, remote_path):
        """lists all the files and directories in the specified path and returns them"""
        for obj in self.connection.listdir(remote_path):
            yield obj

    def listdir_attr(self, remote_path):
        """lists all the files and directories (with their attributes) in the specified path and returns them"""
        for attr in self.connection.listdir_attr(remote_path):
            yield attr

    def download(self, remote_path, target_local_path):
        """
        Downloads the file from remote sftp server to local.
        Also, by default extracts the file to the specified target_local_path
        """

        try:
            print(
                f"downloading from {self.hostname} as {self.username} [(remote path : {remote_path});(local path: {target_local_path})]"
            )

            # Create the target directory if it does not exist
            path, _ = os.path.split(target_local_path)
            if not os.path.isdir(path):
                try:
                    os.makedirs(path)
                except Exception as err:
                    raise Exception(err)

            # Download from remote sftp server to local
            self.connection.get(remote_path, target_local_path)
            print("download completed")

        except Exception as err:
            raise Exception(err)

    def upload(self, source_local_path, remote_path):
        """
        Uploads the source files from local to the sftp server.
        """

        try:
            print(
                f"uploading to {self.hostname} as {self.username} [(remote path: {remote_path});(source local path: {source_local_path})]"
            )

            # Download file from SFTP
            self.connection.put(source_local_path, remote_path)
            print("upload completed")

        except Exception as err:
            raise Exception(err)
def send_ftp(data_folder):
    try:
        sftp = Sftp(
        )

        # Connect to SFTP
        sftp.connect()
        ftp_send_list_ok = []
        # Lists files with attributes of SFTP
        path = "/home/francisco"
        print(f"List of files with attributes at location {path}:")
        for file in sftp.listdir_attr(path):
            print(file.filename, file.st_mode, file.st_size, file.st_atime, file.st_mtime)

        # yesterday = date.today() - timedelta(days=1)
        # yesterday = yesterday.strftime('%Y%m%d')
        # data = str(yesterday)
        # data_folder = date.today() - timedelta(days=1)
        # data_folder = data_folder.strftime('%Y-%m-%d')
        # data_folder = str(data_folder)
        # print(data_folder)
        # Quando for roda no windows editar a string abaixo para o local desejado, e retirar o .format
        directoryfile = '/home/franciscosilva/Documents/test_files_cab/'+ data_folder
        files = os.listdir(directoryfile)
        for file in files:
            print(file)
            # Upload files to SFTP location from local
            local_path = r'/home/franciscosilva/Documents/test_files_cab/'+ data_folder + "/{}".format(file)
            # editar local remoto
            # remote_path = "/incomming/{}".format(file)
            remote_path = "/home/francisco/{}".format(file)
            sftp.upload(local_path, remote_path)
            ftp_send_list_ok.append(file)

        # Lists files of SFTP location after upload
        print(f"List of files at location {path}:")
        print([f for f in sftp.listdir(path)])

        # Download files from SFTP
        # sftp.download(
        #     remote_path, os.path.join(remote_path, local_path + '.backup')
        # )

        # Disconnect from SFTP
        print(ftp_send_list_ok)
        ftp_send_list_ok = ', '.join(ftp_send_list_ok)

        # data_format = datetime.strptime(data_folder + " 00:00:00.000", '%Y-%m-%d %H:%M:%S.%f')

        history_cab(2, "ENVIADO COM SUCESSO", data_folder + " 00:00:00.000", vec_ftp=ftp_send_list_ok)

        sftp.disconnect()
        return "Cab enviado via FTP com sucesso"
    except Exception as erro:
        history_cab(2, "FALHA NO ENVIO", data_folder,vec_ftp=ftp_send_list_ok)
        return "Ocorreu um erro ao enviar o arquivo CAB  :" + str(erro)



