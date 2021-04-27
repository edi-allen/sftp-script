#!/usr/bin/env python3
import socks
import paramiko
from paramiko import Transport, SSHClient, SFTPClient, AutoAddPolicy
from paramiko.proxy import ProxyCommand
import time
import errno
import logging
logging.basicConfig(format='%(levelname)s : %(message)s',
                    level=logging.INFO)


class SftpClient:
    _connection = None
    def __init__(self, host, port, username, key, proxy):
        self.host = host
        self.port = port
        self.username = username
        self.key = key
        self.proxy = proxy
        self.create_connection(self.host, self.port,
                               self.username, self.key, self.proxy)

    @classmethod
    def create_connection(cls, host, port, username, key, proxy):
        proxy_host, proxy_port = proxy.split(':')
        sock = socks.socksocket()
        sock.set_proxy(proxy_type=socks.SOCKS5, addr=proxy_host, port=int(proxy_port))
        sock.connect((host, int(port)))
        transport = paramiko.transport.Transport(sock)
        transport.connect(username=username, pkey=paramiko.rsakey.RSAKey.from_private_key_file(key))
        cls._connection = SFTPClient.from_transport(transport)

    @staticmethod
    def uploading_info(uploaded_file_size, total_file_size):

        logging.info('uploaded_file_size : {} total_file_size : {}'.
                     format(uploaded_file_size, total_file_size))

    def upload(self, local_path, remote_path):
        self._connection.put(localpath=local_path,
                             remotepath=remote_path,
                             callback=self.uploading_info,
                             confirm=True)

    def file_exists(self, remote_path):

        try:
            self._connection.stat(remote_path)
        except IOError as e:
            if e.errno == errno.ENOENT:
                return False
            raise
        else:
            return True

    def download(self, remote_path, local_path, retry=5):

        if self.file_exists(remote_path) or retry == 0:
            self._connection.get(remote_path, local_path,
                                 callback=None)
        elif retry > 0:
            time.sleep(5)
            retry = retry - 1
            self.download(remote_path, local_path, retry=retry)    

    def close(self):
        self._connection.close()

