#!/usr/bin/python3
import os
import shutil
import hashlib
import sftp
import dbutil
from inotify_simple import INotify, flags
import configparser
import logging
logging.basicConfig(format='%(levelname)s : %(message)s',
                    level=logging.INFO)


def watcher(directory):
    inotify = INotify()
    watch_flags = flags.CREATE | flags.MOVED_TO # only watch for files created, copied or moved
    inotify.add_watch(directory, watch_flags)
    WAIT = 1000
    while True:
        for event in inotify.read(WAIT):
            filename = event.name
            yield os.path.join(directory, filename)
        
def file_filter(filenames):
    for filename in filenames:
        if os.path.isfile(filename):
            yield filename
        
def upload(sftpclient, dbclient, remote_dir, success_dir, errors_dir, filenames):
    for filename in filenames:
        basename = os.path.basename(filename)
        remote_path = os.path.join(remote_dir, basename)
        success_path = os.path.join(success_dir, basename)
        
        try:
            sftpclient.upload(filename, remote_path) 
            #dbclient.insert_filename(filename, hash_file(filename))
            shutil.move(filename, success_path)
            logging.info("file [{}] was transferred successfully".format(filename))
        except Exception as e:
            error_path = os.path.join(errors_dir, basename)
            shutil.move(filename, error_path)
            logging.info("An error ocurred transfering file [{}].\nError: {}".format(filename, e))


def create_sftp_client(config):
    host = config['SFTP']['host']
    port = config['SFTP']['port']
    user = config['SFTP']['user']
    key = config['SFTP']['key']
    proxy = config['SFTP']['proxy']
    return sftp.SftpClient(host, port, user, key, proxy)

def create_db_client(config):
    user = config['Cx_ORACLE']['user']
    password = config['Cx_ORACLE']['password']
    url = config['Cx_ORACLE']['url']
    table = config['Cx_ORACLE']['table']
    return dbutil.Oracle(user, password, url, table)

def hash_file(filename):
    if not os.path.isfile(filename):
        raise Exception('{} must be a file'.format(filename))

    BLOCKSIZE = 65536
    hasher = hashlib.sha1()
    with open(filename, 'rb') as afile:
        buf = afile.read(BLOCKSIZE)
        while len(buf) > 0:
            hasher.update(buf)
            buf = afile.read(BLOCKSIZE)
    return hasher.hexdigest()

def create_lockfile(config):
    lockfile = config['DATA']['lockfile']
    if os.path.isfile(lockfile):
        old_pid = open(lockfile).readline().strip()
        logging.error("An instance of this script is already running with pid {} otherwise remove lockfile: {}".format(old_pid, lockfile))
        sys.exit(1)
    with open(lockfile, 'w') as f:
        current_pid = os.getpid()
        f.write(str(current_pid))
        f.write(os.linesep)

def remove_lockfile(config):
    lockfile = config['DATA']['lockfile']
    if os.path.isfile(lockfile):
        os.unlink(lockfile)
    

if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('config.ini')
    watch_dir = config['DATA']['watch_dir']
    remote_dir = config['DATA']['remote_dir']
    success = config['DATA']['success_dir']
    errors = config['DATA']['error_dir']

    sftp_client = create_sftp_client(config) 
    #db_client = create_db_client(config)
    try:
        new_files = file_filter(watcher(watch_dir))
        #upload(sftp_client, db_client, remote_dir, success, errors, new_files)
        upload(sftp_client, None, remote_dir, success, errors, new_files)
    finally:
        sftp_client.close()
        #db_client.close()
