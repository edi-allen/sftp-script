#!/usr/bin/env python3
import argparse
import os
import sftp

parser = argparse.ArgumentParser(description='Upload file via proxy')
parser.add_argument('--host', help='destination host', required=True)
parser.add_argument('--port', type=int, help='destination port', default=22)
parser.add_argument('--username', help='user account for destination host', required=True)
parser.add_argument('--key', help='rsa private key', required=True)
parser.add_argument('--proxy', help='hostname and port of the proxy', required=True)
parser.add_argument('--source',help='source filename', required=True)
parser.add_argument('--dest', help='destination filename')

args = parser.parse_args()

if not os.path.exists(args.source):
    raise Exception("file to be uploaded does not exist")

host = args.host
port = args.port
username = args.username
key = args.key
proxy = args.proxy
source = args.source
dest = args.dest if args.dest else os.path.basename(args.source)

client = sftp.SftpClient(args.host, args.port, args.username, args.key, args.proxy)

client.upload(source, dest)

client.close()
