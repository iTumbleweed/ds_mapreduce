# !/usr/bin/python
# Copyright 2015 (c) Ruslan Aiginin. Distributed Systems, Innopolis University

import socket, sys
selfIP = sys.argv[1]
masterIPstr = sys.argv[2]
masterIPsplit = masterIPstr.split(':')
masterIP = (masterIPsplit[0], int(masterIPsplit[1]))

sock = socket.socket()  # Send socket
address = selfIP.split(':')
sock.connect(('localhost', masterIP[1]))

possibleCommands = ['fread', 'fwrite', 'finfo', 'fdel', 'dread', 'dmake', 'ddel']

print 'List of commands: \n fread <file path>  \n fwrite <source file location> <target folder> \n fdel <file path> ' \
      '\n finfo <file path>  \n dread <directory path>  \n dmake <directory path> \n ddel <directory path>'

while True:
    userInput = raw_input("Please enter a command: ")
    split = userInput.split(' ')
    if split[0] in possibleCommands:
        sock.send(userInput)
        #conn, addr = sock.accept()
        data = sock.recv(4096)
        print data
    else:
        print 'Invalid command'

sock.close()
