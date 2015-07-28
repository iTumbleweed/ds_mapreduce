# !/usr/bin/python
# Copyright 2015 (c) Ruslan Aiginin. Distributed Systems, Innopolis University

import socket, sys, os, time

selfIP = sys.argv[1]

sock = socket.socket()  # Receive socket
address = selfIP.split(':')
sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

sock.bind(('', int(address[1])))  # Bind to IP address specified

sock.listen(10)

folderName = address[1]  # Create a folder based on port number

def makeDirectory(name):
    updatedName = 'DFS/' + name
    if not os.path.exists(updatedName):
        os.makedirs(updatedName)
        print 'Directory', name, 'created.'
    else:
        print 'Directory', name, 'already exists.'

def fileWrite(name, extension):
    filePath = 'DFS/' + folderName + '/' + name + '.' + extension
    with open(filePath, 'w') as f:
        l = conn.recv(4096)
        while l:
            print 'Receiving the file...'
            f.write(l)
            l = conn.recv(4096)
        print 'File received.'

def fileRead(hash, extension):
    print 'Reading the file'
    actualLocation = 'DFS/' + folderName + '/' + hash + '.' + extension
    if os.path.isfile(actualLocation):
        print 'File', actualLocation, 'exists'

        request = 'read|' + str(hash) + '|' + extension
        conn.send(request)

        #time.sleep(0.5)

        with open (actualLocation, 'r') as file:
            l = file.read(4096)
            while (l):
                print 'Sending the file...'
                conn.send(l)
                l = file.read(4096)
            conn.send('done')
            print 'File sent'

    else:
        print 'File', actualLocation,  'does not exist'
        request = 'no file'
        conn.send(request)

def fileInfo(hash, extension):
    actualLocation = 'DFS/' + folderName + '/' + hash + '.' + extension
    if os.path.isfile(actualLocation):
        print 'File', actualLocation, 'exists'

        stats = os.stat(actualLocation)

        print stats

        response = 'info|' + str(stats)
        conn.send(response)

    else:
        print 'File', actualLocation,  'does not exist'
        response = 'no file'
        conn.send(response)

makeDirectory(folderName)

conn, addr = sock.accept()

while True:
    data = conn.recv(4096)
    packet = data.split('|')
    if packet[0] == 'write':
        fileWrite(packet[1], packet[2])
    if packet[0] == 'read':
        fileRead(packet[1], packet[2])
    if packet[0] == 'info':
        fileInfo(packet[1], packet[2])

conn.close()