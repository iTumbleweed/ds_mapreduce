# !/usr/bin/python
# Copyright 2015 (c) Ruslan Aiginin. Distributed Systems, Innopolis University

import socket, sys, os, time, shutil, hashlib, re

directories = []
files = []
nextServer = 0
selfIP = sys.argv[1]
slaveSockets = []

class Directory:
    def __init__(self, name, server):
        self.name = name
        self.serverNumber = server
        self.directories = []
        self.files = []

class File:
    def __init__(self, name, server):
        self.name = name
        self.serverNumber = server

sock = socket.socket()  # Receive socket
address = selfIP.split(':')
sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
sock.bind(('', int(address[1])))    # Bind to IP address specified
sock.listen(10)

sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

serversFileName = 'servers.txt'

# Read lines from servers file
with open(serversFileName) as f:
    serversLines = f.readlines()

# Create list for tuples of slaves' IP addresses
serversCount = len(serversLines)

# Add servers to list
for i in xrange(0, serversCount):
    ip = serversLines[i].split(':')
    ip[1] = int(ip[1])
    slaveSocket = socket.socket()
    slaveSocket.connect((ip[0], ip[1]))
    slaveSockets.append(slaveSocket)

def makeDirectory(name):
    updatedName = 'DFS/' + name
    if not os.path.exists(updatedName):
        os.makedirs(updatedName)
        print 'Directory', name, 'created.'
    else:
        print 'Directory', name, 'already exists.'

def deleteDirectory(name):
    updatedName = 'DFS/' + name
    if os.path.exists(updatedName):
        shutil.rmtree(updatedName)
        print 'Directory', name, 'deleted.'
    else:
        print 'Directory', name, 'does not exist.'

def directoryRead(name):
    if not '..' in name:
        updatedName = 'DFS/' + name
        if os.path.exists(updatedName):
            # print folder contents
            directoryFiles = os.listdir(updatedName)
            if len(directoryFiles) > 0:
                output = ''
                for member in directoryFiles:
                    memberStr = str(member)
                    memberStr = re.sub('\.txt$', '', memberStr)  # REGULAR EXPRESSION !!!!
                    output = output + memberStr + '   '
                return output
            else:
                return 'This directory is empty.'
        else:
            return 'Directory', name, 'does not exist.'
    else:
        return 'No messing with .. please ;) Try again'

def fileWrite(sourceFilePath, targetLocation):
    global nextServer
    if os.path.isfile(sourceFilePath):
        fileName = os.path.basename(sourceFilePath)
        actualLocation = 'DFS/' + targetLocation
        actualFilePath = actualLocation + '/' + fileName + '.txt'
        makeDirectory(targetLocation)
        with open(actualFilePath, 'w') as f:
            f.write(str(nextServer) + '\n')
            s = hashlib.sha1()
            s.update(fileName)
            hash = s.hexdigest()
            f.write(str(hash))

        name, extension = os.path.splitext(fileName)

        request = 'write|' + str(hash) + '|' + extension
        slaveSockets[nextServer].send(request)
        time.sleep(0.5)

        with open (sourceFilePath, 'r') as file:
            l = file.read(4096)
            while (l):
                print 'Sending the file...'
                slaveSockets[nextServer].send(l)
                l = file.read(4096)
            print 'File sent'

        if nextServer < serversCount - 1:
            nextServer += 1
        else:
            nextServer = 0

        return 'File was written successfully.'

    else:
        return 'This file does not exist.'

def fileRead(filePath):
    print 'Reading the file'
    actualLocation = 'DFS/' + filePath + '.txt'
    if os.path.isfile(actualLocation):
        fileName = os.path.basename(actualLocation)
        fileName = re.sub('\.txt$', '', fileName)
        name, extension = os.path.splitext(fileName)

        with open (actualLocation, 'r') as f:
            data = f.readlines()
        serverNumber = int(data[0])
        hash = data[1]

        print 'server number:', serverNumber
        print 'hash:', hash

        request = 'read|' + str(hash) + '|' + extension
        slaveSockets[serverNumber].send(request)

        response = slaveSockets[serverNumber].recv(4096)

        print 'response received.'

        packet = response.split('|')
        if packet[0] == 'read':
            incName = packet[1] + '.' + packet[2]
            with open(incName, 'w') as file:
                l = slaveSockets[serverNumber].recv(4096)
                while (l):
                    print 'Receiving the file...'
                    file.write(l)
                    l = slaveSockets[serverNumber].recv(4096)

                print 'file received.'
                return 'File received.'

        else:
            return 'This file does not exist.'

        # send read request to server
        # print reply
    else:
        return 'This file does not exist.'

def fileDelete(filePath):
        actualLocation = 'DFS/' + filePath + '.txt'
        if os.path.isfile(actualLocation):
            os.remove(actualLocation)
            return 'File deleted.'
        else:
            return 'This file does not exist'
        # Open text file
        # extract server number and file name
        # send delete request to server
        # print reply


def fileInfo(filePath):
        actualLocation = 'DFS/' + filePath + '.txt'
        if os.path.isfile(actualLocation):
            fileName = os.path.basename(actualLocation)
            fileName = re.sub('\.txt$', '', fileName)
            name, extension = os.path.splitext(fileName)

            with open (actualLocation, 'r') as f:
                data = f.readlines()
            serverNumber = int(data[0])
            hash = data[1]

            print 'server number:', serverNumber
            print 'hash:', hash

            request = 'info|' + str(hash) + '|' + extension
            slaveSockets[serverNumber].send(request)

            response = slaveSockets[serverNumber].recv(4096)

            print 'response', response

            packet = response.split('|')
            if packet[0] == 'info':
                return str(packet[1])
            else:
                return 'This file does not exist.'

conn, addr = sock.accept()

while True:
    data = conn.recv(1024)
    packet = data.split(' ')
    if packet[0] == 'fread':
        response = fileRead(packet[1])
        conn.send(response)
    elif packet[0] == 'fwrite':
        response = fileWrite(packet[1], packet[2])
        conn.send(response)
    elif packet[0] == 'fdel':
        response = fileDelete(packet[1])
        conn.send(response)
    elif packet[0] == 'finfo':
        response = fileInfo(packet[1])
        conn.send(response)
    elif packet[0] == 'dread':
        if len(packet) == 1:
            packet.append('')
        response = directoryRead(packet[1])
        conn.send(response)
    elif packet[0] == 'dmake':
        makeDirectory(packet[1])
        response = 'Directory created'
        conn.send(response)
    elif packet[0] == 'ddel':
        deleteDirectory(packet[1])
        response = 'Directory deleted'
        conn.send(response)

conn.close()
for i in xrange(0, serversCount):
    slaveSockets[i].close()
