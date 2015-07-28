# !/usr/bin/python
# Copyright 2015 (c) Ruslan Aiginin. Distributed Systems, Innopolis University

import socket, sys, threading, os, pickle, time

nextServer = 0
selfIP = sys.argv[1]

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
address = selfIP.split(':')
sock.bind(('', int(address[1])))    # Bind to IP address specified

serversFileName = 'servers.txt'

# Read lines from servers file
with open(serversFileName) as f:
    serversLines = f.readlines()

# Create list for tuples of slaves' IP addresses
serversCount = len(serversLines)
servers = []  # List of IP addresses

# Add servers to list
for i in xrange(0, serversCount):
    ip = serversLines[i].split(':')
    ip[1] = int(ip[1])
    ipTuple = tuple(ip)
    print ipTuple
    servers.append(ipTuple)

def getServerNumberForDirectory(name):
    for i in xrange(0, len(directories)):
        if directories[i].name == name:
            return directories[i].serverNumber
    return -1

def getServerNumberForFile(name):
    for i in xrange(0, len(directories)):
        if files[i].name == name:
            return files[i].serverNumber
    return -1

def fileRead(name):
    filePath = name.split('/')
    if len(filePath) == 1:
        f = open(name, 'w')
        print(f.read())
    else:
        request = 'fread|' + name
        sock.sendto(request, servers[serverID])

        data, addr = sock.recvfrom(1024)
        print data

def makeDirectory(name):
    os.

while True:
    data, addr = sock.recvfrom(1024)
    packet = data.split('|')
    if packet[0] == 'fread':
        fileRead(packet[1])
    elif packet[0] == 'fwrite':
        fileWrite(packet[1], packet[2])
    elif packet[0] == 'fdel':
        fileDelete(packet[1])
    elif packet[0] == 'finfo':
        fileInfo(packet[1])
    elif packet[0] == 'dread':
        directoryRead(packet[1])
    elif packet[0] == 'dmake':
        makeDirectory(packet[1])
    elif packet[0] == 'ddel':
        deleteDirectory(packet[1])


