import logging, math, sys, threading, time, Pyro4
from chunk import *

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename='masterLog.txt',
                    filemode='w')

console = logging.StreamHandler()
console.setLevel(logging.INFO)

formatter = logging.Formatter('%(message)s')
console.setFormatter(formatter)

logging.getLogger('').addHandler(console)


MAX_CHUNKS = 10
MAX_CHUNK_SIZE = 5242880

# This is an index server
class Master(object):
	def __init__(self):
		self.chunkServers = []
		self.fs = {}

		# hash is a key. name, size and chunkServer are values
		self.files = {} 

		for i in xrange(MAX_CHUNKS):
			c = Pyro4.Proxy('PYRONAME:KOUR.C{0}'.format(i))

			try:
				if c.ping():
					self.chunkServers.append(('C{0}'.format(i), c))

			except Pyro4.errors.NamingError:
				break

		if len(self.chunkServers) == 0:
			sys.exit('No chunk servers are found. Please check configuration.')

		logging.info('Master has been started')


	def ping(self):
		logging.info('Ping request received')
		return True


	def initClient(self, clientName):
		totalSpace = 0

		logging.info('Init request accepted from client {0}'.format(clientName))
		self.fs[clientName] = {'root': {}}

		for cname, c in self.chunkServers:
			logging.info('Sending request to a chunk server {0}'.format(cname))
			totalSpace += c.initFS(clientName)

		return True, totalSpace, 'root', ''


	def fileRead(self, clientName, path, filename):
		logging.info('Reading file {0}'.format(filename))

		try:
			tree = self.fs[clientName]
		except Exception, e:
			logging.error('FS is not initialized! Please run initialization command first.')
			return 

		pParts = path.split('/')
		for part in pParts:
			tree = tree[part]

		fhash = tree[filename]
		chunkName = self.files[clientName][fHash]


	def fileWrite(self, clientName, path, filename, filehash, size, chunkname, chunks=None):
		'''
		Saves file info into DFS.
		Parameters
			clientName: String. Name of the user of the system.
			path: String. Current DFS path.
			filename: String. Human readable file name.
			filehash: String. Hash of the whole file.
			size: Float. File size.
			chunkname: String. Name of the chunk that stores the file.
			chunks: List. Contains tuples in form of (chunk server name, physical file name)
		'''

		logging.info('Writing file {0}'.format(path))

		# Build a new file into a DFS tree. Save filename and file hash which acts as a key for
		# the dictionary that stores file info.
		try:
			tree = self.fs[clientName]
		except Exception, e:
			logging.error('FS is not initialized! Please run initialization command first.')
			return 

		pParts = path.split('/')
		for part in pParts:
			tree = tree[part]

		tree[filename] = filehash
		if not clientName in self.files.keys():
			self.files[clientName] = {}

		# self.files[clientName][filehash] = {'size': size, 'cName': chunkname, 'date': time.strftime("%d/%m/%Y")}
		# Store file info and addresses of file parts
		self.files[clientName][filehash] = {'size': size, 'date': time.strftime("%d/%m/%Y"), 'chunks': chunks}


	def fileDelete(self, filename):
		logging.info('Deleting file {0}'.format(filename))


	def fileInfo(self, clientName, path, filename):
		logging.info('Getting file info for {0}'.format(filename))

		try:
			tree = self.fs[clientName]
		except Exception, e:
			logging.error('FS is not initialized! Please run initialization command first.')
			return 

		pParts = path.split('/')
		for part in pParts:
			tree = tree[part]

		fhash = tree[filename]
		return 'File name: {0}\nSize: {1}\nChunk server: {2}\nCreated: {3}'.format(filename, self.files[clientName][fhash]['size'], self.files[clientName][fhash]['cName'], self.files[clientName][fhash]['date'])


	def openDirectory(self, dirname):
		logging.info('Opening directory {0}'.format(filename))
		if dirname == '..':
			pass
			# navigate up
			logging.warn('You are at the root directory')
		else:
			pass


	def readDirectory(self, clientname, path):
		logging.info('Reading directory {0}'.format(path))

		try:
			tree = self.fs[clientname]
		except Exception, e:
			logging.error('FS is not initialized! Please run initialization command first.')
			return 

		pParts = path.split('/')
		for part in pParts:
			tree = tree[part]

		return tree.keys()



	def makeDirectory(self, clientname, path, dirname):
		'''
		Parses path and travel from root to the end. If another key does not exist,
		returns a piece of path and error message.

		Adds a new directory name to the end of the path.

		Returns
			result: result of the operation
			path: new current path (unchanged if operation is successful)
			message: error message
		'''
		logging.info('Creating directory {0}'.format(dirname))
		pParts = path.split('/')

		# TODO: make a separated method
		try:
			tree = self.fs[clientname]
		except Exception, e:
			logger.error('FS is not initialized! Please run initialization command first.')
			return 
		
		# TODO: make a separated method
		for part in pParts:
			tree = tree[part]

		tree[dirname] = {}
		return True, path, ''


	def delDirectory(self, clientname, path, dirname):
		logging.info('Deleting directory {0}'.format(dirname))
		pParts = path.split('/')

		# TODO: make a separated method
		try:
			tree = self.fs[clientname]
		except Exception, e:
			logger.error('FS is not initialized! Please run initialization command first.')
			return 
		
		# TODO: make a separated method
		for part in pParts:
			tree = tree[part]

		del tree[dirname]


	def heartbeat(name):
		logging.info('Client {0} sent a heartbeat message')


	def hasSpace(self, clientName, hash, size):
		'''
		Finds the most appropriate chunk servers to store the files.
		'''

		# Find out if we have necessary amount of free space
		maxNumberOfChunks = 0

		for cname, c in self.chunkServers:
			maxNumberOfChunks += c.freeStorage(clientName) / MAX_CHUNK_SIZE

		numberOfChunks = int(math.ceil(size / float(MAX_CHUNK_SIZE)))

		if maxNumberOfChunks < numberOfChunks:
			return False, None

		currentChunkServerIndex = int(hash, 16) % len(self.chunkServers)
		chunkServers = []
		for i in xrange(numberOfChunks):
			cname, c = self.chunkServers[currentChunkServerIndex]
			while not c.freeStorage(clientName) > size:
				currentChunkServerIndex = (currentChunkServerIndex + 1) % len(self.chunkServers)

			chunkServers.append(self.chunkServers[currentChunkServerIndex][1])
			currentChunkServerIndex = (currentChunkServerIndex + 1) % len(self.chunkServers)

		for c in chunkServers:
			print c.getName()

		return True, chunkServers

		# cname, c = self.chunkServers[int(hash, 16) % len(self.chunkServers)]

		# if c.freeStorage(clientName) > size:
		# 	return True, c

		# for cname, c in self.chunkServers:
		# 	if c.freeStorage(clientName) > size:
		# 		return True, c

		return False, None


class fileInfo(object):
	def __init__(self, name, fHash):
		self.name = name
		self.hash = fHash
		self.type = name.split('.')[-1]
		self.size = 1



def main():
	daemon = Pyro4.Daemon()
	try:
		ns = Pyro4.locateNS()
	except Exception, e:
		sys.exit('Nameserver is not found. Please ensure that it is running and accessible.')

	m = Master()
	uri = daemon.register(m)
	ns.register('KOUR.M', uri)

	# m.run()
	daemon.requestLoop()
	

if __name__ == '__main__':
	main()

''' Distribution between chunkServers
digest = md5.hexdigest()
number = int(digest, 16)

print(number % YOUR_NUMBER)
'''