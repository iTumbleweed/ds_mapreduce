import hashlib, logging, os, shutil, threading, Pyro4
from file_acceptor import *

logging.basicConfig(level = logging.DEBUG,
                    format='%(message)s')

MAX_CHUNKS = 5

# This is a storage server
class Chunk(object):
	def __init__(self, chunkName):
		self.fileAcceptor = FileAcceptor()
		self.name = chunkName
		self.storage = 52428800


	def ping(self):
		logging.info('Ping request received')
		return True


	def path(self, clientName):
		return '{0}/{1}/{2}'.format(os.getcwd(), self.name, hashlib.md5(clientName.encode()).hexdigest())


	def initFS(self, clientName):
		logging.info('Init request accepted from master on behalf of client {0}'.format(clientName))
		path = self.path(clientName)
		logging.info('Initializing FS in local directory {0}'.format(path))

		if os.path.exists(path):
			self.rmDir(path)

		os.makedirs(path)

		return self.freeStorage(path)


	def rmDir(self, path):
		shutil.rmtree(path)
		# for f in os.listdir(path):
		#     fPath = os.path.join(path, f)
		#     try:
		#         if os.path.isfile(fPath):
		#             os.unlink(fPath)
		#         #elif os.path.isdir(file_path): shutil.rmtree(file_path)
		#     except Exception, e:
		#         print e

	def rmFile(self, path):
		try:
			os.unlink(path)
		except:
			return false

		return true


	def freeStorage(self, clientName):
		total_size = 0
		path = self.path(clientName)

		for dirpath, dirnames, filenames in os.walk(path):
			for f in filenames:
				fp = os.path.join(dirpath, f)
				total_size += os.path.getsize(fp)

		return self.storage - total_size


	def prepareToWriteFile(self, clientName, filename):
		path = self.path(clientName)
		self.fileAcceptor.start(path + '/' + filename)


	def writeLine(self, line):
		if self.fileAcceptor != None:
			self.fileAcceptor.writeLine(line)


	def endWriteFile(self):
		if self.fileAcceptor != None:
			self.fileAcceptor.close()


	def getName(self):
		return self.name


def main():
	daemon = Pyro4.Daemon()
	try:
		ns = Pyro4.locateNS()
	except Exception, e:
		sys.exit('Nameserver is not found. Please ensure that it is running and accessible.')

	for i in xrange(MAX_CHUNKS):
		c = Chunk('C{0}'.format(i))
		uri = daemon.register(c)
		ns.register('KOUR.C{0}'.format(i), uri)

	daemon.requestLoop()
	
	# Discover root directory
	
	pass


if __name__ == '__main__':
	main()