import csv, hashlib, logging, os, sys, threading, Pyro4

# Constants
MAX_CHUNK_SIZE = 5242880

# Util parameters 
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename='clientLog.txt',
                    filemode='w')

console = logging.StreamHandler()
console.setLevel(logging.INFO)

formatter = logging.Formatter('%(message)s')
console.setFormatter(formatter)

logging.getLogger('').addHandler(console)


# Startup parameters
currentDirectory = ''


def printWrongCommand():
	print "The command is wrong. Type \'h\' to get help."

def printFullHelp():
	print('The list of commands:')
	print('h:				Prints this message')
	print('q:				Exits the system')
	print('init:				Initializes the storage, removes all the items and prints available size.')
	print('rf <filename>:			Reads the file from KOUR.')
	print('wf <filename>:			Writes the file into KOUR current directory.')
	print('df <filename>:			Deletes the file from KOUR.')
	print('fi <filename>:			Prints the information about the file such as size and addition date.')
	print('od <directory name>:		Makes the chosen directory active.')
	print('rd <directory name>:		Reads and displays a content of the chosen directory.')
	print('md <directory name>:		Creates a new directory inside an active one.')
	print('dd <directory name>:		Deletes the chosen directory.')
	print('mr <parameters file name>:		Deletes the chosen directory.')


class Client(object):
	def __init__(self, name):
		self.name = name
		self.master = Pyro4.Proxy('PYRONAME:KOUR.M')
		self.path = ''

		try:
			self.master.ping()

		except Pyro4.errors.CommunicationError:
			sys.exit('Connection is refused. Please check configuration.')

		except Pyro4.errors.NamingError:
			sys.exit('Master is not found. Please check configuration.')


	def run(self):
		result, size, path, message = self.master.initClient(self.name)

		if not result:
			logging.error(message)
			return

		self.setPath(path)
		logging.info('Path: ' + self.path)

		logging.info('The system is initialized. Available size is {0} bytes'.format(size))


	def md(self, dirName):
		'''
		Just for now I assume that directory name will be well-formed.
		Later I will split string into separate dirs if symbol '/' will occur.
		'''

		contents = self.rd()
		if dirName in contents:
			logging.warn('Directory already exists')
			return
		
		result, path, message = self.master.makeDirectory(self.name, self.path, dirName)

		if not result:
			logging.error(message)


	def rd(self, out=False, dName=''):
		path = self.path

		if dName != '':
			path += '/{0}'.format(dName)

		contents = self.master.readDirectory(self.name, path)
		contents.sort()

		if out:
			if currentDirectory != 'root':
				print '..'

			for name in contents:
				print name

		return contents


	def od(self, dirName):
		if dirName == '..':
			if currentDirectory == 'root':
				logging.warn('You are already at the root of the FS')
				return

			path = '/'.join(self.path.split('/')[:-1])
			self.setPath(path)

		else:
			contents = self.rd()
			if dirName not in contents:
				logging.warn('Directory does not exist')
				return

			self.setPath('{0}/{1}'.format(self.path, dirName))


	def dd(self, dirName):
		logging.info('Deleting dir ' + dirName)
		contents = self.rd(dName=dirName)
		if len(contents) == 0:
			self.master.delDirectory(self.name, self.path, dirName)
			return

		s = raw_input('Directory is not empty. Do you really want to delete it (y/n)? ')
		if s.strip().lower() == 'y':
			self.master.delDirectory(self.name, self.path, dirName)


	def wf(self, path):
		stat = os.stat(path)
		size = stat.st_size

		hasher = hashlib.md5()
		with open(path, 'rb') as f:
			buf = f.read()
			hasher.update(buf)

		fHash = hasher.hexdigest()

		# chunkServers: ordered list of chunk servers to store the portions of the file
		hasSpace, chunkServers = self.master.hasSpace(self.name, fHash, size)

		if not hasSpace:
			logging.warn('It\'s not enough space to write out this file')
			return

		# Storage is available. Write information about file into DFS
		filename = path.split('/')[-1]

		# chunks: [(chunkServerName, chunkName), (..., ...), ...]
		chunks = []
		numberOfChunkServers = len(chunkServers)
		for i in xrange(numberOfChunkServers):
			chunkName = fHash + str(i)
			chunks.append((chunkServers[i].getName(), chunkName))
			chunkServers[i].prepareToWriteFile(self.name, chunkName)

		self.master.fileWrite(self.name, self.path, filename, fHash, size, '', chunks)

		# Send pieces of file to each chunkserver
		currentChunkServerIndex = 0
		
		with open(path, 'rb') as csvfile:
			fileReader = csv.reader(csvfile)

			for row in fileReader:
				print str(currentChunkServerIndex) + ': ' + str(row)
				chunkServers[currentChunkServerIndex].writeLine(row)
				
				currentChunkServerIndex += 1
				if currentChunkServerIndex >= numberOfChunkServers:
					currentChunkServerIndex %= numberOfChunkServers

		# for chunkServer in chunkServers:
		# 	chunkServer.endWriteFile()

		'''
		for row in file_read:
			# print ','.join(row)
			fas[i].writeLine(','.join(row))
			
			i += 1
			i %= 3
			fa0.close()
			fa1.close()
			fa2.close()
		'''

	def rf(self, filename):
		pass


	def fi(self, filename):
		print self.master.fileInfo(self.name, self.path, filename)


	def setPath(self, path):
		self.path = path
		global currentDirectory
		currentDirectory = path.split('/')[-1]
		

def main(argv):
	try:
		name = argv[0].strip().lower()
	except:
		exit('Arguments are incorrect or abscent. Please try again.')

	print('Hello, {0}. Welcome to KOUR file system. Type \'h\' to get help. Type \'q\' to exit.'.format(name))
	client = None

	while True:
		s = raw_input('{0} > '.format(currentDirectory))
		args = s.split(' ')

		command = args[0].strip().lower()

		# Initialize <init>
		if command == 'init':
			client = Client(name)
			logging.info('Initializing system')
			client.run()
			# Try to connect to a server

			# If connected, send a command to clear/create a directory and return it's name

			# Change directory name
			pass

		# File read <rf>
		elif command == 'rf':
			if not clientIsInitialized(client):
				continue

			if len(args) < 2:
				printWrongCommand()
				continue

			filename = args[1].strip().lower()
			client.rf(filename)


		# File write <wf>
		elif command == 'wf':
			if not clientIsInitialized(client):
				continue

			if len(args) < 2:
				printWrongCommand()
				continue

			filename = args[1].strip().lower()
			path = '{0}/{1}'.format(os.getcwd(), filename)
			if not os.path.isfile(path):
				logging.error('File does not exist.')
				continue

			client.wf(filename)
			

		# File delete <df>
		elif command == 'df':
			pass

		# File info <fi>
		elif command == 'fi':
			if len(args) < 2:
				printWrongCommand()
				continue

			fName = args[1].strip().lower()
			client.fi(fName)

		elif command == 'mr':
			if len(args) < 2:
				printWrongCommand()
				continue

			fName = args[1].strip().lower()
			# Parse the file
			# Start the JobTracker on Master
			settings = {}
			with open(fName, 'rb') as settingsFile:
				string = settingsFile.Read()
				settings = eval(string)


		# Open directory <od>
		elif command == 'od':
			if not clientIsInitialized(client):
				continue

			if len(args) < 2:
				printWrongCommand()
				continue

			dirName = args[1].strip().lower()
			client.od(dirName)

		# Read directory <rd>
		elif command == 'rd':
			contents = client.rd(True)
			

		# Make directory <md>
		elif command == 'md':
			if not clientIsInitialized(client):
				continue

			if len(args) < 2:
				printWrongCommand()
				continue

			dirName = args[1].strip().lower()
			client.md(dirName)


		# Delete directory <dd>
		elif command == 'dd':
			if not clientIsInitialized(client):
				continue

			if len(args) < 2:
				printWrongCommand()
				continue

			dirName = args[1].strip().lower()
			client.dd(dirName)

		# Quit <q>
		elif command == 'q':
			break

		# Help <h>
		elif command == 'h':
			printFullHelp()

		else:
			printWrongCommand()


def clientIsInitialized(client):
	if client == None:
		logging.error('Client FS is not initialized, please run init command first.')
		return False

	return True


def test(argv):
	client = Client(argv[0].strip().lower())
	client.run()
	client.md('first_dir')
	client.rd(True)
	print('==============')
	client.md('second_dir')
	client.rd(True)
	print('==============')
	client.od('second_dir')
	client.rd(True)
	print('==============')
	client.md('third_dir')
	client.md('fourth_dir')
	client.rd(True)
	print('==============')
	client.dd('fourth_dir')
	client.rd(True)
	print('==============')
	client.md('fifth_dir')
	client.rd(True)
	print('==============')
	client.od('..')
	client.rd(True)
	print('==============')
	client.wf('client.py')
	client.wf('master.py')
	client.wf('chunk.py')
	client.wf('clientLog.txt')
	client.rd(True)
	print('==============')
	client.fi('client.py')
	# main(argv)

if __name__ == "__main__":
	# test(sys.argv[1:])
	main(sys.argv[1:])