import csv, Pyro4, sys

class FileAcceptor(object):
	def __init__(self):
		self.f = None
		self.fWriter = None


	def start(self, filename):
		# Open file and save a pointer to it
		self.f = open(filename, 'wb')
		self.fWriter = csv.writer(self.f, delimiter=',', quotechar='|')


	def writeLine(self, line):
		# Append a line. If something goes wrong, close the file.
		try:
			self.fWriter.writerow([s.encode("utf-8") for s in line])
		except Exception, e:
			self.close()
			

	def close(self):
		self.f.close()
		self.f = None


def main():
	# Initiate and run Job Tracker. If nameserver does not exist, exit and inform user
	daemon = Pyro4.Daemon()

	try:
		ns = Pyro4.locateNS()
	except Exception, e:
		sys.exit('Nameserver is not found. Please ensure that it is running and accessible.')
	
	fa0 = FileAcceptor(0)
	fa1 = FileAcceptor(1)
	fa2 = FileAcceptor(2)

	uri = daemon.register(fa0)
	ns.register('KOUR.FA0', uri)

	uri = daemon.register(fa1)
	ns.register('KOUR.FA1', uri)

	uri = daemon.register(fa2)
	ns.register('KOUR.FA2', uri)

	fa0.start()
	fa1.start()
	fa2.start()

	daemon.requestLoop()


if __name__ == '__main__':
	main()