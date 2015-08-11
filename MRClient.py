import logging, sys, Pyro4

logging.basicConfig(level = logging.DEBUG, format='%(message)s')


# Constants
JOB_TRACKER_NAME = 'KOUR.JT'

class MRClient(object):
	def __init__(self, jobTracker):
		self.jobTracker = jobTracker
		pass


	def runJob(self, aJob):
		'''
		Parameters
			aJob: Job instance to run.

		Returns
			Nothing
		'''
		pass


def main(argv):
	# Initiate and run Job Tracker. If nameserver does not exist, exit and inform user
	daemon = Pyro4.Daemon()

	try:
		ns = Pyro4.locateNS()
	except Exception, e:
		sys.exit('Nameserver is not found. Please ensure that it is running and accessible.')

	jobTracker = Pyro4.Proxy('PYRONAME:{0}'.format(JOB_TRACKER_NAME))

	try:
		jobTracker.ping()

	except Pyro4.errors.NamingError:
		exit('Job Tracker is not accessible')

	except AttributeError, exc:
		exit('Error: {0}'.format(exc)

	# Initiate and run a client. Pass a Job Tracker as a parameter.
	# client = MRClient(jobTracker)

	# TODO: Check and open file. Initialize conf variable with contents of the file
	conf = ''



	# jc = JobClient()
	# jc.runJob(conf)


if __name__ == '__main__':
	# main()
	main(sys.argv[1:])