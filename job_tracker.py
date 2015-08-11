'''
JobTracker stores the number of available slots which it receives from TaskTrackers along
with heartbeat messages.
'''

import logging, Pyro4

logging.basicConfig(level = logging.DEBUG, format='%(message)s')


class JobTracker(object):
	def __init__(self):
		# {name -> numberOfSlots}
		self.taskTrackers = {}


	def registerTaskTracker(self, taskTrackerName):
		'''
		Registers a new Task Tracker. Adds number of available slotd to a dictionary.
		'''
		pass


	def startJob(self, job):
		'''
		Finds out available Task Trackers and distributes the job.

		Returns
			Result: boolean value indicating operation success

		Throws:
			TODO
		'''
		pass


	def stopJob(self, job):
		'''
		Stops the running job.

		Returns
			Result: boolean value indicating operation success

		Throws:
			TODO
		'''
		pass


	def findData(self, job):
		'''
		Refer to Name Node to find data files for the job. List of data files is stored internally and
		is available through TODO function

		Returns
			Result: boolean value indicating operation success
		'''
		pass


	def receiveHeartbeat(self, taskTrackerName, slots):
		'''
		Receives heartbeat messages from Task Trackers and updates abailable slots number.

		Returns
			Nothing
		'''
		pass


	def getInfo(self, jobId):
		'''
		Returns the information about the job.

		Returns
			Result: a JobResult object
		'''
		pass


	def ping(self):
		'''
		Allows clients check whether Job Tracker is alive.

		Returns
			Result: True 
		'''

		logging.info('Ping request received')
		return True


def main():
	# Initiate and run Job Tracker. If nameserver does not exist, exit and inform user
	daemon = Pyro4.Daemon()

	try:
		ns = Pyro4.locateNS()
	except Exception, e:
		sys.exit('Nameserver is not found. Please ensure that it is running and accessible.')
	
	jobTracker = JobTracker()
	uri = daemon.register(jobTracker)
	ns.register('KOUR.JT', uri)

	daemon.requestLoop()


if __name__ == '__main__':
	main()