'''
JobTracker stores the number of available slots which it receives from TaskTrackers along
with heartbeat messages.
'''

class JobTracker(object):
	def __init__(self):
		// {name -> numberOfSlots}
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


def main():
	pass


if __name__ == '__main__':
	main()