''' This file contains an implementation of a TaskTracker which can perform
Map, Reduce and Shuffle operations. It also sends heartbeat operations to a JobTracker'''

# Constants
MAX_SLOTS = 4

class TaskTracker(object):
	def __init__(self):
		self.emptySlots = MAX_SLOTS
		

	def processJob(self):
		'''
		Processes any job.

		Returns:
			Nothing
		'''
		pass


	def Map(self):
		'''
		Executes a Map task.

		Returns: 
			Nothing
		'''
		pass


	def Reduce(self):
		'''
		Executes a Reduce task

		Returns: 
			Nothing
		'''
		pass


	def Shuffle(self):
		'''
		Executes a Shuffle task.

		Returns: 
			Nothing
		'''
		pass


	def sendHeartbeat(self):
		'''
		Sends a heartbeat message containing number of empty slots to the JobTracker

		Returns: 
			Nothing
		'''
		pass