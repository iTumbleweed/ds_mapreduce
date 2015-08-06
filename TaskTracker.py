''' This file contains an implementation of a TaskTracker which can perform
Map, Reduce and Shuffle operations. It also sends heartbeat operations to a JobTracker'''



class TaskTracker(object):
	def __init__(self):
		self.busySlots = 0
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