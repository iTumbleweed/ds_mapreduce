def Reduce(key, values, list):
	'''
	This Reduce function takes a string from file with accidents in United Kingdom,
	gets the weekday and writes out (weekday: 1) pair

	Parameters
		key: Line offset. Can be ignored.
		value: String from comma-separated value file.
		list: output parameter. A list that stores all the output keys and values.
	'''

	if list is None:
		list = []

	list.append((key, len(values)))