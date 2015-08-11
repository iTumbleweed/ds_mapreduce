import csv, Pyro4, time
from datetime import datetime

# row_count = sum(1 for row in file_read)

def main():
	daemon = Pyro4.Daemon()

	try:
		ns = Pyro4.locateNS()
	except Exception, e:
		sys.exit('Nameserver is not found. Please ensure that it is running and accessible.')

	fa0 = Pyro4.Proxy('PYRONAME:KOUR.FA0')
	fa1 = Pyro4.Proxy('PYRONAME:KOUR.FA1')
	fa2 = Pyro4.Proxy('PYRONAME:KOUR.FA2')
	fas = [fa0, fa1, fa2]

	fName = 'dataset1.csv'
	i = 0
	print datetime.now().strftime('%Y-%m-%d %H:%M:%S')
	with open(fName, 'rb') as csvfile:
		file_read = csv.reader(csvfile)

		for row in file_read:
			# print ','.join(row)
			fas[i].writeLine(row)
			
			i += 1
			i %= 3


		'''
		for row in file_read:
			# print ','.join(row)
			fas[i].writeLine(','.join(row))
			
			i += 1
			i %= 3
		'''


	fa0.close()
	fa1.close()
	fa2.close()

	print datetime.now().strftime('%Y-%m-%d %H:%M:%S')
		# line = file_read.
		# row_count = sum(1 for row in file_read)


	# print row_count
# for row in spamreader:
# 	print ', '.join(row)


if __name__ == '__main__':
	main()