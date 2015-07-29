import logging, sys

logging.basicConfig(level = logging.DEBUG, format='%(message)s')

class JobClient(object):
	def __init__(self):
		pass

	def runJob(self, conf):
		pass


def main(argv):
	# TODO: Check and open file. Initialize conf variable with contents of the file
	conf = ''
	jc = JobClient()
	jc.runJob(conf)


if __name__ == '__main__':
	main(sys.argv[1:])