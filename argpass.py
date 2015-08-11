import random

def paramChangingFunc(key, value, dict):
	dict[key] = value


def main():
	dict = {}

	for i in xrange(10):
		paramChangingFunc(i, random.random(), dict)
	
	print dict

if __name__ == '__main__':
	main()