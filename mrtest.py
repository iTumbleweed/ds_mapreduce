import csv, sys
sys.path.insert(0, '/Users/cmu/Documents/A4/Example1')
import map1, reduce1
# from map1 import *
# from reduce1 import *

def main():
	m = map1.Map
	mapResult = []
	lineOffset = 0

	# Map
	with open('dataset1.csv', 'rb') as csvfile:
		fileReader = csv.reader(csvfile)
		for row in fileReader:
			if lineOffset != 0:
				m(lineOffset, row, mapResult)
				
			lineOffset += 1			

	# Shuffle and Sort [(a, 1)] -> {a:[1]}
	reduceData = {}
	for key, value in mapResult:
		if key not in reduceData.keys():
			reduceData[key] = []

		reduceData[key].append(value)

	# Reduce
	reduceResult = []
	for key in reduceData.keys():
		reduce1.Reduce(key, reduceData[key], reduceResult)

	print reduceResult


if __name__ == '__main__':
	main()