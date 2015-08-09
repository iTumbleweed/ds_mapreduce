__author__ = 'ruslan'

import csv, os

def map():
    with open('Breeding_Bird_Atlases.csv', 'r') as csvfile:
        csvreader = csv.reader(csvfile)
        number = 0
        os.mkdir('textfiles')
        for row in csvreader:
            tuple = row[4], 1
            string = str(tuple)
            fileName = 'textfiles/file' + str(number) + '.txt'
            with open (fileName, 'w') as file:
                file.write(string)
            number += 1


def reduce():
    dict = {}
    for f in os.listdir('textfiles'):
        with open (f, 'r') as file:
            fileContent = file.read()
            split = fileContent.split(',')
            if split[0] in dict:
                dict[split[0]] += 1
    for i in dict:
        print i, ": ", dict[i]

map()
reduce()


