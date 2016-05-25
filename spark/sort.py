import sys
import os
from pyspark import SparkContext
from pyspark.conf import SparkConf
from datetime import datetime


def main(argList):	
	# Process command line args
	if len(argList) == 4:
		pass
	else:
		print ("no input file specified and or output")
		usage()
		sys.exit()

	inp = int(argList[2])
	onp = int(argList[3])

	sc = SparkContext() 
	
	
	irdd = sc.textFile(argList[0], inp, use_unicode=True).map(lambda x: (x[0:10],x[10:]))
	ordd = irdd.sortByKey(True, onp).map(lambda x: (x[0] + x[1].strip('\n')) + '\r')
	ordd.saveAsTextFile(argList[1]+'/output')

def usage():
		print 'sort.py <input file or directory> <output directory> <int number of input partitions> <int number of output partitions>'
		return
if __name__ == '__main__':
	main(sys.argv[1:])