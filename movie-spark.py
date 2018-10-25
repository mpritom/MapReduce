from pyspark import SparkContext, SparkConf
sc = SparkContext()
from itertools import combinations
import numpy as np
from datetime import datetime
 
#### read the files
with open("ratings.dat") as f:
    ratingdata = f.readlines()
ratingdata = sc.parallelize(ratingdata)
 
with open("movies.dat") as f:
    moviedata = f.readlines()
moviedata = sc.parallelize(moviedata)
 
movie = ["Toy Story (1995)","Waiting to Exhale (1995)","Sudden Death (1995)"]

#### read the file line by line and split items

m1=moviedata.map(lambda x: x.strip().split('::')).map(lambda x: (x[0],x[1]))
mid=m1.filter(lambda x: x[1] in movie).map(lambda x: x[0]).take(100)
mname=m1.filter(lambda x: x[1] in movie).take(100)
r1=ratingdata.map(lambda x: x.strip().split('::')).map(lambda x: (x[0],(x[1],x[2])))
 

  
#### just to print out the first 10 items of m1 and r1
m_head = m1.take(10)
r_head = r1.take(10)
print "first 10 items in m1:", m_head
print "first 10 items in r1:", r_head
