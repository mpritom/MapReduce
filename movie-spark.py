from pyspark import SparkContext, SparkConf
import sys
sc = SparkContext()
from itertools import combinations
import numpy as np
from datetime import datetime
from math import sqrt

def loadMovieNames():
    movieNames = []
    with open("movies.dat") as f:
        moviedata = f.readlines()
    moviedata = sc.parallelize(moviedata)
    movieNames=moviedata.map(lambda x: x.strip().split('::')).map(lambda x: x[1])
    return movieNames

def makePairs((user, ratings)):
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return ((movie1, movie2), (rating1, rating2))

def filterDuplicates( (userID, ratings) ):
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return movie1 < movie2
  
def computeCosineSimilarity(ratingPairs):
    numPairs = 0
    sum_xx = sum_yy = sum_xy = 0
    for ratingX, ratingY in ratingPairs:
        sum_xx += ratingX * ratingX
        sum_yy += ratingY * ratingY
        sum_xy += ratingX * ratingY
        numPairs += 1

    numerator = sum_xy
    denominator = sqrt(sum_xx) * sqrt(sum_yy)

    score = 0
    if (denominator):
        score = (numerator / (float(denominator)))

    return (score, numPairs)
   
#### read the files
with open("ratings.dat") as f:
    ratingdata = f.readlines()
ratingdata = sc.parallelize(ratingdata)


   
#with open("movies.dat") as f:
#    moviedata = f.readlines()
#moviedata = sc.parallelize(moviedata)
#movie = ["Toy Story (1995)"]

#### read the file line by line and split items

#m1=moviedata.map(lambda x: x.strip().split('::')).map(lambda x: (x[0],x[1]))
#mid=m1.filter(lambda x: x[0] in movie).map(lambda x: x[0]).take(100)
#mname=m1.filter(lambda x: x[0] in movie).take(100)
r1=ratingdata.map(lambda x: x.strip().split('::')).map(lambda x: (int(x[0]),(int(x[1]),float(x[2]))))


ratingsPartitioned = r1.partitionBy(100)
joinedRatings = ratingsPartitioned.join(ratingsPartitioned)
uniqueJoinedRatings = joinedRatings.filter(filterDuplicates)

nameDict = loadMovieNames()
#m_head= nameDict.take(10)
print "first 10 items in m1:", m_head
moviePairs = uniqueJoinedRatings.map(makePairs).partitionBy(100)
moviePairRatings = moviePairs.groupByKey()

moviePairSimilarities = moviePairRatings.mapValues(computeCosineSimilarity).persist()

# Save the results if desired
moviePairSimilarities.sortByKey()
moviePairSimilarities.saveAsTextFile("movie-sims")

scoreThreshold = 0.60
coOccurenceThreshold = 15
movieID = int(sys.argv[1])
filteredResults = moviePairSimilarities.filter(lambda((pair,sim)): \
        (pair[0] == movieID or pair[1] == movieID) \
        and sim[0] > scoreThreshold and sim[1] > coOccurenceThreshold)
results = filteredResults.map(lambda((pair,sim)): (sim, pair)).sortByKey(ascending = False).take(10)

m_head= nameDict.take(10)
print("Top 10 similar movies for " +nameDict[movieID])
for result in results:
 (sim, pair) = result
 similarMovieID = pair[0]
 if (similarMovieID == movieID):
  similarMovieID = pair[1]
 print(nameDict[similarMovieID] + "\tscore: " + str(sim[0]) + "\tstrength: " + str(sim[1]))
 
 
#### just to print out the first 10 items of m1 and r1
#m_head = m1.take(10)
#r_head = r1.take(10)
#print "first 10 items in m1:", m_head
