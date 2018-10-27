from pyspark import SparkContext, SparkConf
import sys
import io
sc = SparkContext()
from itertools import combinations
import numpy as np
from datetime import datetime
from math import sqrt


def makePairs((user, ratings)):
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return ((movie1, movie2), (rating1, rating2))

def filterDuplicates( (userID, ratings) ):
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return movie1 < movie2
  

def stat_correlation(ratingPairs):
    numPairs = 0
    sum_xx = sum_yy = sum_xy = sum_x = sum_y = 0
    for ratingX, ratingY in ratingPairs:
        sum_x += ratingX
        sum_y += ratingY
        sum_xx += ratingX * ratingX
        sum_yy += ratingY * ratingY
        sum_xy += ratingX * ratingY
        numPairs += 1
    sum_x_y = sum_x * sum_y

    numerator = numPairs * sum_xy - sum_x_y 
    denominator = sqrt(numPairs * sum_xx - sum_x * sum_x) * sqrt(numPairs * sum_yy - sum_y * sum_y )

    score = 0
    if (denominator):
        score = (numerator / (float(denominator)))

    return (score, numPairs)
   
#### read the files
with open("ratings.dat") as f:
    ratingdata = f.readlines()
ratingdata = sc.parallelize(ratingdata)
moviedata={}
with open("movies.dat") as f:
    for line in f:
       (mid, name) = line.split('::')
       moviedata[int(mid)] = name
    
#moviedata = sc.parallelize(moviedata)

movie = ["Toy Story (1995)","Waiting to Exhale (1995)","Sudden Death (1995)"]

#m1=moviedata.map(lambda x: x.strip().split('::')).map(lambda x: (x[0],x[1]))
#mid=m1.filter(lambda x: x[1] in movie).map(lambda x: x[0]).take(100)
#mname=m1.filter(lambda x: x[1] in movie).map(lambda x: x[1]).take(100)


r1=ratingdata.map(lambda x: x.strip().split('::')).map(lambda x: (int(x[0]),(int(x[1]),float(x[2]))))

ratingsPartitioned = r1.partitionBy(100)
joinedRatings = ratingsPartitioned.join(ratingsPartitioned)
uniqueJoinedRatings = joinedRatings.filter(filterDuplicates)

moviePairs = uniqueJoinedRatings.map(makePairs).partitionBy(100)
moviePairRatings = moviePairs.groupByKey()

moviePairSimilarities = moviePairRatings.mapValues(stat_correlation).persist()

# sort the results by key
moviePairSimilarities.sortByKey()

scoreThreshold = 0.4
coOccurenceThreshold = 5
movieID = int(sys.argv[1]) #movieID = 1 which is Toy Story; Given as command line argv

filteredResults = moviePairSimilarities.filter(lambda((pair,sim)): \
        (pair[0] == movieID or pair[1] == movieID) \
        and sim[0] > scoreThreshold and sim[1] > coOccurenceThreshold)
results = filteredResults.map(lambda((pair,sim)): (sim, pair)).sortByKey(ascending = False).take(10)

print("Top 10 similar movies for Toy story: ")

for result in results:
   # movie_list= []
    (sim, pair) = result
    similarMovieID = pair[0]
    if (similarMovieID == movieID):
        similarMovieID = pair[1]
    #movie_list.append(similarMovieID)
    #similarMovieName=m1.filter(lambda x: x[0] in movie_list).map(lambda x: x[1])
    similarMovieName = moviedata.get(int(similarMovieID))
    print("["+str(moviedata.get(int(movieID)))+","+ str(similarMovieName) + ", correlation = " + str(sim[0]) + ", co-occurance =  " + str(sim[1])+ "]")
  
