import numbers
from six import string_types
from mrjob.job import MRJob
from mrjob.step import MRStep
from itertools import *
import numpy
from scipy import spatial

class MRMovie(MRJob):

	def steps(self):
		return [
		MRStep(mapper=self.mapper_get_names,
			reducer=self.reducer_get_names),
        	MRStep(reducer=self.reducer_create_pairs),
		MRStep(reducer=self.reducer_rating_pairs),
          	MRStep(reducer=self.r_similarity)
		MRStep(reducer=self.reducer_output),
		MRStep(reducer=self.reducer_sort_results)]	

	def configure_options(self):
		super(MRMovie, self).configure_options()
		self.add_passthrough_option(
            '-l', '--lowerbound', action="store", type='float',
            default=0.4, help='Lower bound of similarity.')
		self.add_passthrough_option(
            '-p', '--minpairs', action="store", type='int',
            default=8, help='Minimum number of rating pairs.')
		self.add_passthrough_option(
            '-m', '--moviename', action="append",
            default=[], help='Movie name to look up for similar movies')
		self.add_passthrough_option(
            '-k', '--numofitems', action="store", type='int',
            default=15, help='Number of similar movies to show.')

	def mapper_get_names(self, _, line):

		if len(self.options.moviename) == 0:
			raise Exception('Must specify at least one search item.')

		mid, uid, name, rating = '0','0','0','0'
		line = unicode(line, errors='ignore')
		splits = line.strip().split('::')
		if len(splits) == 4:
			uid,mid,rating = splits[0],splits[1],splits[2]
		else:
			mid,name = splits[0],splits[1]
		yield mid, (uid,name,rating)
	

	def reducer_get_names(self, key, values):
		last_name = None
		for value in sorted(values):
			list_values = list(value)
			name, uid, rating = list_values[1], list_values[0], list_values[2]
			if uid == '0':
				last_name = name
			else:
				yield uid, (last_name,rating)


	def reducer_create_pairs(self, _, values):
		
        	for value1, value2 in combinations(values, 2):
			m1 = value1[0]
			r1 = value1[1]
			m2 = value2[0]
			r2 = value2[1]
			
			yield (m1, m2), (r1, r2)				
	
			
	def reducer_rating_pairs(self,titles,values):
		rating=[]
		#myval = 1000000
        	for value in values:
            		#list_values = list(value)
			#rate_pair = list_values[2], list_values[3]
			rating.append(value)
        	yield titles,rating
	
	def r_similarity(self,titles,values):
		#k= self.options.items
		rating =list(values)
        	for ratings in rating:
            		n=len(ratings)
        	q1=[]
        	q2=[]

        # Condition to NAN and not string values
		for r1 in ratings:
			if(isinstance(r1[0], numbers.Integral) or isinstance(r1[0], basestring)):
				q1.append((float(r1[0])))
			else:
				q1.append((float(1)))
			if(isinstance(r1[1], numbers.Integral) or isinstance(r1[1], basestring)):
				q2.append((float(r1[1])))
			else:
				q2.append((float(1)))

		if(n>self.options.minpairs):
			for movie in self.options.moviename:
				cor = numpy.corrcoef(q1,q2)[0,1]
				cos_cor = 1-spatial.distance.cosine(q1,q2)
				avg_cor = 0.5*(cor+cos_cor)
				if titles[0] == movie:
					yield titles[0],titles[1], (titles[0],titles[1],avg_cor,cor,cos_cor,n)
				elif titles[1]==movie:
					yield titles[1],titles[0] (titles[1],titles[0],avg_cor,cor,cos_cor,n)

                #  while(k>0):
		
		
#	def reducer_compute_similarities(self, movie_pair, values):
#		for value in sorted(values):
#			list_values = list(value)
#			m1 = list_values[0]
#			m2 = list_values[1]
#			r1 = list_values[2]
#			r2 = list_values[3]
#			yield m1,m2, (r1,r2)	

	def reducer_output(self, _, values):
		for value in values:
			list_values = list(value)
			m1,m2,avg_cor,stat_cor,cos_cor,counter = list_values
			for movie in self.options.moviename:
				if m1 == movie:
					yield m1,(str(1-float(avg_cor)),m1,m2,stat_cor,cos_cor,counter)
				elif m2 == movie:
					yield m2,(str(1-float(avg_cor)),m2,m1,stat_cor,cos_cor,counter)

	def reducer_sort_results(self, _, values):
		index = 0
		for value in sorted(values):
			list_values = list(value)
			avg_cor,m1,m2,stat_cor,cos_cor,counter = list_values
			if index < self.options.numofitems:
				yield m1,(m2,1-float(avg_cor),stat_cor,cos_cor,counter)
			index += 1

if __name__ == '__main__':
    MRMovie.run()
