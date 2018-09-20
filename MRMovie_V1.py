from mrjob.job import MRJob
from mrjob.step import MRStep
import numpy
from scipy import spatial

class MRMovie(MRJob):

	def steps(self):
		return [
		MRStep(mapper=self.mapper_get_names,
			reducer=self.reducer_get_names),
        MRStep(reducer=self.reducer_create_pairs)]	

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
				yield uid, (uid, last_name,rating)


	def reducer_create_pairs(self, _, values):
		movie_list=[]
		rating_list=[]
		global_uid=None
		creating_pairs=[]
        	for value in values:
			list_values = list(value)
			uid, mname, rating = list_values[0], list_values[1], list_values[2]
			if global_uid == None:
				global_uid = uid
			if global_uid == uid:
				movie_list.append(mname)
				rating_list.append(rating)
			else:
				movie_pairings = pairs(movie_list)
				rating_pairings = pairs(rating_list)
				movie_list.clear
				rating_list.clear
				
				global_uid=uid
				if len(movie_list) == 0:
					movie_list.append(mname)
				if len(rating_list) == 0:
					rating_list.append(rating)
				
				i=0
				while i < len(movie_pairings):
					#creating_pairs.append(movie_pairings[i], rating_pairings[i])
					yield movie_pairings[i], (movie_pairings[i], rating_pairings[i])
					i += 1
				break
				#print len(creating_pairs)+"\n"
		#for pair_element in creating_pairs:
			#pair_list = list(pair_element)
			#m1,m2 = pair_list[0][0], pair_list[0][1]
			#r1,r2 = pair_list [1][0], pair_list[1][1]
			#yield m1,m2, (m1,m2, r1,r2)
			
				
				
			
			
	def pairs(source):
		result = []
		for p1 in range(len(source)):
			for p2 in range(p1+1,len(source)):
				result.append([source[p1],source[p2]])
        	return result		
			

	#def reducer_compute_similarities(self, _, values):
		### SOMETHING


	def reducer_output(self, _, values):
		for value in values:
			list_values = list(value)
			m1,m2,stat_cor,cos_cor,avg_cor,counter = list_values
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
