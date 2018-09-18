'''

                            Online Python Compiler.
                Code, Compile, Run and Debug python program online.
Write your code in this editor and press "Run" button to execute it.

'''
import csv
with open('movies_new.dat', 'w') as out:
    with open('movies.csv') as csvfile:
        movie = csv.reader(csvfile)
        for row in movie:
            key_list.append(row[0])
            value_list.append(row[1])
            out.write("::".join(row)+'\n' )
        movieMap= dict(zip(key_list, value_list))

with open('ratings_new.dat', 'w') as out:
    with open('ratings.csv') as csvfile:
        ratings = csv.reader(csvfile)
        for row in ratings:
            if row[0] != 'userId':
                row[2] = str(int(float(row[2])*2))
                row[1] = str(movieMap.get(row[1]))
                out.write("::".join(row)+'\n')
                
#keys = ['a', 'b', 'c']
#values = [1, 2, 3]
#dictionary = dict(zip(keys, values))
#print(dictionary['b'])
