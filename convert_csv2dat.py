'''

                            Online Python Compiler.
                Code, Compile, Run and Debug python program online.
Write your code in this editor and press "Run" button to execute it.

'''

import csv
with open('movies.dat', 'w') as out:
    with open('movies.csv') as csvfile:
        movie = csv.reader(csvfile)
        for row in movie:
            out.write("::".join(row)+'\n' )

with open('ratings.dat', 'w') as out:
    with open('ratings.csv') as csvfile:
        ratings = csv.reader(csvfile)
        for row in ratings:
            if row[0] != 'userId':
                row[2] = str(int(float(row[2])*2))
                out.write("::".join(row)+'\n')

