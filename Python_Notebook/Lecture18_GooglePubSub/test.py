input_file = 'ml-latest/movies.csv'

with open(input_file, 'rb') as file:
    for row in file:
        print(row)