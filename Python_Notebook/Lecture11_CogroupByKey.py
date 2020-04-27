import apache_beam as beam

movie_name = [
    (1, 'SpiderMan'),
    (2, 'Avenger'),
    (3, 'Titanic'),
    (4, 'Green Miles'),
]
movies_rating = [
    (1, 3.5),
    (2, 4),
    (1, 4.5),
    (3, 3.5),
    (2, 4.5)
]

p = beam.Pipeline()

name = p | 'Create Name Pcollection' >> beam.Create(movie_name) # | beam.Map(print) # beam.Map(lambda x: print(x))
ratings = p | 'Create Rating Pcollection' >> beam.Create(movies_rating) # | beam.Map(lambda x: print(x))

joinedResult = ({'movie_name': name, 'movie_rating': ratings} | beam.CoGroupByKey()) | beam.Map(print)

p.run()
