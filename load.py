import pymongo

client = pymongo.MongoClient(
    "mongodb+srv://user:AotD8lF0WspDIA4i@cluster0.qtikgbg.mongodb.net/?retryWrites=true&w=majority")
db = client["mydatabase"]
artists = db['artists']


def load_to_database(data: dict):
    artists.insert_one(data)
    print('--- Artist {} insert to DataBase! ---'.format(data['Artist']))
