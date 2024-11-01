import sqlite3
import os.path
import pandas
import json
import re

db_path = "database/example.db"

# Create/Connect to sqlite3 database
con = sqlite3.connect(db_path)
cursor = con.cursor()

# Data for analysis
# Ageranges
ageranges = [18, 25, 30, 39, 50, 60, 80]


def run():
    if not os.path.isfile("~/" + db_path):
        create_sample_database()





def create_sample_database():
    """ Creates a sample database and fills it with random user data from json file 
        JSON-Schema: Firstname, Surname, Age, Hobbys, Location, Gender, Pronouns, FavouriteMusicGenre """

    # Read userdata from json with pandas and create panda DataFrames for SQL insertion
    userdata = pandas.read_json("sample_userdata.json")

    # Create users and locations DataFrame for modular db setup
    locationsdf = pandas.DataFrame(userdata["location"].unique(), columns=["location"])
    genderdf = pandas.DataFrame(userdata["gender"].unique(), columns=["gender"])
    pronounsdf = pandas.DataFrame(userdata["pronouns"].unique(), columns=["pronouns"])

    # Cast userdata DataFrame column "hobbys" to string and remove all nonalpha characters and whitespace. Split string at comma and add unique elements to hobby list
    hobbys = []
    for hobby in re.sub("[^A-Za-z,]+", "", userdata["hobbys"].to_string()).split(","):
        if hobby not in hobbys:
            hobbys.append(hobby)

    hobbysdf = pandas.DataFrame(hobbys, columns=["hobby"])
    favmusicgenresdf = pandas.DataFrame(userdata["favmusicgenre"].unique(), columns=["favmusicgenre"])

    # DataFrame for user analysis
    useranalysisdf = userdata.reindex(columns=["agerange", "bestmatchid", "matchid2", "matchid3"])

    # Create ref ids in users table
    usersdf = pandas.merge(pandas.DataFrame(userdata[["firstname", "surname", "age", "gender", "pronouns", "location", "favmusicgenre"]], columns=["firstname", "surname", "age", "gender", "pronouns", "location", "favmusicgenre"]), locationsdf.reset_index().rename(columns={"index": "location_id"}), on="location")
    usersdf = pandas.merge(usersdf, favmusicgenresdf.reset_index().rename(columns={"index": "favmusicgenre_id"}), on="favmusicgenre")
    usersdf = pandas.merge(usersdf, genderdf.reset_index().rename(columns={"index": "gender_id"}), on="gender")
    usersdf = pandas.merge(usersdf, pronounsdf.reset_index().rename(columns={"index": "pronouns_id"}), on="pronouns")

    # Drop unnessesary columns (TODO: rewrite simpler way)
    usersdf = usersdf.drop(columns=["location", "favmusicgenre", "pronouns", "gender"])

    # Write DataFrames to SQL tables
    usersdf.to_sql(name="users", con=con, if_exists="replace", index_label="id")
    locationsdf.to_sql(name="locations", con=con, if_exists="replace", index_label="id")
    hobbysdf.to_sql(name="hobbys", con=con, if_exists="replace", index_label="id")
    favmusicgenresdf.to_sql(name="favmusicgenres", con=con, if_exists="replace", index_label="id")
    genderdf.to_sql(name="genders", con=con, if_exists="replace", index_label="id")
    pronounsdf.to_sql(name="pronouns", con=con, if_exists="replace", index_label="id")
    useranalysisdf.to_sql(name="useranalysis", con=con, if_exists="replace", index_label="user_id")
    


if __name__ == "__main__":
    run()