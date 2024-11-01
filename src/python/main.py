import sqlite3
import os.path
import pandas
import json
import re

db_path = "database/example.db"

# Create/Connect to sqlite3 database
con = sqlite3.connect(db_path)
cursor = con.cursor()


def run():
    if not os.path.isfile("~/" + db_path):
        create_sample_database()

    users = pandas.read_sql(sql="SELECT * FROM users", con=con)
    useranalysisdf = users.reindex(columns=["agerange", "match_id1", "match_id2", "match_id3"])

    # Give every user a rounded agerange
    for user in users[["id", "age"]].to_dict(orient="records"):
        useranalysisdf.at[user["id"], "agerange"] = 10 * round(user["age"] / 10)
    
    # Find three matches for every user (TODO: Balance so there aren't users that get matched too much, TODO: Integrate hobbys)
    for user in users.to_dict(orient="records"):
        match_1 = [-1, -1]
        match_2 = [-1, -1]
        match_3 = [-1, -1]

        for usermatch in users.to_dict(orient="records"):
            if user["id"] == usermatch["id"]:
                continue
            
            # Compare user with current match and give matchpoints
            matchpoints = 0
            if usermatch["location_id"] == user["location_id"]:
                matchpoints += 4
            if useranalysisdf.at[usermatch["id"], "agerange"] == useranalysisdf.at[user["id"], "agerange"]:
                matchpoints += 3
            if usermatch["favmusicgenre_id"] == user["favmusicgenre_id"]:
                matchpoints += 2
            if usermatch["gender_id"] == user["gender_id"]:
                matchpoints += 1
            if usermatch["pronouns_id"] == user["pronouns_id"]:
                matchpoints += 1

            # Check if better match is found
            if matchpoints > match_1[1]:
                match_1[0] = usermatch["id"]
                match_1[1] = matchpoints
            elif matchpoints > match_2[1]:
                match_2[0] = usermatch["id"]
                match_2[1] = matchpoints
            elif matchpoints > match_3[1]:
                match_3[0] = usermatch["id"]
                match_3[1] = matchpoints

        useranalysisdf.at[user["id"], "match_id1"] = match_1[0]
        useranalysisdf.at[user["id"], "match_id2"] = match_2[0]
        useranalysisdf.at[user["id"], "match_id3"] = match_3[0]

    # Write user analysis to SQLite db
    useranalysisdf.to_sql(name="useranalysis", con=con, if_exists="replace", index_label="id")
    print("Useranalysis complete")



def create_sample_database():
    """ Creates a sample database and fills it with random user data from json file 
        JSON-Schema: Firstname, Surname, Age, Hobbys, Location, Gender, Pronouns, FavouriteMusicGenre """

    # Read userdata from json with pandas and create panda DataFrames for SQL insertion
    userdata = pandas.read_json("sample_userdata.json")

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

    # Create ref ids in users table, TODO: Add hobbys ref key
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

    print("Database setup complete")
    


if __name__ == "__main__":
    run()