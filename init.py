import random
import string
import psycopg2
import logging
import os
import time

from datetime import timedelta
from faker import Faker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

fake = Faker()



class Generator():
    def __init__(
        self,
        dbname: string,
        user: string, password: string,
        host: string, port: int,
        connect_retires: int,
        connect_interval: int,
        crypt_key: string,
        random_seed: int
    ):
        random.seed(random_seed)
        self.batch_size = 100000
        Faker.seed(random_seed)
        self.db_name = dbname + '.'
        is_connected = False
        for i in range(connect_retires):
            try:
                self.connection = psycopg2.connect(dbname=dbname,
                                             user=user,
                                             password=password,
                                             host=host,
                                             port=port)
                is_connected = True
                logger.info("Successfully connected to DB!")
                break
            except Exception as e:
                logger.error("Cannot connect to db" + str(e))
                time.sleep(connect_interval)
        
        if not is_connected:
            raise ConnectionError("Could not connect to db, retries exeeded")

        self.crypt_key = crypt_key
        if not crypt_key:
            raise ValueError("No crypt key for hashing")
        
        self.cursor = self.connection.cursor()
        self.table_names = [
            "user_profiles",
            "user_roles",
            "roles_permissions",
            "roles",
            "permissions",
            "users_achievements",
            "friendships",
            "event_users",
            "place_beer_assortment",
            "reviews",
            "users",
            "beer",
            "beer_styles",
            "brewery",
            "events",
            "places"
        ]
    

    def close_connection(self):
        self.connection.close()
        logger.info("Connection successfully closed!")

    def clean_tables(self):
        logger.info("Starting cleaning tables")
        for table in self.table_names:
            table_name = self.db_name + table
            try:
                self.cursor.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE;")
                self.connection.commit()
                logger.info(f"Successfully truncated table '{table_name}'")
            except Exception as e:
                logger.error(f"Error truncating table '{table}': {str(e)}")
        logger.info("Finished cleaning tables")
    
    def is_table_empty(self, table_name):
        try:
            self.cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = self.cursor.fetchone()[0]
            result = count == 0
        except Exception as e:
            logger.error(f"Error checking if table '{table_name}' is empty: {str(e)}")
            result = False

        return result

    def init_data(self, n: int):
        logger.info("Starting full generation!")
        self._generate_users(n) # users
        self._generate_user_profiles() # user_profiles for users
        self._generate_roles() # roles
        self._generate_user_roles()
        self._generate_permissions()
        self._generate_role_permissions()
        self._generate_achievements(int(n**0.5))
        self._generate_user_achievements(int(n**0.5)) # same as achievements num
        self._generate_user_friendships() 
        self._generate_beer_styles()
        self._generate_breweries(int(n**0.5))
        self._generate_beer(n)
        self._generate_places(int(n**0.5))
        self._generate_events(5 * int(n**0.5))
        self._generate_event_users()
        self._generate_place_beer_assortment(int(n**0.5))
        self._generate_reviews()



        logger.info("Generation ended successfully!")


    def _generate_users(self, n: int):
        table = self.db_name + 'users'
        if not self.is_table_empty(table):
            logger.info(f"Table '{table}' is not empty, skip")
            return

        logger.info(f"Start generation of {n} users")

        users = []
        for _ in range(n):
            username = fake.user_name()
            email = fake.email()
            password_hash = fake.sha1()
            is_active = random.choice([*[True for k in range(10)], False])
            created_at = fake.date_time_this_decade()
            users.append((username, email, password_hash, is_active, created_at))

        try:
            query = "INSERT INTO untappd_db.users (username, email, password_hash, is_active, created_at) VALUES (%s, %s, %s, %s, %s)"
            self.cursor.executemany(query, users)
            self.connection.commit()
        except Exception as e:
            logger.error(f"Error inserting into table '{table}': {str(e)}")
            return
        
        logger.info(f"Finish generation of {n} users")

    def _generate_user_profiles(self):
        table = self.db_name + 'user_profiles'
        if not self.is_table_empty(table):
            logger.info(f"Table '{table}' is not empty, skip")
            return

        self.cursor.execute(f"SELECT u.user_id FROM untappd_db.users as u")
        ids = list(map(lambda x: x[0], self.cursor.fetchall()))

        logger.info(f"Found {len(ids)} ids of users start generating their profiles")

        profiles = []

        for i in range(len(ids)):
            user_image_url = fake.image_url()
            sex = random.choice(['male', 'male', 'male', 'female', 'female', 'female', 'not_applicable'])
            first_name = fake.first_name_male() if sex == 'male' else fake.first_name_female()
            last_name = fake.last_name_male() if sex == 'male' else fake.last_name_female()
            date_of_birth = fake.date_of_birth(minimum_age=18, maximum_age=80)
            profile_desc = fake.text(max_nb_chars=512)

            profiles.append((ids[i], user_image_url, first_name, last_name, sex, date_of_birth, profile_desc))

        try:
            query = "INSERT INTO untappd_db.user_profiles (user_id, user_image_url, first_name, last_name, sex, date_of_birth, profile_desc) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            self.cursor.executemany(query, profiles)
            self.connection.commit()
        except Exception as e:
            logger.error(f"Error inserting into table '{table}': {str(e)}")
            return
        
        logger.info(f"Finish generation of {len(ids)} user profiles")

    def _generate_roles(self):
        table = self.db_name + 'roles'
        if not self.is_table_empty(table):
            logger.info(f"Table '{table}' is not empty, skip")
            return

        roles = ["user", "brewer", "celebrity", "beer god"]
        logger.info(f"Start generation of {len(roles)} roles")
        
    

        for role in roles:
            role_name = role
            role_desc = f"{role} role description"

            try:
                self.cursor.execute(
                    "INSERT INTO untappd_db.roles (role_name, role_description) VALUES (%s, %s)",
                    (role_name, role_desc)
                )
                self.connection.commit()
            except Exception as e:
                logger.error(f"Error inserting into table '{table}': {str(e)}")
                return
        
        logger.info(f"Finish generation of {len(roles)} roles")

    def _generate_user_roles(self):
        table = self.db_name + 'user_roles'
        if not self.is_table_empty(table):
            logger.info(f"Table '{table}' is not empty, skip")
            return

        self.cursor.execute(f"SELECT u.user_id FROM untappd_db.users as u")
        ids = list(map(lambda x: x[0], self.cursor.fetchall()))

        logger.info(f"Found {len(ids)} ids of users start generating their roles")
        users_roles = []
        count = 0
        ind = 0
        for i in range(len(ids)):
            role_ids = [2, 3, 4]
            user_roles = [1]
            role_id = random.choice([*[False for k in range(100)], *role_ids])
            if role_id:
                user_roles.append(role_id)
            
            for ur in user_roles:
                count += 1
                users_roles.append((ids[i], ur))

            if count > self.batch_size or i + 1 == len(ids):
                try:
                    query = "INSERT INTO untappd_db.user_roles (user_id, role_id) VALUES (%s, %s)"
                    self.cursor.executemany(query, users_roles)
                    self.connection.commit()
                except Exception as e:
                    logger.error(f"Error inserting into table '{table}': {str(e)}")
                    return
                count = 0
                users_roles = []
        
        logger.info(f"Finish generation of roles for users")
    

    def _generate_permissions(self):
        table = self.db_name + 'permissions'
        if not self.is_table_empty(table):
            logger.info(f"Table '{table}' is not empty, skip")
            return

        permissions = ["edit", "create", "delete", "drink", "fight", "swear", "rotate doors", "like milfs"]
        logger.info(f"Start generation of {len(permissions)} roles")
    

        for perm in permissions:
            permission_name = perm
            permission_desc = f"{permission_name} permission description"

            try:
                self.cursor.execute(
                    "INSERT INTO untappd_db.permissions (permission_name, permission_description) VALUES (%s, %s)",
                    (permission_name, permission_desc)
                )
                self.connection.commit()
            except Exception as e:
                logger.error(f"Error inserting into table '{table}': {str(e)}")
                return
        
        logger.info(f"Finish generation of {len(permissions)} permissions")
    
    def _generate_role_permissions(self):
        table = self.db_name + 'roles_permissions'
        if not self.is_table_empty(table):
            logger.info(f"Table '{table}' is not empty, skip")
            return

        ids = [i for i in range(1, 5)]


        logger.info(f"Found {len(ids)} ids of roles start generating their permissions")
        
        role_permissions = []
        count = 0
        for i in range(len(ids)):
            perm_ids = [i for i in range(1, 9)]
            role_perms = random.choices(perm_ids, [5, 5, 4, 4, 3, 3, 2, 2], k=random.choice([2, 2, 2, 3, 3, 4, 4, 5]))
            
            for rp in role_perms:
                count += 1
                role_permissions.append((ids[i], rp))
            
            if count > self.batch_size or i + 1 == len(ids):
                try:
                    query = "INSERT INTO untappd_db.roles_permissions (role_id, permission_id) VALUES (%s, %s)"
                    self.cursor.executemany(query, role_permissions)
                    self.connection.commit()
                except Exception as e:
                    logger.error(f"Error inserting into table '{table}': {str(e)}")
                    return
                
                count = 0
                role_permissions = []
        
        logger.info(f"Finish generation of permissions for roles")

    def _generate_achievements(self, n: int):
        table = self.db_name + 'achievements'
        if not self.is_table_empty(table):
            logger.info(f"Table '{table}' is not empty, skip")
            return

        logger.info(f"Start generation of {n} achievements")
        
        for _ in range(n):
            name = fake.word()
            desc = fake.text(max_nb_chars=512)
            try:
                self.cursor.execute(
                    "INSERT INTO untappd_db.achievements (achievement_name, achievement_desc) VALUES (%s, %s)",
                    (name, desc)
                )
                self.connection.commit()
            except Exception as e:
                logger.error(f"Error inserting into table '{table}': {str(e)}")
                return
        
        logger.info(f"Finish generation of {n} achievements")

    def _generate_user_achievements(self, n: int):
        table = self.db_name + 'users_achievements'
        if not self.is_table_empty(table):
            logger.info(f"Table '{table}' is not empty, skip")
            return

        self.cursor.execute(f"SELECT u.user_id FROM untappd_db.users as u")
        ids = list(map(lambda x: x[0], self.cursor.fetchall()))

        self.cursor.execute(f"SELECT a.achievement_id FROM untappd_db.achievements as a")
        achievement_ids = list(map(lambda x: x[0], self.cursor.fetchall()))

        logger.info(f"Found {len(ids)} ids of users start generating their achievements")
        
        count = 0
        for i in range(len(ids)):
            achievements = random.choices(achievement_ids, k = random.choice([0, 0, 0, 0, 0, 1, 1, 1, 2,2, 3, 4, 5, 6, 7, 8]))
            
            for ach in achievements:
                count += 1
                try:
                    self.cursor.execute(
                        "INSERT INTO untappd_db.users_achievements (user_id, achievement_id)"
                        "VALUES (%s, %s)",
                        (ids[i], ach)
                    )
                    self.connection.commit()
                except Exception as e:
                    logger.error(f"Error inserting into table '{table}': {str(e)}")
                    return
        
        logger.info(f"Finish generation of {count} achievements for users")

    def _generate_user_friendships(self):
        table = self.db_name + 'friendships'
        if not self.is_table_empty(table):
            logger.info(f"Table '{table}' is not empty, skip")
            return

        self.cursor.execute(f"SELECT u.user_id FROM untappd_db.users as u")
        ids = list(map(lambda x: x[0], self.cursor.fetchall()))

        logger.info(f"Found {len(ids)} ids of users start generating friendships")
        
        global_count = 0
        count = 0
        friendships = []
        for i in range(len(ids)):
            if i == len(ids)-1:
                try:
                    query = "INSERT INTO untappd_db.friendships (user1_id, user2_id, status) VALUES (%s, %s, %s)"
                    self.cursor.executemany(query, friendships)
                    self.connection.commit()
                except Exception as e:
                    logger.error(f"Error inserting into table '{table}': {str(e)}")
                    return
                continue
            friend_ids = set([random.randint(ids[i+1], ids[len(ids)-1]) for k in range(int((random.gauss(10, 4)**2)**0.5))])
            
            for id in friend_ids:
                count += 1
                global_count += 1
                status = random.choice(['active' for k in range(8)] + ['canceled'])
                friendships.append((ids[i], id, status))
            
            if count > self.batch_size or i + 1 == len(ids):
                logger.info(f"inserted {global_count}")
                try:
                    query = "INSERT INTO untappd_db.friendships (user1_id, user2_id, status) VALUES (%s, %s, %s)"
                    self.cursor.executemany(query, friendships)
                    self.connection.commit()
                except Exception as e:
                    logger.error(f"Error inserting into table '{table}': {str(e)}")
                    return
                
                count = 0
                friendships = []
        
        logger.info(f"Finish generation of friendships of users")
    
    def _generate_beer_styles(self):
        table = self.db_name + 'beer_styles'
        if not self.is_table_empty(table):
            logger.info(f"Table '{table}' is not empty, skip")
            return

        styles = [
            "wheat",
            "stout",
            "milk stout",
            "nitro stout",
            "citrus stout",
            "lager",
            "ipa",
            "apa",
            "double ipa",
            "triple ipa",
            "milkshake ipa",
            "ale",
            "dark ale",
            "quadrupel",
            "sour",
            "goose",
            "irish stout",
            "hyper dipa",
            "imperial stout",
            "porter",
        ]
        
        logger.info(f"Start generation of {len(styles)} beer styles")
        
    

        for style in styles:
            style_desc = f"{style} is very tasty and flavoured"

            try:
                self.cursor.execute(
                    "INSERT INTO untappd_db.beer_styles (style_name, style_desc) VALUES (%s, %s)",
                    (style, style_desc)
                )
                self.connection.commit()
            except Exception as e:
                logger.error(f"Error inserting into table '{table}': {str(e)}")
                return
        
        logger.info(f"Finish generation of {len(styles)} beer styles")

    def _generate_breweries(self, n: int):
        table = self.db_name + 'brewery'
        if not self.is_table_empty(table):
            logger.info(f"Table '{table}' is not empty, skip")
            return

        logger.info(f"Start generation of {n} breweries")

        for _ in range(n):
            brewery_name = fake.company()
            brewery_image_url = fake.image_url()
            brewery_desc = fake.sentence(nb_words=10, variable_nb_words=True, ext_word_list=None)

            try:
                self.cursor.execute(
                    "INSERT INTO untappd_db.brewery (brewery_name, brewery_image_url, brewery_desc) VALUES (%s, %s, %s)",
                    (brewery_name, brewery_image_url, brewery_desc)
                )
                self.connection.commit()
            except Exception as e:
                logger.error(f"Error inserting into table '{table}': {str(e)}")
                return
        
        logger.info(f"Finish generation of {n} breweries")
    

    def _generate_beer(self, n: int):
        table = self.db_name + 'beer'
        if not self.is_table_empty(table):
            logger.info(f"Table '{table}' is not empty, skip")
            return

        logger.info(f"Start generation of {n} beer")

        self.cursor.execute(f"SELECT b.brewery_id FROM untappd_db.brewery AS b")
        breweries = list(map(lambda x: x[0], self.cursor.fetchall()))

        self.cursor.execute(f"SELECT style_id FROM untappd_db.beer_styles AS bs")
        styles =  list(map(lambda x: x[0], self.cursor.fetchall()))

        beer = []
        count = 0

        for _ in range(n):
            count += 1
            beer_name = fake.word().capitalize() + " " + fake.word()
            beer_desc = fake.text(max_nb_chars=100)
            beer_image_url = fake.image_url()
            brewery_id = random.choice(breweries)
            style_id = random.choice(styles)
            abv = round(random.uniform(3.0, 12.0), 2)
            ibu = random.randint(5, 120)
            beer.append((beer_name, beer_desc, beer_image_url, brewery_id, style_id, abv, ibu))

            if count > self.batch_size or _ + 1== n:
                try:
                    query = "INSERT INTO untappd_db.beer (beer_name, beer_desc, beer_image_url, brewery_id, style_id, abv, ibu) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                    self.cursor.executemany(query, beer)
                    self.connection.commit()
                except Exception as e:
                    logger.error(f"Error inserting into table '{table}': {str(e)}")
                    return
                
                count = 0
                beer = []
        
        logger.info(f"Finish generation of {n} beer")

    
    def _generate_places(self, n: int):
        table = self.db_name + 'places'
        if not self.is_table_empty(table):
            logger.info(f"Table '{table}' is not empty, skip")
            return

        logger.info(f"Start generation of {n} places")

        for _ in range(n):
            place_name = fake.company()
            place_type = random.choice(['bar', 'shop', 'restaurant'])
            place_desc = fake.sentence(nb_words=15, variable_nb_words=True, ext_word_list=None)
            address = fake.address()
            place_phone_number = fake.phone_number()
            place_website = fake.url()
            try:
                self.cursor.execute(
                    "INSERT INTO untappd_db.places (place_name, place_type, place_desc, address, place_phone_number, place_website) VALUES (%s, %s, %s, %s, %s, %s)",
                    (place_name, place_type, place_desc, address, place_phone_number, place_website)
                )
                self.connection.commit()
            except Exception as e:
                logger.error(f"Error inserting into table '{table}': {str(e)}")
                return
        
        logger.info(f"Finish generation of {n} places")


    def _generate_events(self, n: int):
        table = self.db_name + 'events'
        if not self.is_table_empty(table):
            logger.info(f"Table '{table}' is not empty, skip")
            return

        logger.info(f"Start generation of {n} events")

        self.cursor.execute(f"SELECT p.place_id FROM untappd_db.places AS p")
        places = list(map(lambda x: x[0], self.cursor.fetchall()))

        for _ in range(n):
            event_name = fake.sentence(nb_words=3, variable_nb_words=True, ext_word_list=None)
            event_desc = fake.sentence(nb_words=15, variable_nb_words=True, ext_word_list=None)
            place_id = random.choice(places)
            start_time = fake.date_time_between(start_date='-1y', end_date='+1y')
            end_time = start_time + timedelta(hours=random.randint(1, 8))
            try:
                self.cursor.execute(
                    "INSERT INTO untappd_db.events (event_name, event_desc, place_id, start_time, end_time) VALUES (%s, %s, %s, %s, %s)",
                    (event_name, event_desc, place_id, start_time, end_time)
                )
                self.connection.commit()
            except Exception as e:
                logger.error(f"Error inserting into table '{table}': {str(e)}")
                return
        
        logger.info(f"Finish generation of {n} events")

    def _generate_event_users(self):
        table = self.db_name + 'event_users'
        if not self.is_table_empty(table):
            logger.info(f"Table '{table}' is not empty, skip")
            return

        logger.info(f"Start generation user events")

        self.cursor.execute(f"SELECT u.user_id FROM untappd_db.users AS u")
        users = list(map(lambda x: x[0], self.cursor.fetchall()))

        self.cursor.execute(f"SELECT e.event_id FROM untappd_db.events AS e")
        events = list(map(lambda x: x[0], self.cursor.fetchall()))
        count = 0

        event_users = []

        for event_id in events:
            user_ids = random.choices(users, k=random.randint(0, 100))
            
            for user_id in user_ids:
                count += 1
                status = random.choice(['dislike', 'like', 'willbe'])
                event_users.append((event_id, user_id, status))
            
            if count > self.batch_size or event_id == events[len(events)-1]:
                try:
                    query = "INSERT INTO untappd_db.event_users (event_id, user_id, status) VALUES (%s, %s, %s)"
                    self.cursor.executemany(query, event_users)
                    self.connection.commit()
                except Exception as e:
                    logger.error(f"Error inserting into table '{table}': {str(e)}")
                    return
                
                count = 0
                event_users = []
        
        logger.info(f"Finish generation of user events")
    
    def _generate_place_beer_assortment(self, n: int):
        table = self.db_name + 'place_beer_assortment'
        if not self.is_table_empty(table):
            logger.info(f"Table '{table}' is not empty, skip")
            return

        logger.info(f"Start generation of beer assortment for places")

        self.cursor.execute(f"SELECT b.beer_id FROM untappd_db.beer AS b")
        beer = list(map(lambda x: x[0], self.cursor.fetchall()))

        self.cursor.execute(f"SELECT p.place_id FROM untappd_db.places AS p")
        place = list(map(lambda x: x[0], self.cursor.fetchall()))
        count = 0

        beer_assortment = []

        for place_id in place:
            beers = random.choices(beer, k=int(random.gauss(500, 200)))
            for beer_id in beers:
                count += 1
                serving = random.choice(['bottle', 'tap', 'can'])
                beer_assortment.append((place_id, beer_id, serving))

            if count > self.batch_size or place_id == place[len(place)-1]:
                try:
                    query = "INSERT INTO untappd_db.place_beer_assortment (place_id, beer_id, serving) VALUES (%s, %s, %s)"
                    self.cursor.executemany(query, beer_assortment)
                    self.connection.commit()
                except Exception as e:
                    logger.error(f"Error inserting into table '{table}': {str(e)}")
                    return
                
                count = 0
                beer_assortment = []
        
        logger.info(f"Finish generation of beer assortment for places")

    def _generate_reviews(self):
        table = self.db_name + 'reviews'
        if not self.is_table_empty(table):
            logger.info(f"Table '{table}' is not empty, skip")
            return

        logger.info(f"Start generation of reviews for places")

        self.cursor.execute(f"SELECT b.beer_id FROM untappd_db.beer AS b")
        beer = list(map(lambda x: x[0], self.cursor.fetchall()))

        self.cursor.execute(f"SELECT p.place_id FROM untappd_db.places AS p")
        place = list(map(lambda x: x[0], self.cursor.fetchall()))

        self.cursor.execute(f"SELECT u.user_id FROM untappd_db.users AS u")
        user = list(map(lambda x: x[0], self.cursor.fetchall()))

        self.cursor.execute(f"SELECT e.event_id FROM untappd_db.events AS e")
        event = list(map(lambda x: x[0], self.cursor.fetchall()))
        count = 0

        reviews = []

        for user_id in user:   
            beers = list(set([random.randint(beer[0], beer[len(beer)-1]) for i in range(random.randint(0, 10))]))
            for beer_id in beers:
                count += 1
                rating = round(random.uniform(0.0, 5.0), 1)
                serving = random.choice(['bottle', 'tap', 'can', None])
                place_id = random.choice(place)
                comment = fake.sentence(nb_words=15, variable_nb_words=True, ext_word_list=None)
                photo_url = fake.image_url()
                event_id = None
                if random.randint(0,1):
                    event_id = random.choice(event)
                serving = random.choice(['bottle', 'tap', 'can'])
                reviews.append((user_id, beer_id, rating, serving, place_id, comment, photo_url, event_id))
            
            if count > self.batch_size or user_id == user[len(user)-1]:
                try:
                    query = "INSERT INTO untappd_db.reviews (user_id, beer_id, rating, serving, place_id, comment, photo_url, event_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
                    self.cursor.executemany(query, reviews)
                    self.connection.commit()
                except Exception as e:
                    logger.error(f"Error inserting into table '{table}': {str(e)}")
                    return
                count = 0
                reviews = []
        
        logger.info(f"Finish generation of reviews")

        

def main():
    rand_seed = int(os.environ.get("RANDOM_SEED"))
    n = int(os.environ.get("USERS_NUM"))
    crypt_key = os.environ.get("CRYPT_KEY")
    user = os.environ.get("POSTGRES_USER")
    pwd = os.environ.get("POSTGRES_PASSWORD")
    dbname = os.environ.get("POSTGRES_DB")
    host = os.environ.get("POSTGRES_HOST")
    port = int(os.environ.get("POSTGRES_PORT"))
    connect_retries = int(os.environ.get("POSTGRES_CONNECT_RETRIES"))
    connect_interval = int(os.environ.get("POSTGRES_CONNECT_INTERVAL"))

    generator = Generator(
        dbname, user, pwd, host, port,
        connect_retries, connect_interval,
        crypt_key, rand_seed
    )

    if os.environ.get("GENERATE_WITH_CLEANING") == "true":
        generator.clean_tables()

    generator.init_data(n)
    generator.close_connection()


if __name__ == '__main__':
    main()
