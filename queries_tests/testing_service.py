import random
import string
import psycopg2
import logging
import os
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ExplainQueriesTester:
    def __init__(self, dbname: string,
        user: string, password: string,
        host: string, port: int, test_tries_num: int):
        self.db_name = dbname + '.'
        self.n = test_tries_num
        self.queries = []

        try:
            self.connection = psycopg2.connect(dbname=dbname,
                                            user=user,
                                            password=password,
                                            host=host,
                                            port=port)
            logger.info("Successfully connected to DB!")
        except Exception as e:
            logger.error("Cannot connect to db" + str(e))

        cwd = os.getcwd()
        if not os.path.exists(cwd + "/test_data"):
            os.mkdir(cwd + "/test_data")
        
        max_file_num = 0

        for filename in os.listdir(cwd + "/test_data"):
            num = int(filename.split('_')[-1].split('.')[0])
            max_file_num = max(max_file_num, num)
        
        self.file = open(cwd + "/test_data/" + f"test_{max_file_num + 1}.txt", "w")
        self.cursor = self.connection.cursor()
    
    def add_query(self, query: string):
        self.queries.append(query)

    def analyze_queries(self):
        logger.info("Starting tests")
        query_num = 1
        for query in self.queries:
            logger.info(f"Testing query #{query_num}")
            logger.info(f"Query: {query[:min(30, len(query))]}")

            min_cost = float('inf')
            max_cost = float('-inf')
            total_cost = 0

            for _ in range(self.n):
                self.cursor.execute("EXPLAIN ANALYZE " + query)
                self.connection.commit()

                fetched_row = self.cursor.fetchall()[0][0]
                cost = float(fetched_row.split("cost=")[1].split(" rows=")[0].split("..")[1])
                min_cost = min(min_cost, cost)
                max_cost = max(max_cost, cost)
                total_cost += cost
            logger.info("Finished testing query")
            self.file.write(f"{query_num}): min({min_cost}, max({max_cost}, avg({total_cost/self.n})))\n")
            query_num += 1
        
        logger.info("All tests finished")

        self.file.close()
        self.connection.close()

        
        


def main():
    # user = os.environ.get("POSTGRES_USER")
    # pwd = os.environ.get("POSTGRES_PASSWORD")
    # dbname = os.environ.get("POSTGRES_DB")
    # host = os.environ.get("POSTGRES_HOST")
    # port = int(os.environ.get("POSTGRES_PORT"))
    # test_tries_num = int(os.environ.get("TEST_TRIES_NUM"))

    user = "postgres"
    pwd = "postgres"
    dbname = "untappd_db"
    host = "localhost"
    port = 5432
    test_tries_num = 2

    tester = ExplainQueriesTester(dbname, user, pwd, host, port, test_tries_num)
    tester.add_query(
        """SELECT styles.style_name, AVG(reviews.rating) AS style_rating FROM untappd_db.brewery AS brewery
                JOIN untappd_db.beer beer ON brewery.brewery_id = beer.brewery_id
                JOIN untappd_db.reviews reviews ON beer.beer_id = reviews.beer_id
                JOIN untappd_db.beer_styles AS styles ON beer.style_id = styles.style_id
        WHERE beer.brewery_id = 226
        GROUP BY styles.style_name
        ORDER BY style_rating DESC;
        """
    )
    tester.add_query(
        """SELECT users.user_id, users.username, COUNT(DISTINCT friendships.user2_id)  AS friends_count,
            COUNT(DISTINCT achievements.achievement_id) AS achievements_count
        FROM untappd_db.users AS users
                JOIN untappd_db.friendships friendships
                    ON friendships.user1_id = users.user_id
                JOIN untappd_db.users_achievements achievements ON users.user_id = achievements.user_id
        GROUP BY users.user_id
        ORDER BY users.user_id;
        """
    )
    tester.add_query(
        """SELECT events.event_id, places.place_id, events.event_name, place_name, avg(reviews.rating)
        FROM untappd_db.places as places
                JOIN untappd_db.events events ON places.place_id = events.place_id
                JOIN untappd_db.reviews reviews ON events.event_id = reviews.event_id
        GROUP BY events.event_id, places.place_id;
        """
    )

    tester.analyze_queries()



if __name__ == '__main__':
    main()