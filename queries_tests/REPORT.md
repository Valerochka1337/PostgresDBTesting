# Report

### Given queries:
- Select beer types and their average rating for certain brewery <br/>
Query:
```sql
SELECT styles.style_name, AVG(reviews.rating) AS style_rating
FROM untappd_db.brewery AS brewery
         JOIN untappd_db.beer beer ON brewery.brewery_id = beer.brewery_id
         JOIN untappd_db.reviews reviews ON beer.beer_id = reviews.beer_id
         JOIN untappd_db.beer_styles AS styles ON beer.style_id = styles.style_id
WHERE beer.brewery_id = 512
GROUP BY styles.style_name
ORDER BY style_rating DESC
```
Execution time: 1s 158ms

- For each user select count of it's friends and achievements <br/>
Query:
```sql
SELECT users.user_id,
       users.username,
       COUNT(DISTINCT friendships.friendship_id)  AS friends_count,
       COUNT(DISTINCT achievements.achievement_id) AS achievements_count
FROM untappd_db.users AS users
         JOIN untappd_db.friendships friendships
              ON friendships.user1_id = users.user_id OR friendships.user2_id = users.user_id
         JOIN untappd_db.users_achievements achievements ON users.user_id = achievements.user_id
GROUP BY users.user_id
ORDER BY users.user_id
```
Execution time: 59s
- Select average rating of all beer in every review, that was written at every event <br/>
Query:
```sql
SELECT events.event_id, places.place_id, events.event_name, place_name, AVG(reviews.rating) AS avg_rating
FROM untappd_db.places AS places
         JOIN untappd_db.events events ON places.place_id = events.place_id
         JOIN untappd_db.reviews reviews ON events.event_id = reviews.event_id
GROUP BY events.event_id, places.place_id;
```
Execution time: 1s 562ms


