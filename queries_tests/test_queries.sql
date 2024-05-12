SELECT styles.style_name, AVG(reviews.rating) AS style_rating
FROM untappd_db.brewery AS brewery
         JOIN untappd_db.beer beer ON brewery.brewery_id = beer.brewery_id
         JOIN untappd_db.reviews reviews ON beer.beer_id = reviews.beer_id
         JOIN untappd_db.beer_styles AS styles ON beer.style_id = styles.style_id
WHERE beer.brewery_id = 512
GROUP BY styles.style_name
ORDER BY style_rating DESC;


SELECT users.user_id,
       users.username,
       COUNT(DISTINCT friendships.user2_id)  AS friends_count,
       COUNT(DISTINCT achievements.achievement_id) AS achievements_count
FROM untappd_db.users AS users
         JOIN untappd_db.friendships friendships
              ON friendships.user1_id = users.user_id
         JOIN untappd_db.users_achievements achievements ON users.user_id = achievements.user_id
GROUP BY users.user_id
ORDER BY users.user_id;

EXPLAIN ANALYZE SELECT events.event_id, places.place_id, events.event_name, place_name, avg(reviews.rating)
FROM untappd_db.places as places
         JOIN untappd_db.events events ON places.place_id = events.place_id
         JOIN untappd_db.reviews reviews ON events.event_id = reviews.event_id
GROUP BY events.event_id, places.place_id;

