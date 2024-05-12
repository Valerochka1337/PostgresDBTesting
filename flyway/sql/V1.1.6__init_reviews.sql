CREATE TABLE reviews (
  review_id SERIAL PRIMARY KEY,
  user_id INTEGER REFERENCES users(user_id) NOT NULL,
  beer_id INTEGER REFERENCES beer(beer_id) NOT NULL,
  rating FLOAT NOT NULL CHECK (rating >= 0.0 AND rating <= 5.0),
  serving serving,
  place_id INTEGER REFERENCES places(place_id),
  comment VARCHAR(512),
  photo_url VARCHAR(2048),
  event_id INTEGER REFERENCES events(event_id)
);