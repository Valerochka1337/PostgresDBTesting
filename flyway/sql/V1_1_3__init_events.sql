CREATE TABLE events (
  event_id SERIAL PRIMARY KEY,
  event_name VARCHAR(64),
  event_desc VARCHAR(512),
  place_id INTEGER REFERENCES places(place_id) NOT NULL,
  start_time TIMESTAMP,
  end_time TIMESTAMP
);