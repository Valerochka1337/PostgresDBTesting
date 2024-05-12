CREATE TABLE achievements (
  achievement_id SERIAL PRIMARY KEY,
  achievement_name VARCHAR(64) NOT NULL,
  achievement_desc VARCHAR(512)
);