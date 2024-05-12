CREATE TYPE sex AS ENUM ('male', 'female', 'not_applicable');

CREATE TABLE user_profiles (
  user_id INTEGER REFERENCES users(user_id) NOT NULL,
  user_image_url VARCHAR(2048),
  first_name VARCHAR(64) NOT NULL,
  last_name VARCHAR(64),
  sex sex,
  date_of_birth DATE NOT NULL,
  profile_desc VARCHAR(512)
);