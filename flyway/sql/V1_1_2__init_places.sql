CREATE TYPE place_type AS ENUM ('bar', 'shop', 'restaurant');

CREATE TABLE places (
  place_id SERIAL PRIMARY KEY,
  place_name VARCHAR(64) NOT NULL,
  place_type place_type NOT NULL,
  place_desc VARCHAR(512),
  address VARCHAR(2048) NOT NULL,
  place_phone_number VARCHAR(32),
  place_website VARCHAR(512)
);