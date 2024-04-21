CREATE TABLE users (
  user_id SERIAL PRIMARY KEY,
  username VARCHAR(64) NOT NULL,
  email VARCHAR(320) NOT NULL,
  password_hash VARCHAR(128) NOT NULL,
  is_active BOOLEAN NOT NULL,
  created_at TIMESTAMP NOT NULL
);