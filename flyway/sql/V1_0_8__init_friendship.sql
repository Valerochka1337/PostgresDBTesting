CREATE TYPE friendship_status AS ENUM ('active', 'canceled');

CREATE TABLE friendships (
  friendship_id SERIAL PRIMARY KEY,
  user1_id INTEGER REFERENCES users(user_id) NOT NULL,
  user2_id INTEGER REFERENCES users(user_id) NOT NULL,
  status friendship_status not null
);