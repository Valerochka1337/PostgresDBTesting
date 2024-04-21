CREATE TYPE event_user_status AS ENUM ('dislike', 'like', 'willbe');

CREATE TABLE event_users (
  event_id INTEGER REFERENCES events(event_id) NOT NULL,
  user_id INTEGER REFERENCES users(user_id) NOT NULL,
  status event_user_status
);