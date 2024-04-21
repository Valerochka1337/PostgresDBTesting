CREATE TABLE users_achievements (
  user_id INTEGER REFERENCES users(user_id) NOT NULL,
  achievement_id INTEGER REFERENCES achievements(achievement_id) NOT NULL
);