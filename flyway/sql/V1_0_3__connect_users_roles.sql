CREATE TABLE user_roles (
  user_id INTEGER REFERENCES users(user_id) NOT NULL,
  role_id INTEGER REFERENCES roles(role_id) NOT NULL
);