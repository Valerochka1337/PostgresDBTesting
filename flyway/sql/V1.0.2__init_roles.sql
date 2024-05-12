CREATE TABLE roles (
  role_id SERIAL PRIMARY KEY,
  role_name VARCHAR(64) NOT NULL,
  role_description VARCHAR(512)
);