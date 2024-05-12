CREATE TABLE permissions (
  permission_id SERIAL PRIMARY KEY,
  permission_name VARCHAR(64) NOT NULL,
  permission_description VARCHAR(512)
);