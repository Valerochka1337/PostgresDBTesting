CREATE TABLE roles_permissions (
  role_id INTEGER REFERENCES roles(role_id) NOT NULL,
  permission_id INTEGER REFERENCES permissions(permission_id) NOT NULL
);