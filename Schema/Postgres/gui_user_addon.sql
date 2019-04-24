CREATE TABLE gui_user (
  user_id VARCHAR(32) PRIMARY KEY,
  password VARCHAR(64) NOT NULL,
  email VARCHAR(64) NOT NULL,
  name VARCHAR(64) NOT NULL,
  jwt VARCHAR(255),
  is_admin BOOLEAN NOT NULL,
  added_on timestamp default now()
);

