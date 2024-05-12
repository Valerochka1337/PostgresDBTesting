CREATE TABLE beer_styles (
  style_id SERIAL PRIMARY KEY,
  style_name VARCHAR(64) NOT NULL,
  style_desc VARCHAR(512)
);