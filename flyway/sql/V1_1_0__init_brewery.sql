CREATE TABLE brewery (
  brewery_id SERIAL PRIMARY KEY,
  brewery_name VARCHAR(64) NOT NULL,
  brewery_image_url VARCHAR(2048),
  brewery_desc VARCHAR(512)
);