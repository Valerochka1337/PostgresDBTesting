CREATE TABLE beer (
  beer_id SERIAL PRIMARY KEY,
  beer_name VARCHAR(64) NOT NULL,
  beer_desc VARCHAR(512),
  beer_image_url VARCHAR(2000),
  brewery_id INTEGER REFERENCES brewery(brewery_id),
  style_id INTEGER REFERENCES beer_styles(style_id),
  abv FLOAT,
  ibu FLOAT
);