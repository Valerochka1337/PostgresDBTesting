CREATE TYPE serving AS ENUM ('bottle', 'tap', 'can');

CREATE TABLE place_beer_assortment (
  place_id INTEGER REFERENCES places(place_id) NOT NULL,
  beer_id INTEGER REFERENCES beer(beer_id) NOT NULL,
  serving serving
);