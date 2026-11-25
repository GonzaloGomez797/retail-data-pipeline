CREATE TABLE vuelos (
  id INT AUTO_INCREMENT PRIMARY KEY,
  vuelo VARCHAR(10),
  aerolinea VARCHAR(255),
  fecha_vuelo DATETIME,
  origen VARCHAR(255),
  destino VARCHAR(255),
  latitud FLOAT,
  longitud FLOAT,
  altitud INT,
  velocidad FLOAT,
  timestamp DATETIME,
  user VARCHAR(255)
);
