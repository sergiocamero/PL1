CREATE TABLE Camiones
(
    id_camion SERIAL PRIMARY KEY,
    matricula VARCHAR(10) UNIQUE NOT NULL,
    empresa TEXT NOT NULL,
    kilometros INTEGER
);
