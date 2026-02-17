-- CREATE TABLE etl.Ventas (
--     id SERIAL PRIMARY KEY,
--     fecha DATE NOT NULL,
--     cliente VARCHAR(100) NOT NULL,
--     cedula VARCHAR(20) NOT NULL,
--     email VARCHAR(100),
--     ciudad VARCHAR(50),
--     departamento VARCHAR(50),
--     producto VARCHAR(100) NOT NULL,
--     categoria VARCHAR(50),
--     precio DECIMAL(10,2) NOT NULL,
--     cantidad INT NOT NULL,
--     vendedor VARCHAR(100),    
--     fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
-- );
-- INSERT INTO etl.Ventas (fecha, cliente, cedula, email, ciudad, departamento, producto, categoria, precio, cantidad, vendedor) VALUES
-- ('2024-01-15', 'Juan Pérez', '123', 'juan@mail.com', 'Medellín', 'Antioquia', 'Mouse', 'Accesorios', 50000, 2, 'Laura'),
-- ('2024-01-15', 'Juan Pérez', '123', 'juan@mail.com', 'Medellín', 'Antioquia', 'Teclado', 'Accesorios', 120000, 1, 'Laura'),
-- ('2024-01-16', 'Ana Ruiz', '456', 'ana@mail.com', 'Bogotá', 'Cundinamarca', 'Laptop', 'Computadores', 3500000, 1, 'Carlos'),
-- ('2024-02-01', 'Pedro Gómez', '789', 'pedro@mail.com', 'Cali', 'Valle', 'Monitor', 'Pantallas', 900000, 2, 'Laura');


CREATE TABLE dim_tiempo (
    tiempo_id SERIAL PRIMARY KEY,
    fecha DATE,
    anno INT NOT NULL,
    mes INT NOT NULL,
    nombre_mes VARCHAR(20),
    trimestre INT
);

CREATE TABLE dim_cliente (
    cliente_id SERIAL PRIMARY KEY,
    nombre_cliente VARCHAR(100) NOT NULL
);

CREATE TABLE dim_vendedor (
    vendedor_id SERIAL PRIMARY KEY,
    nombre_vendedor VARCHAR(100) NOT NULL,
    region VARCHAR(45) NOT NULL
);

CREATE TABLE dim_region (
    region_id SERIAL PRIMARY KEY,
    nombre_region VARCHAR(45) NOT NULL
);


CREATE TABLE dim_ciudad (
    ciudad_id SERIAL PRIMARY KEY,
    nombre_ciudad VARCHAR(100)
);