-- Archivo: DDL.sql

CREATE TABLE IF NOT EXISTS regulations (
    id SERIAL PRIMARY KEY,
    created_at VARCHAR(100),
    update_at TIMESTAMP,
    is_active BOOLEAN,
    title VARCHAR(255),
    gtype VARCHAR(100),
    entity VARCHAR(255),
    external_link TEXT,
    rtype_id INTEGER,
    summary TEXT,
    classification_id INTEGER,
    
    -- Restricción de unicidad del lambda.py original
    CONSTRAINT unique_regulation UNIQUE (title, created_at, external_link)
);

-- 2. Crear la tabla 'regulations_component'
CREATE TABLE IF NOT EXISTS regulations_component (
    id SERIAL PRIMARY KEY,
    -- 'regulations_id' (plural) coincide con el DataFrame de lambda.py
    regulations_id INTEGER REFERENCES regulations(id) ON DELETE CASCADE,
    
    -- 'components_id' (entero) coincide con el DataFrame de lambda.py
    components_id INTEGER,
    
    -- Unicidad para evitar duplicados
    UNIQUE(regulations_id, components_id)
);
);