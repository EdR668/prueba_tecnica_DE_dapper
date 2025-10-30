CREATE TABLE IF NOT EXISTS regulations (
    id SERIAL PRIMARY KEY,
    title TEXT,
    summary TEXT,
    created_at DATE,
    entity TEXT,
    external_link TEXT,
    classification_id INT,
    rtype_id INT,
    gtype TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    update_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS regulations_component (
    id SERIAL PRIMARY KEY,
    regulations_id INT NOT NULL REFERENCES regulations(id) ON DELETE CASCADE,
    components_id INT NOT NULL
);