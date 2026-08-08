-- multi-statement file with a comment header, like a real-world init migration
CREATE TABLE widgets (
    id   BIGINT PRIMARY KEY,
    name VARCHAR(100) NOT NULL
);

CREATE INDEX idx_widgets_name ON widgets(name);

INSERT INTO widgets (id, name) VALUES (1, 'widget-one');
