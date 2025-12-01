CREATE TABLE IF NOT EXISTS storms.ecmwf_storms_hannah(
    number VARCHAR NOT NULL,
    provider VARCHAR NOT NULL,
    season BIGINT CHECK (season BETWEEN 2005 AND 2050) NOT NULL,
    genesis_basin VARCHAR NOT NULL,
    name VARCHAR NULL,
    storm_id VARCHAR,
    CONSTRAINT ecmwf_storms_hannah_unique PRIMARY KEY (storm_id)
);

TABLESPACE pg_default;

ALTER TABLE IF EXISTS storms.ecmwf_storms
    OWNER to {owner};

