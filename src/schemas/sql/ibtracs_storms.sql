
CREATE TABLE IF NOT EXISTS storms.ibtracs_storms(
    sid VARCHAR NOT NULL,
    atcf_id VARCHAR,
    number SMALLINT NOT NULL,
    season BIGINT CHECK (season BETWEEN 1840 AND 2100) NOT NULL,
    name VARCHAR,
    genesis_basin VARCHAR NOT NULL,
    provisional BOOLEAN NOT NULL,
    storm_id VARCHAR NOT NULL,
    CONSTRAINT ibtracs_storms_unique UNIQUE (sid, storm_id)
);


TABLESPACE pg_default;

ALTER TABLE IF EXISTS storms.ibtracs_storms
    OWNER to {owner};
-- Index: ix_storms_ibtracs_storms_index

-- DROP INDEX IF EXISTS storms.ix_storms_ibtracs_storms_index;

CREATE INDEX IF NOT EXISTS ix_storms_ibtracs_storms_index
    ON storms.ibtracs_storms USING btree
    (index ASC NULLS LAST)
    TABLESPACE pg_default;