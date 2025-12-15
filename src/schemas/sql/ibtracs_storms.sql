
CREATE TABLE IF NOT EXISTS storms.ibtracs_storms(
    sid VARCHAR PRIMARY KEY,
    atcf_id VARCHAR,
    number SMALLINT NOT NULL,
    season BIGINT CHECK (season BETWEEN 1840 AND 2100) NOT NULL,
    name VARCHAR,
    genesis_basin VARCHAR NOT NULL,
    provisional BOOLEAN NOT NULL,
    storm_id VARCHAR,-- TODO: check with Hannah
    -- ocha-lens (datasources/ibtracs) unique=["sid", "storm_id"],
    -- the dataset contains empty storm_ids breaking this constraint
    CONSTRAINT ibtracs_storms_unique UNIQUE (sid)
);


TABLESPACE pg_default;

ALTER TABLE IF EXISTS storms.ibtracs_storms
    OWNER to {owner};