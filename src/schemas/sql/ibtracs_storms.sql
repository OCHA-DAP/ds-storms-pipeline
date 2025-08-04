
CREATE TABLE IF NOT EXISTS storms.ibtracs_storms
(
    index bigint,
    sid text COLLATE pg_catalog."default",
    atcf_id text COLLATE pg_catalog."default",
    "number" smallint,
    season bigint,
    name text COLLATE pg_catalog."default",
    genesis_basin text COLLATE pg_catalog."default",
    provisional boolean,
    storm_id text COLLATE pg_catalog."default"
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS storms.ibtracs_storms
    OWNER to dbwriter;
-- Index: ix_storms_ibtracs_storms_index

-- DROP INDEX IF EXISTS storms.ix_storms_ibtracs_storms_index;

CREATE INDEX IF NOT EXISTS ix_storms_ibtracs_storms_index
    ON storms.ibtracs_storms USING btree
    (index ASC NULLS LAST)
    TABLESPACE pg_default;