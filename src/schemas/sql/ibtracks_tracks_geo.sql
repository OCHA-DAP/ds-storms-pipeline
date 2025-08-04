-- Table: storms.ibtracs_tracks_geo

-- DROP TABLE IF EXISTS storms.ibtracs_tracks_geo;

CREATE TABLE IF NOT EXISTS storms.ibtracs_tracks_geo
(
    wind_speed bigint,
    pressure bigint,
    max_wind_radius bigint,
    last_closed_isobar_radius bigint,
    last_closed_isobar_pressure bigint,
    gust_speed bigint,
    sid text COLLATE pg_catalog."default",
    provider text COLLATE pg_catalog."default",
    basin text COLLATE pg_catalog."default",
    nature text COLLATE pg_catalog."default",
    valid_time timestamp without time zone,
    quadrant_radius_34 text COLLATE pg_catalog."default",
    quadrant_radius_50 text COLLATE pg_catalog."default",
    quadrant_radius_64 text COLLATE pg_catalog."default",
    point_id text COLLATE pg_catalog."default",
    storm_id text COLLATE pg_catalog."default",
    geometry geometry(Point,4326)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS storms.ibtracs_tracks_geo
    OWNER to dbwriter;
-- Index: idx_ibtracs_tracks_geo_geometry

-- DROP INDEX IF EXISTS storms.idx_ibtracs_tracks_geo_geometry;

CREATE INDEX IF NOT EXISTS idx_ibtracs_tracks_geo_geometry
    ON storms.ibtracs_tracks_geo USING gist
    (geometry)
    TABLESPACE pg_default;