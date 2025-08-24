-- Table: storms.ibtracs_tracks_geo

-- DROP TABLE IF EXISTS storms.ibtracs_tracks_geo;

CREATE TABLE IF NOT EXISTS storms.ibtracs_tracks_geo(
    wind_speed INTEGER CHECK (wind_speed BETWEEN -1 AND 300),
    pressure INTEGER CHECK (pressure BETWEEN 800 AND 1100),
    max_wind_radius INTEGER CHECK (max_wind_radius >= 0),
    last_closed_isobar_radius INTEGER CHECK (last_closed_isobar_radius >= 0),
    last_closed_isobar_pressure INTEGER CHECK (last_closed_isobar_pressure BETWEEN 800 AND 1100),
    gust_speed INTEGER CHECK (gust_speed BETWEEN 0 AND 400),
    sid VARCHAR NOT NULL,
    provider VARCHAR,
    basin VARCHAR NOT NULL,
    nature VARCHAR,
    valid_time TIMESTAMP NOT NULL,
    quadrant_radius_34 TEXT NOT NULL,
    quadrant_radius_50 TEXT NOT NULL,
    quadrant_radius_64 TEXT,
    point_id VARCHAR NOT NULL,
    storm_id VARCHAR NOT NULL,
    geometry geometry(Point,4326) NOT NULL,
    CONSTRAINT unique_sid_storm_time UNIQUE (sid, storm_id, valid_time)
);

TABLESPACE pg_default;

ALTER TABLE IF EXISTS storms.ibtracs_tracks_geo
    OWNER to {owner};
-- Index: idx_ibtracs_tracks_geo_geometry

-- DROP INDEX IF EXISTS storms.idx_ibtracs_tracks_geo_geometry;

CREATE INDEX IF NOT EXISTS idx_ibtracs_tracks_geo_geometry
    ON storms.ibtracs_tracks_geo USING gist
    (geometry)
    TABLESPACE pg_default;