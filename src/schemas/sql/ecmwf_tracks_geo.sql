-- Table: storms.ecmwf_tracks_geo

-- DROP TABLE IF EXISTS storms.ecmwf_tracks_geo;

CREATE TABLE IF NOT EXISTS storms.ecmwf_tracks_geo_hannah(
    issued_time timestamp without time zone,
    provider VARCHAR,
    forecast_id VARCHAR,
    basin VARCHAR,
    leadtime bigint,
    valid_time timestamp without time zone,
    pressure double precision,
    wind_speed double precision,
    storm_id VARCHAR,
    point_id VARCHAR,
    geometry geometry(Point,4326),
    CONSTRAINT ecmwf_tracks_geo_hannah_unique UNIQUE (storm_id, valid_time, issued_time),
	CONSTRAINT foreign_key_sid FOREIGN KEY (storm_id)
	REFERENCES storms.ecmwf_storms_hannah(storm_id)
);

ALTER TABLE IF EXISTS storms.ecmwf_tracks_geo_hannah
    OWNER to {owner};

CREATE INDEX IF NOT EXISTS idx_ecmwf_tracks_geo_geometry_hannah
    ON storms.ecmwf_tracks_geo_hannah USING gist
    (geometry)
    TABLESPACE pg_default;