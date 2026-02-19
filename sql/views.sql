-- parcel_features: one row per parcel with aggregated counts for LLM embeddings
CREATE OR REPLACE VIEW parcel_features AS
SELECT
    p.id AS parcel_id,
    p.address,
    p.address_norm,
    p.city,
    p.state,
    p.zip_code,
    p.county,
    p.latitude,
    p.longitude,
    p.base_zoning,
    p.zoning_desc,
    p.lot_size_sqft,
    p.apn,

    -- Permit counts
    (SELECT COUNT(*) FROM permits pm WHERE pm.address_norm = p.address_norm)
        AS total_permits,
    (SELECT COUNT(*) FROM permits pm WHERE pm.address_norm = p.address_norm
        AND pm.issued_date >= CURRENT_DATE - INTERVAL '5 years')
        AS permits_5yr,
    (SELECT COUNT(*) FROM permits pm WHERE pm.address_norm = p.address_norm
        AND pm.status IN ('Active', 'In Review', 'Issued'))
        AS active_permits,

    -- Zoning cases
    (SELECT COUNT(*) FROM zoning_cases zc WHERE zc.address_norm = p.address_norm)
        AS total_zoning_cases,
    (SELECT COUNT(*) FROM zoning_cases zc WHERE zc.address_norm = p.address_norm
        AND zc.status NOT IN ('Closed', 'Withdrawn', 'Denied'))
        AS open_zoning_cases,

    -- BOA cases
    (SELECT COUNT(*) FROM boa_cases bc WHERE bc.address_norm = p.address_norm)
        AS total_boa_cases,

    -- Environmental
    (SELECT COUNT(*) FROM environmental_constraints ec
        WHERE ec.address_norm = p.address_norm OR (
            ec.latitude IS NOT NULL AND p.latitude IS NOT NULL
            AND ABS(ec.latitude - p.latitude) < 0.001
            AND ABS(ec.longitude - p.longitude) < 0.001
        ))
        AS environmental_flags,

    -- Zoning overlays (count by proximity)
    (SELECT COUNT(*) FROM zoning_overlays zo WHERE zo.source_id IN (
        SELECT s.id FROM sources s WHERE s.jurisdiction_id = p.jurisdiction_id
            AND s.target_table = 'zoning_overlays'
    )) AS overlay_count,

    p.fetched_at

FROM parcels p;
