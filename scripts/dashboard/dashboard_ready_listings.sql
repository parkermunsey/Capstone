CREATE OR REPLACE VIEW dashboard_ready_listings AS
WITH base AS (
    SELECT
        s.*,
        COALESCE(NULLIF(s.address, ''), NULLIF(s.address_raw, ''), NULLIF(s.listing_title, '')) AS display_address,
        COALESCE(NULLIF(s.cross_source_fingerprint, ''), NULLIF(s.listing_fingerprint, ''), s.source || ':' || s.source_record_id) AS dashboard_group_key,
        CASE s.source
            WHEN 'mha' THEN 1
            WHEN 'adea' THEN 2
            WHEN 'mpm' THEN 3
            WHEN 'caras' THEN 4
            WHEN 'plum' THEN 5
            WHEN 'craigslist' THEN 6
            ELSE 9
        END AS source_priority
    FROM stg_listings s
),
parsed AS (
    SELECT
        b.*,
        COALESCE(b.rent_min, b.rent_max) AS dashboard_rent,
        CASE
            WHEN b.rent_min IS NOT NULL AND b.rent_max IS NOT NULL AND b.rent_min <> b.rent_max THEN '$' || b.rent_min || ' - $' || b.rent_max
            WHEN b.rent_min IS NOT NULL THEN '$' || b.rent_min
            WHEN b.rent_max IS NOT NULL THEN '$' || b.rent_max
            ELSE NULL
        END AS dashboard_rent_label,
        NULLIF(TRIM(SUBSTRING(b.display_address FROM '(?i)(?:\bunit\b|\bapt\b|\bapartment\b|\bsuite\b|#|\blot\b)\s*([A-Za-z0-9\-]+)')), '') AS unit,
        NULLIF(TRIM(SUBSTRING(b.display_address FROM ',\s*([A-Za-z .''-]+),\s*[A-Z]{2}(?:\s+\d{5}(?:-\d{4})?)?$')), '') AS city,
        NULLIF(TRIM(SUBSTRING(b.display_address FROM ',\s*([A-Z]{2})(?:\s+\d{5}(?:-\d{4})?)?$')), '') AS state,
        NULLIF(TRIM(SUBSTRING(b.display_address FROM '([0-9]{5}(?:-[0-9]{4})?)$')), '') AS postal_code,
        COALESCE(
            b.is_currently_available,
            CASE
                WHEN b.availability_status = 'available' AND (b.available_date IS NULL OR b.available_date <= CURRENT_DATE) THEN TRUE
                ELSE FALSE
            END
        ) AS dashboard_is_currently_available
    FROM base b
),
ranked AS (
    SELECT
        p.*,
        COUNT(*) OVER (PARTITION BY p.dashboard_group_key) AS duplicate_count,
        ROW_NUMBER() OVER (
            PARTITION BY p.dashboard_group_key
            ORDER BY
                CASE WHEN p.dashboard_is_currently_available THEN 0 ELSE 1 END,
                p.observed_at DESC NULLS LAST,
                p.source_priority ASC,
                p.listing_url NULLS LAST
        ) AS dedupe_rank
    FROM parsed p
)
SELECT
    source,
    source_record_id,
    listing_title,
    display_address AS address,
    address_raw,
    unit,
    city,
    state,
    postal_code,
    bedrooms,
    bathrooms,
    sqft,
    rent_min,
    rent_max,
    rent_period,
    dashboard_rent,
    dashboard_rent_label,
    availability_status,
    available_date,
    availability_text_raw,
    dashboard_is_currently_available AS is_currently_available,
    COALESCE(is_available_soon, FALSE) AS is_available_soon,
    contact_name,
    contact_phone,
    contact_email,
    listing_url,
    observed_at,
    listing_fingerprint,
    cross_source_fingerprint,
    dashboard_group_key,
    duplicate_count,
    latitude,
    longitude
FROM ranked r
WHERE dedupe_rank = 1
  AND (
    dashboard_is_currently_available = TRUE
    OR COALESCE(is_available_soon, FALSE) = TRUE
  )
  AND NOT EXISTS (
    SELECT 1
    FROM listing_flags lf
    WHERE lf.is_resolved = FALSE
      AND (
        (
          lf.flag_scope = 'listing'
          AND lf.source = r.source
          AND lf.source_record_id = r.source_record_id
        )
        OR
        (
          lf.flag_scope = 'property'
          AND lf.cross_source_fingerprint = r.cross_source_fingerprint
        )
      )
  );