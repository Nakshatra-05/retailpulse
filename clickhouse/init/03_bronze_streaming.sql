CREATE TABLE IF NOT EXISTS bronze.retail_events
(
    event_id          String,
    event_type        String,
    user_id           String,
    session_id        String,
    event_timestamp   String,
    product_category  String,
    price             Nullable(String),
    city              String,
    state             String,
    _ingested_at       DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(parseDateTimeBestEffortOrZero(event_timestamp))
ORDER BY (event_type, user_id, event_timestamp);