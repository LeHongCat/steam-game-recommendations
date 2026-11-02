import pyarrow as pa

UTC = pa.timestamp("us", tz="UTC")
GAME_SCHEMA = pa.schema([
    ("item_id", pa.string()), ("title", pa.string()), ("description", pa.string()),
    ("developers", pa.list_(pa.string())), ("publishers", pa.list_(pa.string())),
    ("tag_ids", pa.list_(pa.int64())), ("category_ids", pa.list_(pa.int64())),
    ("is_free", pa.bool_()), ("price_minor_units", pa.int64()), ("price_currency", pa.string()),
    ("price_currency_source", pa.string()), ("purchase_option_name", pa.string()),
    ("purchase_options_json", pa.string()), ("country_code", pa.string()),
    ("observed_at", UTC), ("source", pa.string()),
])
REVIEW_SCHEMA = pa.schema([
    ("review_id", pa.string()), ("user_id", pa.string()), ("item_id", pa.string()),
    ("review_text", pa.string()), ("recommended", pa.bool_()), ("language", pa.string()),
    ("created_at", UTC), ("updated_at", UTC), ("observed_at", UTC),
    ("playtime_at_review_minutes", pa.int64()), ("playtime_forever_minutes", pa.int64()),
    ("votes_up", pa.int64()), ("votes_funny", pa.int64()),
    ("steam_purchase", pa.bool_()), ("received_for_free", pa.bool_()), ("refunded", pa.bool_()),
    ("absa_eligible", pa.bool_()), ("source_page", pa.string()), ("source", pa.string()),
])
INTERACTION_SCHEMA = pa.schema([
    ("event_id", pa.string()), ("source_review_id", pa.string()), ("user_id", pa.string()),
    ("item_id", pa.string()), ("event_type", pa.string()), ("event_time", UTC),
    ("source", pa.string()),
])
STATS_SCHEMA = pa.schema([
    ("item_id", pa.string()), ("review_count", pa.int64()), ("positive_count", pa.int64()),
    ("negative_count", pa.int64()), ("positive_ratio", pa.float64()), ("run_id", pa.string()),
])
STREAM_SCHEMA = pa.schema(list(REVIEW_SCHEMA) + [
    pa.field("event_id", pa.string()), pa.field("revision", pa.string()),
    pa.field("source_run_id", pa.string()), pa.field("source_mode", pa.string()),
])
