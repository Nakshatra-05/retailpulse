CREATE TABLE IF NOT EXISTS bronze.customers
(
    customer_id             String,
    customer_unique_id      String,
    customer_zip_code_prefix String,
    customer_city           String,
    customer_state          String,
    _ingested_at            DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY customer_id;

CREATE TABLE IF NOT EXISTS bronze.orders
(
    order_id                      String,
    customer_id                   String,
    order_status                  String,
    order_purchase_timestamp      String,
    order_approved_at             String,
    order_delivered_carrier_date  String,
    order_delivered_customer_date String,
    order_estimated_delivery_date String,
    _ingested_at                  DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY order_id;

CREATE TABLE IF NOT EXISTS bronze.order_items
(
    order_id             String,
    order_item_id        String,
    product_id           String,
    seller_id            String,
    shipping_limit_date  String,
    price                String,
    freight_value        String,
    _ingested_at         DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (order_id, order_item_id);

CREATE TABLE IF NOT EXISTS bronze.order_payments
(
    order_id             String,
    payment_sequential   String,
    payment_type         String,
    payment_installments String,
    payment_value        String,
    _ingested_at         DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (order_id, payment_sequential);

CREATE TABLE IF NOT EXISTS bronze.order_reviews
(
    review_id                String,
    order_id                 String,
    review_score             String,
    review_comment_title     String,
    review_comment_message   String,
    review_creation_date     String,
    review_answer_timestamp  String,
    _ingested_at              DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (review_id, order_id);

CREATE TABLE IF NOT EXISTS bronze.products
(
    product_id                   String,
    product_category_name        String,
    product_name_lenght          String,
    product_description_lenght   String,
    product_photos_qty           String,
    product_weight_g             String,
    product_length_cm            String,
    product_height_cm            String,
    product_width_cm             String,
    _ingested_at                 DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY product_id;

CREATE TABLE IF NOT EXISTS bronze.sellers
(
    seller_id              String,
    seller_zip_code_prefix String,
    seller_city             String,
    seller_state            String,
    _ingested_at            DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY seller_id;

CREATE TABLE IF NOT EXISTS bronze.geolocation
(
    geolocation_zip_code_prefix String,
    geolocation_lat              String,
    geolocation_lng              String,
    geolocation_city             String,
    geolocation_state            String,
    _ingested_at                 DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY geolocation_zip_code_prefix;

CREATE TABLE IF NOT EXISTS bronze.product_category_name_translation
(
    product_category_name          String,
    product_category_name_english  String,
    _ingested_at                    DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY product_category_name;