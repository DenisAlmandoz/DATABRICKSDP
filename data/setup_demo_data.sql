CREATE CATALOG IF NOT EXISTS learning;
CREATE SCHEMA IF NOT EXISTS learning.sdp_demo;

CREATE OR REPLACE TABLE learning.sdp_demo.users_src (
  user_id BIGINT,
  name STRING,
  email STRING,
  updated_at TIMESTAMP
);

INSERT INTO learning.sdp_demo.users_src VALUES
(1, 'Alice', 'alice@example.com', current_timestamp()),
(2, 'Bob', NULL, current_timestamp()),
(3, 'Charlie', 'charlie@example.com', current_timestamp()),
(4, 'Dina', 'dina@example.com', current_timestamp());

CREATE OR REPLACE TABLE learning.sdp_demo.bookings_src (
  booking_id BIGINT,
  user_id BIGINT,
  amount DECIMAL(10,2),
  booking_ts TIMESTAMP
);

INSERT INTO learning.sdp_demo.bookings_src VALUES
(1001, 1, 125.50, current_timestamp()),
(1002, 1, 80.00, current_timestamp()),
(1003, 3, 210.00, current_timestamp()),
(1004, 4, 45.00, current_timestamp());

CREATE OR REPLACE TABLE learning.sdp_demo.users_cdc_events (
  user_id BIGINT,
  name STRING,
  email STRING,
  op STRING,
  event_ts TIMESTAMP
);

INSERT INTO learning.sdp_demo.users_cdc_events VALUES
(1, 'Alice', 'alice@example.com', 'I', timestamp('2026-01-01 00:00:00')),
(2, 'Bob', NULL, 'I', timestamp('2026-01-01 00:01:00')),
(1, 'Alice A', 'alice.a@example.com', 'U', timestamp('2026-01-01 00:02:00')),
(2, 'Bob', NULL, 'D', timestamp('2026-01-01 00:03:00')),
(3, 'Charlie', 'charlie@example.com', 'I', timestamp('2026-01-01 00:04:00'));
