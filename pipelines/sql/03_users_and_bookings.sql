CREATE OR REFRESH MATERIALIZED VIEW users_and_bookings AS
SELECT
  u.name,
  COUNT(b.booking_id) AS booking_count
FROM users_cleaned u
JOIN learning.sdp_demo.bookings_src b
  ON u.user_id = b.user_id
GROUP BY u.name
ORDER BY booking_count DESC
LIMIT 100;
