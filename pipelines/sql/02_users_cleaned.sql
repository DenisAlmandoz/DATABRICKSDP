-- Drop records with null email
CREATE OR REFRESH MATERIALIZED VIEW users_cleaned
(
  CONSTRAINT non_null_email EXPECT (email IS NOT NULL) ON VIOLATION DROP ROW
) AS
SELECT *
FROM sample_users;
