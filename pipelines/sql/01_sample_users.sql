CREATE OR REFRESH MATERIALIZED VIEW sample_users AS
SELECT user_id, name, email, updated_at
FROM learning.sdp_demo.users_src;
