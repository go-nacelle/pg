-- Drop new indexes
DROP INDEX idx_users_created_at;
DROP INDEX idx_users_email;

-- Drop new column
ALTER TABLE users DROP COLUMN last_login;
