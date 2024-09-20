-- Remove the index and drop the column
DROP INDEX IF EXISTS idx_users_created_at;
ALTER TABLE users DROP COLUMN IF EXISTS created_at;
