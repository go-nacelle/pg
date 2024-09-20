-- Recreate the index using CREATE INDEX CONCURRENTLY
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_users_created_at ON users(created_at);
