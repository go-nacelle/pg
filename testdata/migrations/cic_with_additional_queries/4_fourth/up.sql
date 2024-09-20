-- Add and backfill last_login column
ALTER TABLE users ADD COLUMN last_login TIMESTAMP;
UPDATE users SET last_login = NOW() WHERE email IN ('user1@example.com', 'user2@example.com');

-- Create a index concurrently
CREATE INDEX CONCURRENTLY idx_users_email ON users (email);

-- Create another index
CREATE INDEX idx_users_created_at ON users (created_at);
