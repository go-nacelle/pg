-- Add a new column and create an index
ALTER TABLE users ADD COLUMN created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW();
CREATE INDEX idx_users_created_at ON users(created_at);
