-- Create a concurrent index
CREATE INDEX CONCURRENTLY idx_users_email ON users (email);
