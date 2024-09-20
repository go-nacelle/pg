-- Create the posts table
CREATE TABLE posts (
    id         SERIAL PRIMARY KEY,
    user_id    INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    title      TEXT NOT NULL,
    content    TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
