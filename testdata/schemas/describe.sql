-- Extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- Enums
CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');
CREATE TYPE weather AS ENUM ('sunny', 'rainy', 'cloudy', 'snowy');

-- Functions
CREATE OR REPLACE FUNCTION get_random_mood() RETURNS mood AS $$
BEGIN
    RETURN (ARRAY['sad', 'ok', 'happy'])[floor(random() * 3 + 1)];
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_weather_description(w weather) RETURNS TEXT AS $$
BEGIN
    CASE 
        WHEN w = 'sunny' THEN
            RETURN 'Pack some SPF!';
        WHEN w = 'rainy' THEN
            RETURN 'Bring an umbrella!';
        WHEN w = 'cloudy' THEN
            RETURN 'Wear a jacket!';
        WHEN w = 'snowy' THEN
            RETURN 'Bundle up!';
        ELSE
            RETURN 'Unknown weather';
    END CASE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_last_modified() RETURNS TRIGGER AS $$
BEGIN
    NEW.last_modified = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Sequences
CREATE SEQUENCE IF NOT EXISTS user_id_seq START 1000;

-- Tables
CREATE TABLE users (
    id INTEGER PRIMARY KEY DEFAULT nextval('user_id_seq'),
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    password_hash VARCHAR(100) NOT NULL,
    mood mood,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    last_modified TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE posts (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    title VARCHAR(200) NOT NULL,
    content TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    last_modified TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE comments (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    post_id UUID NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    content TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Views
CREATE VIEW active_users AS
SELECT id, username, email, mood
FROM users
WHERE last_modified > CURRENT_TIMESTAMP - INTERVAL '30 days';

CREATE VIEW post_stats AS
SELECT p.id AS post_id, p.title, p.user_id, u.username, COUNT(c.id) AS comment_count
FROM posts p
JOIN users u ON p.user_id = u.id
LEFT JOIN comments c ON p.id = c.post_id
GROUP BY p.id, p.title, p.user_id, u.username;

-- Triggers
CREATE TRIGGER update_user_last_modified
BEFORE UPDATE ON users
FOR EACH ROW
EXECUTE FUNCTION update_last_modified();

CREATE TRIGGER update_post_last_modified
BEFORE UPDATE ON posts
FOR EACH ROW
EXECUTE FUNCTION update_last_modified();

-- Indexes
CREATE INDEX idx_users_username ON users USING btree (username);
CREATE INDEX idx_users_email ON users USING btree (email);
CREATE INDEX idx_posts_user_id ON posts USING btree (user_id);
CREATE INDEX idx_comments_post_id ON comments USING btree (post_id);
CREATE INDEX idx_comments_user_id ON comments USING btree (user_id);

-- Full-text search index
CREATE INDEX idx_posts_content_trgm ON posts USING gin (content gin_trgm_ops);

-- EnumDependencies
-- The 'mood' enum is used in the 'users' table

-- ColumnDependencies
-- The 'users.id' column is referenced by 'posts.user_id' and 'comments.user_id'
-- The 'posts.id' column is referenced by 'comments.post_id'
