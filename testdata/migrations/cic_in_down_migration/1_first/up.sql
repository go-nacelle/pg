-- Create a sample table
CREATE TABLE users (
    id       SERIAL PRIMARY KEY,
    username TEXT NOT NULL,
    email    TEXT NOT NULL
);
