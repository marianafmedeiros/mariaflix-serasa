CREATE TABLE IF NOT EXISTS Users (
    user_id SERIAL PRIMARY KEY,
    username VARCHAR (35) UNIQUE NOT NULL,
    email VARCHAR (255) UNIQUE NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS User_Show (
    id SERIAL PRIMARY KEY,
    user_id INT NOT NULL,
    show_id INT NOT NULL,
    finished_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ DEFAULT NOW(),
    FOREIGN KEY (user_id)
        REFERENCES Users (user_id)
        ON DELETE CASCADE,
    FOREIGN KEY (show_id)
        REFERENCES Shows (show_id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS Shows (
    show_id SERIAL PRIMARY KEY,
    show_type SMALLINT NOT NULL,
    title VARCHAR (255),
    UNIQUE (show_type, title)
);

CREATE TABLE IF NOT EXISTS Movies (
    id SERIAL PRIMARY KEY,
    show_id INT NOT NULL,
    movie_length SMALLINT,
    year SMALLINT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    FOREIGN KEY (show_id)
        REFERENCES Shows (show_id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS Episodes (
    episode_number SMALLINT NOT NULL,
    series_id INT NOT NULL,
    season SMALLINT NOT NULL,
    show_id INT NOT NULL,
    episode_length SMALLINT,
    release_date DATE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    FOREIGN KEY (show_id)
        REFERENCES Shows (show_id)
        ON DELETE CASCADE,
    FOREIGN KEY (series_id)
        REFERENCES Series (series_id)
        ON DELETE CASCADE,
    PRIMARY KEY (episode_number, series_id, season)
);

CREATE TABLE IF NOT EXISTS Series (
    series_id SERIAL PRIMARY KEY,
    series_title VARCHAR (255) NOT NULL,
    release_year SMALLINT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (series_title, release_year)
);


