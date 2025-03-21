-- Create sports table
CREATE TABLE IF NOT EXISTS sports (
    id INTEGER PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

-- Create leagues table
CREATE TABLE IF NOT EXISTS leagues (
    id INTEGER PRIMARY KEY,
    sport_id INTEGER NOT NULL REFERENCES sports(id),
    name VARCHAR(255) NOT NULL,
    group_name VARCHAR(255),
    is_hidden BOOLEAN DEFAULT false,
    is_promoted BOOLEAN DEFAULT false,
    is_sticky BOOLEAN DEFAULT false,
    sequence INTEGER DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create teams table
CREATE TABLE IF NOT EXISTS teams (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create matches table
CREATE TABLE IF NOT EXISTS matches (
    id INTEGER PRIMARY KEY,
    best_of_x INTEGER NOT NULL DEFAULT 1,
    is_live BOOLEAN DEFAULT false,
    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    parent_id INTEGER NULL,
    league_id INTEGER NOT NULL REFERENCES leagues(id),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create match_participants table (junction table for matches and teams)
CREATE TABLE IF NOT EXISTS match_participants (
    id SERIAL PRIMARY KEY,
    match_id INTEGER REFERENCES matches(id) ON DELETE CASCADE,
    team_id INTEGER REFERENCES teams(id) ON DELETE CASCADE,
    alignment VARCHAR(50) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create straights table (bets)
CREATE TABLE IF NOT EXISTS straights (
    id SERIAL PRIMARY KEY,
    key VARCHAR(255) NOT NULL,
    matchup_id INTEGER NOT NULL,
    period INTEGER NOT NULL DEFAULT 0,
    side VARCHAR(50),
    status VARCHAR(50) NOT NULL,
    type VARCHAR(50) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create prices table
CREATE TABLE IF NOT EXISTS prices (
    id SERIAL PRIMARY KEY,
    straight_id INTEGER NOT NULL REFERENCES straights(id) ON DELETE CASCADE,
    designation VARCHAR(50) NOT NULL,
    price INTEGER NOT NULL,
    points DOUBLE PRECISION,
    participant_id INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_leagues_sport_id ON leagues(sport_id);
CREATE INDEX IF NOT EXISTS idx_matches_league_id ON matches(league_id);
CREATE INDEX IF NOT EXISTS idx_matches_parent_id ON matches(parent_id);
CREATE INDEX IF NOT EXISTS idx_match_participants_match_id ON match_participants(match_id);
CREATE INDEX IF NOT EXISTS idx_match_participants_team_id ON match_participants(team_id);
CREATE INDEX IF NOT EXISTS idx_straights_matchup_id ON straights(matchup_id);
CREATE INDEX IF NOT EXISTS idx_prices_straight_id ON prices(straight_id);
CREATE INDEX IF NOT EXISTS idx_prices_participant_id ON prices(participant_id);
