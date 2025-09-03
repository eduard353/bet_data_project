CREATE TABLE IF NOT EXISTS bets (
    bet_id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    game VARCHAR(100) NOT NULL,
    amount NUMERIC(10,2) NOT NULL,
    status VARCHAR(20) NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Индексы для аналитики
CREATE INDEX IF NOT EXISTS idx_bets_game ON bets(game);
CREATE INDEX IF NOT EXISTS idx_bets_status ON bets(status);
CREATE INDEX IF NOT EXISTS idx_bets_user_id ON bets(user_id);


-- Таблица для хранения истории топ-игр
CREATE TABLE IF NOT EXISTS top_games_history (
    id BIGSERIAL PRIMARY KEY,
    game VARCHAR(100) NOT NULL,
    count BIGINT NOT NULL,
    event_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Индекс для быстрого поиска по игре
CREATE INDEX IF NOT EXISTS idx_top_games_history_game ON top_games_history(game);

-- Индекс по времени (например, для аналитики по периодам)
CREATE INDEX IF NOT EXISTS idx_top_games_history_time ON top_games_history(event_time);
