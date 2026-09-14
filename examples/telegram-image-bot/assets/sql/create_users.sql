create table if not exists users (
  telegram_id text primary key,
  credits     integer not null default 0
)
