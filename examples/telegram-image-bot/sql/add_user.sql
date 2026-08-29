insert into users (telegram_id, credits)
values ($1, $2::integer)
on conflict (telegram_id)
  do update set credits = users.credits + excluded.credits
returning telegram_id, credits
