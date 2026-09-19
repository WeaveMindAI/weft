insert into users (telegram_id, credits)
values ($telegram_id, coalesce(nullif($credits, ''), '5')::integer)
on conflict (telegram_id)
  do update set credits = users.credits + excluded.credits
returning telegram_id, credits
