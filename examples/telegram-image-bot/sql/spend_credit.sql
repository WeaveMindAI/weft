with account as (
  select 1 from users where telegram_id = $1
),
spent as (
  update users set credits = credits - 1
  where telegram_id = $1 and credits > 0
  returning telegram_id
)
select
  case
    when not exists (select 1 from account) then 'this Telegram account is not linked to an account'
    when not exists (select 1 from spent) then 'you have no credits left'
  end as refusal
