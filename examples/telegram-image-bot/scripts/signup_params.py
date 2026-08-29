# `ready` arriving means the table exists; the form's two values become
# the insert's parameter list. An empty credits field enrolls with 5.
return {"params": [telegramId, credits or "5"]}
