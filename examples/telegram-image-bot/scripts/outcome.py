# The query returns one row: user_id when the credit was taken, refusal
# when it was not. Returning no "refusal" key closes that port, which is
# what skips the apology message downstream.
row = rows[0]
if row.get("refusal"):
    return {"paid": False, "refusal": row["refusal"]}
return {"paid": True}
