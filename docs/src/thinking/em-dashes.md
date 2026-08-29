# Why we hunt em dashes

The problem with AI slop is not that a machine wrote it. It is that nobody went
back over it afterwards.

Models reach for the em dash far more than people do, so the mark is a tell.
Not a tell that a machine wrote the line, a tell that nobody has read it again
since it appeared.

We use it as one. The review pass ends by stripping every em dash, so the two
can never be in the same file. Find one in this repo and you have found a
paragraph, a comment or a function that skipped review. Find none and somebody
went through it line by line.

That only holds if the sweep is the last thing the review does, and if nothing
else in the pipeline ever removes them quietly. So: comma, colon, parentheses,
or two sentences. Never the dash, in prose, in code comments, in commit
messages, anywhere.
