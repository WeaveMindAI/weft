"""The words every adapter speaks to weft: event types, turn statuses, and
the shape of a question.

One shape for every agent, so the nodes on the weft side never learn which
agent they talk to.

Events (each also carries `n`, its number, and `turn`):

  text         {text}              a whole message the agent wrote
  text_delta   {text}              a piece of one, when streaming by token
                                   (the whole message still follows as text)
  thinking     {text}              the agent's reasoning, when it shows it
  tool_use     {id, name, input}   the agent used one of ITS OWN tools
  tool_result  {id, output, isError}
                                   what that tool gave back
  file_change  {path, change}      a file the agent changed, when it says so
  tool_call    {callId, name, arguments}
                                   the agent calls one of the PROGRAM's tools
                                   and waits for its result
  question     {question}          the agent asks something (its ask tool, or
                                   a permission request) and waits
  login        {reason}            the agent asks for a fresh login (the one
                                   it holds dies soon, or its service refused
                                   it), which its agent node hands it over
                                   `/login`; the turn goes on meanwhile
  done         {reply}             the turn ended with this reply
  error        {message}           the turn failed

A question (`question` event, and the `AgentQuestion` type on the weft side):

  {"id": <what the answer names>, "kind": "question" | "permission",
   "questions": [{"id", "header", "question", "options": [{"label", "description"}],
                  "multiSelect", "allowOther", "secret"}],          kind question
   "permission": {"tool", "title", "detail", "input",
                  "choices": [{"id", "label", "kind"}]}}            kind permission

  AskAgent adds "statusUrl" on the weft side: this server's `/question?id=`,
  where AnswerAgent reads whether the agent still waits on it.

An answer (`AgentAnswer`):

  {"questionId", "answers": {<question id>: [<label or text>, ...]},  kind question
   "choice": <choice id>, "note": <text the agent reads>}            kind permission
  {"questionId", "dropped": true}     no answer: the agent stopped waiting for
                                      one, and the turn reads on from there
"""

class Refused(Exception):
    """A request the server cannot do, said back to the caller as its answer."""

    def __init__(self, message: str, status: int = 409):
        super().__init__(message)
        self.status = status


# SYNC: event types and statuses <-> catalog/agents/ask_agent/mod.rs (DONE, FAILED, LOST, TOOL_CALL, QUESTION),
#       catalog/agents/agent.rs (LOGIN_EVENT)
TEXT = "text"
TEXT_DELTA = "text_delta"
THINKING = "thinking"
TOOL_USE = "tool_use"
TOOL_RESULT = "tool_result"
FILE_CHANGE = "file_change"
TOOL_CALL = "tool_call"
QUESTION = "question"
LOGIN = "login"
DONE = "done"
ERROR = "error"

QUEUED = "queued"
WORKING = "working"
WAITING_TOOL = "waiting_tool"
WAITING_QUESTION = "waiting_question"
FAILED = "failed"
LOST = "lost"
ENDED = (DONE, FAILED, LOST)

# The kinds of a permission choice. No Rust peer: weft carries a choice's
# kind as text (`AgentChoice.kind`, catalog/agents/metadata.json), and
# AnswerAgent reads only its id and label.
ALLOW_ONCE = "allow_once"
ALLOW_ALWAYS = "allow_always"
REJECT = "reject"

# The kinds of a question.
# SYNC: question kinds <-> catalog/agents/answer_agent/mod.rs (QuestionKind)
KIND_QUESTION = "question"
KIND_PERMISSION = "permission"


def permission(question_id: str, tool: str, title: str, detail: str, tool_input, choices: list) -> dict:
    return {"id": question_id, "kind": KIND_PERMISSION, "questions": [],
            "permission": {"tool": tool, "title": title, "detail": detail, "input": tool_input, "choices": choices}}


def questions(question_id: str, asked: list) -> dict:
    return {"id": question_id, "kind": KIND_QUESTION, "questions": asked, "permission": None}


def asked(qid: str, header: str, text: str, options: list, multi: bool, allow_other: bool, secret: bool = False) -> dict:
    return {"id": qid, "header": header or "", "question": text, "options": options,
            "multiSelect": bool(multi), "allowOther": bool(allow_other), "secret": bool(secret)}


def choice(choice_id: str, label: str, kind: str) -> dict:
    return {"id": choice_id, "label": label, "kind": kind}


def chosen(answer: dict, question: dict) -> dict:
    """The permission choice an answer picked, checked against what was offered."""
    picked = answer.get("choice")
    offered = question["permission"]["choices"]
    for option in offered:
        if option["id"] == picked:
            return option
    names = ", ".join(o["id"] for o in offered)
    raise Refused(f"the answer picks {picked!r}, and this permission request offers {names}", 400)


def answers_for(answer: dict, question: dict) -> dict:
    """Each question's answers, checked: every question answered, as a list of strings."""
    given = answer.get("answers")
    if not isinstance(given, dict):
        raise Refused("the answer carries no `answers`: one list of strings per question id", 400)
    out = {}
    for q in question["questions"]:
        values = given.get(q["id"])
        if isinstance(values, str):
            values = [values]
        if not isinstance(values, list) or not values or not all(isinstance(v, str) for v in values):
            raise Refused(f"question {q['id']!r} ({q['question']!r}) has no answer; give it a list of strings", 400)
        if len(values) > 1 and not q["multiSelect"]:
            raise Refused(f"question {q['id']!r} takes one answer and got {len(values)}", 400)
        labels = {o["label"] for o in q["options"]}
        if not q["allowOther"] and labels and not all(v in labels for v in values):
            raise Refused(f"question {q['id']!r} takes one of {', '.join(sorted(labels))}", 400)
        out[q["id"]] = values
    return out
