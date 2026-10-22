#!/usr/bin/env python3
import json
import os
import sys

sys.stdin.read()
path = os.environ.get("BRIDGE_INBOX", "")
event = sys.argv[1] if len(sys.argv) > 1 else "PostToolUse"
if not path:
    sys.exit(0)
taken = "%s.taking.%d" % (path, os.getpid())
try:
    os.rename(path, taken)
except OSError:
    sys.exit(0)
with open(taken) as f:
    text = f.read().strip()
os.unlink(taken)
if not text:
    sys.exit(0)
note = ("The user sent this message from Telegram while you were working "
        "(via /go). Read it now and take it into account:\n" + text)
if event == "Stop":
    print(json.dumps({"decision": "block", "reason": note}))
else:
    print(json.dumps({"hookSpecificOutput": {"hookEventName": event, "additionalContext": note}}))
