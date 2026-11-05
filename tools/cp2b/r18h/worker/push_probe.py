"""Read-only proof for one synthetic owner Push."""
from datetime import datetime, timezone
import json
import os
import re
import sys
import time
from pathlib import Path
from urllib.request import HTTPRedirectHandler, Request, build_opener

REPO = "malin-bg/bgzona-private-intake"
REPO_ID = 1429436685
OWNER = 252877943
REF = "refs/heads/cp2b-private-push-r1"


class Stop(Exception):
    pass


def oid(value):
    return isinstance(value, str) and bool(re.fullmatch("[0-9a-f]{40}", value)) and value != "0" * 40



class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise Stop("REDIRECT_REFUSED")


class GitHub:
    def __init__(self, token):
        self.token = token
        self.opener = build_opener(NoRedirect)

    def get(self, path):
        request = Request("https://api.github.com/repos/" + REPO + path, headers={
            "Authorization": "Bearer " + self.token,
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2026-03-10",
        })
        with self.opener.open(request, timeout=5) as response:
            raw = response.read(4_000_001)
            if len(raw) > 4_000_000:
                raise Stop("RESPONSE_LIMIT")
            return json.loads(raw), 'rel="next"' in response.headers.get("Link", "")


def verify_event(event, source, bootstrap, *, ref=REF):
    if (not oid(source) or not oid(bootstrap) or source == bootstrap
            or event["repository"]["id"] != REPO_ID or event["repository"]["full_name"] != REPO
            or event["repository"]["private"] is not True or event["sender"]["id"] != OWNER
            or ref not in (REF, "refs/heads/cp2b-private-standard-source-r1")
            or event["ref"] != ref or event["before"] != bootstrap or event["after"] != source
            or event["forced"] is not False or event["created"] is not False or event["deleted"] is not False):
        raise Stop("PUSH_EVENT_MISMATCH")


def find_activity(rows, before, after, *, ref=REF):
    matches = []
    for row in rows:
        if row["ref"] != ref or row["after"] != after:
            continue
        if row["activity_type"] != "push" or row["actor"]["id"] != OWNER or row["before"] != before:
            raise Stop("ACTIVITY_CONFLICT")
        matches.append(row)
    if len(matches) > 1:
        raise Stop("ACTIVITY_AMBIGUOUS")
    return matches[0] if matches else None


def prove(api, event, source, bootstrap, started, *, clock=time.monotonic, pause=time.sleep,
          utcnow=lambda: datetime.now(timezone.utc), ref=REF):
    verify_event(event, source, bootstrap, ref=ref)
    repo, _ = api.get("")
    if (repo["id"], repo["full_name"], repo["private"], repo["owner"]["id"]) != (REPO_ID, REPO, True, OWNER):
        raise Stop("REPOSITORY_MISMATCH")
    commit, _ = api.get("/git/commits/" + source)
    if commit["sha"] != source or [p["sha"] for p in commit["parents"]] != [bootstrap]:
        raise Stop("ANCESTRY_MISMATCH")
    deadline = clock() + 45
    for attempt in range(10):
        rows, more = api.get("/activity?per_page=100")
        if more:
            raise Stop("ACTIVITY_WINDOW_TRUNCATED")
        activity = find_activity(rows, bootstrap, source, ref=ref)
        observed = utcnow()
        if activity:
            pushed = datetime.fromisoformat(activity["timestamp"].replace("Z", "+00:00"))
            if pushed.tzinfo is None or pushed > observed or pushed > started:
                raise Stop("ACTIVITY_TIMESTAMP")
            return dict(push_to_step_seconds=(started-pushed).total_seconds(),
                        push_to_observed_seconds=(observed-pushed).total_seconds())
        left = deadline - clock()
        if left <= 0 or attempt == 9:
            raise Stop("ACTIVITY_NOT_OBSERVED_IN_WINDOW")
        pause(min(5, left))
    raise Stop("ACTIVITY_NOT_OBSERVED_IN_WINDOW")


def main():
    if os.environ["GITHUB_EVENT_NAME"] != "push" or os.environ["GITHUB_REF"] != REF:
        raise Stop("TRIGGER_MISMATCH")
    event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text(encoding="utf-8"))
    started = datetime.fromisoformat(os.environ["STEP_STARTED_UTC"])
    result = prove(GitHub(os.environ["GH_TOKEN"]), event, os.environ["GITHUB_SHA"],
                   os.environ["WORKER_COMMIT"], started)
    print("PUSH_PROOF_OK " + json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except Stop as exc:
        print(f"STOP {exc}", file=sys.stderr)
        sys.exit(1)
    except Exception:
        print("STOP PROVIDER_UNAVAILABLE_OR_MALFORMED", file=sys.stderr)
        sys.exit(1)
