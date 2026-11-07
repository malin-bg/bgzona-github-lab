"""Lab scheduler candidate. Only its protected auto target may process media."""
import base64
from datetime import datetime, timedelta, timezone
import json
import os
import re
import sys
from urllib.error import HTTPError
from urllib.request import HTTPRedirectHandler, Request, build_opener

REPO = "malin-bg/bgzona-github-lab"
REPO_ID = 1426765395
TARGET_ID = 408732936
TARGET_PATH = ".github/workflows/shared-media.yml"
SCHEDULE_PATH = ".github/workflows/lab-scheduled.yml"
ACTIVITY = ".github/bgzona-scheduler-activity.json"
CONTROL = "refs/heads/automation-control"
MAIN = "refs/heads/main"
PERMISSIONS = {"actions": "write", "contents": "read"}
KEEPALIVE_DAYS = 30
MAX_BYTES = 1_000_000
OID = re.compile(r"[0-9a-f]{40}")
QUERY = """mutation($input: CreateCommitOnBranchInput!) {
  createCommitOnBranch(input: $input) { commit { oid } }
}"""

class Stop(Exception):
    pass

def require(condition, code):
    if not condition:
        raise Stop(code)

def encode(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()

def moment(value):
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError, AttributeError):
        raise Stop("SCHEDULE_TIME") from None
    require(result.tzinfo is not None, "SCHEDULE_TIME")
    return result.astimezone(timezone.utc)

def sha(value):
    require(type(value) is str and OID.fullmatch(value), "SCHEDULE_COMMIT")
    return value

class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None

class HTTP:
    def __init__(self, token):
        require(bool(token), "SCHEDULE_AUTH")
        self.token = token
        self.opener = build_opener(NoRedirect)

    def request(self, method, path, payload=None):
        require(path.startswith("/repos/" + REPO + "/") or path == "/graphql",
                "SCHEDULE_API_SCOPE")
        headers = {"Authorization": "Bearer " + self.token,
                   "Accept": "application/vnd.github+json",
                   "X-GitHub-Api-Version": "2026-03-10",
                   "User-Agent": "BGZONA-Lab-Scheduler"}
        req = Request("https://api.github.com" + path, method=method,
                      data=None if payload is None else encode(payload),
                      headers=headers)
        try:
            with self.opener.open(req, timeout=15) as response:
                raw = response.read(MAX_BYTES + 1)
                require(len(raw) <= MAX_BYTES, "SCHEDULE_API_SIZE")
                body = json.loads(raw) if raw else None
                return response.status, body
        except HTTPError as error:
            return error.code, None
        except (OSError, ValueError):
            raise Stop("SCHEDULE_HTTP_UNRESOLVED") from None

def get(api, path):
    status, row = api.request("GET", "/repos/" + REPO + path)
    require(status == 200 and row is not None, "SCHEDULE_READ_UNRESOLVED")
    return row

def ref(api, name):
    row = get(api, "/git/ref/" + name.removeprefix("refs/"))
    require(row["ref"] == name and row["object"]["type"] == "commit",
            "SCHEDULE_REF")
    return sha(row["object"]["sha"])

def file_at(api, path, commit):
    status, row = api.request("GET", "/repos/" + REPO + "/contents/" + path + "?ref=" + commit)
    if status == 404:
        return None
    require(status == 200 and row["path"] == path and row["type"] == "file"
            and row["encoding"] == "base64", "SCHEDULE_FILE")
    try:
        raw = base64.b64decode("".join(row["content"].splitlines()), validate=True)
    except (ValueError, TypeError):
        raise Stop("SCHEDULE_FILE") from None
    from hashlib import sha1
    require(len(raw) == row["size"] and
            sha1(f"blob {len(raw)}\0".encode() + raw).hexdigest() == row["sha"],
            "SCHEDULE_BLOB")
    return raw

def compatible_target(api, control):
    workflow = get(api, "/actions/workflows/" + str(TARGET_ID))
    require((workflow["id"], workflow["path"], workflow["state"]) ==
            (TARGET_ID, TARGET_PATH, "active"), "SCHEDULE_TARGET")
    raw = file_at(api, TARGET_PATH, control)
    require(raw is not None, "SCHEDULE_TARGET")
    try:
        inputs = json.loads(raw)["on"]["workflow_dispatch"]["inputs"]
    except (ValueError, KeyError, TypeError):
        raise Stop("SCHEDULE_TARGET_CONTRACT") from None
    require("auto" in inputs["mode"]["options"] and "schedule_run" in inputs,
            "SCHEDULE_AUTO_ENTRYPOINT_REQUIRED")

def prior_tick(api, current):
    history = get(api, "/actions/workflows/lab-scheduled.yml/runs?event=schedule&per_page=2")
    rows = history["workflow_runs"]
    require(history["total_count"] >= 1 and len(rows) <= 2, "SCHEDULE_HISTORY")
    require(any(r["id"] == current for r in rows), "SCHEDULE_CURRENT_RUN")
    older = [r for r in rows if r["id"] != current]
    if older:
        previous, = older
        require(previous["status"] == "completed" and previous["conclusion"] == "success",
                "SCHEDULE_PREVIOUS_UNRESOLVED")

def keepalive(api, head, now, *, allow_write=False):
    raw = file_at(api, ACTIVITY, head)
    if raw is not None:
        try:
            marker = json.loads(raw)
            require(set(marker) == {"schema", "updated_at"} and marker["schema"] == 1,
                    "SCHEDULE_ACTIVITY")
            previous = moment(marker["updated_at"])
        except (ValueError, TypeError):
            raise Stop("SCHEDULE_ACTIVITY") from None
        require(previous <= now, "SCHEDULE_ACTIVITY_TIME")
        if now - previous < timedelta(days=KEEPALIVE_DAYS):
            return head
    require(allow_write, "SCHEDULE_ACTIVITY_OWNER_GO_REQUIRED")
    payload = encode({"schema": 1, "updated_at": now.isoformat()})
    require(ref(api, MAIN) == head, "SCHEDULE_MAIN_MOVED")
    args = {"branch": {"repositoryNameWithOwner": REPO, "branchName": "main"},
            "expectedHeadOid": head,
            "message": {"headline": "Maintain BGZONA scheduled activity [skip ci]"},
            "fileChanges": {"additions": [{"path": ACTIVITY,
                "contents": base64.b64encode(payload).decode()}]}}
    status, reply = api.request("POST", "/graphql", {"query": QUERY, "variables": {"input": args}})
    require(status == 200 and type(reply) is dict and not reply.get("errors"),
            "SCHEDULE_ACTIVITY_UNRESOLVED")
    try:
        commit = sha(reply["data"]["createCommitOnBranch"]["commit"]["oid"])
    except (KeyError, TypeError):
        raise Stop("SCHEDULE_ACTIVITY_UNRESOLVED") from None
    row = get(api, "/git/commits/" + commit)
    require([p["sha"] for p in row["parents"]] == [head]
            and ref(api, MAIN) == commit and file_at(api, ACTIVITY, commit) == payload,
            "SCHEDULE_ACTIVITY_UNRESOLVED")
    delta = get(api, "/compare/" + head + "..." + commit)
    require(delta["total_commits"] == 1 and
            [f["filename"] for f in delta["files"]] == [ACTIVITY],
            "SCHEDULE_ACTIVITY_UNRESOLVED")
    return commit

def tick(api, env, now):
    current = int(env["GITHUB_RUN_ID"])
    require(current > 0 and env["GITHUB_ACTIONS"] == "true" and
            env["GITHUB_EVENT_NAME"] == "schedule" and
            env["GITHUB_REPOSITORY"] == REPO and
            env["GITHUB_REPOSITORY_ID"] == str(REPO_ID) and
            env["GITHUB_REF"] == MAIN and env["GITHUB_RUN_ATTEMPT"] == "1" and
            env["GITHUB_WORKFLOW_REF"] == REPO + "/" + SCHEDULE_PATH + "@" + MAIN,
            "SCHEDULE_CONTEXT")
    head = sha(env["GITHUB_SHA"])
    require(ref(api, MAIN) == head, "SCHEDULE_MAIN_MOVED")
    prior_tick(api, current)
    control = ref(api, CONTROL)
    compatible_target(api, control)
    listing = get(api, "/actions/workflows/" + str(TARGET_ID) +
                  "/runs?branch=automation-control&event=workflow_dispatch&per_page=100")
    rows = listing["workflow_runs"]
    require(all(r["workflow_id"] == TARGET_ID and
                r["head_branch"] == "automation-control" for r in rows),
            "SCHEDULE_TARGET_HISTORY")
    for state in ("queued", "in_progress", "waiting", "pending", "requested"):
        active = get(api, "/actions/workflows/" + str(TARGET_ID) +
                     "/runs?branch=automation-control&event=workflow_dispatch&status=" +
                     state + "&per_page=100")
        require(active["total_count"] == len(active["workflow_runs"]),
                "SCHEDULE_ACTIVE_WINDOW")
        if active["workflow_runs"]:
            keepalive(api, head, now, allow_write=env.get('BGZONA_ACTIVITY_COMMIT_GO')=='true')
            return "BUSY"
    if any(r["status"] != "completed" for r in rows):
        keepalive(api, head, now, allow_write=env.get('BGZONA_ACTIVITY_COMMIT_GO')=='true')
        return "BUSY"
    automatic = [r for r in rows if r["display_title"].startswith("BGZONA auto ")]
    require(bool(automatic) or listing["total_count"] == len(rows),
            "SCHEDULE_RUN_WINDOW")
    if automatic:
        latest = max(automatic, key=lambda r: r["run_number"])
        require(latest["conclusion"] == "success", "SCHEDULE_TARGET_UNRESOLVED")
    keepalive(api, head, now, allow_write=env.get('BGZONA_ACTIVITY_COMMIT_GO')=='true')
    require(ref(api, CONTROL) == control, "SCHEDULE_CONTROL_MOVED")
    status, row = api.request("POST", "/repos/" + REPO + "/actions/workflows/" +
                             str(TARGET_ID) + "/dispatches",
                             {"ref": "automation-control", "return_run_details": True,
                              "inputs": {"mode": "auto", "schedule_run": str(current)}})
    require(status == 200 and type(row) is dict and
            type(row.get("workflow_run_id")) is int, "SCHEDULE_DISPATCH_UNRESOLVED")
    launched = get(api, "/actions/runs/" + str(row["workflow_run_id"]))
    require((launched["id"], launched["workflow_id"], launched["head_branch"],
             launched["head_sha"], launched["event"], launched["run_attempt"]) ==
            (row["workflow_run_id"], TARGET_ID, "automation-control", control,
             "workflow_dispatch", 1), "SCHEDULE_DISPATCH_UNRESOLVED")
    return "DISPATCHED"

def main():
    try:
        result = tick(HTTP(os.environ["GH_TOKEN"]), os.environ, datetime.now(timezone.utc))
        print("LAB_SCHEDULE_" + result)
    except (Stop, KeyError, ValueError) as error:
        code = str(error) if type(error) is Stop else "SCHEDULE_CONTEXT"
        print("STOP " + code, file=sys.stderr)
        raise SystemExit(1) from None

if __name__ == "__main__":
    main()
