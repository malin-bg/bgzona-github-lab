"""One-way PRIVATE -> protected Lab wake. It grants no registration authority."""
from pathlib import Path
from urllib.request import Request, build_opener
from urllib.parse import quote
import json, os, re, sys, time
import push_probe as proof

REPO = proof.REPO
REPO_ID = proof.REPO_ID
OWNER = proof.OWNER
REF = "refs/heads/cp2b-private-standard-source-r1"
PATH = ".github/workflows/c5-private-lab-relay.yml"
LAB = "malin-bg/bgzona-github-lab"
LAB_WORKFLOW = 408732936
LAB_REF = "automation-control"
SOURCE_CODE = "a385319f4c7a5e09d8a5d3a815f6daffbc4de41b"
SOURCE_MANIFEST = "b3c63ff02985c666f2c6b1cc2b100cd6421df7ecbb67c7f52eef5521a95c0c27"
CATCHUP_RUN = 42374329727
CATCHUP_SOURCE = "d398cf61b16d5e80ffe1212580249cedd1a3edae"
CATCHUP_ARTIFACT = 13574868148
CATCHUP_DIGEST = "sha256:913a07cd569698f6ebd2e0aa9424e328b57777cd5a3f85fb9e4c7cdfe3bc8636"

class Stop(RuntimeError):
    pass

def need(condition, code):
    if not condition:
        raise Stop(code)

def number(value):
    need(type(value) is str and re.fullmatch(r"[1-9][0-9]*", value), "RELAY_RUN_ID")
    return int(value)

def get(api, path):
    try:
        value, more = api.get(path)
        need(type(value) is dict or type(value) is list, "RELAY_PROVIDER_FORMAT")
        return value, more
    except Stop:
        raise
    except Exception:
        raise Stop("RELAY_PROVIDER_UNRESOLVED") from None

def source_evidence(api, source_run):
    """Reuse the original R13 artifact/run/Activities verifier, without media execution."""
    import push_artifact as artifact
    from registration_entry import read_archive
    need(type(source_run) is int and source_run > 0, "RELAY_SOURCE_ID")
    repository, _ = get(api, "")
    need((repository["id"], repository["full_name"], repository["private"], repository["owner"]["id"]) ==
         (REPO_ID, REPO, True, OWNER), "RELAY_REPOSITORY")
    source, _ = get(api, f"/actions/runs/{source_run}/attempts/1")
    need((source["id"], source["run_attempt"], source["workflow_id"], source["event"],
          source["status"], source["conclusion"], source["actor"]["id"],
          source["repository"]["id"], source["head_branch"], source["path"].split("@", 1)[0]) ==
         (source_run, 1, artifact.WORKFLOW, "push", "completed", "success", OWNER,
          REPO_ID, REF.removeprefix("refs/heads/"), artifact.WORKFLOW_PATH), "RELAY_SOURCE_IDENTITY")
    need(source.get("triggering_actor", source["actor"])["id"] == OWNER and
         proof.oid(source["head_sha"]), "RELAY_SOURCE_ACTOR")
    commit, _ = get(api, "/git/commits/" + source["head_sha"])
    need(commit["sha"] == source["head_sha"] and len(commit["parents"]) == 1, "RELAY_SOURCE_PARENT")
    rows, more = get(api, f"/actions/runs/{source_run}/artifacts?per_page=100")
    need(not more, "RELAY_ARTIFACT_WINDOW")
    item = artifact.select(rows["artifacts"], source_run, 1, source["head_sha"], ref=REF)
    packet = artifact.decode(read_archive(api, item["id"]), item)
    activities, _ = get(api, "/activity?per_page=100")
    artifact.verify(packet, source_run, 1, source["head_sha"], source, activities,
                    [p["sha"] for p in commit["parents"]], ref=REF)
    return source, item

def verify(api, source_run, relay_run, main_commit, *, running=False, pause=time.sleep, completed_recovery=False):
    """A wake is a hint; both Lab intake and PRIVATE writers independently verify it."""
    need(type(relay_run) is int and relay_run > 0 and proof.oid(main_commit), "RELAY_PIN")
    workflow, _ = get(api, "/actions/workflows/" + PATH.rsplit("/", 1)[1])
    if completed_recovery:
        need(not running and (source_run, relay_run, main_commit, workflow["id"]) ==
             (CATCHUP_RUN, 42471208063, "7c15826ee3fd93dd327ae321970afe139ad71ffd", 411050691),
             "RELAY_RECOVERY_SCOPE")
    need(workflow["path"] == PATH and
         workflow["state"] == ("disabled_manually" if completed_recovery else "active") and
         type(workflow["id"]) is int and workflow["id"] > 0, "RELAY_WORKFLOW")
    for attempt in range(21):
        row, _ = get(api, f"/actions/runs/{relay_run}/attempts/1")
        if running or row["status"] == "completed":
            break
        need(attempt < 20 and row["status"] in ("queued", "in_progress", "waiting", "pending", "requested"),
             "RELAY_NOT_FINAL")
        pause(2)
    need((row["id"], row["workflow_id"], row["run_attempt"], row["repository"]["id"],
          row["actor"]["id"], row["head_branch"], row["head_sha"], row["path"].split("@", 1)[0],
          row["display_title"]) ==
         (relay_run, workflow["id"], 1, REPO_ID, OWNER, "main", main_commit, PATH,
          f"BGZONA relay {source_run}"), "RELAY_IDENTITY")
    need(row.get("triggering_actor", row["actor"])["id"] == OWNER and
         row["event"] in ("workflow_run", "workflow_dispatch"), "RELAY_EVENT")
    need(row["status"] == ("in_progress" if running else "completed") and
         (running or row["conclusion"] == "success"), "RELAY_RESULT")
    source, item = source_evidence(api, source_run)
    if row["event"] == "workflow_dispatch":
        need((source_run, source["head_sha"], item["id"], item["digest"]) ==
             (CATCHUP_RUN, CATCHUP_SOURCE, CATCHUP_ARTIFACT, CATCHUP_DIGEST), "RELAY_CATCHUP_SCOPE")
    # Do not let unrelated old runs eventually fill the replay-check window.
    created = quote(">=" + source["created_at"], safe="")
    history, more = get(api, f"/actions/workflows/{workflow['id']}/runs?created={created}&per_page=100")
    need(not more, "RELAY_HISTORY_WINDOW")
    hits = [r for r in history["workflow_runs"] if r["display_title"] == f"BGZONA relay {source_run}"]
    need(len(hits) == 1 and hits[0]["id"] == relay_run and hits[0]["run_attempt"] == 1,
         "RELAY_DUPLICATE_OR_UNKNOWN")
    return source

def dispatch(pat, source_run, relay_run, *, opener=None):
    """Exactly one POST. A lost/ambiguous response is terminal, never retried."""
    need(type(pat) is str and bool(pat), "RELAY_PAT_MISSING")
    need(type(source_run) is int and source_run > 0 and type(relay_run) is int and relay_run > 0,
         "RELAY_DISPATCH_INPUT")
    payload = dict(ref=LAB_REF, return_run_details=True,
                   inputs=dict(mode="auto", source_run=str(source_run), relay_run=str(relay_run)))
    request = Request(f"https://api.github.com/repos/{LAB}/actions/workflows/{LAB_WORKFLOW}/dispatches",
        data=json.dumps(payload, separators=(",", ":")).encode(),
        headers={"Authorization": "Bearer " + pat, "Accept": "application/vnd.github+json",
                 "X-GitHub-Api-Version": "2026-03-10", "Content-Type": "application/json"}, method="POST")
    try:
        with (opener or build_opener(proof.NoRedirect)).open(request, timeout=15) as response:
            need(response.status == 200, "RELAY_DISPATCH_UNRESOLVED")
            raw = response.read(8193)
            need(len(raw) <= 8192, "RELAY_DISPATCH_UNRESOLVED")
            value = json.loads(raw)
            need(type(value.get("workflow_run_id")) is int and value["workflow_run_id"] > 0,
                 "RELAY_DISPATCH_UNRESOLVED")
            return value["workflow_run_id"]
    except Exception:
        raise Stop("RELAY_DISPATCH_UNRESOLVED") from None

def main(env):
    need(env["GITHUB_ACTIONS"] == "true" and env["GITHUB_REPOSITORY"] == REPO and
         env["GITHUB_REPOSITORY_ID"] == str(REPO_ID) and env["GITHUB_REF"] == "refs/heads/main" and
         env["GITHUB_ACTOR_ID"] == str(OWNER) and env["GITHUB_RUN_ATTEMPT"] == "1" and
         env["GITHUB_WORKFLOW_REF"] == REPO + "/" + PATH + "@refs/heads/main" and
         proof.oid(env["GITHUB_WORKFLOW_SHA"]), "RELAY_CONTEXT")
    event = json.loads(Path(env["GITHUB_EVENT_PATH"]).read_bytes())
    need(event["repository"]["id"] == REPO_ID and event["repository"]["private"] is True, "RELAY_CONTEXT")
    if env["GITHUB_EVENT_NAME"] == "workflow_run":
        source_run = event["workflow_run"]["id"]
        need(event["workflow_run"]["workflow_id"] == 407190822 and
             event["workflow_run"]["event"] == "push" and
             event["workflow_run"]["conclusion"] == "success" and
             event["workflow_run"]["head_branch"] == REF.removeprefix("refs/heads/"), "RELAY_TRIGGER")
    elif env["GITHUB_EVENT_NAME"] == "workflow_dispatch":
        need(set(event["inputs"]) == {"source_run"}, "RELAY_CATCHUP_INPUT")
        source_run = number(event["inputs"]["source_run"])
        need(source_run == CATCHUP_RUN, "RELAY_CATCHUP_SCOPE")
    else:
        raise Stop("RELAY_TRIGGER")
    relay_run = number(env["GITHUB_RUN_ID"])
    verify(proof.GitHub(env["GH_TOKEN"]), source_run, relay_run, env["GITHUB_WORKFLOW_SHA"], running=True)
    print(f"RELAY_DISPATCH_INTENT source_run={source_run} relay_run={relay_run}", flush=True)
    launched = dispatch(env.get("LAB_DISPATCH_PAT"), source_run, relay_run)
    print(f"RELAY_DISPATCHED lab_run={launched}", flush=True)

if __name__ == "__main__":
    try:
        main(dict(os.environ))
    except Exception as error:
        print("STOP " + (str(error) if type(error) is Stop else "RELAY_UNRESOLVED"), file=sys.stderr)
        raise SystemExit(1) from None
