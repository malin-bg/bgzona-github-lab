"""Continuous STANDARD orchestration. Existing recovery, queue, guard and D3 only."""
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4
import json
import os
import push_relay

import github_backend as gb
import push_artifact as artifact
import push_probe as proof
from e4f_github import ASSETS, CONTROL, CONTROL_ID, LEDGER, github, require, ProviderStop
from git_transport import Lab, MAIN, files, git, value
from importer import data, digest, parse
from media_preflight import verify_delta
from private_queue import PrivateQueue, TrustedRegistration, request_identity
from registration_entry import read_archive
import standard_entry as standard

DISPATCH_ACTOR = gb.CP2B_OWNER_ID
CONTROL_WORKFLOW = 408732936
CONTROL_REF = "refs/heads/automation-control"
SOURCE_BASELINE = "8bdf183c8863c4329faa907391a58dd14d36a64d"
HISTORICAL_CODE = "3ae07545f5f8b980f4494333e3924e7f2f17321f"
HISTORICAL_SOURCE = "e34e70806a01c38bc826af00a40cddebf87891c0"
HISTORICAL_RUN = 42311186081

# One approved pre-write failure, not a general retry policy.
RECOVERY_FAILED = 42471229260
RECOVERY_RELAY = 42471208063
RECOVERY_RELAY_MAIN = "7c15826ee3fd93dd327ae321970afe139ad71ffd"
RECOVERY_FAILED_CODE = "03c3b7f3a455a1160dbd9ffe7f74873003192e61"
RECOVERY_NUMBER = 15
RECOVERY_TITLE = f"BGZONA auto-resume {push_relay.CATCHUP_RUN}:{RECOVERY_RELAY}"
RECOVERY_INITIAL = {
    "recovery": "35e6420968a22b48332596eb583d984e6957bbd8",
    "ledger": "64ca8d8b32556fe110002af6c242574f683e4c13",
    "authority": "f907c50305c9f3204c41675f2c830a7d5226efa0",
    "witness": "6f0218208cfa71e49c100ee645cb5939a37ca19a",
    "dev": "3bbc6e1320858ee7ca81370971bc6ff046ac3680",
}


# Exactly one recovery of the already prepared F1 candidate, never a new prepare.
F1_PREFIX = "BGZONA f1-resume "
F1_TITLE = F1_PREFIX + "42525151296:42528043016"
F1_RECORD = "7d6504d41eb420b7930253b9a7398679b595365290b0b48804ffa9a20e774625"
F1_PROPOSAL = "62e1dc6ba72e5e50e26613dbf84d19d9732a014f8f94ea5533defc09c57f0360"
F1_PREPARE_YAML = "986432581166d23cca762af816a41a9440adf05d6ca95534fe48d4742161a30c"
F1_ORIGIN_MAIN = "6dcd8139e7b603531ede09128dba8fd3637e0a1a"
F1_FAILED_LAB = 42528066889
F1_FAILED_GUARD = 42528340713
F1_NUMBER = 17


def f1_failed(api=None):
    failed = public(f"/actions/runs/{F1_FAILED_LAB}/attempts/1")
    require((failed["id"],failed["workflow_id"],failed["run_number"],failed["run_attempt"],
             failed["event"],failed["repository"]["id"],failed["actor"]["id"],failed["head_branch"],
             failed["head_sha"],failed["display_title"],failed["status"],failed["conclusion"]) ==
            (F1_FAILED_LAB,CONTROL_WORKFLOW,16,1,"workflow_dispatch",CONTROL_ID,DISPATCH_ACTOR,
             "automation-control","8970217fab6aa70ca548e9498ccdddbcd67e01d4",
             "BGZONA auto 42525151296:42528043016","completed","failure"),"F1_FAILED_LAB")
    listing=public(f"/actions/runs/{F1_FAILED_LAB}/attempts/1/jobs?per_page=100")
    jobs=listing["jobs"]
    require(listing["total_count"]==len(jobs)==12 and len({j["name"] for j in jobs})==12 and
            all(j["conclusion"]==("failure" if j["name"]=="auto-intake" else "skipped")
                and j["status"]=="completed" for j in jobs) and
            next(j for j in jobs if j["name"]=="auto-intake")["id"]==128875981674,
            "F1_FAILED_LAB_JOBS")
    if api is not None:
        row,_=api.get(f"/actions/runs/{F1_FAILED_GUARD}/attempts/1")
        require((row["id"],row["workflow_id"],row["run_attempt"],row["event"],
                 row["repository"]["id"],row["actor"]["id"],row["head_branch"],row["head_sha"],
                 row["display_title"],row["status"],row["conclusion"]) ==
                (F1_FAILED_GUARD,408733315,1,"workflow_dispatch",standard.PIN.numeric_id,353092585,
                 "main",F1_ORIGIN_MAIN,
                 "E4F D3 guard d7f1e827227c0259f46949d2203b8123d59857b5bbe9612abbd1ecf108efa943",
                 "completed","failure"),"F1_FAILED_GUARD")
        jobs,more=api.get(f"/actions/runs/{F1_FAILED_GUARD}/attempts/1/jobs?per_page=100")
        require(not more and jobs["total_count"]==len(jobs["jobs"])==1 and
                jobs["jobs"][0]["id"]==128876805874 and jobs["jobs"][0]["conclusion"]=="failure",
                "F1_FAILED_GUARD_JOB")


def f1_prepared(api, *, item=None):
    """Authenticate the successful original prepare and permanent immutable proposal."""
    import base64
    from hashlib import sha1
    row,_=api.get("/actions/runs/42528096060/attempts/1")
    require((row["id"],row["workflow_id"],row["run_attempt"],row["event"],row["repository"]["id"],
             row["actor"]["id"],row["head_branch"],row["head_sha"],row["display_title"],
             row["status"],row["conclusion"]) ==
            (42528096060,408733317,1,"workflow_dispatch",standard.PIN.numeric_id,353092585,"main",
             F1_ORIGIN_MAIN,"R18H prepare 42525151296","completed","success"),"F1_PREPARE_RUN")
    jobs,more=api.get("/actions/runs/42528096060/attempts/1/jobs?per_page=100")
    require(not more and jobs["total_count"]==len(jobs["jobs"])==1 and
            (jobs["jobs"][0]["name"],jobs["jobs"][0]["conclusion"])==("prepare","success"),
            "F1_PREPARE_JOB")
    path=".github/workflows/e4f-standard-prepare.yml"
    obj,_=api.get("/contents/"+path+"?ref="+F1_ORIGIN_MAIN)
    require(obj["encoding"]=="base64" and obj["path"]==path,"F1_PREPARE_WORKFLOW")
    raw=base64.b64decode("".join(obj["content"].splitlines()),validate=True)
    require(len(raw)==obj["size"] and sha1(b"blob "+str(len(raw)).encode()+b"\0"+raw).hexdigest()==obj["sha"] and
            digest(raw)==F1_PREPARE_YAML,"F1_PREPARE_WORKFLOW")
    # No Actions candidate download, expired-flag override or cached artifact authority.
    release=standard.public_release("r18h-d3-"+F1_RECORD)
    require(release["id"]==438553152,"F1_RELEASE_ID")
    if item is None:
        item=standard.proposal(F1_RECORD,normal=True)
    require(digest(data(item))==F1_PROPOSAL and digest(data(item["record"]))==F1_RECORD and
            item["source_run"]==42525151296 and item["record"]["phase"]=="staged" and
            item["ledger_parent"]=="106c5e16252162fc841874758808a79177b10f72" and
            item["handoff"]==dict(head="93e65b45807a992b8230592b0ea9cbbc2fb1f130",
                parent="53a02c5f4054bd2b7310be140e5092a43983de6d",
                request="f7938c9d84bf808e26ed96c9e16a064ca0901f765af28739139b6050d288b5c3",
                mirror="7ada5243bac2039efaf1f834596d0f0785f50b8e4edf50253f8e35dbaabab9ab",
                sha256="bdd07554e5f6e1c2e0c539db9a4c7e75f2af79e8ec3d619547a51ee439c76765",size=63748),
            "F1_PREPARED_CANDIDATE")
    return item


def f1_initial(api, private_main):
    for ref,expected in (("heads/main",private_main),
        ("heads/cp2b-private-standard-source-r1","4aa95e9a84bc3fde909ccc111da156d8844ff263"),
        ("heads/cp2b-private-standard-recovery-c2","93e65b45807a992b8230592b0ea9cbbc2fb1f130")):
        row,_=api.get("/git/ref/"+ref)
        require(row["ref"]=="refs/"+ref and row["object"]["type"]=="commit" and
                row["object"]["sha"]==expected,"F1_INITIAL_PRIVATE_STATE")
    history,more=api.get("/actions/runs?per_page=100")
    require(not more and history["total_count"]==len(history["workflow_runs"])==33 and
            all(r["status"]=="completed" for r in history["workflow_runs"]),"F1_INITIAL_PRIVATE_HISTORY")
    for repo,ref,expected in (
        (CONTROL,"heads/automation-ledger","106c5e16252162fc841874758808a79177b10f72"),
        (ASSETS,"heads/main","fc48d532693af1cef349211dbb951c65be7554bf"),
        (ASSETS,"bgzona/witness","b22c7f0ddc4cb41e38473a429fc54b820f70abb1"),
        (ASSETS,"heads/r18h-staging-authority","fc48d532693af1cef349211dbb951c65be7554bf"),
        (ASSETS,"heads/r18h-staging-witness","b22c7f0ddc4cb41e38473a429fc54b820f70abb1"),
        ("malin-bg/bgzona-win-dev","heads/main","3bbc6e1320858ee7ca81370971bc6ff046ac3680")):
        status,row=github("/repos/"+repo+"/git/ref/"+ref,standard.lab_read_token() if repo==CONTROL else None)
        require(status==200 and row["ref"]=="refs/"+ref and row["object"]["type"]=="commit" and
                row["object"]["sha"]==expected,"F1_INITIAL_PUBLIC_STATE")

def recovery_failed():
    """Match the independently reviewed immutable pre-CAS failure."""
    old = public(f"/actions/runs/{RECOVERY_FAILED}/attempts/1")
    require((old["id"], old["workflow_id"], old["run_number"], old["run_attempt"],
             old["repository"]["id"], old["actor"]["id"], old["event"], old["head_sha"],
             old["head_branch"], old["path"].split("@", 1)[0], old["display_title"],
             old["status"], old["conclusion"]) ==
            (RECOVERY_FAILED, CONTROL_WORKFLOW, 13, 1, CONTROL_ID, DISPATCH_ACTOR,
             "workflow_dispatch", RECOVERY_FAILED_CODE, "automation-control",
             ".github/workflows/shared-media.yml",
             f"BGZONA auto {push_relay.CATCHUP_RUN}:{RECOVERY_RELAY}", "completed", "failure"),
            "AUTO_RECOVERY_FAILED_IDENTITY")
    listing = public(f"/actions/runs/{RECOVERY_FAILED}/attempts/1/jobs?per_page=100")
    jobs = listing["jobs"]
    require(listing["total_count"] == len(jobs) == 12 and
            len({j["name"] for j in jobs}) == 12, "AUTO_RECOVERY_FAILED_JOBS")
    intake = [j for j in jobs if j["name"] == "auto-intake"]
    require(len(intake) == 1 and intake[0]["id"] == 128702198736 and
            all(j["run_id"] == RECOVERY_FAILED and j["head_sha"] == RECOVERY_FAILED_CODE and
                j["status"] == "completed" and
                j["conclusion"] == ("failure" if j["name"] == "auto-intake" else "skipped")
                for j in jobs), "AUTO_RECOVERY_FAILED_JOBS")
    steps = [(s["number"], s["name"], s["conclusion"]) for s in intake[0]["steps"]]
    require(steps == [
        (1, "Set up job", "success"),
        (2, "Run actions/checkout@11bd71901bbe5b1630ceea73d27597364c9af683", "success"),
        (3, "Run actions/create-github-app-token@bcd2ba49218906704ab6c1aa796996da409d3eb1", "success"),
        (4, "Verify immutable control code and execute", "failure"),
        (7, "Post Run actions/create-github-app-token@bcd2ba49218906704ab6c1aa796996da409d3eb1", "success"),
        (8, "Post Run actions/checkout@11bd71901bbe5b1630ceea73d27597364c9af683", "success"),
        (9, "Complete job", "success")], "AUTO_RECOVERY_FAILED_STEPS")


def recovery_initial(api, private_main):
    """GET-only fence, before any PRIVATE dispatch. Never used as a CAS retry."""
    ref, _ = api.get("/git/ref/heads/main")
    require(ref["object"]["sha"] == private_main, "AUTO_RECOVERY_PRIVATE_PIN")
    ref, _ = api.get("/git/ref/heads/cp2b-private-standard-recovery-c2")
    require(ref["ref"] == "refs/heads/cp2b-private-standard-recovery-c2" and
            ref["object"]["type"] == "commit" and ref["object"]["sha"] == RECOVERY_INITIAL["recovery"],
            "AUTO_RECOVERY_INITIAL_STATE")
    rows, more = api.get("/actions/runs?per_page=100")
    require(not more and rows["total_count"] == len(rows["workflow_runs"]) == 27 and
            all(r["status"] == "completed" for r in rows["workflow_runs"]),
            "AUTO_RECOVERY_PRIVATE_HISTORY")
    for repo, refname, expected in (
        (CONTROL, LEDGER.removeprefix("refs/"), RECOVERY_INITIAL["ledger"]),
        (ASSETS, "heads/main", RECOVERY_INITIAL["authority"]),
        (ASSETS, "bgzona/witness", RECOVERY_INITIAL["witness"]),
        (ASSETS, "heads/r18h-staging-authority", RECOVERY_INITIAL["authority"]),
        (ASSETS, "heads/r18h-staging-witness", RECOVERY_INITIAL["witness"]),
        ("malin-bg/bgzona-win-dev", "heads/main", RECOVERY_INITIAL["dev"])):
        status, ref = github("/repos/" + repo + "/git/ref/" + refname, standard.lab_read_token()
                             if repo == CONTROL else None)
        require(status == 200 and ref["ref"] == "refs/" + refname and
                ref["object"]["type"] == "commit" and ref["object"]["sha"] == expected,
                "AUTO_RECOVERY_INITIAL_STATE")


def public(path):
    status, row = github("/repos/" + CONTROL + path,standard.lab_read_token())
    require(status == 200 and type(row) is dict, "AUTO_CONTROL_READ")
    return row

def is_auto_control(run):
    require(gb.positive(run), "AUTO_CONTROL_ID")
    row = public(f"/actions/runs/{run}/attempts/1")
    return row.get("display_title", "").startswith(("BGZONA auto ", "BGZONA auto-resume ", F1_PREFIX))

def control_run(run, source=None, relay=None, *, private_api=None, private_main=None):
    row = public(f"/actions/runs/{run}/attempts/1")
    ref = public("/git/ref/heads/automation-control")
    require((row["id"], row["workflow_id"], row["run_attempt"], row["event"],
             row["repository"]["id"], row["actor"]["id"], row["head_branch"],
             row["path"].split("@",1)[0], row["head_sha"], row["status"]) ==
            (run, CONTROL_WORKFLOW, 1, "workflow_dispatch", CONTROL_ID, DISPATCH_ACTOR,
             "automation-control", ".github/workflows/shared-media.yml",
             ref["object"]["sha"], "in_progress"), "AUTO_CONTROL_IDENTITY")
    title = row["display_title"]
    recovering = title.startswith("BGZONA auto-resume ")
    f1 = title.startswith(F1_PREFIX)
    require(title.startswith("BGZONA auto ") or recovering or f1, "AUTO_RELAY_LINK")
    if f1:
        require(title==F1_TITLE and row["run_number"]==F1_NUMBER and
                row.get("triggering_actor",row["actor"])["id"]==DISPATCH_ACTOR,"F1_CONTROL_SCOPE")
        f1_failed(private_api)
    if recovering:
        require(title == RECOVERY_TITLE and row["run_number"] == RECOVERY_NUMBER and
                row.get("triggering_actor", row["actor"])["id"] == DISPATCH_ACTOR,
                "AUTO_RECOVERY_SCOPE")
        recovery_failed()
    parts = title[len(F1_PREFIX) if f1 else (len("BGZONA auto-resume ") if recovering else 12):].split(":")
    require(len(parts) == 2, "AUTO_RELAY_LINK")
    linked_source, linked_relay = (push_relay.number(v) for v in parts)
    require((source is None or source == linked_source) and
            (relay is None or relay == linked_relay), "AUTO_RELAY_LINK")
    if private_api is not None:
        verified = push_relay.verify(private_api, linked_source, linked_relay,
                                     F1_ORIGIN_MAIN if f1 else (RECOVERY_RELAY_MAIN if recovering else private_main),
                                     completed_recovery=(recovering or f1))
        row = row | {"source_sha": verified["head_sha"]}
    else:
        # Assets/DEV inherit the same immutable workflow and accepted D3 proof.
        # No additional PRIVATE credential is exposed to either job.
        jobs = public(f"/actions/runs/{run}/attempts/1/jobs?per_page=100")["jobs"]
        intake = [j for j in jobs if j["name"] == "auto-intake"]
        require(len(intake) == 1 and intake[0]["run_id"] == run and
                intake[0]["head_sha"] == row["head_sha"] and intake[0]["status"] == "completed" and
                intake[0]["conclusion"] == "success", "AUTO_INTAKE_REQUIRED")
    history = public(f"/actions/workflows/{CONTROL_WORKFLOW}/runs?event=workflow_dispatch&branch=automation-control&per_page=100")
    hits = [r for r in history["workflow_runs"] if r["display_title"] == title]
    require(len(hits)==1 and hits[0]["id"]==run and hits[0]["run_attempt"]==1,
            "AUTO_REPLAY_OR_CONCURRENT")
    for other in history["workflow_runs"]:
        require(other["id"]==run or other["status"]=="completed", "AUTO_CONCURRENT")
    previous = [r for r in history["workflow_runs"] if r["id"] != run and
                r.get("display_title", "").startswith(("BGZONA auto ", "BGZONA auto-resume ", F1_PREFIX)) and
                not (recovering and r["id"] == RECOVERY_FAILED) and
                not (f1 and r["id"] == F1_FAILED_LAB)]
    if f1:
        failed=[r for r in history["workflow_runs"] if r["id"]==F1_FAILED_LAB]
        require(history["total_count"]==len(history["workflow_runs"])==F1_NUMBER and
                len(failed)==1 and failed[0]["conclusion"]=="failure" and failed[0]["run_attempt"]==1 and
                all(r["id"]==run or r["run_number"]<F1_NUMBER for r in history["workflow_runs"]),
                "F1_CONTROL_HISTORY")
    if recovering:
        failed = [r for r in history["workflow_runs"] if r["id"] == RECOVERY_FAILED]
        require(history["total_count"] == len(history["workflow_runs"]) == RECOVERY_NUMBER and
                len(failed) == 1 and failed[0]["conclusion"] == "failure" and
                failed[0]["run_attempt"] == 1 and
                all(r["id"] == run or r["run_number"] < RECOVERY_NUMBER
                    for r in history["workflow_runs"]), "AUTO_RECOVERY_HISTORY")
    require(previous or history["total_count"] == len(history["workflow_runs"]),
            "AUTO_HISTORY_WINDOW")
    if previous:
        require(max(previous, key=lambda r: r["run_number"])["conclusion"] == "success",
                "AUTO_PREVIOUS_UNRESOLVED")
    protection=public("/rulesets/26657290")
    require(protection["enforcement"]=="active" and
            {r["type"] for r in protection["rules"]}=={"update","deletion","non_fast_forward"} and
            protection["conditions"]["ref_name"]["include"]==[CONTROL_REF] and
            protection["conditions"]["ref_name"]["exclude"]==[],"AUTO_CONTROL_PROTECTION")
    return row


def context(env):
    params = parse(Path(env["GITHUB_EVENT_PATH"]).read_bytes())["inputs"]
    require(env["GITHUB_ACTIONS"]=="true" and env["GITHUB_REPOSITORY"]==CONTROL and
            env["GITHUB_REPOSITORY_ID"]==str(CONTROL_ID) and
            env["GITHUB_REF"]==CONTROL_REF and env["GITHUB_REF_PROTECTED"]=="true" and
            env["GITHUB_ACTOR_ID"]==str(DISPATCH_ACTOR) and env["GITHUB_RUN_ATTEMPT"]=="1" and
            env["GITHUB_EVENT_NAME"]=="workflow_dispatch" and
            env["GITHUB_WORKFLOW_REF"]==CONTROL+"/.github/workflows/shared-media.yml@"+CONTROL_REF and
            gb.oid(env["GITHUB_WORKFLOW_SHA"]) and params.get("mode") in ("auto", "auto-resume", "f1-resume") and
            set(params)<={"mode","relay_run","source_run","private_run","record_sha256"} and
            (params.get("mode")=="f1-resume" and params.get("private_run")=="42528096060" and
             params.get("record_sha256")==F1_RECORD or
             params.get("mode")!="f1-resume" and all(params.get(k,"")=="" for k in ("private_run","record_sha256"))),
            "AUTO_CONTEXT")
    source, relay = push_relay.number(params.get("source_run","")), push_relay.number(params.get("relay_run",""))
    recovering = params["mode"] == "auto-resume"
    f1 = params["mode"]=="f1-resume"
    require(not f1 or (source,relay)==(42525151296,42528043016),"F1_CONTROL_INPUT")
    require(not recovering or (source, relay) == (push_relay.CATCHUP_RUN, RECOVERY_RELAY),
            "AUTO_RECOVERY_SCOPE")
    phase = env["AUTO_PHASE"]
    require(phase in ("intake","assets","dev") and
            env["GITHUB_JOB"] == "auto-" + phase, "AUTO_JOB")
    api = proof.GitHub(env["PRIVATE_ACTIONS_TOKEN"]) if phase == "intake" else None
    row=control_run(int(env["GITHUB_RUN_ID"]), source, relay, private_api=api,
                    private_main=env.get("PRIVATE_WORKFLOW_COMMIT"))
    require(row["head_sha"]==env["GITHUB_WORKFLOW_SHA"] and
            row["display_title"].startswith("BGZONA auto-resume ") == recovering and
            row["display_title"].startswith(F1_PREFIX)==f1, "AUTO_WORKER_PIN")
    if f1 and phase=="intake":
        f1_initial(api,env["PRIVATE_WORKFLOW_COMMIT"])
    if recovering and phase == "intake":
        recovery_initial(api, env["PRIVATE_WORKFLOW_COMMIT"])
    return params | ({"source_sha":row["source_sha"]} if phase == "intake" else {})


def run_identity(api, run, source, *, complete=True):
    row, _ = api.get(f"/actions/runs/{run}/attempts/1")
    require((row["id"],row["run_attempt"],row["workflow_id"],row["repository"]["id"],
             row["actor"]["id"],row["head_branch"],row["head_sha"],row["event"],
             row["path"].split("@",1)[0]) ==
            (run,1,artifact.WORKFLOW,standard.PIN.numeric_id,gb.CP2B_OWNER_ID,
             standard.SOURCE_REF.removeprefix("refs/heads/"),source,"push",artifact.WORKFLOW_PATH)
            and (not complete or row["status"]=="completed" and row["conclusion"]=="success"),
            "AUTO_SOURCE_RUN")
    return row

def original(api, reader, run, source):
    row = run_identity(api,run,source)
    listing, more = api.get(f"/actions/runs/{run}/artifacts?per_page=100")
    require(not more,"AUTO_ARTIFACT_WINDOW")
    item = artifact.select(listing["artifacts"],run,1,source,ref=standard.SOURCE_REF)
    packet = artifact.decode(read_archive(api,item["id"]),item)
    activities, _ = api.get("/activity?per_page=100")
    # Authenticate this exact Push within the bounded window; older unrelated
    # activities do not invalidate it. Replay is fenced by ancestry + durable request identity.
    require(proof.find_activity(activities,packet["before"],source,ref=standard.SOURCE_REF) is not None,
            "AUTO_ACTIVITY_WINDOW")
    _,parents = reader._commit("intake",source)
    return artifact.verify(packet,run,1,source,row,activities,parents,ref=standard.SOURCE_REF)

def source_reader(api, repo):
    tip=value(repo,"rev-parse",standard.SOURCE_REF)
    reader=standard.SourceReader(repo,api,SOURCE_BASELINE,tip)
    require(reader._ref("intake",standard.SOURCE_REF)==tip,"AUTO_SOURCE_MOVED")
    return reader

def verify_source(api, repo, reader, run, source, code, manifest, *, event=None):
    require(source in reader.allowed, "AUTO_SOURCE_HISTORY")
    _,parents=reader._commit("intake",source)
    require(len(parents)==1,"AUTO_SOURCE_PARENT")
    before,=parents
    if event is None:
        event=original(api,reader,run,source)
    else:
        run_identity(api,run,source,complete=False)
        proof.verify_event(event,source,before,ref=standard.SOURCE_REF)
        identity,_=api.get("")
        require((identity["id"],identity["full_name"],identity["private"],identity["owner"]["id"])==
                (standard.PIN.numeric_id,standard.PIN.name,True,gb.CP2B_OWNER_ID),"AUTO_REPOSITORY")
        remote,_=api.get("/git/commits/"+source)
        require(remote["sha"]==source and [p["sha"] for p in remote["parents"]]==[before],
                "AUTO_SOURCE_PARENT")
        import time
        for remaining in range(10):
            activities,_=api.get("/activity?per_page=100")
            activity=proof.find_activity(activities,before,source,ref=standard.SOURCE_REF)
            if activity is not None: break
            require(remaining<9,"AUTO_ACTIVITY_WINDOW")
            time.sleep(5)
        stamp=datetime.fromisoformat(activity["timestamp"].replace("Z","+00:00"))
        require(stamp.tzinfo is not None and stamp<=datetime.now(timezone.utc),"AUTO_ACTIVITY_TIME")
    require((event["before"],event["after"])==(before,source),"AUTO_SOURCE_PARENT")
    standard.check_source_yaml(repo,source,code,manifest)
    # A one-time owner Push may include the exact approved source workflow replacement.
    # No setup-only request and no invented acceptance record are inserted.
    verify_delta(reader,before,source,workflow_change=True)
    return dict(run=run,attempt=1,workflow=artifact.WORKFLOW,code=code,
                repository=standard.PIN.numeric_id,actor=gb.CP2B_OWNER_ID,
                ref=standard.SOURCE_REF,event="push",forced=False,deleted=False,before=before,tip=source)

def source_main(env):
    api=proof.GitHub(env["GH_TOKEN"])
    require(env["GITHUB_ACTIONS"]=="true" and env["GITHUB_EVENT_NAME"]=="push" and
            env["GITHUB_REF"]==standard.SOURCE_REF and env["GITHUB_RUN_ATTEMPT"]=="1" and
            env["GITHUB_REPOSITORY"]==standard.PIN.name and
            env["GITHUB_REPOSITORY_ID"]==str(standard.PIN.numeric_id),"AUTO_SOURCE_CONTEXT")
    repo=Path(env["RUNNER_TEMP"])/"auto-source.git"
    standard.fetch_source(repo,api.token)
    reader=source_reader(api,repo)
    source=env["GITHUB_SHA"]; run=int(env["GITHUB_RUN_ID"])
    require(source in reader.allowed,"AUTO_SOURCE_DIVERGENT")
    verify_source(api,repo,reader,run,source,env["WORKER_COMMIT"],env["MANIFEST_SHA256"],
                  event=parse(Path(env["GITHUB_EVENT_PATH"]).read_bytes()))
    artifact.emit(env["GITHUB_EVENT_PATH"],Path(env["RUNNER_TEMP"])/artifact.FILE,
                  run,1,artifact.WORKFLOW,source,ref=standard.SOURCE_REF)
    print("PROPOSED_ONLY registered=False")

def registration(api, lab, repo, pins, *, source, run, code, manifest, actor, handoff=None):
    reader=source_reader(api,repo)
    policy=gb.Policy((standard.PIN,),(("independent_verify",actor),),code,code,
                     pins["library_id"],gb.CP2B_OWNER_ID,())
    backend=gb.Backend(policy)
    queue=PrivateQueue(lab,lab.root/"queue",backend,repository=standard.PIN,
        owner_actor_id=gb.CP2B_OWNER_ID,baseline=SOURCE_BASELINE,source_ref=standard.SOURCE_REF,
        cp2b_reader=reader)
    if not queue.directory.exists(): queue.initialize()
    ref=pins["c3"]["recovery_ref"]
    def http(write=None):
        return gb.PrivateRecoveryHTTP(standard.PIN,gb.CP2B_OWNER_ID,code,api.token,
            writable=write is not None,standard_bootstrap=pins["recovery_bootstrap"],
            standard_handoff=handoff,standard_write=write,standard_ref=ref,
            standard_chain=True,standard_baseline=SOURCE_BASELINE)
    ro=http(); node=ro.node_id(standard.PIN.numeric_id)
    def head(): return ro.head(standard.PIN.numeric_id,ref)
    # Only durable, hash-checked envelopes may supply historical facts.
    durable={}
    for raw in ro.snapshot(standard.PIN.numeric_id,head())["files"].values():
        packet=parse(raw)
        if isinstance(packet,dict) and packet.get("format")==TrustedRegistration.FORMAT:
            facts=packet["facts"]
            require(data(packet)==raw and facts["run"] not in durable,"AUTO_HISTORY")
            durable[facts["run"]]=facts
    cache={}
    def facts(r,a):
        require(a==1,"AUTO_ATTEMPT")
        if r in cache: return dict(cache[r])
        if r in durable:
            f=durable[r]
            require(f["code"] in (code,HISTORICAL_CODE) and f["tip"] in reader.allowed and
                    reader._commit("intake",f["tip"])[1]==[f["before"]],"AUTO_HISTORY")
            run_identity(api,r,f["tip"])
            if f["code"]==HISTORICAL_CODE:
                require((r,f["tip"],f["before"])==(HISTORICAL_RUN,HISTORICAL_SOURCE,SOURCE_BASELINE),
                        "AUTO_HISTORICAL_PIN")
        else:
            require(r==run,"AUTO_SOURCE_RUN")
            f=verify_source(api,repo,reader,r,source,code,manifest)
        cache[r]=dict(f); return dict(f)
    provider=SimpleNamespace(run_facts=facts,recovery_head=head)
    def client(expected,write=None):
        return gb.PrivateRecovery(standard.PIN,node,gb.CP2B_OWNER_ID,code,
            ro if write is None else http(write),expected_head=expected,
            writable=write is not None,ref=ref)
    def factory(recovery):
        return TrustedRegistration(queue,recovery,provider,workflow=artifact.WORKFLOW,code=code,
                                   standard_codes=(HISTORICAL_CODE,code) if code!=HISTORICAL_CODE else (code,))
    current=head()
    reg=factory(client(current)); reg.restore()
    return reg,client,factory

def next_source(api, cursor, *, known=None):
    """Find the oldest subsequent authenticated Push, including after a missed tick."""
    repo,_=api.get("")
    require((repo["id"],repo["full_name"],repo["private"],repo["owner"]["id"])==
            (standard.PIN.numeric_id,standard.PIN.name,True,gb.CP2B_OWNER_ID),"AUTO_REPOSITORY")
    row,_=api.get("/git/ref/"+standard.SOURCE_REF.removeprefix("refs/"))
    require(row["ref"]==standard.SOURCE_REF and row["object"]["type"]=="commit" and
            gb.oid(row["object"]["sha"]),"AUTO_SOURCE_REF")
    tip=row["object"]["sha"]
    require(gb.oid(cursor),"AUTO_CURSOR")
    chain=[]; here=tip
    for _ in range(512):
        if here==cursor: break
        commit,_=api.get("/git/commits/"+here)
        require(commit["sha"]==here and len(commit["parents"])==1,"AUTO_SOURCE_PARENT")
        parent=commit["parents"][0]["sha"]
        require(gb.oid(parent),"AUTO_SOURCE_PARENT")
        chain.append((parent,here)); here=parent
    require(here==cursor,"AUTO_SOURCE_DIVERGENT")
    if known is not None:
        require(gb.oid(known), "AUTO_RELAY_SOURCE")
        if known == cursor: return None
        positions = [i for i, (_, after) in enumerate(chain) if after == known]
        require(len(positions) == 1, "AUTO_RELAY_SOURCE")
        chain = chain[positions[0]:]
    if not chain: return None
    before,after=chain[-1]
    listing,more=api.get(f"/actions/workflows/{artifact.WORKFLOW}/runs?event=push&branch="+
                        standard.SOURCE_REF.removeprefix("refs/heads/")+"&head_sha="+after+"&per_page=100")
    hits=[r for r in listing["workflow_runs"] if r["head_sha"]==after]
    require(not more and len(hits)<=1,"AUTO_SOURCE_REPLAY")
    if not hits: return "WAIT"
    require(hits[0]["run_attempt"]==1,"AUTO_SOURCE_ATTEMPT")
    if hits[0]["status"]!="completed": return "WAIT"
    run_identity(api,hits[0]["id"],after)
    return dict(run=hits[0]["id"],before=before,after=after,tip=tip)

def continuous_views(raw_old, raw_new, baseline, registry, view, raw_parent):
    """Apply only an authenticated raw delta to the verified editing mirror.

    The originals remain in PRIVATE Git. Accepted paths and revisions are never overwritten.
    """
    require(view is not None and view.rows and view.rows[-1][3]==raw_parent,
            "AUTO_EDITING_PARENT")
    aliases={}
    for packet in view.handoffs:
        for entry in packet["binding"]["files"]:
            old=aliases.get(entry["private_path"])
            require(old is None or old==entry["path"],"AUTO_EDITING_ALIAS")
            aliases[entry["private_path"]]=entry["path"]
    owners={}
    for receipt in registry["receipts"].values():
        for path in receipt["input"].get("files",{}):
            owners.setdefault(path,set()).add(receipt["item"])
    changed={p for p in raw_old.keys()|raw_new.keys() if raw_old.get(p)!=raw_new.get(p)}
    result=dict(baseline); private={}
    for path in sorted(changed):
        target=aliases.get(path,path)
        if path.startswith("incoming/") and not path.startswith("incoming/replace/"):
            parts=Path(path).as_posix().split("/")
            leaf=parts[-1]
            stem=leaf[:-7] if leaf.lower().endswith(".og.jpg") or leaf.lower().endswith(".og.png") else leaf.rsplit(".",1)[0]
            related=[(p,a) for p,a in aliases.items()
                     if p.startswith("/".join(parts[:-1])+"/"+stem+".") and
                     p.lower().endswith((".png",".jpg",".jpeg",".webp")) and ".og." not in p.lower()]
            if related:
                targets=set()
                for _,alias in related: targets.update(owners.get(alias,set()))
                require(len(targets)==1,"AUTO_EDITING_ALIAS")
                key,=targets; item=registry["items"][key]
                require(item["state"]=="published","AUTO_EDITING_STATE")
                if leaf.lower().endswith(".txt"):
                    target=f"library/{item['collection']}/published/{key}/{key}.txt"
                elif ".og." in leaf.lower():
                    suffix=leaf.rsplit(".",1)[1].lower()
                    require(suffix in ("png","jpg"),"AUTO_EDITING_OG")
                    target=f"library/{item['collection']}/published/{key}/og.{suffix}"
                else:
                    require(False,"AUTO_REPLACEMENT_REQUIRED")
        require(target not in private or private[target]==path,"AUTO_EDITING_COLLISION")
        if path in raw_new:
            private[target]=path
            result[target]=raw_new[path]
        else:
            require(target in result and target.endswith((".txt","/og.png","/og.jpg")),
                    "AUTO_DELETE_SCOPE")
            result.pop(target)
    return dict(baseline),result,private

def independent_prefix(reg,domain):
    from queued_pipeline import verified_accepted_prefix
    reg.queue._accepted_view=None
    return verified_accepted_prefix(reg,domain,domain.lab.root/("cursor-"+uuid4().hex),allow_progress=True)

def private_prepare(env):
    from queued_pipeline import prepare_registered_next, _handoff_payload
    from intake_plan import SourceEvidence, change_digest
    from request_executor import RequestOperation
    api=proof.GitHub(env["GH_TOKEN"])
    params=parse(Path(env["GITHUB_EVENT_PATH"]).read_bytes())["inputs"]
    require(set(params)=={"route","source_run","control_run"} and params["route"]=="auto" and
            params["source_run"].isdigit() and params["control_run"].isdigit(),"AUTO_PREPARE_INPUT")
    run=int(params["source_run"])
    standard.verified_dispatch(api,env,".github/workflows/e4f-standard-prepare.yml",run,normal=True)
    control_run(int(params["control_run"]), private_api=api, private_main=env["GITHUB_WORKFLOW_SHA"])
    lab=Lab.create(Path(env["RUNNER_TEMP"])/"auto-private")
    pins=parse(standard.BOOTSTRAP.read_bytes())
    domain=standard.public_baseline(lab,pins,normal=True)
    repo=lab.root/"source.git"; standard.fetch_source(repo,api.token)
    source,_=api.get(f"/actions/runs/{run}/attempts/1"); source=source["head_sha"]
    args=dict(source=source,run=run,code=push_relay.SOURCE_CODE,manifest=push_relay.SOURCE_MANIFEST,
              actor=int(env["TRUSTED_DISPATCH_ACTOR"]))
    reg,client,factory=registration(api,lab,repo,pins,**args)
    view=independent_prefix(reg,domain)
    pending=reg.verify()
    expected=request_identity(standard.PIN.numeric_id,
                             reg.queue.source_reader._commit("intake",source)[1][0],source)
    if pending:
        require(pending[0][1]==expected,"AUTO_FIFO_CONFLICT")
    reg=standard.register_chained(reg,client,factory,run,1)
    view=independent_prefix(reg,domain)
    rows=reg.verify()
    require(rows and rows[0][1]==expected,"AUTO_ALREADY_ACCEPTED")
    context=domain.read()
    mirror=digest(data(context["latest"]))
    if view.pending_handoff is not None:
        row,payload,path,restored=view.pending_handoff
        require(tuple(row)==rows[0],"AUTO_FIFO_CONFLICT")
        packet=parse(payload)
        git(restored,"update-ref",MAIN,packet["change"]["commit"])
        evidence=SourceEvidence(standard.PIN.numeric_id,standard.PIN.name,
            digest(data(packet["before"])),change_digest(packet["change"]),
            packet["before"]["ledger_record_sha256"],int(datetime.now(timezone.utc).timestamp()),"private")
        operation=RequestOperation(lab,restored,packet["before"],packet["change"],evidence,lambda _:None)
        operation.bind(context)
        require(operation.pending,"AUTO_ALREADY_ACCEPTED")
        pin=gb.StandardHandoffPin(expected,packet["before"]["ledger_record_sha256"],digest(payload),len(payload))
        snapshot=reg.recovery.read(reg.provider.recovery_head())
        for _ in range(256):
            require(len(snapshot["parents"])==1,"AUTO_HANDOFF_ANCESTRY")
            parent,=snapshot["parents"]
            older=reg.recovery.port.snapshot(standard.PIN.numeric_id,parent)
            if path not in older["files"]:
                require(snapshot["files"].get(path)==payload,"AUTO_HANDOFF_ANCESTRY")
                head=snapshot["head"]; break
            snapshot=older
        else: raise ProviderStop("AUTO_HANDOFF_ANCESTRY")
    else:
        prepared=prepare_registered_next(reg,repo,domain,mirror_record_sha256=mirror)
        operation=prepared.operation
        payload=_handoff_payload(prepared.binding,operation.before,operation.change,base=view.handoff_base)
        pin=gb.StandardHandoffPin(expected,mirror,digest(payload),len(payload))
        parent=reg.provider.recovery_head()
        pinned,clients,factories=registration(api,lab,repo,pins,handoff=pin,**args)
        require(pinned.recovery.expected_head==parent,"AUTO_RECOVERY_MOVED")
        writer=clients(parent,(parent,pin.path,pin.sha256))
        head=writer.persist(pin.path.split("/")[1],pin.mirror,payload)
        observed=clients(head).read(head)
        require(observed["parents"]==[parent] and observed["files"].get(pin.path)==payload,
                "AUTO_HANDOFF_READBACK")
        require(factories(clients(head)).restore()==pinned.verify(),"AUTO_HANDOFF_REGISTRATION")
    handoff=dict(request=pin.request,mirror=pin.mirror,sha256=pin.sha256,size=pin.size,head=head,parent=parent)
    standard.produce_candidate(domain,operation,context,pins["witness"],run,handoff,
                               Path(env["RUNNER_TEMP"])/"r18h-candidate")
    print("STANDARD_HANDOFF_DURABLE")

def private_guard(env):
    import standard_guard
    from authority_coordinator import LocalAppendGuard
    api=proof.GitHub(env["GH_TOKEN"])
    params=parse(Path(env["GITHUB_EVENT_PATH"]).read_bytes())["inputs"]
    require(set(params)=={"mode","ledger_parent","record_sha256","control_run","challenge","proof"} and
            params["mode"]==env["GUARD_MODE"] and gb.sha(params["challenge"]) and
            gb.sha(params["proof"]) and digest(data({k:v for k,v in params.items() if k!="proof"}))==params["proof"],
            "AUTO_GUARD_INPUT")
    mode=params["mode"]
    path=".github/workflows/e4f-d3-guard"+("-observe" if mode=="observe" else "")+".yml"
    standard.verified_dispatch(api,env,path,None,normal=True)
    control_run(int(params["control_run"]), private_api=api, private_main=env["GITHUB_WORKFLOW_SHA"])
    item=standard.proposal(params["record_sha256"],normal=True)
    require(item["ledger_parent"]==params["ledger_parent"],"AUTO_GUARD_LEDGER")
    p=item["handoff"]
    require(set(p)=={"request","mirror","sha256","size","head","parent"},"AUTO_GUARD_HANDOFF")
    pin=gb.StandardHandoffPin(*(p[k] for k in ("request","mirror","sha256","size")))
    lab=Lab.create(Path(env["RUNNER_TEMP"])/"auto-guard")
    pins=parse(standard.BOOTSTRAP.read_bytes())
    domain=standard.public_baseline(lab,pins,extra=item["record"],normal=True)
    repo=lab.root/"source.git"; standard.fetch_source(repo,api.token)
    run=item["source_run"]; source,_=api.get(f"/actions/runs/{run}/attempts/1")
    reg,clients,_=registration(api,lab,repo,pins,source=source["head_sha"],run=run,
        code=push_relay.SOURCE_CODE,manifest=push_relay.SOURCE_MANIFEST,
        actor=int(env["TRUSTED_DISPATCH_ACTOR"]),handoff=pin)
    view=independent_prefix(reg,domain)
    pending=reg.verify()
    matches=[r for r in [*view.rows,*pending] if r[1]==pin.request]
    require(len(matches)==1,"AUTO_GUARD_REQUEST")
    row,=matches
    require(pending and row==pending[0] or view.rows and row==view.rows[-1] and
            domain.read()["latest"]==item["record"],"AUTO_GUARD_FIFO")
    head=reg.recovery.expected_head; write=None
    if mode=="reserve":
        packet=parse(reg.recovery.read(head)["files"][pin.path])
        operation=SimpleNamespace(request=packet["binding"]["derived_request_sha256"],change=packet["change"])
        scope,base,marker=LocalAppendGuard.marker(domain,{"latest":item["previous"]},operation,item["record"])
        write=(head,f"private-recovery/{scope}/{base}/checkpoint.json",digest(marker))
    guard=LocalAppendGuard(clients(head,write),hosted_standard=True)
    result=standard_guard.attest(mode,guard=guard,registration=reg,domain=domain,
        destination=lab.root/"restored.git",record=item["record"],ledger_parent=item["ledger_parent"],
        record_sha256=params["record_sha256"],registration_head=head,handoff_head=p["head"],
        handoff_parent=p["parent"],registered_row=tuple(row))
    require(result in ("fresh","existing"),"AUTO_GUARD_VERDICT")
    with open(env["GITHUB_OUTPUT"],"a",encoding="ascii") as f: f.write("guard="+result+"\n")
    print("STANDARD_CORE_GUARD_VERIFIED")

def last_final(context, *, exclude_latest=False):
    from request_executor import binding
    rows=context["records"][:-1] if exclude_latest else context["records"]
    for row in reversed(rows):
        bound=binding(context["recovered"],row["primary_main"])
        if row["index"]==0 or bound is not None and bound["final"] and bound["phase"]=="staged":
            return row
    raise ProviderStop("AUTO_FINAL_PREFIX")

def stable_registry(before,after):
    require(before["library_id"]==after["library_id"] and
            set(before["items"])<=set(after["items"]) and
            all(after["counters"][k]>=v for k,v in before["counters"].items()),"AUTO_REGISTRY_IDENTITY")
    for key,old in before["items"].items():
        new=after["items"][key]
        require(all(new[k]==old[k] for k in ("id","collection","number","page_url")) and
                new["revisions"][:len(old["revisions"])]==old["revisions"] and
                new["desired_revision"]>=old["desired_revision"] and new["state"]==old["state"],
                "AUTO_HISTORICAL_IDENTITY")

def main(env):
    import control_entry as control
    from e4f_github import GitHubLedgerAppend
    from request_executor import binding
    params=context(env)
    phase=env["AUTO_PHASE"]
    pins=parse(standard.BOOTSTRAP.read_bytes())
    if phase=="intake" and params["mode"]=="f1-resume":
        api=proof.GitHub(env["PRIVATE_ACTIONS_TOKEN"])
        item=f1_prepared(api)
        ledger=GitHubLedgerAppend(env["GH_TOKEN"],control.attach_guard(env,c3=True))
        status,commit=ledger.append(item["ledger_parent"],item["previous"],item["record"])
        require(status in ("APPENDED","OBSERVED") and gb.oid(commit),"AUTO_D3_READBACK")
        verified=standard.public_baseline(Lab.create(Path(env["RUNNER_TEMP"])/"f1-accepted"),pins,normal=True).read()
        require(verified["latest"]==item["record"],"AUTO_ACCEPTANCE_MOVED")
        bound=binding(verified["recovered"],item["record"]["primary_main"])
        require(bound is not None and bound["final"] and bound["phase"]=="staged","AUTO_FINAL_REQUIRED")
        with open(env["GITHUB_OUTPUT"],"a",encoding="ascii") as f:
            f.write("record="+F1_RECORD+"\nwork=true\n")
        print("F1_PREPARED_D3_FINAL_VERIFIED",commit,F1_RECORD)
        return
    if phase=="intake":
        api=proof.GitHub(env["PRIVATE_ACTIONS_TOKEN"])
        if fast_idle(api,pins):
            with open(env["GITHUB_OUTPUT"],"a",encoding="ascii") as f: f.write("work=false\n")
            print("AUTO_IDLE")
            return
        domain=standard.public_baseline(Lab.create(Path(env["RUNNER_TEMP"])/"auto-discovery"),pins,normal=True)
        state=domain.read(); latest=state["latest"]
        item=standard.proposal(digest(data(latest)),normal=True)
        last_run=item["source_run"]
        previous,_=api.get(f"/actions/runs/{last_run}/attempts/1")
        run_identity(api,last_run,previous["head_sha"])
        bound=binding(state["recovered"],latest["primary_main"])
        require(bound is not None,"AUTO_ACCEPTED_REQUEST")
        if bound["final"]:
            work=next_source(api,previous["head_sha"],known=params["source_sha"])
            if work is None or work=="WAIT":
                with open(env["GITHUB_OUTPUT"],"a",encoding="ascii") as f: f.write("work=false\n")
                print("AUTO_IDLE" if work is None else "AUTO_SOURCE_PENDING")
                return
            run=work["run"]
        else:
            run=last_run
        require(params["mode"] != "auto-resume" or
                (run, params["source_sha"]) == (push_relay.CATCHUP_RUN, push_relay.CATCHUP_SOURCE),
                "AUTO_RECOVERY_SOURCE")
        # A whole accepted operation may need multiple pre-existing E4G core steps.
        # Every step retains its own PRIVATE fence and independent D3 CAS.
        from intake_plan import MAX_STEPS
        for _ in range(MAX_STEPS*2):
            item=control.private_candidate(env,"prepare",run,None,normal=True)
            key=digest(data(item["record"]))
            require(standard.proposal(key,normal=True)==item,"AUTO_PROPOSAL_READBACK")
            ledger=GitHubLedgerAppend(env["GH_TOKEN"],control.attach_guard(env,c3=True))
            status,commit=ledger.append(item["ledger_parent"],item["previous"],item["record"])
            require(status in ("APPENDED","OBSERVED") and gb.oid(commit),"AUTO_D3_READBACK")
            temp=Path(env["RUNNER_TEMP"])/("auto-accepted-"+uuid4().hex)
            verified=standard.public_baseline(Lab.create(temp),pins,normal=True).read()
            require(verified["latest"]==item["record"],"AUTO_ACCEPTANCE_MOVED")
            bound=binding(verified["recovered"],item["record"]["primary_main"])
            require(bound is not None,"AUTO_ACCEPTED_REQUEST")
            if bound["final"]:
                require(bound["phase"]=="staged","AUTO_FINAL_REQUIRED")
                with open(env["GITHUB_OUTPUT"],"a",encoding="ascii") as f:
                    f.write("record="+key+"\nwork=true\n")
                print("AUTO_D3_FINAL_VERIFIED",commit,key)
                return
        raise ProviderStop("AUTO_STEP_LIMIT")
    require(phase in ("assets","dev"),"AUTO_PHASE")
    key=env["AUTO_RECORD"]; item=standard.proposal(key,normal=True)
    if phase=="assets":
        control.materialize(env,"assets",item,normal=True)
        return
    for mode in ("project","dev-pin"):
        temp=Path(env["RUNNER_TEMP"])/("auto-"+mode); temp.mkdir()
        raw=control.dev_input(env|{"RUNNER_TEMP":str(temp)},mode,item,normal=True)
    control.c3_dispatch_dev(env,raw)
    packet=parse((temp/"r18h-dev-input/public-verification.json").read_bytes())
    checked=verify_public_result(packet)
    print("AUTO_PUBLIC_DEV_VERIFIED",packet["cid"],digest(data(checked)))


def verify_public_result(proof_packet, *, get=None):
    """Verify actual DEV bytes using the same existing Privacy Gate."""
    from html import unescape
    from html.parser import HTMLParser
    from urllib.parse import urlsplit,quote
    from urllib.request import Request,build_opener
    from e4f_github import NoRedirect
    import privacy_gate
    if get is None:
        def get(path,limit):
            require(path.startswith("/") and not path.startswith("//") and
                    ".." not in path.split("/") and "\\" not in path,"AUTO_PUBLIC_PATH")
            url="https://dev.bgzona.win"+quote(path,safe="/")+"?cid="+proof_packet["cid"]
            with build_opener(NoRedirect).open(Request(url,headers={"Cache-Control":"no-cache"}),timeout=10) as response:
                require(response.status==200,"AUTO_PUBLIC_HTTP")
                raw=response.read(limit+1)
            require(len(raw)<=limit,"AUTO_PUBLIC_LIMIT")
            return raw
    require(gb.sha(proof_packet["cid"]),"AUTO_PUBLIC_CID")
    require(parse(get("/media-version.json",32000))["cid"]==proof_packet["cid"],"AUTO_PUBLIC_CID")
    class Page(HTMLParser):
        def __init__(self):
            super().__init__(convert_charrefs=True);self.words=[];self.og=None
        def handle_data(self,text):self.words.append(text)
        def handle_starttag(self,tag,attrs):
            attributes=dict(attrs)
            if tag=="meta" and attributes.get("property")=="og:image":self.og=attributes.get("content")
    checked=[]
    for row in proof_packet["items"]:
        page=Page();page.feed(get(row["page_url"],2_000_000).decode("utf-8","strict"))
        content=" ".join(unescape(" ".join(page.words)).split())
        description=" ".join(row["description"].split())
        require(not description or description in content,"AUTO_PUBLIC_DESCRIPTION")
        original=get(row["original"],16_000_000)
        require(digest(original)==row["sha256"],"AUTO_PUBLIC_ORIGINAL")
        if row["clean_original"]:
            cleaned=privacy_gate.prepare(original,Path(row["original"]).suffix,"original")
            require(cleaned.evidence["status"]=="SANITIZED_AND_VERIFIED" and cleaned.output==original,
                    "AUTO_PUBLIC_PRIVACY")
        require(page.og is not None,"AUTO_PUBLIC_OG")
        url=urlsplit(page.og)
        require(url.scheme=="https" and url.netloc=="dev.bgzona.win" and not url.query and not url.fragment,
                "AUTO_PUBLIC_OG_SCOPE")
        og=get(url.path,16_000_000)
        if row.get("og"):
            require(url.path==row["og"] and digest(og)==row["og_sha256"],"AUTO_PUBLIC_OG")
        if row["clean_og"]:
            cleaned=privacy_gate.prepare(og,Path(url.path).suffix,"og")
            require(cleaned.evidence["status"]=="SANITIZED_AND_VERIFIED" and cleaned.output==og,
                    "AUTO_PUBLIC_PRIVACY")
        checked.append(dict(id=row["id"],page=row["page_url"],original_sha256=digest(original),og_sha256=digest(og)))
    require(checked,"AUTO_PUBLIC_EMPTY")
    return checked

def fast_idle(api,pins):
    """A no-write hint only. It never issues an accepted cursor or registration."""
    from base64 import b64decode
    from hashlib import sha1
    from authority_coordinator import LocalWitness
    from request_executor import PREFIX
    status,identity=github("/repos/"+CONTROL,standard.lab_read_token())
    require(status==200 and (identity["id"],identity["full_name"],identity["private"])==
            (CONTROL_ID,CONTROL,False),"AUTO_CONTROL_REPOSITORY")
    head=public("/git/ref/"+LEDGER.removeprefix("refs/"))
    require(head["ref"]==LEDGER and head["object"]["type"]=="commit" and
            gb.oid(head["object"]["sha"]),"AUTO_LEDGER_HEAD")
    blob=public("/contents/anchor.json?ref="+head["object"]["sha"])
    require(blob["path"]=="anchor.json" and blob["type"]=="file" and blob["encoding"]=="base64" and
            0<blob["size"]<=65536,"AUTO_LEDGER_BLOB")
    raw=b64decode("".join(blob["content"].splitlines()),validate=True)
    require(len(raw)==blob["size"] and sha1(f"blob {len(raw)}\0".encode()+raw).hexdigest()==blob["sha"],
            "AUTO_LEDGER_BLOB")
    record=parse(raw)
    require(data(record)==raw,"AUTO_LEDGER_BLOB")
    LocalWitness.validate_record(SimpleNamespace(library_id=pins["library_id"]),record)
    item=standard.proposal(digest(raw),normal=True)
    require(item["record"]==record,"AUTO_LEDGER_PROPOSAL")
    source,_=api.get(f"/actions/runs/{item['source_run']}/attempts/1")
    run_identity(api,item["source_run"],source["head_sha"])
    ref,_=api.get("/git/ref/"+standard.SOURCE_REF.removeprefix("refs/"))
    require(ref["ref"]==standard.SOURCE_REF and ref["object"]["type"]=="commit" and
            gb.oid(ref["object"]["sha"]),"AUTO_SOURCE_REF")
    if ref["object"]["sha"]!=source["head_sha"]:
        return False
    status,commit=github("/repos/"+ASSETS+"/git/commits/"+record["primary_main"])
    require(status==200 and commit["sha"]==record["primary_main"],"AUTO_HINT_AUTHORITY")
    message=commit["message"].strip()
    require(message.startswith(PREFIX),"AUTO_HINT_BINDING")
    bound=parse(message[len(PREFIX):].encode("utf-8"))
    if bound.get("final") is not True:
        return False
    require(record["phase"]==bound.get("phase")=="staged" and
            type(bound.get("step")) is int and bound["step"]==bound.get("steps") and
            0<bound["step"]<=__import__("intake_plan").MAX_STEPS,"AUTO_HINT_BINDING")
    require(public("/git/ref/"+LEDGER.removeprefix("refs/"))==head,"AUTO_HINT_MOVED")
    return True
