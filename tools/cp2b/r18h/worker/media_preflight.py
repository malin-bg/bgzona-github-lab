"""Read-only CP2B Git object reader for one authenticated Push."""
from datetime import datetime, timezone
from hashlib import sha1, sha256
from pathlib import PurePosixPath
import intake_plan as planner
from importer import probe, text
import json
import os
from pathlib import Path
import subprocess
import sys

import github_backend as gb
import push_probe as proof
from private_queue import QueueStop, TrustedRegistration, request_identity
from push_provider import VerifiedPushProvider

WORKFLOW = ".github/workflows/cp2b-push-proof.yml"
STEM = "incoming/kolaji/Тестов колаж 44"
MEDIA = {
    STEM+".png": gb.ObjectPin("91634ad9c7b52052dfba6443388b79e508473c8a",
        "29987c0eb3ce15a7e3bdc8b34d724990f435715f7f3346b7f20b15e5b00b27a1",3483),
    STEM+".txt": gb.ObjectPin("e43cfab9199d6a26828fd58a094035af4d84a6eb",
        "c95f386b79908eb0ae8b48b5be5df08ceeef970e258d66a057b45903988fc41b",38),
    STEM+".og.jpg": gb.ObjectPin("5bf128a305acbb0460cb0631be7ffc024d187399",
        "f7e47ae5b612b8c354db78ef157a1077b71592106e21a187dccd4ef4d36bd704",46534),
}


class GitReader:
    def _path(self, role, commit, path):
        if self.dynamic:
            planner._path(path)
            if self.allowed.get(commit) != "commit":
                raise QueueStop("SOURCE_SCOPE")
            if commit not in self.snapshots:
                self.snapshots[commit] = media_snapshot(self, commit)
            oid = self.snapshots[commit].get(path)
            if not oid:
                raise QueueStop("SOURCE_BLOB")
            self.allowed[oid] = "blob"
        return gb.Backend._path(self, role, commit, path)

    def __init__(self, git, before, after, *, dynamic=False):
        self.git = git
        self.roots = {before, after}
        self.allowed = {} if dynamic else {oid: "commit" for oid in self.roots}
        self.dynamic = dynamic
        self.snapshots = {}
        self.media_oids = {pin.oid for pin in MEDIA.values()}

    def _object(self, role, oid, kind):
        if role != "intake" or self.allowed.get(oid) != kind or not gb.oid(oid):
            raise QueueStop("OBJECT_SCOPE")
        size = int(self.git("cat-file","-s",oid))
        if not 0 <= size <= (planner.MAX_FILE if self.dynamic and kind == "blob" else 1_000_000) or (size == 0 and kind != "tree"):
            raise QueueStop("OBJECT_LIMIT")
        if self.git("cat-file","-t",oid).strip() != kind.encode("ascii"):
            raise QueueStop("OBJECT_TYPE")
        raw = self.git("cat-file",kind,oid)
        if len(raw)!=size or sha1(f"{kind} {size}\0".encode("ascii")+raw).hexdigest()!=oid:
            raise QueueStop("OBJECT_IDENTITY")
        if kind=="tree":
            at=0
            while at<len(raw):
                end=raw.index(b"\0",at)
                mode,_=raw[at:end].split(b" ",1)
                target=raw[end+1:end+21].hex()
                if len(target)!=40:
                    raise QueueStop("TREE_FORMAT")
                if mode==b"40000":
                    self.allowed[target]="tree"
                elif not self.dynamic and mode==b"100644" and target in self.media_oids:
                    self.allowed[target]="blob"
                at=end+21
        return raw

    def allow_history(self, tip, baseline):
        """Authorize only the bounded single-parent range ending at the pinned baseline."""
        if not self.dynamic:
            raise QueueStop("SOURCE_SCOPE")
        for _ in range(512):
            self.allowed[tip] = "commit"
            _, parents = self._commit("intake", tip)
            if tip == baseline:
                return
            if len(parents) != 1:
                break
            tip = parents[0]
        raise QueueStop("SOURCE_DIVERGENT_OR_UNVERIFIED")

    def _commit(self, role, oid):
        tree, parents = gb.Backend._commit(self, role, oid)
        self.allowed[tree] = "tree"
        return tree, parents


def media_snapshot(reader, commit):
    entries = {}
    rows = reader.git("ls-tree", "-r", "-z", commit).split(b"\0")
    if rows[-1] or len(rows) > planner.MAX_FILES + 1024:
        raise QueueStop("SOURCE_TREE_LIMIT")
    for row in rows[:-1]:
        header, name = row.split(b"\t", 1)
        path = name.decode("utf-8", "strict")
        if not path.startswith(("incoming/", "library/")):
            continue
        mode, kind, oid = header.decode("ascii").split()
        if mode != "100644" or kind != "blob" or not gb.oid(oid) or path in entries:
            raise QueueStop("SOURCE_GIT_MODE")
        entries[path] = oid
    # The planner's manifest rules also apply to Git paths before any blob is read.
    planner._manifest({path: "0" * 64 for path in entries})
    return entries


def verify_delta(reader, before, after, *, workflow_change=False):
    rows = reader.git("diff-tree", "--no-commit-id", "-r", "--no-renames",
                      "--name-only", "-z", before, after).split(b"\0")
    if rows[-1] or not 1 <= len(rows) - 1 <= planner.MAX_OPERATIONS:
        raise QueueStop("SOURCE_DELTA")
    changed = [p.decode("utf-8", "strict") for p in rows[:-1]]
    paths = set(changed)
    if len(paths) != len(changed):
        raise QueueStop("SOURCE_DELTA")
    if workflow_change:
        paths.discard(WORKFLOW)
    old, new = (media_snapshot(reader, c) for c in (before, after))
    actual = {p for p in old.keys() | new.keys() if old.get(p) != new.get(p)}
    if not actual or actual != paths:
        raise QueueStop("SOURCE_DELTA")
    planner._manifest({p: "0" * 64 for p in old.keys() | new.keys()})
    planner._incoming_groups(old)
    groups, replacements = planner._incoming_groups(new)
    roles = {p: role for group in groups.values() for role,p in group.items()}
    roles.update({p: "original" for p in replacements.values()})
    total = 0
    pins = {}
    for commit, view in ((before, old), (after, new)):
        for path in sorted(actual & view.keys()):
            reader.allowed[view[path]] = "blob"
            oid, raw = reader._path("intake", commit, path)
            if oid != view[path]:
                raise QueueStop("SOURCE_BLOB")
            total += len(raw)
            if total > planner.MAX_BYTES:
                raise QueueStop("SOURCE_LIMIT")
            if commit != after:
                continue
            pins[path] = gb.ObjectPin(oid, sha256(raw).hexdigest(), len(raw))
            role = roles.get(path)
            if path.startswith("library/"):
                parts = planner._path(path).parts
                if len(parts) != 5 or parts[1] not in planner.COLS or parts[2] != "published":
                    raise QueueStop("SOURCE_LIBRARY_SCOPE")
                if parts[4] == parts[3] + ".txt":
                    role = "txt"
                elif parts[4] in ("og.jpg", "og.png"):
                    role = "og"
            if role == "txt":
                text(raw)
            elif role in ("original", "og"):
                probe(raw, PurePosixPath(path).suffix.lower(), role == "og")
            else:
                raise QueueStop("SOURCE_MEDIA_ROLE")
    # Deletions from the editing mirror can only remove an optional sidecar.
    for path in actual - new.keys():
        parts = planner._path(path).parts
        if (len(parts) != 5 or parts[0] != "library" or parts[1] not in planner.COLS
                or parts[2] != "published" or parts[4] not in (parts[3] + ".txt", "og.jpg", "og.png")):
            raise QueueStop("SOURCE_DELETE_SCOPE")
    return pins


def check(api, git, event, *, run, attempt, workflow, code, before, after, started,
          baseline=None, workflow_change=False):
    # Authenticate provider facts before traversing the local, fetched object graph.
    def read_run(r, a):
        row, _ = api.get(f"/actions/runs/{r}/attempts/{a}")
        if row["path"].split("@",1)[0] != WORKFLOW:
            raise QueueStop("WORKFLOW_PATH")
        return row

    provider = VerifiedPushProvider(proof,api,event,read_run,None,run=run,attempt=attempt,
               workflow=workflow,code=code,bootstrap=before,source=after,started=started)
    facts = provider.run_facts(run,attempt)
    if baseline is not None:
        if not proof.oid(baseline):
            raise QueueStop("SOURCE_SCOPE")
        observed = git("rev-parse", "FETCH_HEAD").decode("ascii").strip()
        reader = GitReader(git, before, after, dynamic=True)
        reader.allow_history(observed, baseline)
        if after not in reader.allowed or before not in reader.allowed:
            raise QueueStop("SOURCE_DIVERGENT_OR_UNVERIFIED")
        if reader._commit("intake", after)[1] != [before]:
            raise QueueStop("CP2B_PREFLIGHT_ANCESTRY")
        verify_delta(reader, before, after, workflow_change=workflow_change)
        return dict(status="PROPOSED_ONLY", registered=False,
                    rows=[[1, request_identity(facts["repository"], before, after), before, after]])
    if git("rev-parse","FETCH_HEAD").strip()!=after.encode("ascii"):
        raise QueueStop("SOURCE_REF_MOVED")
    changed=git("diff-tree","--no-commit-id","-r","--name-only","-z",before,after)
    paths=changed.decode("utf-8").split("\0")
    if paths[-1]!="" or len(paths)!=5 or set(paths[:-1])!=set(MEDIA)|{WORKFLOW}:
        raise QueueStop("SOURCE_DELTA")
    reader=GitReader(git,before,after)
    pin=gb.RepoPin("intake",proof.REPO_ID,*gb.NAMES["intake"])
    class VerifiedFacts:
        def run_facts(self, r, a):
            if (r,a)!=(run,attempt):
                raise QueueStop("PUSH_RUN_PIN")
            return facts
    return TrustedRegistration.preflight_cp2b_read_only(run,attempt,MEDIA,repository=pin,
               actor=proof.OWNER,baseline=before,workflow=workflow,code=code,
               provider=VerifiedFacts(),reader=reader,writable=False)


def main():
    env=os.environ
    if env["GITHUB_EVENT_NAME"]!="push" or env["GITHUB_REF"]!=proof.REF:
        raise QueueStop("TRIGGER_SCOPE")
    event=json.loads(Path(env["GITHUB_EVENT_PATH"]).read_bytes())
    before,after,code=env["SOURCE_BASELINE"],env["GITHUB_SHA"],env["WORKER_COMMIT"]
    if not proof.oid(code):
        raise QueueStop("CODE_PIN")
    proof.verify_event(event,after,before)
    repo=env["RAW_REPO"]
    def git(*args):
        return subprocess.run(["git","--no-optional-locks","--git-dir",repo,*args],
                  check=True,capture_output=True,timeout=10).stdout
    result=check(proof.GitHub(env["GH_TOKEN"]),git,event,run=int(env["GITHUB_RUN_ID"]),
              attempt=int(env["GITHUB_RUN_ATTEMPT"]),workflow=int(env["WORKFLOW_ID"]),
              code=code,before=before,after=after,started=datetime.now(timezone.utc))
    if result["status"]!="PROPOSED_ONLY" or result["registered"] is not False:
        raise QueueStop("RESULT_SCOPE")
    print("PROPOSED_ONLY registered=False rows=1")


if __name__=="__main__":
    try:main()
    except (QueueStop,proof.Stop) as exc:
        print("STOP "+str(exc),file=sys.stderr)
        sys.exit(1)
    except Exception:
        print("STOP PROVIDER_UNRESOLVED",file=sys.stderr)
        sys.exit(1)
