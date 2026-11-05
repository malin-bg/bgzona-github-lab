"""Bounded Push evidence handoff. This module grants no registration authority."""
from hashlib import sha256
from io import BytesIO
import json
from pathlib import Path
from private_queue import request_identity
import re
import stat
from zipfile import ZipFile

REPO = "malin-bg/bgzona-private-intake"
REPO_ID = 1429436685
OWNER = 252877943
REF = "refs/heads/cp2b-private-push-r1"
STANDARD_REF = "refs/heads/cp2b-private-standard-source-r1"
WORKFLOW = 407190822
WORKFLOW_PATH = ".github/workflows/cp2b-push-proof.yml"
FILE = "push-facts.json"
FORMAT = "bgzona-cp2b-push-facts-r1"
LIMIT = 2048
ARCHIVE_LIMIT = 8192


class Stop(ValueError):
    pass


def data(row):
    return json.dumps(row, sort_keys=True, separators=(",", ":")).encode("utf-8")


def oid(value):
    return type(value) is str and bool(re.fullmatch("[0-9a-f]{40}", value)) and value != "0"*40


def positive(value):
    return type(value) is int and value > 0


def name(run, attempt):
    if not positive(run) or not positive(attempt):
        raise Stop("RUN_IDENTITY")
    return f"cp2b-push-facts-r1-{run}-{attempt}"


def encode(event, run, attempt, workflow, head, *, ref=REF):
    name(run,attempt)
    repo = event["repository"]
    before, after = event["before"], event["after"]
    if ((repo["id"],repo["full_name"],repo["private"],event["sender"]["id"],event["ref"],workflow)
            != (REPO_ID,REPO,True,OWNER,ref,WORKFLOW) or ref not in (REF,STANDARD_REF)
            or not oid(before) or not oid(after) or before == after or after != head
            or any(event[key] is not False for key in ("forced","created","deleted"))):
        raise Stop("PUSH_SCOPE")
    request = request_identity(REPO_ID,before,after)
    return data(dict(format=FORMAT,repository=REPO,repository_id=REPO_ID,actor=OWNER,
                    ref=ref,before=before,after=after,forced=False,created=False,deleted=False,
                    run=run,attempt=attempt,workflow=workflow,request=request))


def select(rows, run, attempt, head, *, ref=REF):
    expected = name(run,attempt)
    matches = [row for row in rows if row.get("name") == expected]
    if len(matches) != 1:
        raise Stop("ARTIFACT_COUNT")
    row = matches[0]
    link = row.get("workflow_run",{})
    if (not positive(row.get("id")) or row.get("expired") is not False
            or not positive(row.get("size_in_bytes")) or row["size_in_bytes"] > ARCHIVE_LIMIT
            or (link.get("id"),link.get("repository_id"),link.get("head_repository_id"),
                link.get("head_branch"),link.get("head_sha")) !=
               (run,REPO_ID,REPO_ID,ref.removeprefix("refs/heads/"),head)
            or not re.fullmatch("sha256:[0-9a-f]{64}",row.get("digest") or "")):
        raise Stop("ARTIFACT_METADATA")
    return row


def decode(archive, metadata):
    if (type(archive) is not bytes or len(archive)>ARCHIVE_LIMIT
            or "sha256:"+sha256(archive).hexdigest()!=metadata["digest"]):
        raise Stop("ARTIFACT_DIGEST")
    try:
        with ZipFile(BytesIO(archive)) as z:
            entries=z.infolist()
            if len(entries)!=1:
                raise Stop("ARTIFACT_MEMBERS")
            entry=entries[0]
            mode=stat.S_IFMT(entry.external_attr>>16)
            if entry.filename!=FILE or not 0<entry.file_size<=LIMIT or mode not in (0,stat.S_IFREG):
                raise Stop("ARTIFACT_MEMBER")
            with z.open(entry) as f:
                raw=f.read(LIMIT+1)
        value=json.loads(raw)
        if len(raw)>LIMIT or type(value) is not dict or data(value)!=raw:
            raise Stop("ARTIFACT_JSON")
        return value
    except Stop:
        raise
    except Exception:
        raise Stop("ARTIFACT_INVALID") from None


def event_for(row):
    return dict(repository=dict(id=row["repository_id"],full_name=row["repository"],private=True),
                sender=dict(id=row["actor"]),ref=row["ref"],before=row["before"],after=row["after"],
                forced=row["forced"],created=row["created"],deleted=row["deleted"])


def verify(row, run, attempt, head, metadata, activities, parents, *, ref=REF):
    expected = (run,attempt,WORKFLOW,REPO_ID,OWNER,ref.removeprefix("refs/heads/"),head,"push","success")
    observed = (metadata["id"],metadata["run_attempt"],metadata["workflow_id"],
                metadata["repository"]["id"],metadata["actor"]["id"],metadata["head_branch"],
                metadata["head_sha"],metadata["event"],metadata["conclusion"])
    if observed!=expected or metadata["path"].split("@",1)[0]!=WORKFLOW_PATH:
        raise Stop("RUN_MISMATCH")
    if data(row)!=encode(event_for(row),run,attempt,WORKFLOW,head,ref=ref):
        raise Stop("FACTS_MISMATCH")
    matches=[a for a in activities if a.get("ref")==ref and a.get("after")==head]
    if len(matches)!=1 or (matches[0]["activity_type"],matches[0]["actor"]["id"],matches[0]["before"])!=(
            "push",OWNER,row["before"]):
        raise Stop("ACTIVITIES_MISMATCH")
    if parents!=[row["before"]]:
        raise Stop("SOURCE_PARENT")
    return event_for(row)


def emit(event_path, destination, run, attempt, workflow, head, *, ref=REF):
    event=json.loads(Path(event_path).read_bytes())
    with Path(destination).open("xb") as f:
        f.write(encode(event,run,attempt,workflow,head,ref=ref))
