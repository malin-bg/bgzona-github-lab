"""Trusted entrypoints for one CP2B registration, with no STANDARD or D3 call."""
from datetime import datetime,timezone
from hashlib import sha1,sha256
from base64 import b64decode
import json
import os
from pathlib import Path
import subprocess
import sys
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

import github_backend as gb
from git_transport import Lab
from importer import data,digest
from media_preflight import GitReader,WORKFLOW,check,proof,verify_delta
from private_queue import PrivateQueue,QueueStop,TrustedRegistration
import push_artifact as artifact

BEFORE="c4a208881f371152b82a77119211f48eb850ebbc"
BOOT="3119a5dd91d2ca5dd45c341e66bc74d68f870564"
HISTORY="5e10f852f0c86fbea7f86df4378da670b511912a"
HISTORY_PACKET="24ddb390c9b31f8224f4c90d8fc6f1ecb1ed0db92c8b5077216c81b369ef6f7c"
HISTORY_SOURCE="ed163dc182eac227cd9978a7cabcb238aee2f266"
HISTORY_CODE="29c9c54b18b4030fa5ddfb96ecd734c5b603ee24"
# The old source preflight and the new trusted registration worker have separate roles.
SOURCE_CODE="0ed657094fcc59fc39a3a58ab71690905f691cf7"
SOURCE_TEMPLATE_SHA256="a032053029a4fbad2e69d311ccde1e12422d29b1c88d80d250cb149ac83be648"
LEGACY_RUNS=(42037080687,42037576509)
HISTORY_PREFIX=(
    ("0b6a7addbd0ae3d88101a8ff315bd66053b38c6e943f1a56908272cc041cd66b", SOURCE_CODE),
    ("127afc00f73e41e9f85d948d9a9f59a393a2c147abad74d7f450d3caa4c0f3c8", SOURCE_CODE),
)
PIN=gb.RepoPin("intake",artifact.REPO_ID,*gb.NAMES["intake"])


def git(*args):
    return subprocess.run(["git","--no-optional-locks","--git-dir",os.environ["RAW_REPO"],*args],
                          check=True,capture_output=True,timeout=10).stdout


def read_archive(api, artifact_id, *, limit=artifact.ARCHIVE_LIMIT):
    url="https://api.github.com/repos/"+artifact.REPO+f"/actions/artifacts/{artifact_id}/zip"
    req=Request(url,headers={"Authorization":"Bearer "+api.token,"Accept":"application/vnd.github+json"})
    class NoRedirect(HTTPRedirectHandler):
        def redirect_request(self, *args, **kwargs):
            return None

    opener=build_opener(NoRedirect)
    try:
        with opener.open(req,timeout=10) as r:
            return r.read(limit+1)
    except HTTPError as exc:
        location=exc.headers.get("Location","")
        target=urlsplit(location)
        if (exc.code!=302 or target.scheme!="https" or target.username is not None
                or target.port not in (None,443) or not target.hostname
                or not target.hostname.endswith(".blob.core.windows.net")):
            raise QueueStop("ARTIFACT_DOWNLOAD") from None
    with opener.open(Request(location),timeout=10) as r:
        return r.read(limit+1)


class SourceReader(GitReader):
    def __init__(self, objects, api, before, after):
        super().__init__(objects,before,after,dynamic=True)
        self.api=api

    def _ref(self, role, ref):
        if (role,ref)!=("intake",artifact.REF):
            raise QueueStop("SOURCE_SCOPE")
        row,_=self.api.get("/git/ref/"+ref.removeprefix("refs/"))
        tip=row["object"]["sha"]
        if row["ref"]!=ref or row["object"]["type"]!="commit" or not proof.oid(tip):
            raise QueueStop("SOURCE_MOVED")
        return tip



TRUSTED_PATH=".github/workflows/cp2b-trusted-registration.yml"
TRUSTED_WORKFLOW=407892919
PLACEHOLDER=b"__CP2B_IMMUTABLE_WORKER_COMMIT__"


TRUSTED_TEMPLATE_SHA256="01d1c0a38bc539b4b528d185472dc208579b979a8b3e11033b67dd619227c3a9"
WORKER_PACKAGE="tools/cp2b/r18d/"


def pinned_file(api, path, commit):
    file,_=api.get("/contents/"+path+"?ref="+commit)
    if file.get("path")!=path or file.get("type")!="file" or file.get("encoding")!="base64":
        raise QueueStop("TRUSTED_WORKFLOW_OBJECT")
    raw=b64decode("".join(file["content"].splitlines()),validate=True)
    if (not 0<len(raw)<=65536 or file["size"]!=len(raw)
            or sha1(f"blob {len(raw)}\0".encode()+raw).hexdigest()!=file["sha"]):
        raise QueueStop("TRUSTED_WORKFLOW_OBJECT")
    return raw


def verify_workflow_context(api, e, worker, template):
    expected_ref=f"{artifact.REPO}/{TRUSTED_PATH}@refs/heads/main"
    workflow=e["GITHUB_WORKFLOW_SHA"]
    manifest=e["MANIFEST_SHA256"]
    if (e["GITHUB_WORKFLOW_REF"]!=expected_ref or not proof.oid(workflow)
            or not proof.oid(worker) or worker==SOURCE_CODE or not gb.sha(manifest)
            or e["GITHUB_REPOSITORY"]!=artifact.REPO
            or e["GITHUB_REPOSITORY_ID"]!=str(artifact.REPO_ID)):
        raise QueueStop("TRUSTED_WORKFLOW_CONTEXT")
    run=int(e["GITHUB_RUN_ID"])
    if not artifact.positive(run):
        raise QueueStop("TRUSTED_WORKFLOW_CONTEXT")
    meta,_=api.get(f"/actions/runs/{run}/attempts/1")
    if (meta["id"],meta["workflow_id"],meta["run_attempt"],meta["event"],meta["repository"]["id"],
        meta["head_branch"],meta["head_sha"],meta["path"].split("@",1)[0])!=(
        run,TRUSTED_WORKFLOW,1,"workflow_run",artifact.REPO_ID,"main",workflow,TRUSTED_PATH):
        raise QueueStop("TRUSTED_WORKFLOW_RUN")
    if sha256(template).hexdigest()!=TRUSTED_TEMPLATE_SHA256:
        raise QueueStop("TRUSTED_WORKFLOW_TEMPLATE")
    expected=template.replace(b"__R18D_WORKER_COMMIT__",worker.encode()).replace(
        b"__R18D_MANIFEST_SHA256__",manifest.encode())
    if pinned_file(api,TRUSTED_PATH,workflow)!=expected:
        raise QueueStop("TRUSTED_WORKFLOW_BYTES")
    if sha256(pinned_file(api,WORKER_PACKAGE+"worker.sha256",worker)).hexdigest()!=manifest:
        raise QueueStop("WORKER_MANIFEST_PIN")
    return workflow


def verify_source_yaml(actual, template, template_sha, code):
    if (not proof.oid(code) or len(template)>65536 or sha256(template).hexdigest()!=template_sha
            or template.count(PLACEHOLDER)!=1
            or template.count(b"      WORKER_COMMIT: "+PLACEHOLDER+b"\n")!=1
            or actual!=template.replace(PLACEHOLDER,code.encode("ascii"),1)):
        raise QueueStop("SOURCE_WORKFLOW_PIN")


def verify_single_writer(api, run, attempt, trusted_run, workflow_sha):
    # GitHub attempt 1 and the trusted workflow's own run record bind one writer
    # to a source attempt. A separate delivery after an UNKNOWN must not resend.
    listing, more = api.get(f"/actions/workflows/{TRUSTED_WORKFLOW}/runs?event=workflow_run&per_page=100")
    title = f"CP2B-R18 register {run}/{attempt}"
    candidates = [row for row in listing["workflow_runs"] if row.get("display_title") == title]
    if more or len(candidates)!=1 or (
        candidates[0]["id"],candidates[0]["workflow_id"],candidates[0]["run_attempt"],
        candidates[0]["head_sha"],candidates[0]["event"],candidates[0]["status"])!=(
        trusted_run,TRUSTED_WORKFLOW,1,workflow_sha,"workflow_run","in_progress"):
        raise QueueStop("REGISTRATION_ATTEMPT_UNVERIFIED")


def trusted(api, objects, event, code, work, mode, template, template_sha, download=read_archive, *,
            trusted_run=None, workflow_sha=None):
    if mode not in ("writer","reader") or event.get("action")!="completed":
        raise QueueStop("WORKFLOW_RUN_SCOPE")
    incoming=event["workflow_run"]
    run,attempt,after=incoming["id"],incoming["run_attempt"],incoming["head_sha"]
    if (attempt!=1 or not artifact.positive(run) or not proof.oid(after)
            or incoming.get("conclusion")!="success" or not proof.oid(code)):
        raise QueueStop("WORKFLOW_RUN_SCOPE")
    if template_sha!=SOURCE_TEMPLATE_SHA256:
        raise QueueStop("SOURCE_WORKFLOW_PIN")
    verify_source_yaml(objects("show",after+":"+WORKFLOW),template,template_sha,SOURCE_CODE)
    meta,_=api.get(f"/actions/runs/{run}/attempts/{attempt}")
    listing,more=api.get(f"/actions/runs/{run}/artifacts?per_page=100")
    if more:
        raise QueueStop("ARTIFACT_WINDOW")
    item=artifact.select(listing["artifacts"],run,attempt,after)
    row=artifact.decode(download(api,item["id"]),item)
    activities,more=api.get("/activity?per_page=100")
    if more:
        raise QueueStop("ACTIVITY_WINDOW")
    parents=objects("show","-s","--format=%P",after).decode().split()
    original=artifact.verify(row,run,attempt,after,meta,activities,parents)
    before=original["before"]
    proposal=check(api,objects,original,run=run,attempt=attempt,workflow=artifact.WORKFLOW,
                    code=SOURCE_CODE,before=before,after=after,started=datetime.now(timezone.utc),
                    baseline=BEFORE,workflow_change=before==HISTORY_SOURCE)
    if proposal["registered"] or len(proposal["rows"])!=1 or proposal["rows"][0][1]!=row["request"]:
        raise QueueStop("PROPOSAL_MISMATCH")

    reader=SourceReader(objects,api,before,after)
    observed=reader._ref("intake",artifact.REF)
    reader.allow_history(observed,BEFORE)
    if observed!=objects("rev-parse","FETCH_HEAD").decode("ascii").strip() or after not in reader.allowed:
        raise QueueStop("SOURCE_MOVED")
    ro=gb.PrivateRecoveryHTTP(PIN,artifact.OWNER,code,api.token,cp2b_bootstrap=BOOT,cp2b_history=HISTORY)
    node=ro.node_id(PIN.numeric_id)

    def facts_for(r,a,parent,source):
        return dict(run=r,attempt=a,workflow=artifact.WORKFLOW,
                    code=SOURCE_CODE if r in LEGACY_RUNS else code,
                    repository=artifact.REPO_ID,actor=artifact.OWNER,ref=artifact.REF,
                    event="push",forced=False,deleted=False,before=parent,tip=source)

    facts=facts_for(run,attempt,before,after)
    class Provider:
        def run_facts(self,r,a):
            if (r,a)==(run,attempt):
                return facts.copy()
            if a!=1 or not artifact.positive(r):
                raise QueueStop("RUN_MISMATCH")
            previous,_=api.get(f"/actions/runs/{r}/attempts/{a}")
            source=previous["head_sha"]
            if (previous["id"],previous["run_attempt"],previous["workflow_id"],
                previous["repository"]["id"],previous["actor"]["id"],previous["head_branch"],
                previous["event"],previous["conclusion"],previous["path"].split("@",1)[0])!=(
                r,a,artifact.WORKFLOW,artifact.REPO_ID,artifact.OWNER,
                artifact.REF.removeprefix("refs/heads/"),"push","success",WORKFLOW):
                raise QueueStop("RUN_MISMATCH")
            _, parents=reader._commit("intake",source)
            if len(parents)!=1 or not proof.find_activity(activities,parents[0],source):
                raise QueueStop("ACTIVITIES_MISMATCH")
            verify_source_yaml(objects("show",source+":"+WORKFLOW),template,template_sha,SOURCE_CODE)
            verify_delta(reader,parents[0],source,workflow_change=parents[0]==HISTORY_SOURCE)
            return facts_for(r,a,parents[0],source)

        def recovery_head(self):
            return ro.head(PIN.numeric_id,gb.CP2B_RECOVERY_REF)

    provider=Provider()
    tip=provider.recovery_head()
    def client(http,writable):
        return gb.PrivateRecovery(PIN,node,artifact.OWNER,code,http,expected_head=tip,
                 writable=writable,ref=gb.CP2B_RECOVERY_REF,cp2b_bootstrap=BOOT)
    lab_path=Path(work)/"registration-lab"
    lab=Lab(lab_path) if lab_path.exists() else Lab.create(lab_path)
    queue=PrivateQueue(lab,lab.root/"queue",gb.Backend(),repository=PIN,owner_actor_id=artifact.OWNER,
                       baseline=BEFORE,source_ref=artifact.REF,cp2b_reader=reader)
    if not queue.directory.exists():
        queue.initialize()
    def registration(recovery):
        return TrustedRegistration(queue,recovery,provider,workflow=artifact.WORKFLOW,
                                   code=code,cp2b_history_packet=HISTORY_PACKET,cp2b_history_prefix=HISTORY_PREFIX)
    reg=registration(client(ro,False))
    rows=reg.restore()
    seen=[r for r in rows if r[1]==row["request"]]
    if seen:
        if len(seen)!=1 or seen[0][2:]!=(before,after):
            raise QueueStop("REGISTRATION_RUN_CONFLICT")
        _,_,runs,_,_,_=reg._read()
        if run not in runs or runs[run]["facts"]!=facts:
            raise QueueStop("REGISTRATION_RUN_CONFLICT")
        return "ALREADY_REGISTERED" if mode=="writer" else "INDEPENDENT_FIFO_OK"
    if mode!="writer" or run in LEGACY_RUNS:
        raise QueueStop("REGISTRATION_READBACK")
    _,proven,_,cursor,previous,_=reg._read()
    packet=reg._envelope(facts,cursor,len(proven),previous)
    verify_single_writer(api,run,attempt,trusted_run,workflow_sha)
    path=f"private-recovery/{reg.scope}/{reg._slot(facts)}/checkpoint.json"
    transport=gb.PrivateRecoveryHTTP(PIN,artifact.OWNER,code,api.token,writable=True,
                  cp2b_bootstrap=BOOT,cp2b_history=HISTORY,cp2b_expected=tip,
                  cp2b_registration=(path,digest(data(packet))))
    reg=registration(client(transport,True))
    if reg.register(run,attempt)!="REGISTERED" or reg.verify()!=rows+[tuple(packet["rows"][0])]:
        raise QueueStop("REGISTRATION_READBACK")
    if queue.snapshot()["acceptance_cursor"] is not None:
        raise QueueStop("ACCEPTANCE_SCOPE")
    return "DURABLE_REGISTRATION_OK"


def main():
    e=os.environ
    event=json.loads(Path(e["GITHUB_EVENT_PATH"]).read_bytes())
    api=proof.GitHub(e["GH_TOKEN"])
    mode=sys.argv[1]
    if mode=="source":
        if e["GITHUB_EVENT_NAME"]!="push" or e["GITHUB_REF"]!=artifact.REF:
            raise QueueStop("SOURCE_TRIGGER")
        run,attempt,head=int(e["GITHUB_RUN_ID"]),int(e["GITHUB_RUN_ATTEMPT"]),e["GITHUB_SHA"]
        before=event["before"]
        result=check(api,git,event,run=run,attempt=attempt,workflow=artifact.WORKFLOW,
                     code=e["WORKER_COMMIT"],before=before,after=head,started=datetime.now(timezone.utc),
                     baseline=BEFORE,workflow_change=before==HISTORY_SOURCE)
        if result["registered"] or len(result["rows"])!=1:
            raise QueueStop("PROPOSAL_SCOPE")
        artifact.emit(e["GITHUB_EVENT_PATH"],Path(e["RUNNER_TEMP"])/artifact.FILE,
                      run,attempt,artifact.WORKFLOW,head)
        print("PROPOSED_ONLY registered=False rows=1")
    else:
        if e["GITHUB_EVENT_NAME"]!="workflow_run" or e["GITHUB_REF"]!="refs/heads/main" or e["GITHUB_RUN_ATTEMPT"]!="1":
            raise QueueStop("TRUSTED_TRIGGER")
        code=e["WORKER_COMMIT"]
        workflow=verify_workflow_context(api,e,code,Path(e["TRUSTED_TEMPLATE"]).read_bytes())
        print(trusted(api,git,event,code,e["R17D_WORK"],mode,
                      Path(e["SOURCE_TEMPLATE"]).read_bytes(),e["SOURCE_TEMPLATE_SHA256"],
                      trusted_run=int(e["GITHUB_RUN_ID"]), workflow_sha=workflow))


if __name__=="__main__":
    try:main()
    except (QueueStop,gb.RecoveryStop,artifact.Stop,proof.Stop) as exc:
        print("STOP "+str(exc),file=sys.stderr)
        sys.exit(1)
    except Exception:
        print("STOP PROVIDER_UNRESOLVED",file=sys.stderr)
        sys.exit(1)
