"""Promote a completed, exact REAL DEV deployment using Lab's existing app."""
import argparse
import json
import os
import re
import subprocess
import time
from pathlib import Path
from urllib.request import Request, build_opener

import pipeline as P

LAB = 'malin-bg/bgzona-github-lab'
DEV = 'malin-bg/bgzona-win-dev'
PROD = 'malin-bg/bgzona-win'
DATA_REF = 'refs/heads/publisher-data'
OWNER = 252877943
APP = 353092585
BOOTSTRAP = 45990664799
BOOTSTRAP_CODE = '3f7c934ff06cee6dfb1ddc410f6958a6d12f8434'
BOOTSTRAP_COMMIT = '6ea4db490e779ac4632f3117ac7fa55e9e442d61'
PROOF = r'real-dev-[a-f0-9]{32}-([a-f0-9]{40})-([a-f0-9]{64})-[a-f0-9]{64}'
RUN = r'[1-9][0-9]{0,15}'


def rows(repo, path, key, token):
    data, headers = P.request('/repos/' + repo + path, token, with_headers=True)
    items = data[key]
    P.need(isinstance(items, list) and data['total_count'] == len(items)
           and 'rel="next"' not in (headers.get('Link') or ''), 'PROOF_LIST_INCOMPLETE')
    return items


def context(token):
    policy = P.verified_code()
    env = os.environ
    code = env['GITHUB_SHA']
    P.need(env['GITHUB_REPOSITORY'] == LAB and env['GITHUB_REPOSITORY_ID'] == '1426765395'
           and env['GITHUB_REF'] == 'refs/heads/automation-control' and env['GITHUB_REF_PROTECTED'] == 'true' and env['GITHUB_ACTOR_ID'] == str(OWNER)
           and env['GITHUB_RUN_ATTEMPT'] == '1' and env['BGZONA_REAL_ENABLED'] == 'true'
           and env['GITHUB_WORKFLOW_REF'] == LAB + '/.github/workflows/real-prod.yml@refs/heads/automation-control'
           and subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip() == code
           and P.ref(LAB, 'refs/heads/main', token) == P.ref(LAB, policy['control_ref'], token) == code,
           'PROMOTION_CONTEXT')
    current = P.api(LAB, '/actions/runs/' + env['GITHUB_RUN_ID'], token)
    P.control_protection(policy, token, current['created_at'])
    event = P.strict(Path(env['GITHUB_EVENT_PATH']).read_bytes())
    P.need(event['repository']['id'] == 1426765395 and event['sender']['id'] == OWNER,
           'PROMOTION_EVENT')
    P.need(env['GITHUB_EVENT_NAME'] in ('push', 'workflow_dispatch')
           and not event.get('inputs'), 'BOOTSTRAP_EVENT')
    return BOOTSTRAP, BOOTSTRAP_CODE, True


def admit(token):
    lab_run, lab_code, bootstrap = context(token)
    path = '/actions/runs/' + str(lab_run)
    run = P.api(LAB, path, token)
    P.need(run['id'] == lab_run and run['workflow_id'] == 408732936
           and run['event'] == 'workflow_dispatch' and run['run_attempt'] == 1
           and run['display_title'] == 'BGZONA B real' and run['conclusion'] == 'success'
           and run['head_branch'] == 'automation-control' and run['head_sha'] == lab_code
           and run['actor']['id'] == run['triggering_actor']['id'] == OWNER, 'LAB_AUTHORITY')
    artifacts = rows(LAB, path + '/artifacts?per_page=100', 'artifacts', token)
    proofs = [r for r in artifacts if r['name'].startswith('real-dev-') and not r['expired']]
    admissions = [r for r in artifacts if r['name'].startswith('dev-admitted-') and not r['expired']]
    P.need(len(proofs) == len(admissions) == 1, 'LAB_PROOFS')
    proof = re.fullmatch(PROOF, proofs[0]['name'])
    admission = re.fullmatch(r'dev-admitted-(' + RUN + ')', admissions[0]['name'])
    P.need(proof and admission, 'PROOF_FORMAT')
    commit, sid = proof.groups()
    dev_run = admission[1]
    P.need(not bootstrap or commit == BOOTSTRAP_COMMIT, 'BOOTSTRAP_SNAPSHOT')
    deadline = time.monotonic() + 540
    while True:
        dev = P.api(DEV, '/actions/runs/' + dev_run, token)
        P.need(dev['id'] == int(dev_run) and dev['workflow_id'] == 232539432
               and dev['event'] == 'workflow_dispatch' and dev['run_attempt'] == 1
               and dev['head_branch'] == 'main' and dev['actor']['id'] == APP
               and dev['display_title'] == 'DEV automatic real publication', 'DEV_AUTHORITY')
        if dev['status'] == 'completed':
            P.need(dev['conclusion'] == 'success', 'DEV_RESULT')
            break
        P.need(time.monotonic() < deadline, 'DEV_OUTCOME_UNKNOWN')
        time.sleep(12)
    P.need(P.ref(LAB, DATA_REF, token) == commit, 'PUBLIC_SUPERSEDED')
    return lab_run, dev_run, commit, sid


def dispatch(token):
    lab_run, dev_run, commit, sid = admit(token)
    title = f'PROD PUBLIC {sid} rs4n-prod:{lab_run}:{dev_run}:{commit}'
    runs = rows(PROD, '/actions/workflows/249807408/runs?per_page=100&event=workflow_dispatch'
                + '&created=%3E%3D' + P.api(LAB, '/actions/runs/' + str(lab_run), token)['created_at'],
                'workflow_runs', token)
    if any(run['display_title'] == title for run in runs):
        print('PROD_REQUEST_ALREADY_EXISTS')
        return
    P.need(P.ref(LAB, DATA_REF, token) == commit, 'PUBLIC_SUPERSEDED')
    return send(str(lab_run), dev_run, commit, sid)


def forward_context():
    env = os.environ
    P.need(env['GITHUB_REPOSITORY'] == LAB and env['GITHUB_REPOSITORY_ID'] == '1426765395'
           and env['GITHUB_REF'] == 'refs/heads/automation-control'
           and env['GITHUB_REF_PROTECTED'] == 'true' and env['GITHUB_ACTOR_ID'] == str(OWNER)
           and env['GITHUB_EVENT_NAME'] == 'workflow_dispatch' and env['GITHUB_RUN_ATTEMPT'] == '1'
           and env['GITHUB_WORKFLOW_REF'] == LAB + '/.github/workflows/shared-media.yml@refs/heads/automation-control'
           and env['GITHUB_JOB'] == 'h1-dispatch-dev' and env['H1_PUBLIC_SENDS'] == '1'
           and env['H1_WORKFLOW_SHA'] == env['GITHUB_SHA']
           and re.fullmatch(RUN, env['GITHUB_RUN_ID']), 'FORWARD_CONTEXT')
    proof = re.fullmatch(PROOF, env['H1_DEV_PROOF'])
    P.need(proof, 'FORWARD_PROOF')
    return (env['GITHUB_RUN_ID'], *proof.groups())


def forward():
    lab_run, commit, sid = forward_context()
    dev_run = os.environ['DEV_RUN_ID']
    P.need(re.fullmatch(RUN, dev_run), 'FORWARD_DEV_RUN')
    send(lab_run, dev_run, commit, sid)


def send(lab_run, dev_run, commit, sid):
    body = json.dumps({'ref': 'main', 'inputs': {'accepted_cid': sid,
                       'input_trust': f'rs4n-prod:{lab_run}:{dev_run}:{commit}'}}).encode()
    req = Request('https://api.github.com/repos/' + PROD + '/actions/workflows/249807408/dispatches',
                  data=body, method='POST', headers={
                      'Authorization': 'Bearer ' + os.environ['PROD_ACTIONS_TOKEN'],
                      'Accept': 'application/vnd.github+json', 'Content-Type': 'application/json',
                      'X-GitHub-Api-Version': '2026-03-10', 'User-Agent': 'BGZONA-PROD-promotion'})
    try:
        with build_opener(P.NoRedirect).open(req, timeout=10) as response:
            from owner_flow import dispatch_run_id
            prod_run = dispatch_run_id(response, PROD)
    except Exception:
        raise P.Stop('PROD_DISPATCH_UNKNOWN_READBACK_NO_RETRY') from None
    print('PROD_DISPATCH_SENT_ONCE', prod_run)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--dispatch', action='store_true')
    parser.add_argument('--forward', action='store_true')
    parser.add_argument('--check-forward', action='store_true')
    args = parser.parse_args()
    try:
        if args.check_forward:
            P.verified_code()
            forward_context()
        elif args.forward:
            forward()
        elif args.dispatch:
            dispatch(os.environ['GH_TOKEN'])
        else:
            admit(os.environ['GH_TOKEN'])
            print('PROD_DEV_RUN_VERIFIED')
    except Exception as error:
        code = str(error) if isinstance(error, P.Stop) and re.fullmatch(r'[A-Z0-9_]+', str(error)) else 'UNKNOWN'
        raise SystemExit('PROD_PROMOTION_STOP:' + code) from None
