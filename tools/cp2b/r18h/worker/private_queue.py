"""Inactive PRIVATE Push registration in an isolated local Lab.

A registration is a cursor over a verified linear history, not an authority
receipt. This stage has no executor, acceptance writer or publishing port.
"""
from contextlib import closing, contextmanager
import sqlite3

import github_backend as gb
from git_transport import Lab, MAIN
from importer import digest, data
from pipeline import PushEvent


class QueueStop(RuntimeError):
    pass


def require(condition, code):
    if not condition:
        raise QueueStop(code)


def request_identity(repository_id, parent, commit):
    """The same raw source commitment used by E4N R2 SourceBinding."""
    return digest(data({"repository_id": repository_id, "source": commit, "parent": parent}))


class PrivateQueue:
    def __init__(self, lab, directory, backend, *, repository, owner_actor_id, baseline, source_ref=MAIN, cp2b_reader=None):
        require(type(lab) is Lab and type(backend) is gb.Backend, "TRUSTED_QUEUE_SCOPE_REQUIRED")
        require(type(repository) is gb.RepoPin and repository.role == "intake"
                and (repository.name, repository.visibility) == gb.NAMES["intake"]
                and gb.positive(repository.numeric_id) and gb.positive(owner_actor_id)
                and gb.oid(baseline), "INVALID_QUEUE_IDENTITY")
        require(source_ref == MAIN or (source_ref in (gb.CP2B_SOURCE_REF, gb.STANDARD_SOURCE_REF)
                and repository.numeric_id == gb.CP2B_REPO_ID and owner_actor_id == gb.CP2B_OWNER_ID),
                "SOURCE_REF_SCOPE")
        require(cp2b_reader is None or source_ref in (gb.CP2B_SOURCE_REF, gb.STANDARD_SOURCE_REF), "SOURCE_REF_SCOPE")
        self.source_ref = source_ref
        self.cp2b_reader = cp2b_reader
        self.source_reader = backend if cp2b_reader is None else cp2b_reader
        self.lab, self.backend = lab, backend
        self.directory = lab.path(directory)
        self.path = self.directory / "private-queue.sqlite3"
        self.identity = (1, repository.numeric_id, repository.name, owner_actor_id, baseline)
        self.repository, self.owner_actor_id, self.baseline = repository, owner_actor_id, baseline

    def _paths(self):
        require(self.lab.path(self.directory).is_dir(), "QUEUE_MISSING")
        require(self.path.is_file(), "QUEUE_MISSING")
        # SQLite's rollback journal lives in this dedicated PRIVATE directory.
        for entry in self.directory.iterdir():
            self.lab.path(entry)
            require(entry.name in (self.path.name, self.path.name + "-journal")
                    and entry.is_file() and entry.stat().st_nlink == 1, "UNSAFE_QUEUE_STORAGE")

    @contextmanager
    def _transaction(self):
        try:
            self._paths()
            with closing(sqlite3.connect(self.path.as_uri() + "?mode=rw", uri=True,
                                         timeout=8, isolation_level=None)) as db:
                db.execute("PRAGMA trusted_schema=OFF")
                require(db.execute("PRAGMA journal_mode").fetchone() == ("delete",), "QUEUE_JOURNAL_MODE")
                db.execute("PRAGMA synchronous=FULL")
                db.execute("BEGIN IMMEDIATE")
                try:
                    yield db
                    db.commit()
                except BaseException:
                    db.rollback()
                    raise
        except QueueStop:
            raise
        except (sqlite3.Error, OSError) as exc:
            raise QueueStop("QUEUE_STORAGE_UNRESOLVED") from None

    def _source_tip(self):
        if self.cp2b_reader is not None:
            return self.source_reader._ref("intake", self.source_ref)
        _, repository_id = self.backend._repo("intake")
        require(self.backend.repos["intake"] == self.repository
                and repository_id == self.repository.numeric_id, "REPOSITORY_MISMATCH")
        return self.backend._ref("intake", self.source_ref)

    def _on_main(self, expected, tip):
        # The accepted input contract is a single-parent owner commit.
        for _ in range(512):
            _, parents = self.source_reader._commit("intake", tip)
            if tip == expected:
                return
            if len(parents) != 1:
                break
            tip = parents[0]
        raise QueueStop("SOURCE_DIVERGENT_OR_UNVERIFIED")

    def initialize(self):
        """Explicit first bootstrap from a trusted discovery baseline."""
        try:
            tip = self._source_tip()
            self._on_main(self.baseline, tip)
            require(self._source_tip() == tip, "SOURCE_MOVED")
        except QueueStop:
            raise
        except Exception:
            raise QueueStop("SOURCE_UNVERIFIED") from None
        try:
            self.directory.mkdir()
            with self.path.open("xb"):
                pass
        except OSError:
            raise QueueStop("QUEUE_BOOTSTRAP_CONFLICT") from None
        with self._transaction() as db:
            db.execute("""CREATE TABLE identity (
                version INTEGER, repository_id INTEGER, repository TEXT,
                owner_actor_id INTEGER, baseline TEXT)""")
            db.execute("INSERT INTO identity VALUES (?, ?, ?, ?, ?)", self.identity)
            db.execute("""CREATE TABLE requests (
                sequence INTEGER PRIMARY KEY, request TEXT NOT NULL UNIQUE,
                parent TEXT NOT NULL, source TEXT NOT NULL UNIQUE,
                state TEXT NOT NULL CHECK(state IN ('pending', 'processing')),
                outcome TEXT CHECK(outcome IN ('UNKNOWN', 'UNRESOLVED')))""")

    def _entries(self, db):
        require(db.execute("SELECT * FROM identity").fetchall() == [self.identity],
                "QUEUE_IDENTITY_MISMATCH")
        rows = db.execute("SELECT * FROM requests ORDER BY sequence").fetchall()
        cursor = self.baseline
        for number, (sequence, identity, parent, commit, state, outcome) in enumerate(rows, 1):
            require(sequence == number and parent == cursor and gb.oid(commit)
                    and commit != parent and identity == request_identity(self.repository.numeric_id, parent, commit)
                    and (state == "pending" and outcome is None or sequence == 1
                         and state == "processing" and outcome in (None, "UNKNOWN", "UNRESOLVED")),
                    "QUEUE_STATE_CONFLICT")
            cursor = commit
        return rows, cursor

    def snapshot(self):
        with self._transaction() as db:
            rows, cursor = self._entries(db)
            keys = ("sequence", "request", "parent", "source", "state", "outcome")
            return {"discovery_cursor": cursor, "acceptance_cursor": None,
                    "requests": [dict(zip(keys, row)) for row in rows]}

    def register(self, event, *, interrupt=lambda point: None):
        require(type(event) is PushEvent and type(event.actor_id) is int
                and event.actor_id == self.owner_actor_id and event.event == "push"
                and self.source_ref == MAIN and event.ref == MAIN and event.forced is False and event.deleted is False,
                "OWNER_EVENT_REJECTED")
        require(type(event.repository_id) is int and event.repository_id == self.repository.numeric_id
                and event.repository == self.repository.name, "REPOSITORY_MISMATCH")
        require(gb.oid(event.commit) and gb.oid(event.parent) and event.commit != event.parent,
                "SOURCE_IDENTITY")
        identity = request_identity(event.repository_id, event.parent, event.commit)
        with self._transaction() as db:
            rows, cursor = self._entries(db)
            try:
                tip = self._source_tip()
                tree, parents = self.backend._commit("intake", event.commit)
                require(parents == [event.parent], "SOURCE_PARENT")
                parent_tree, _ = self.backend._commit("intake", event.parent)
                self.backend._object("intake", tree, "tree")
                self.backend._object("intake", parent_tree, "tree")
                self._on_main(event.commit, tip)
                self._on_main(cursor, tip)
                require(self._source_tip() == tip, "SOURCE_MOVED")
            except QueueStop:
                raise
            except Exception:
                raise QueueStop("SOURCE_UNVERIFIED") from None
            for sequence, existing, _, source, _, _ in rows:
                if source == event.commit:
                    require(existing == identity, "REQUEST_CONFLICT")
                    return {"status": "ALREADY_REGISTERED", "request": identity, "sequence": sequence}
            require(event.parent == cursor, "DISCOVERY_CURSOR_CONFLICT")
            sequence = len(rows) + 1
            db.execute("INSERT INTO requests VALUES (?, ?, ?, ?, 'pending', NULL)",
                       (sequence, identity, event.parent, event.commit))
        # The commit has finished before the caller is notified.
        interrupt("after-private-queue-commit")
        return {"status": "REGISTERED", "request": identity, "sequence": sequence}

    def begin(self, request):
        """Record one processing state. This method does not run any operation."""
        with self._transaction() as db:
            rows, _ = self._entries(db)
            require(rows and rows[0][1] == request and rows[0][4] == "pending", "QUEUE_HEAD_CONFLICT")
            db.execute("UPDATE requests SET state='processing' WHERE sequence=1")

    def record_outcome(self, request, status):
        # Future independent evidence must bind this raw request to the
        # admitted derived operation and accepted D3 record. Status alone
        # cannot authorize that transition. UNKNOWN remains fenced.
        require(status in ("UNKNOWN", "UNRESOLVED"), "INDEPENDENT_ACCEPTANCE_REQUIRED")
        with self._transaction() as db:
            rows, _ = self._entries(db)
            require(rows and rows[0][1] == request and rows[0][4] == "processing",
                    "QUEUE_HEAD_CONFLICT")
            db.execute("UPDATE requests SET outcome=? WHERE sequence=1", (status,))


class TrustedRegistration:
    """Inactive Lab contract. Provider and recovery pins come from trusted wiring.

    The provider supplies immutable run facts and a separately observed recovery
    head. An offline provider implements this interface without GitHub credentials.
    """
    FORMAT = "bgzona-private-registration-r1"

    def __init__(self, queue, recovery, provider, *, workflow, code, cp2b_blobs=None,
                 cp2b_history_packet=None, cp2b_history_prefix=()):
        require(type(queue) is PrivateQueue and type(recovery) is gb.PrivateRecovery
                and recovery.repository == queue.repository and gb.positive(workflow)
                and gb.oid(code), "REGISTRATION_TRUST_SCOPE")
        self.queue, self.recovery, self.provider = queue, recovery, provider
        self.workflow, self.code = workflow, code
        cp2b = queue.source_ref == gb.CP2B_SOURCE_REF
        dynamic = recovery.cp2b_history is not None
        require(cp2b == (cp2b_blobs is not None or dynamic)
                 and cp2b == (recovery.cp2b_bootstrap is not None)
                 and (not dynamic or cp2b_blobs is None and
                      (gb.sha(cp2b_history_packet) or recovery.cp2b_history == recovery.cp2b_bootstrap
                       and cp2b_history_packet is None))
                 and (dynamic or cp2b_history_packet is None), "REGISTRATION_TRUST_SCOPE")
        require(type(cp2b_history_prefix) is tuple and
                (not cp2b_history_prefix or dynamic and gb.sha(cp2b_history_packet) and
                 all(type(pin) is tuple and len(pin) == 2 and gb.sha(pin[0]) and
                     gb.oid(pin[1]) for pin in cp2b_history_prefix)),
                "REGISTRATION_TRUST_SCOPE")
        self.cp2b_history_prefix = cp2b_history_prefix
        self.cp2b_dynamic = dynamic
        self.cp2b_history_packet = cp2b_history_packet
        self.cp2b_blobs = dict(cp2b_blobs) if cp2b_blobs is not None else None
        hosted = type(recovery.port) is gb.PrivateRecoveryHTTP and recovery.cp2b_bootstrap is not None
        standard = queue.source_ref == gb.STANDARD_SOURCE_REF
        require((queue.cp2b_reader is not None) == (hosted or standard), "REGISTRATION_TRUST_SCOPE")
        if standard:
            require(type(recovery.port) is gb.PrivateRecoveryHTTP and
                    gb.oid(recovery.port.standard_bootstrap) and recovery.ref==gb.STANDARD_RECOVERY_REF and
                    recovery.code_oid==code, "REGISTRATION_TRUST_SCOPE")
        self.standard = standard
        self.cp2b_hosted = hosted
        identity = list(queue.identity)
        if standard:
            identity.append(queue.source_ref)
        if cp2b:
            require(recovery.ref == gb.CP2B_RECOVERY_REF and recovery.code_oid == code,
                    "REGISTRATION_TRUST_SCOPE")
            identity.append(queue.source_ref)
        self.scope = digest(data(identity))

    def preflight_cp2b(self, run, attempt, expected_blobs):
        """Propose one synthetic FIFO row from trusted facts and raw Git blobs."""
        return self.preflight_cp2b_read_only(
            run, attempt, expected_blobs, repository=self.queue.repository,
            actor=self.queue.owner_actor_id, baseline=self.queue.baseline,
            workflow=self.workflow, code=self.code, provider=self.provider,
            reader=self.queue.backend, writable=self.recovery.writable)

    @staticmethod
    def preflight_cp2b_read_only(run, attempt, expected_blobs, *, repository, actor,
                                 baseline, workflow, code, provider, reader, writable):
        """Validate a proposal without constructing a queue or recovery client."""
        from hashlib import sha1
        from intake_plan import _incoming_groups

        repo_id, actor_id = 1429436685, 252877943
        require(gb.positive(run) and gb.positive(attempt) and gb.positive(workflow)
                and gb.oid(code) and gb.oid(baseline)
                and repository == gb.RepoPin("intake", repo_id, *gb.NAMES["intake"])
                and actor == actor_id and writable is False,
                "CP2B_PREFLIGHT_SCOPE")
        try:
            facts = provider.run_facts(run, attempt)
            expected = dict(run=run, attempt=attempt, workflow=workflow, code=code,
                            repository=repo_id, actor=actor_id,
                            ref="refs/heads/cp2b-private-push-r1", event="push",
                            forced=False, deleted=False, before=baseline)
            require(type(facts) is dict and set(facts) == set(expected) | {"tip"}
                    and all(type(facts[k]) is type(v) and facts[k] == v for k, v in expected.items())
                    and gb.oid(facts["tip"]) and facts["tip"] != baseline,
                    "CP2B_PREFLIGHT_FACTS")
            source = facts["tip"]
            tree, parents = reader._commit("intake", source)
            require(parents == [baseline], "CP2B_PREFLIGHT_ANCESTRY")
            parent_tree, _ = reader._commit("intake", baseline)
            reader._object("intake", tree, "tree")
            reader._object("intake", parent_tree, "tree")
            rows = [[1, request_identity(repo_id, baseline, source), baseline, source]]
            require(type(expected_blobs) is dict and len(expected_blobs) == 3,
                    "CP2B_PREFLIGHT_MANIFEST")
            groups, replacements = _incoming_groups(expected_blobs)
            require(len(groups) == 1 and not replacements
                    and set(next(iter(groups.values()))) == {"original", "txt", "og"},
                    "CP2B_PREFLIGHT_MANIFEST")
            for path, pin in expected_blobs.items():
                require(type(pin) is gb.ObjectPin and gb.oid(pin.oid) and gb.sha(pin.sha256)
                        and type(pin.size) is int and 0 < pin.size <= 1_000_000,
                        "CP2B_PREFLIGHT_MANIFEST")
                oid, raw = reader._path("intake", facts["tip"], path)
                header = b"blob " + str(len(raw)).encode("ascii") + b"\0"
                require(type(raw) is bytes and len(raw) == pin.size and digest(raw) == pin.sha256
                        and oid == pin.oid == sha1(header + raw).hexdigest(), "CP2B_PREFLIGHT_BLOB")
            return dict(status="PROPOSED_ONLY", registered=False, rows=rows)
        except QueueStop:
            raise
        except Exception:
            raise QueueStop("CP2B_PREFLIGHT_UNRESOLVED") from None

    def _facts(self, run, attempt, *, historical=None, historical_code=None):
        require(gb.positive(run) and gb.positive(attempt), "REGISTRATION_RUN")
        facts = self.provider.run_facts(run, attempt) if historical is None else historical
        expected = dict(run=run, attempt=attempt, repository=self.queue.repository.numeric_id,
                        actor=self.queue.owner_actor_id, workflow=self.workflow, code=(historical["code"] if historical is not None else historical_code or self.code),
                        ref=self.queue.source_ref, event="push", forced=False, deleted=False)
        require(type(facts) is dict and set(facts) == set(expected) | {"before", "tip"}
                and all(type(facts[k]) is type(v) and facts[k] == v for k, v in expected.items())
                and gb.oid(facts["before"]) and gb.oid(facts["tip"])
                and facts["before"] != facts["tip"], "REGISTRATION_RUN")
        if self.cp2b_blobs is not None:
            # The validated snapshot is reused only inside this check; the
            # resulting proposal never grants registration or send authority.
            from types import SimpleNamespace
            self.preflight_cp2b_read_only(run, attempt, self.cp2b_blobs,
                repository=self.queue.repository, actor=self.queue.owner_actor_id,
                baseline=self.queue.baseline, workflow=self.workflow, code=self.code,
                provider=SimpleNamespace(run_facts=lambda *_: facts),
                reader=self.queue.source_reader, writable=False)
        return dict(facts)

    def _envelope(self, facts, cursor, count, previous):
        require(facts["before"] == cursor, "REGISTRATION_DISCOVERY_CONFLICT")
        chain = []
        source = facts["tip"]
        while source != cursor:
            require(len(chain) < 512, "REGISTRATION_ANCESTRY")
            tree, parents = self.queue.source_reader._commit("intake", source)
            require(len(parents) == 1, "REGISTRATION_ANCESTRY")
            self.queue.source_reader._object("intake", tree, "tree")
            parent = parents[0]
            chain.append((parent, source))
            source = parent
        tree, _ = self.queue.source_reader._commit("intake", cursor)
        self.queue.source_reader._object("intake", tree, "tree")
        rows = [[n, request_identity(facts["repository"], parent, source), parent, source]
                for n, (parent, source) in enumerate(reversed(chain), count + 1)]
        return dict(format=self.FORMAT, facts=facts, discovery=cursor, previous=previous, rows=rows)

    def _slot(self, facts):
        return digest(data([self.FORMAT, facts["run"], facts["attempt"]]))

    def _read(self):
        mode = self.recovery.port.session().mode
        require(mode == "OFFLINE_TEST_DOUBLE" or
                ((self.cp2b_hosted or self.standard) and mode == "HOSTED_SYNTHETIC_PRIVATE_ONLY"),
                "REGISTRATION_OFFLINE_ONLY")
        pin = self.provider.recovery_head()
        require(gb.oid(pin), "REGISTRATION_RECOVERY_PIN")
        snapshot = self.recovery.read(pin)
        prefix = "private-recovery/" + self.scope + "/"
        if self.standard:
            boot = self.recovery.port.standard_bootstrap
            marker = data({"format":"bgzona-private-recovery-r1", "repository":self.queue.repository.numeric_id})
            seed = self.recovery.port.snapshot(self.queue.repository.numeric_id, boot)
            require(seed["parents"]==[] and seed["files"]=={gb.RECOVERY_MARKER:marker},
                    "STANDARD_REGISTRATION_BOOTSTRAP")
        remaining = {}
        for path, raw in snapshot["files"].items():
            if not path.startswith(prefix):
                continue
            packet = gb.parse(raw)
            require(type(packet) is dict and set(packet) ==
                    {"format", "facts", "discovery", "previous", "rows"}
                    and packet["format"] == self.FORMAT and data(packet) == raw
                    and type(packet["facts"]) is dict
                    and path == prefix + self._slot(packet["facts"]) + "/checkpoint.json"
                    and packet["previous"] not in remaining, "REGISTRATION_ENVELOPE")
            remaining[packet["previous"]] = (packet, digest(raw))
        rows, runs = [], {}
        cursor, previous = self.queue.baseline, None
        while previous in remaining:
            packet, packet_hash = remaining.pop(previous)
            historical = (packet["facts"] if previous is None and self.cp2b_history_packet is not None
                          and packet_hash == self.cp2b_history_packet else None)
            if previous is None and self.cp2b_history_packet is not None:
                require(historical is not None, "REGISTRATION_HISTORICAL_PREFIX")
            historical_code = None
            if 0 < len(runs) <= len(self.cp2b_history_prefix):
                expected_hash, historical_code = self.cp2b_history_prefix[len(runs) - 1]
                require(packet_hash == expected_hash, "REGISTRATION_HISTORICAL_PREFIX")
            facts = self._facts(packet["facts"]["run"], packet["facts"]["attempt"],
                                historical=historical, historical_code=historical_code)
            require(data(packet) == data(self._envelope(facts, cursor, len(rows), previous))
                    and facts["run"] not in runs, "REGISTRATION_ENVELOPE")
            rows.extend(packet["rows"])
            runs[facts["run"]] = packet
            cursor, previous = facts["tip"], packet_hash
        require(not remaining, "REGISTRATION_CHAIN")
        if self.standard:
            boot = self.recovery.port.standard_bootstrap
            require(len(rows)==len(runs)==int(pin!=boot),"STANDARD_REGISTRATION_BOOTSTRAP")
            if rows:
                require(rows[0][2]==self.queue.baseline,"STANDARD_REGISTRATION_BOOTSTRAP")
                packet, = runs.values()
                path = prefix + self._slot(packet["facts"]) + "/checkpoint.json"
                commit = snapshot
                if self.recovery.port.standard_handoff is not None:
                    # Later E4M objects are verified separately by the STANDARD guard.
                    while commit["parents"]!=[boot]:
                        require(len(commit["parents"])==1 and len(commit["files"])>2,
                                "STANDARD_REGISTRATION_BOOTSTRAP")
                        parent, = commit["parents"]
                        prior = self.recovery.port.snapshot(self.queue.repository.numeric_id,parent)
                        require(len(prior["files"])+1==len(commit["files"]) and
                                all(commit["files"].get(k)==v for k,v in prior["files"].items()),
                                "STANDARD_REGISTRATION_BOOTSTRAP")
                        commit = prior
                require(commit["parents"]==[boot] and commit["files"]==seed["files"]|{path:data(packet)},
                        "STANDARD_REGISTRATION_BOOTSTRAP")
        if self.cp2b_history_prefix:
            require(len(runs) > len(self.cp2b_history_prefix), "REGISTRATION_HISTORICAL_PREFIX")
        if self.cp2b_history_packet is not None:
            require(rows, "REGISTRATION_HISTORICAL_PREFIX")
        if self.cp2b_blobs is not None:
            expected_count = 0 if pin == self.recovery.cp2b_bootstrap else 1
            require(len(rows) == len(runs) == expected_count, "CP2B_REGISTRATION_MISSING")
        tip = self.queue._source_tip()
        self.queue._on_main(cursor, tip)
        require(self.queue._source_tip() == tip and self.provider.recovery_head() == pin,
                "REGISTRATION_STATE_MOVED")
        return pin, rows, runs, cursor, previous, tip

    @staticmethod
    def _match(rows, proven):
        require(len(rows) <= len(proven) and
                all(list(row[:4]) == proof for row, proof in zip(rows, proven)),
                "REGISTRATION_QUEUE_MISMATCH")

    def _attempt_path(self, previous):
        name = ".registration-" + self.scope + "-" + (previous or "genesis")
        return self.queue.lab.path(self.queue.directory.parent / name)

    def verify(self):
        """Return only after every local row matches the independently read chain."""
        try:
            with self.queue._transaction() as db:
                rows, _ = self.queue._entries(db)
                _, proven, _, _, previous, _ = self._read()
                require(not self._attempt_path(previous).exists(), "REGISTRATION_UNRESOLVED")
                self._match(rows, proven)
                require(len(rows) == len(proven), "REGISTRATION_RECOVERY_PENDING")
                require(not rows or rows[0][4] == "pending", "REGISTRATION_PROCESSING_STOP")
                return [tuple(row) for row in proven]
        except QueueStop:
            raise
        except Exception:
            raise QueueStop("REGISTRATION_UNRESOLVED") from None

    def restore(self):
        """Materialize the independently verified pending rows without a recovery write."""
        require(not self.recovery.writable, "REGISTRATION_TRUST_SCOPE")
        with self.queue._transaction() as db:
            rows, _ = self.queue._entries(db)
            _, proven, _, _, previous, _ = self._read()
            require(not self._attempt_path(previous).exists(), "REGISTRATION_UNRESOLVED")
            self._match(rows, proven)
            require(not rows or rows[0][4] == "pending", "REGISTRATION_PROCESSING_STOP")
            db.executemany("INSERT INTO requests VALUES (?, ?, ?, ?, 'pending', NULL)", proven[len(rows):])
        return self.verify()

    def register(self, run, attempt, *, interrupt=lambda point: None):
        """Persist before SQLite commit. Recovery replay never resends an append."""
        try:
            with self.queue._transaction() as db:
                rows, _ = self.queue._entries(db)
                pin, proven, runs, cursor, previous, tip = self._read()
                self._match(rows, proven)
                require(not rows or rows[0][4] == "pending", "REGISTRATION_PROCESSING_STOP")
                facts = self._facts(run, attempt)
                replay = run in runs
                if replay:
                    require(runs[run]["facts"] == facts, "REGISTRATION_RUN_CONFLICT")
                else:
                    require(len(rows) == len(proven), "REGISTRATION_RECOVERY_PENDING")
                    packet = self._envelope(facts, cursor, len(proven), previous)
                    self.queue._on_main(facts["tip"], tip)
                    require(self.queue._source_tip() == tip and self.recovery.expected_head == pin,
                            "REGISTRATION_STATE_MOVED")
                    # Persist a local no-resend fence before possible transport I/O.
                    # Only independently verified readback can resolve this fence.
                    import os
                    fence = self._attempt_path(previous)
                    with fence.open("xb") as attempt_file:
                        attempt_file.write(data({"head": pin, "packet": digest(data(packet))}))
                        attempt_file.flush()
                        os.fsync(attempt_file.fileno())
                    interrupt("before-registration-persistence")
                    saved = self.recovery.persist(self.scope, self._slot(facts), data(packet))
                    interrupt("after-registration-persistence")
                    observed, recovered, new_runs, _, _, observed_tip = self._read()
                    require(observed == saved and observed_tip == tip
                            and new_runs.get(run) == packet and recovered == proven + packet["rows"],
                            "REGISTRATION_READBACK")
                    proven = recovered
                db.executemany("INSERT INTO requests VALUES (?, ?, ?, ?, 'pending', NULL)",
                               proven[len(rows):])
                return "ALREADY_REGISTERED" if replay and len(rows) == len(proven) else "REGISTERED"
        except QueueStop:
            raise
        except Exception:
            raise QueueStop("REGISTRATION_UNRESOLVED") from None

