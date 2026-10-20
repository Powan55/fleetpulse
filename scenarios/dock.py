"""Docker calls with fixed argv lists and no shell. Targets are resolved by compose project label and a service allowlist."""
import json
import os
import re
import subprocess

from lib import Inconclusive

PROJECT = os.environ.get("COMPOSE_PROJECT_NAME", "fleetpulse")
SERVICES = frozenset({"simulator", "sink", "processor", "api", "postgres", "kafka"})
CONTROL = frozenset({"kill", "start", "stop", "restart"})
_ID = re.compile(r"^[0-9a-f]{12,64}$")


def _run(argv, timeout=60):
    try:
        return subprocess.run(argv, capture_output=True, text=True, timeout=timeout, shell=False)
    except (OSError, subprocess.TimeoutExpired) as e:
        raise Inconclusive(f"docker unavailable: {e}")


def container(service):
    if service not in SERVICES:
        raise ValueError(f"service not allowed: {service!r}")
    r = _run(["docker", "ps", "-a", "--no-trunc", "--filter", f"label=com.docker.compose.project={PROJECT}",
              "--filter", f"label=com.docker.compose.service={service}", "--format", "{{.ID}}"])
    ids = r.stdout.split()
    if r.returncode or len(ids) != 1 or not _ID.match(ids[0]):
        raise Inconclusive(f"no single container for service {service} in project {PROJECT}: rc={r.returncode} {r.stderr.strip()[:200]}")
    return ids[0]


def control(service, verb):
    """kill uses SIGKILL. Returns after docker returns; raises Inconclusive on failure."""
    if verb not in CONTROL:
        raise ValueError(f"docker verb not allowed: {verb!r}")
    argv = ["docker", verb] + (["-s", "KILL"] if verb == "kill" else []) + [container(service)]
    r = _run(argv)
    if r.returncode:
        raise Inconclusive(f"docker {verb} {service}: {r.stderr.strip()[:200]}")


def inspect(service):
    """{'running': bool, 'started_at': str, 'restarts': int, 'exit_code': int, 'env': {k: v}}"""
    r = _run(["docker", "inspect", container(service)])
    if r.returncode:
        raise Inconclusive(f"docker inspect {service}: {r.stderr.strip()[:200]}")
    d = json.loads(r.stdout)[0]
    env = dict(e.split("=", 1) for e in d["Config"].get("Env") or [] if "=" in e)
    return {"running": d["State"]["Running"], "started_at": d["State"]["StartedAt"], "restarts": d["RestartCount"],
            "exit_code": d["State"]["ExitCode"], "env": env}
