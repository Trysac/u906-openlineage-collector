"""Run every quality gate of this repository, in order, stopping at the first failure.

Usage: ``uv run python scripts/check.py`` (the same command runs locally, in the factory and in CI).

Gates: lint, format, types, security (Bandit, high severity), semgrep (Semgrep CE with the
factory's own rules in .semgrep/), collection of the CI-only conformance tests,
tests with branch coverage >= 80 %, documentation manifest, secrets, known vulnerabilities in
runtime dependencies (pip-audit) and in the lockfile (OSV-Scanner, high severity).
``check.py semgrep osv`` runs only the named gates.
Standard library only, no shell: it behaves the same on Windows and Linux.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Callable
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS_MANIFEST = ROOT / "docs-manifest.json"
ALLOW_MISSING_GITLEAKS = "FABRICA_ALLOW_MISSING_GITLEAKS"
ALLOW_OFFLINE_AUDIT = "FABRICA_ALLOW_OFFLINE_AUDIT"
# 1 = CI: also run the tests marked conformance, and a missing scanner fails
CI_TIER = "FABRICA_CI"
# The factory's own rules; never Semgrep-maintained ones (license).
SEMGREP_RULES = ".semgrep/factory-rules.yml"
OSV_LOCKFILE = "uv.lock"
HIGH_CVSS = 7.0
OSV_NO_PACKAGES = 128  # osv-scanner's exit code when the lockfile lists no package
GITLEAKS_HINT = (
    "gitleaks is not installed. Install it (https://github.com/gitleaks/gitleaks#installing), "
    "e.g. `winget install Gitleaks.Gitleaks`, `brew install gitleaks` or a release binary, "
    f"or set {ALLOW_MISSING_GITLEAKS}=1 to skip this gate locally."
)


def run(argv: list[str]) -> int:
    """Run one tool from the repository root and return its exit code."""
    return subprocess.run(argv, cwd=ROOT, check=False).returncode


def py_tool(module: str, *args: str) -> Callable[[], int]:
    """Run a Python tool through the current interpreter, so it works inside ``uv run``."""
    return lambda: run([sys.executable, "-m", module, *args])


def docs_errors(root: Path = ROOT) -> list[str]:
    """Return every violation of ``docs-manifest.json``; an empty list means the gate passes."""
    manifest = json.loads((root / "docs-manifest.json").read_text(encoding="utf-8"))
    errors: list[str] = []
    for rel in manifest.get("required", []):
        path = root / rel
        if not path.is_file():
            errors.append(f"missing required file: {rel}")
        elif not path.read_text(encoding="utf-8").strip():
            errors.append(f"required file is empty: {rel}")
    for pattern in manifest.get("required_globs", []):
        if not any(p.is_file() for p in root.glob(pattern)):
            errors.append(f"no file matches required pattern: {pattern}")
    for rel, limit in manifest.get("max_lines", {}).items():
        path = root / rel
        if path.is_file():
            count = len(path.read_text(encoding="utf-8").splitlines())
            if count > limit:
                errors.append(f"{rel} has {count} lines (max {limit})")
    for rel, needle in manifest.get("must_reference", {}).items():
        path = root / rel
        if path.is_file() and needle not in path.read_text(encoding="utf-8"):
            errors.append(f"{rel} must reference {needle}")
    return errors


def docs_gate() -> int:
    errors = docs_errors()
    for error in errors:
        print(f"docs: {error}", file=sys.stderr)
    if not errors:
        print("docs: every file in docs-manifest.json is present")
    return 1 if errors else 0


def secrets_gate() -> int:
    exe = shutil.which("gitleaks")
    if exe is None:
        if os.environ.get(ALLOW_MISSING_GITLEAKS) == "1":
            print("gitleaks: SKIPPED (not installed)")
            return 0
        print(f"gitleaks: FAIL - {GITLEAKS_HINT}", file=sys.stderr)
        return 1
    return run([exe, "dir", ".", "--redact", "--verbose", "--no-banner"])


def security_gate() -> int:
    """Static security scan of the library with Bandit; only high-severity findings fail.

    ``# nosec`` comments are ignored, so a finding is fixed in the code (for example
    ``hashlib.md5(data, usedforsecurity=False)``), never silenced. Ruff's ``S`` rules already
    report every severity during lint; Bandit adds the checks Ruff lacks (trojan-source
    characters, insecure imports such as pycrypto, unsafe model downloads).
    """
    return run(
        [
            sys.executable,
            "-m",
            "bandit",
            "--recursive",
            "--quiet",
            "--severity-level",
            "high",
            "--ignore-nosec",
            "src",
        ]
    )


def audit_gate() -> int:
    """Check runtime dependencies against the PyPI/OSV advisory databases with pip-audit.

    Runtime means what users install: the core dependencies and every extra (optional
    integration). Development dependencies (tests, benchmarks) are not audited.
    """
    if os.environ.get(ALLOW_OFFLINE_AUDIT) == "1":
        print("audit: SKIPPED (FABRICA_ALLOW_OFFLINE_AUDIT=1)")
        return 0
    uv = shutil.which("uv")
    if uv is None:
        print("audit: FAIL - uv is required to export the dependency list", file=sys.stderr)
        return 1
    export = subprocess.run(
        [
            uv,
            "export",
            "--no-dev",
            "--all-extras",
            "--no-hashes",
            "--no-emit-project",
            "--format",
            "requirements-txt",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if export.returncode != 0:
        print(export.stderr, file=sys.stderr)
        return export.returncode
    lines = [
        line
        for line in export.stdout.splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    # Factory repositories installed from Git at a tag cannot be pinned for pip-audit; they are
    # audited in their own repository, and their dependencies are still listed here as pins.
    pins = [line for line in lines if " @ git+" not in line]
    if len(pins) < len(lines):
        skipped = len(lines) - len(pins)
        print(f"audit: {skipped} factory repositories from Git skipped (audited in their repos)")
    if not pins:
        print("audit: no runtime dependencies")
        return 0
    with tempfile.TemporaryDirectory() as tmp:
        reqs = Path(tmp) / "requirements.txt"
        reqs.write_text("\n".join(pins) + "\n", encoding="utf-8")
        return run(
            [
                sys.executable,
                "-m",
                "pip_audit",
                "-r",
                str(reqs),
                "--disable-pip",
                "--no-deps",
                "--strict",
            ]
        )


def missing_scanner(tool: str) -> int:
    """A scanner that is not installed: skipped locally, a failure in CI (FABRICA_CI=1)."""
    if os.environ.get(CI_TIER) == "1":
        print(f"{tool}: FAIL - {tool} not installed (required when {CI_TIER}=1)", file=sys.stderr)
        return 1
    print(f"{tool}: skipped: {tool} not installed")
    return 0


def semgrep_gate() -> int:
    """Semgrep CE with the factory's own rules only; ERROR rules fail.

    ``nosemgrep`` comments are ignored, so a finding is fixed in the code, never silenced.
    """
    exe = shutil.which("semgrep")
    if exe is None:
        return missing_scanner("semgrep")
    return run(
        [
            exe,
            "scan",
            "--config",
            SEMGREP_RULES,
            "--error",
            "--severity",
            "ERROR",
            "--metrics",
            "off",
            "--disable-version-check",
            "--disable-nosem",
            "--quiet",
            "src",
        ]
    )


def osv_blocking(report: object) -> list[str]:
    """Advisories in an OSV-Scanner JSON report with CVSS >= 7.0 or no score (counts as high)."""
    out: list[str] = []
    results = report.get("results", []) if isinstance(report, dict) else []
    for result in results if isinstance(results, list) else []:
        for pkg in result.get("packages", []) if isinstance(result, dict) else []:
            info = pkg.get("package", {}) if isinstance(pkg, dict) else {}
            name = f"{info.get('name', '?')}@{info.get('version', '?')}"
            for group in pkg.get("groups", []) if isinstance(pkg, dict) else []:
                raw = str(group.get("max_severity", "") or "")
                try:
                    score: float | None = float(raw)
                except ValueError:
                    score = None
                if score is None or score >= HIGH_CVSS:
                    ids = ",".join(group.get("ids", [])) or "?"
                    out.append(f"{name} {ids} ({raw if score is not None else 'unknown'})")
    return out


def osv_gate() -> int:
    """Known vulnerabilities in every locked package (OSV-Scanner); high or unknown fails."""
    exe = shutil.which("osv-scanner")
    if exe is None:
        return missing_scanner("osv-scanner")
    lock = ROOT / OSV_LOCKFILE
    if not lock.is_file():
        if os.environ.get(CI_TIER) == "1":
            print(f"osv-scanner: FAIL - {OSV_LOCKFILE} not found (run uv lock)", file=sys.stderr)
            return 1
        print(f"osv-scanner: skipped: no {OSV_LOCKFILE} (run uv lock)")
        return 0
    argv = [exe, "scan", "source", "--format", "json", f"--lockfile={lock}"]
    res = subprocess.run(
        argv,
        cwd=ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if res.returncode in (0, OSV_NO_PACKAGES):
        print(f"osv-scanner: no known vulnerabilities in {OSV_LOCKFILE}")
        return 0
    if res.returncode != 1:
        print(res.stderr, file=sys.stderr)
        return res.returncode
    try:
        report = json.loads(res.stdout)
    except json.JSONDecodeError:
        print("osv-scanner: FAIL - unreadable JSON report", file=sys.stderr)
        return 1
    blocking = osv_blocking(report)
    for line in blocking:
        print(f"osv-scanner: {line}", file=sys.stderr)
    if blocking:
        return 1
    print("osv-scanner: only advisories below high severity (reported, not failing)")
    return 0


def pytest_args() -> list[str]:
    """Coverage-gated pytest. Tests marked ``conformance`` (external tools or downloads) run only
    when FABRICA_CI=1, which the CI conformance job sets; locally they are deselected."""
    args = [
        "--cov=openlineage_collector",
        "--cov-branch",
        "--cov-report=term-missing",
        "--cov-fail-under=80",
    ]
    if os.environ.get(CI_TIER) != "1":
        args += ["-m", "not conformance"]
    return args


CONFORMANCE_COLLECT = ["--collect-only", "-q", "-m", "conformance"]
NO_TESTS_COLLECTED = 5  # pytest's exit code when the selection is empty


def conformance_gate() -> int:
    """Collect the CI-only conformance tests without running them, so a tier that does not import
    fails here and not first in CI. Zero collected is fine. With FABRICA_CI=1 the tests gate runs
    the tier itself."""
    if os.environ.get(CI_TIER) == "1":
        print("conformance: CI tier, run by the tests gate")
        return 0
    code = run([sys.executable, "-m", "pytest", *CONFORMANCE_COLLECT])
    if code == NO_TESTS_COLLECTED:
        print("conformance: no CI-only tests")
        return 0
    return code


def tests_gate() -> int:
    if os.environ.get(CI_TIER) == "1":
        print("tests: CI tier, conformance tests included")
    return run([sys.executable, "-m", "pytest", *pytest_args()])


GATES: list[tuple[str, Callable[[], int]]] = [
    ("lint", py_tool("ruff", "check", ".")),
    ("format", py_tool("ruff", "format", "--check", ".")),
    ("types", py_tool("mypy", "--strict", "src")),
    ("security", security_gate),
    ("semgrep", semgrep_gate),
    ("conformance", conformance_gate),
    ("tests", tests_gate),
    ("docs", docs_gate),
    ("secrets", secrets_gate),
    ("audit", audit_gate),
    ("osv", osv_gate),
]


def main(argv: list[str] | None = None) -> int:
    """Run every gate, or only the gates named on the command line (in the standard order)."""
    names = sys.argv[1:] if argv is None else argv
    unknown = sorted(set(names) - {name for name, _ in GATES})
    if unknown:
        known = [name for name, _ in GATES]
        print(f"check: unknown gates {unknown}; known: {known}", file=sys.stderr)
        return 2
    for name, gate in GATES:
        if names and name not in names:
            continue
        print(f"==> {name}", flush=True)
        code = gate()
        if code != 0:
            print(f"==> {name}: FAILED (exit {code})", file=sys.stderr, flush=True)
            return code
        print(f"==> {name}: ok", flush=True)
    print("==> all gates passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
