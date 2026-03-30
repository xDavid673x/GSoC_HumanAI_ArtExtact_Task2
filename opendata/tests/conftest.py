"""Shared fixtures for the test suite."""
import os
import subprocess
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

CONF_FILE = "/usr/local/nga/etc/tmspublicextract.conf"


def _read_conf():
    """Read connection settings from the public extract conf file."""
    conf = {}
    try:
        with open(CONF_FILE) as f:
            for line in f:
                line = line.strip()
                if "=" in line and not line.startswith("#"):
                    key, val = line.split("=", 1)
                    conf[key.strip()] = val.strip().strip('"')
    except (IOError, PermissionError):
        return conf
    return conf


def _kinit_service_account():
    """Get a Kerberos ticket for the service account using a private ccache."""
    conf = _read_conf()
    user = conf.get("tmspublicextract_username")
    password = conf.get("tmspublicextract_password")
    if not user or not password:
        return None
    ccache = os.path.join(tempfile.gettempdir(), "krb5cc_opendata_test")
    principal = "{}@NGA.GOV".format(user)
    proc = subprocess.run(
        ["kinit", principal],
        input=password + "\n",
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        universal_newlines=True,
        env=dict(os.environ, KRB5CCNAME="FILE:" + ccache),
    )
    if proc.returncode != 0:
        return None
    return ccache


def pytest_addoption(parser):
    conf = _read_conf()
    parser.addoption("--server", default=os.environ.get(
        "OPENDATA_SERVER", conf.get("tmspublicextract_server", "ap-tmstst-db")))
    parser.addoption("--database", default=os.environ.get(
        "OPENDATA_DATABASE", conf.get("tmspublicextract_database", "TMSPublicExtract")))


@pytest.fixture(scope="session")
def db(request):
    """Connect via Kerberos using service account credentials from conf file."""
    try:
        import extract_opendata as ext
    except ImportError:
        pytest.skip("extract_opendata not importable")

    # Get a service account Kerberos ticket (separate ccache, won't touch user's ticket)
    ccache = _kinit_service_account()
    if ccache:
        os.environ["KRB5CCNAME"] = "FILE:" + ccache
    # else: fall back to whatever ticket is already available

    server = request.config.getoption("--server")
    database = request.config.getoption("--database")
    try:
        conn = ext.connect(server, database)
    except Exception as e:
        pytest.skip("Cannot connect to {}/{}: {}".format(server, database, e))
    yield conn
    conn.close()
