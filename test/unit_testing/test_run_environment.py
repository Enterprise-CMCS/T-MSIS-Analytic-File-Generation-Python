"""
Unit tests for the run_environment parameter (selects the catalog TAF reads T-MSIS input from).

Pure unit tests: no Spark session or data access is needed. The one Spark call made while
constructing a runner (TAF_Metadata.getFormatsForValidationAndRecode) is stubbed out.

Run from the repo root:  pytest -m unit
"""
import importlib
import inspect
import os
import re
import sys

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# Make sure the repo's taf package is tested, not an installed taf wheel
sys.path.insert(0, REPO_ROOT)
for _name in [m for m in sys.modules if m == "taf" or m.startswith("taf.")]:
    del sys.modules[_name]

from taf import TAF_Source  # noqa: E402
from taf.TAF_Metadata import TAF_Metadata  # noqa: E402
from taf.TAF_Source import (  # noqa: E402
    DEFAULT_TMSIS_SCHEMA,
    RUN_ENVIRONMENT_CATALOGS,
    resolve_run_environment,
    resolve_tmsis_schema,
)

RUNNERS = {
    "APL": "taf.APL.APL_Runner",
    "APR": "taf.APR.APR_Runner",
    "BSF": "taf.BSF.BSF_Runner",
    "DE": "taf.DE.DE_Runner",
    "FTX": "taf.FTX.FTX_Runner",
    "IP": "taf.IP.IP_Runner",
    "LT": "taf.LT.LT_Runner",
    "MCP": "taf.MCP.MCP_Runner",
    "OT": "taf.OT.OT_Runner",
    "PRV": "taf.PRV.PRV_Runner",
    "RX": "taf.RX.RX_Runner",
    "UP": "taf.UP.UP_Runner",
}

# Positional arguments accepted by every runner before this change
LEGACY_ARGS = ("taf_python", "2022-02-28", "'05'", "'4863'", 99999, "P1")

EXPECTED_SCHEMA = {
    None: "dc_prod_data_sources_catalog.tmsis",
    "prod": "dc_prod_data_sources_catalog.tmsis",
    "val": "dc_val_tmsis_ce_test_data_catalog.tmsis",
}


def runner_class(file_type):
    module = importlib.import_module(RUNNERS[file_type])
    return getattr(module, f"{file_type}_Runner")


@pytest.fixture(autouse=True)
def no_spark_metadata(monkeypatch):
    """Runner construction calls Spark once to build format views; not needed for these tests."""
    monkeypatch.setattr(TAF_Metadata, "getFormatsForValidationAndRecode", staticmethod(lambda: None))


def test_taf_package_is_repo_source():
    import taf
    assert os.path.abspath(taf.__file__).startswith(REPO_ROOT)


# ---------------------------------------------------------------- TAF_Source

def test_default_matches_previous_hardcoded_source():
    assert DEFAULT_TMSIS_SCHEMA == "dc_prod_data_sources_catalog.tmsis"


def test_catalog_map():
    assert RUN_ENVIRONMENT_CATALOGS == {
        "prod": "dc_prod_data_sources_catalog",
        "val": "dc_val_tmsis_ce_test_data_catalog",
    }


def test_default_is_same_as_prod():
    assert resolve_tmsis_schema(None) == resolve_tmsis_schema("prod")


@pytest.mark.parametrize("value", [None, "", "   "])
def test_missing_value_means_default(value):
    assert resolve_run_environment(value) is None
    assert resolve_tmsis_schema(value) == DEFAULT_TMSIS_SCHEMA


@pytest.mark.parametrize("value,expected", [
    ("prod", "prod"), ("PROD", "prod"), (" Prod ", "prod"),
    ("val", "val"), ("VAL", "val"), (" Val\n", "val"),
])
def test_value_is_normalized(value, expected):
    assert resolve_run_environment(value) == expected


@pytest.mark.parametrize("value", ["dev", "production", "validation", "prod;drop", "dc_prod_data_sources_catalog", 1])
def test_invalid_value_raises(value):
    with pytest.raises(ValueError, match="Invalid run_environment"):
        resolve_run_environment(value)


@pytest.mark.parametrize("env", [None, "prod", "val"])
def test_resolve_tmsis_schema(env):
    assert resolve_tmsis_schema(env) == EXPECTED_SCHEMA[env]


def test_bad_configured_catalog_raises(monkeypatch):
    monkeypatch.setitem(TAF_Source.RUN_ENVIRONMENT_CATALOGS, "val", "bad-catalog; select 1")
    with pytest.raises(ValueError, match="not a valid identifier"):
        resolve_tmsis_schema("val")


# ---------------------------------------------------------------- runners

@pytest.mark.parametrize("file_type", sorted(RUNNERS))
def test_runner_signature_has_optional_run_environment(file_type):
    params = inspect.signature(runner_class(file_type).__init__).parameters
    assert list(params)[-1] == "run_environment"
    assert params["run_environment"].default is None


@pytest.mark.parametrize("env", [None, "", "prod", "val", "VAL"])
@pytest.mark.parametrize("file_type", sorted(RUNNERS))
def test_runner_sets_tmsis_schema(file_type, env):
    runner = runner_class(file_type)(*LEGACY_ARGS, 0, run_environment=env)
    key = resolve_run_environment(env)
    assert runner.RUN_ENVIRONMENT == key
    assert runner.TMSIS_SCHEMA == EXPECTED_SCHEMA[key]
    assert runner.DA_SCHEMA == "taf_python"  # output location is unaffected


@pytest.mark.parametrize("file_type", sorted(RUNNERS))
def test_runner_legacy_positional_call_unchanged(file_type):
    runner = runner_class(file_type)(*LEGACY_ARGS)
    assert runner.TMSIS_SCHEMA == DEFAULT_TMSIS_SCHEMA


@pytest.mark.parametrize("file_type", sorted(RUNNERS))
def test_runner_rejects_invalid_run_environment(file_type):
    with pytest.raises(ValueError, match="Invalid run_environment"):
        runner_class(file_type)(*LEGACY_ARGS, run_environment="dev")


@pytest.mark.parametrize("env", [None, "prod", "val"])
def test_ssn_ind_reads_selected_catalog(env):
    sql = runner_class("BSF")(*LEGACY_ARGS, run_environment=env).ssn_ind()
    assert f"from {EXPECTED_SCHEMA[env]}.tmsis_fhdr_rec_elgblty" in sql


@pytest.mark.parametrize("env", [None, "prod", "val"])
def test_fetch_combined_list_reads_selected_catalog(env, monkeypatch):
    captured = []

    class FakeSpark:
        def sql(self, query):
            captured.append(query)

            class Result:
                class rdd:
                    @staticmethod
                    def map(fn):
                        return []
            return Result()

    from pyspark.sql import SparkSession
    monkeypatch.setattr(SparkSession, "getActiveSession", staticmethod(lambda: FakeSpark()))
    runner_class("BSF")(*LEGACY_ARGS, run_environment=env).fetch_combined_list()
    assert len(captured) == 1
    assert f"{EXPECTED_SCHEMA[env]}.tmsis_fhdr_rec_elgblty" in captured[0]


# ---------------------------------------------------------------- source guards

def _taf_sources():
    for dirpath, _, files in os.walk(os.path.join(REPO_ROOT, "taf")):
        for f in files:
            if f.endswith(".py"):
                path = os.path.join(dirpath, f)
                with open(path) as fh:
                    yield os.path.relpath(path, REPO_ROOT), fh.read()


def test_no_hardcoded_source_catalog_outside_taf_source():
    """T-MSIS source catalogs must only be named in taf/TAF_Source.py."""
    catalogs = [DEFAULT_TMSIS_SCHEMA.split(".")[0], *RUN_ENVIRONMENT_CATALOGS.values()]
    offenders = [
        f"{path}:{i}"
        for path, text in _taf_sources() if not path.endswith("TAF_Source.py")
        for i, line in enumerate(text.splitlines(), 1)
        if not line.strip().startswith("#") and any(c in line.lower() for c in catalogs)
    ]
    assert offenders == []


def test_no_bare_tmsis_table_references():
    """No SQL reads 'tmsis.<table>' without a catalog (it would depend on the cluster default catalog)."""
    pattern = re.compile(r"(?:\bfrom|\bjoin|[\"'])\s*tmsis\.\w", re.IGNORECASE)
    offenders = [
        f"{path}:{i}"
        for path, text in _taf_sources()
        for i, line in enumerate(text.splitlines(), 1)
        if not line.strip().startswith("#") and pattern.search(line)
    ]
    assert offenders == []


@pytest.mark.parametrize("file_type", ["FTX", "IP", "LT", "OT", "RX"])
def test_runner_init_uses_resolved_schema(file_type):
    """These runners pass TMSIS_SCHEMA to their segment extractors from init()."""
    source = inspect.getsource(runner_class(file_type).init)
    assert "TMSIS_SCHEMA = self.TMSIS_SCHEMA" in source


@pytest.mark.parametrize("path,owner,count", [
    ("taf/BSF/ELG.py", "bsf", 2),
    ("taf/PRV/PRV03.py", "prv", 4),
    *[(f"taf/MCP/MCP0{n}.py", "mcp", 1) for n in range(1, 8)],
    *[(f"taf/PRV/PRV{n:02d}.py", "prv", 1) for n in (1, 2, 4, 5, 6, 7, 8, 9, 10)],
])
def test_segment_reads_use_runner_schema(path, owner, count):
    with open(os.path.join(REPO_ROOT, path)) as fh:
        text = fh.read()
    assert text.count(f"{{self.{owner}.TMSIS_SCHEMA}}.") == count
