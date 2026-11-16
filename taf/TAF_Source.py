"""
Resolve where TAF reads T-MSIS input data from.

The run_environment parameter selects the catalog for T-MSIS input tables.
It does not change where TAF output is written (that is da_schema).

    run_environment = None / ""  -> DEFAULT_TMSIS_SCHEMA (same as prod; unchanged behavior)
    run_environment = "prod"     -> "<prod catalog>.tmsis"
    run_environment = "val"      -> "<val catalog>.tmsis"
"""
import re

# prod: production T-MSIS data
# val:  non-production T-MSIS test data (full tmsis schema, incl. community engagement)
RUN_ENVIRONMENT_CATALOGS = {
    "prod": "dc_prod_data_sources_catalog",
    "val": "dc_val_tmsis_ce_test_data_catalog",
}

TMSIS_SCHEMA_NAME = "tmsis"

# Used when run_environment is not supplied: same as prod (the behavior before this parameter existed)
DEFAULT_TMSIS_SCHEMA = f"{RUN_ENVIRONMENT_CATALOGS['prod']}.{TMSIS_SCHEMA_NAME}"

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def resolve_run_environment(run_environment=None):
    """Return the normalized run_environment ('prod'/'val'), or None when not supplied."""
    if run_environment is None:
        return None
    env = str(run_environment).strip().lower()
    if env == "":
        return None
    if env not in RUN_ENVIRONMENT_CATALOGS:
        raise ValueError(
            f"Invalid run_environment '{run_environment}'. "
            f"Must be one of: {sorted(RUN_ENVIRONMENT_CATALOGS)} (or empty for the default)."
        )
    return env


def resolve_tmsis_schema(run_environment=None):
    """Return the '<catalog>.<schema>' prefix used to read T-MSIS input tables."""
    env = resolve_run_environment(run_environment)
    if env is None:
        return DEFAULT_TMSIS_SCHEMA
    catalog = RUN_ENVIRONMENT_CATALOGS[env]
    if not _IDENTIFIER.match(catalog):
        raise ValueError(f"Configured catalog '{catalog}' for run_environment '{env}' is not a valid identifier.")
    return f"{catalog}.{TMSIS_SCHEMA_NAME}"
