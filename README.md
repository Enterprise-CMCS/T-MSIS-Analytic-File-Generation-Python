# CMS T-MSIS Analytic File (TAF) Generation Code

This project aims to provide transparency to state Medicaid agencies and other stakeholders who are interested in the logic and processes that are used to create CMS’ interim T-MSIS Analytic Files (TAF). These new TAF data sets exist alongside T-MSIS and serve as an alternate data source tailored to meet the broad research needs of the Medicaid and CHIP data user community.

Background information about the TAF can be found on Medicaid.Gov at this link:
https://www.medicaid.gov/medicaid/data-systems/macbis/medicaid-chip-research-files/transformed-medicaid-statistical-information-system-t-msis-analytic-files-taf/index.html


### Python Implementation of T-MSIS Analytic File (TAF) Generation for Databricks

This is a Python Library for the generation of the T-MSIS Analytic File (TAF) with distributed computing framework using Databricks.  File type(s) may be independently run within Notebooks, allowing them to be grouped into parallel processes based on state, data dependency, time interval, and T-MSIS run identifier(s).  Each process can be calibrated to optimally meet demand and deliverables. Custom Python libraries will be created to facilitate consistent management and execution of processes as well as simplify the creation of new analyses. This design is ideal for imposing best practices amongst distributed services which are appropriately granted resources and permit focus on test-driven development.

- [ ] Increment the library version number(s)
- [ ] Build the library
- [ ] Upload the WHL file to the Databricks environment
- [ ] Deploy the library to the Databricks cluster


## Increment the library version number(s)

The library version is included the source code. It can be updated in ```_init_()``` method of the TAF module.
```pytion
    __version__ = "7.1.16"    # deployed library version
```


## Build the library

The TAF Python Library is deployed as distributable WHL ("Wheel") file. WHL files are built using [_setuptools_](https://pypi.org/project/setuptools/)

If not done so already, run these commands to create and set up your local virtual environment:

1. > ```python -m venv .venv```
2. > ```.venv/Scripts/Activate.ps1```
3. > ```python -m pip install --upgrade pip```
4. > ```python -m pip install -r requirements.txt```

From the top level folder, run these commands:

5. > ```rm -r -fo .\build; rm -r -fo .\*.egg-info``` (only if you have created a wheel file before)
6. > ```python setup.py bdist_wheel```


### Upload the WHL file to the Databricks environment

This step uses the Databricks command-line interface (CLI) to interface with the Databricks platform. After installing the CLI, there's a manual step (depdendent on your operating system (OS)) to set up [authentication](https://docs.databricks.com/dev-tools/cli/index.html). Windows users may need to add the ```insecure = True``` option to their profile entries stored in the file ```~/.databrickscfg```.

7. > ```databricks --profile val fs cp ./dist/ dbfs:/FileStore/shared_uploads/TAF/lib/ --recursive --overwrite```


## Deploy the library to the Databricks cluster

Deploy the library WHL file to Databricks clusters using [these](https://docs.databricks.com/libraries/cluster-libraries.html) instructions where applicable. Once the library WHL file is saved to DBFS, update any job definitions to install the library WHL file to any job-based clusters at run time.


## More technical documentation

Supplementary information regarding the data quality of state T-MSIS Analytic Files (TAF) Research Identifiable Files (RIF) can be referenced [here](https://www.medicaid.gov/dq-atlas/welcome).

## Contributing

We would be happy to receive suggestions on how to fix bugs or make improvements, though we will not support changes made through this repository. Instead, please send your suggestions to [MACBISData@cms.hhs.gov](mailto:MACBISData@cms.hhs.gov).

## Public domain

This project is in the worldwide [public domain](https://github.com/Enterprise-CMCS/T-MSIS-Analytic-File-Generation-Python/blob/develop/LICENSE).

This project is in the public domain within the United States, and copyright and related rights in the work worldwide are waived through the [CC0 1.0 Universal public domain dedication](https://creativecommons.org/publicdomain/zero/1.0/).
