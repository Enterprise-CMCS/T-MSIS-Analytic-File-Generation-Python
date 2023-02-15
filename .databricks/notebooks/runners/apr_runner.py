{
    "cells": [
        {
            "cell_type": "code",
            "source": ["from taf.APR.APR_Runner import APR_Runner"],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "46e19358-fa3f-43d5-978a-243538967af2",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": [
                'apr = APR_Runner(reporting_period = dbutils.widgets.get("reporting_period")\n                ,state_code       = dbutils.widgets.get("state_code")\n                ,run_id           = dbutils.widgets.get("run_id")\n                ,job_id           = dbutils.widgets.get("job_id"))'
            ],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "a54e3b43-8963-4447-a45c-31f4393b70d7",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": ['apr.job_control_wrt("APR")'],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "f25f2f16-0d39-40a1-9e8a-5f509132546c",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": ["apr.job_control_updt()"],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "1c9b4e2b-11ab-4cde-b052-75f775b0b3f2",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": ["apr.print()"],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "8a9b1eb3-8262-477f-be95-3bf3caa75ff0",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": ["apr.init()"],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "3a172a92-ea4b-44df-a7f2-bb3eb2a36799",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": ["apr.run()"],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "20fb2cdf-df1a-4bbf-81d4-c96aa0cd6403",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": ["apr.view_plan()"],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "7dd2e6a6-5b1d-4692-907f-373f76e837e4",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": ["display(apr.audit())"],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "16b9e3be-75d9-4987-8ce5-2ba06206969d",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": [
                '#TABLE_NAME = "LCTN"\n#FIL_4TH_NODE = "LCP"\n \n#de.get_ann_count(TABLE_NAME)\n#de.create_meta_info(TABLE_NAME, FIL_4TH_NODE)\n#de.create_efts_metadata()'
            ],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "998adf4d-724f-4a5e-9631-d93aebe905ae",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": [
                '#TABLE_NAME = "LCNS"\n#FIL_4TH_NODE = "LIC"\n \n#de.get_ann_count(TABLE_NAME)\n#de.create_meta_info(TABLE_NAME, FIL_4TH_NODE)\n#de.create_efts_metadata()'
            ],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "c522bb34-6485-46ad-b73b-bdc7113a3a83",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": [
                '#TABLE_NAME = "ID"\n#FIL_4TH_NODE = "IDP"\n \n#de.get_ann_count(TABLE_NAME)\n#de.create_meta_info(TABLE_NAME, FIL_4TH_NODE)\n#de.create_efts_metadata()'
            ],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "303e0f31-e8eb-488f-af36-a57534418381",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": [
                '#TABLE_NAME = "TXNMY"\n#FIL_4TH_NODE = "TAX"\n \n#de.get_ann_count(TABLE_NAME)\n#de.create_meta_info(TABLE_NAME, FIL_4TH_NODE)\n#de.create_efts_metadata()'
            ],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "4f2050ec-35d0-435c-8ec3-f6c4a9b98fff",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": [
                '#TABLE_NAME = "ENRLMT"\n#FIL_4TH_NODE = "ENP"\n \n#de.get_ann_count(TABLE_NAME)\n#de.create_meta_info(TABLE_NAME, FIL_4TH_NODE)\n#de.create_efts_metadata()'
            ],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "dfae9300-00ed-49af-8847-8e2d42ed91e4",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": [
                '#TABLE_NAME = "GRP"\n#FIL_4TH_NODE = "GRP"\n \n#de.get_ann_count(TABLE_NAME)\n#de.create_meta_info(TABLE_NAME, FIL_4TH_NODE)\n#de.create_efts_metadata()'
            ],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "06eff76c-3787-4896-97fe-3a0e3bfd0c1b",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": [
                '#TABLE_NAME = "PGM"\n#FIL_4TH_NODE = "PGM"\n \n#de.get_ann_count(TABLE_NAME)\n#de.create_meta_info(TABLE_NAME, FIL_4TH_NODE)\n#de.create_efts_metadata()'
            ],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "21707b45-3509-4b12-a2fc-7a5d57c68467",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": [
                '#TABLE_NAME = "BED"\n#FIL_4TH_NODE = "BED"\n \n#de.get_ann_count(TABLE_NAME)\n#de.create_meta_info(TABLE_NAME, FIL_4TH_NODE)\n#de.create_efts_metadata()'
            ],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "b7b60590-a8f6-4111-8a9c-e530da5fdadb",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": [
                '#TABLE_NAME = "BASE"\n#FIL_4TH_NODE = "BSP"\n \n#de.get_ann_count(TABLE_NAME)\n#de.create_meta_info(TABLE_NAME, FIL_4TH_NODE)\n#de.create_efts_metadata()'
            ],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "50561b0b-22dc-40ea-b579-84c6808811ef",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": ["apr.job_control_updt2()"],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "426b4689-5d09-4866-bd2e-52b4c79ddc0a",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
    ],
    "metadata": {
        "application/vnd.databricks.v1+notebook": {
            "notebookName": "APR Runner",
            "dashboards": [],
            "notebookMetadata": {"pythonIndentUnit": 2},
            "language": "python",
            "widgets": {},
            "notebookOrigID": 433378,
        }
    },
    "nbformat": 4,
    "nbformat_minor": 0,
}
