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
                    "nuid": "341cfc9d-5bc2-4d9b-a7a4-76da20455482",
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
                    "nuid": "f230658c-78b0-466e-8bc7-17af0b4ab40d",
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
                    "nuid": "7c623803-626d-4652-9579-3bf9ed72b263",
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
                    "nuid": "7514b726-1d9a-4683-9ae9-d0217ceef300",
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
                    "nuid": "1b7f27fd-2cb0-423b-ac52-bfed9cbf8a6d",
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
                    "nuid": "ff1d9252-9e35-4d8f-807d-e0551d6b5950",
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
                    "nuid": "d60888fd-7ab0-4ebd-90b9-499ce719e321",
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
                    "nuid": "7b454aae-4b0f-4575-bb6b-77178529f69a",
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
                    "nuid": "cacfab74-8663-44a3-b7eb-07a685939cb9",
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
            "notebookOrigID": 606744,
        }
    },
    "nbformat": 4,
    "nbformat_minor": 0,
}
