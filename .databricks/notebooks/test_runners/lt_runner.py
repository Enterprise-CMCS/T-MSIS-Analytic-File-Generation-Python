{
    "cells": [
        {
            "cell_type": "code",
            "source": ["from taf.LT.LT_Runner import LT_Runner"],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "d48635fe-da1e-4651-a3df-fef860a67b37",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": ["from taf.LT.LT_Metadata import LT_Metadata"],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "0bdb1278-48c8-4402-8983-bbb7b352247c",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": [
                'ltr = LT_Runner(reporting_period = dbutils.widgets.get("reporting_period")\n               ,state_code       = dbutils.widgets.get("state_code")\n               ,run_id           = dbutils.widgets.get("run_id")\n               ,job_id           = dbutils.widgets.get("job_id"))\n'
            ],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "98f85947-1b54-4cf1-a852-46fc34f948d6",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": ['ltr.job_control_wrt("TLT")'],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "7c1dc6dc-ce41-460d-8f86-8287a0f1ea61",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": ["ltr.job_control_updt()"],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "17f887b7-e6a7-4d97-af49-b642cae5189c",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": ["ltr.print()"],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "b6972483-0001-40d1-86c6-33c0f2f5dc18",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": ["ltr.init()"],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "06ffa6d7-d11d-4911-8652-40912892c5f3",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": ["ltr.run()"],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "21da3815-acbe-4a9b-bc26-54bdadf89720",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": ["ltr.view_plan()"],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "def1c74a-d681-4451-8dba-0f126bf872e6",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": ["display(ltr.audit())"],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "7e28d403-d71e-4402-9a8e-1592c84ae464",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": ["ltr.job_control_updt2()"],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "e83e5229-8ce1-4d51-a133-1cde2ebe0d11",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": [
                'TABLE_NAME = "TAF_LTH"\nFIL_4TH_NODE = "LTH"\n \nltr.get_cnt(TABLE_NAME)\nltr.getcounts("AWS_LT_MACROS", "1.1 AWS_Extract_Line_LT")\nltr.create_meta_info(TABLE_NAME, FIL_4TH_NODE)\nltr.create_eftsmeta_info(TABLE_NAME, "AWS_LT_MACROS", "1.1 AWS_Extract_Line_LT", "LT_HEADER", "new_submtg_state_cd")\nltr.file_contents(TABLE_NAME)'
            ],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "36ba8e1d-3803-40d7-b344-91f671b5d42d",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": [
                'TABLE_NAME = "TAF_LTL"\nFIL_4TH_NODE = "LTL"\n \nltr.get_cnt(TABLE_NAME)\nltr.getcounts("AWS_LT_MACROS", "1.1 AWS_Extract_Line_LT")\nltr.create_meta_info(TABLE_NAME, FIL_4TH_NODE)\nltr.create_eftsmeta_info(TABLE_NAME, "AWS_LT_MACROS", "1.1 AWS_Extract_Line_LT", "LT_LINE", "new_submtg_state_cd_line")\nltr.file_contents(TABLE_NAME)'
            ],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "855133bb-d147-4068-8805-52e5fc3d9a1e",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
    ],
    "metadata": {
        "application/vnd.databricks.v1+notebook": {
            "notebookName": "LT Runner",
            "dashboards": [],
            "notebookMetadata": {"pythonIndentUnit": 2},
            "language": "python",
            "widgets": {},
            "notebookOrigID": 606628,
        }
    },
    "nbformat": 4,
    "nbformat_minor": 0,
}
