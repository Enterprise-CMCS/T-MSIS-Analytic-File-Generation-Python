{
    "cells": [
        {
            "cell_type": "code",
            "source": ["from taf.DE.DE_Runner import DE_Runner"],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "38c2ad3b-c18d-4494-9710-d28bc2994e96",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": [
                'de = DE_Runner(reporting_period = dbutils.widgets.get("reporting_period")\n              ,state_code       = dbutils.widgets.get("state_code")\n              ,run_id           = dbutils.widgets.get("run_id")\n              ,job_id           = dbutils.widgets.get("job_id"))'
            ],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "45dd07a1-3aba-433b-8dab-4a9ba55fb3d5",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": ['de.job_control_wrt("ADE")'],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "7781959e-2ef9-4844-a12c-d50ff2005401",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": ["de.job_control_updt()"],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "8176d4fa-18fc-488b-b5d3-fb9d7fdc3430",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": ["de.print()"],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "a52d0531-5785-493c-8dcc-4951f6ec6618",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": ["de.init()"],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "27583994-db23-47bb-ac50-a59678dfa017",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": ["de.run()"],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "28c5eb24-72d9-4b2f-b152-3a1339de7c0e",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": ["de.view_plan()"],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "fece35cc-84c4-4ebf-aa04-2be45b081fc3",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": ["display(de.audit())"],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "c08fc73e-c2e0-403a-83ae-fb166d307f9a",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": [
                '#TABLE_NAME = "ELDTS"\n#FIL_4TH_NODE = "DTS"\n \n#de.get_ann_count(TABLE_NAME)\n#de.create_meta_info(TABLE_NAME, FIL_4TH_NODE)\n#de.create_efts_metadata()'
            ],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "781f9cb5-30f8-49eb-aadf-18748f2826e2",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": [
                '#TABLE_NAME = "CNTCT_DTLS"\n#FIL_4TH_NODE = "ADR"\n \n#de.get_ann_count(TABLE_NAME)\n#de.create_meta_info(TABLE_NAME, FIL_4TH_NODE)\n#de.create_efts_metadata()'
            ],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "09ec73fc-fcd7-492f-98ff-06bf7fceebba",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": [
                '#TABLE_NAME = "MC"\n#FIL_4TH_NODE = "MCR"\n \n#de.get_ann_count(TABLE_NAME)\n#de.create_meta_info(TABLE_NAME, FIL_4TH_NODE)\n#de.create_efts_metadata()'
            ],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "1ee31714-dcfd-4eaf-9d0c-503bed1eda80",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": [
                '#TABLE_NAME = "WVR"\n#FIL_4TH_NODE = "WVR"\n \n#de.get_ann_count(TABLE_NAME)\n#de.create_meta_info(TABLE_NAME, FIL_4TH_NODE)\n#de.create_efts_metadata()'
            ],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "6460d3dd-98a0-489e-a69c-c50f6e2f237f",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": [
                '#TABLE_NAME = "MFP"\n#FIL_4TH_NODE = "MFP"\n \n#de.get_ann_count(TABLE_NAME)\n#de.create_meta_info(TABLE_NAME, FIL_4TH_NODE)\n#de.create_efts_metadata()'
            ],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "4c17a687-4e3a-4b79-8d9d-113696253197",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": [
                '#TABLE_NAME = "HHSPO"\n#FIL_4TH_NODE = "HHSPO"\n \n#de.get_ann_count(TABLE_NAME)\n#de.create_meta_info(TABLE_NAME, FIL_4TH_NODE)\n#de.create_efts_metadata()'
            ],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "ccf692ab-01a0-4c18-b02a-1f82466feccb",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": [
                '#TABLE_NAME = "DSBLTY"\n#FIL_4TH_NODE = "DSB"\n \n#de.get_ann_count(TABLE_NAME)\n#de.create_meta_info(TABLE_NAME, FIL_4TH_NODE)\n#de.create_efts_metadata()'
            ],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "d6c9404c-5eac-43c2-b981-411cf375fdab",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": [
                '#TABLE_NAME = "BASE"\n#FIL_4TH_NODE = "BSE"\n \n#de.get_ann_count(TABLE_NAME)\n#de.create_meta_info(TABLE_NAME, FIL_4TH_NODE)\n#de.create_efts_metadata()'
            ],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "053cfb3e-67d9-42b5-9126-34a38f3c3db1",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": ["de.job_control_updt2()"],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "4716ae80-657b-452f-a723-9724730be530",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
    ],
    "metadata": {
        "application/vnd.databricks.v1+notebook": {
            "notebookName": "DE Runner",
            "dashboards": [],
            "notebookMetadata": {"pythonIndentUnit": 2},
            "language": "python",
            "widgets": {},
            "notebookOrigID": 606755,
        }
    },
    "nbformat": 4,
    "nbformat_minor": 0,
}
