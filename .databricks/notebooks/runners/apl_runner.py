{
    "cells": [
        {
            "cell_type": "code",
            "source": ["from taf.APL.APL_Runner import APL_Runner"],
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
            "source": [
                'apl = APL_Runner(reporting_period = dbutils.widgets.get("reporting_period")\n                ,state_code       = dbutils.widgets.get("state_code")\n                ,run_id           = dbutils.widgets.get("run_id")\n                ,job_id           = dbutils.widgets.get("job_id"))'
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
            "source": ['apl.job_control_wrt("APL")'],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "43a96c83-be9d-478c-8c7b-70bf1652a195",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": ["apl.job_control_updt()"],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "3cadf350-9e5e-4bd3-839a-1f4787d2181c",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": ["apl.print()"],
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
            "source": ["apl.init()"],
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
            "source": ["apl.run()   "],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "dee6fd15-e40a-4939-953f-475177eb2811",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": ["apl.view_plan()"],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "ab9429e8-0009-4631-87b1-d3bf010e31fb",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": ["display(apl.audit())"],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "5c850aba-15ac-4fde-88ef-9fa444b8747c",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": [
                '#TABLE_NAME = "LCTN"\n#FIL_4TH_NODE = "LCM"\n \n#de.get_ann_count(TABLE_NAME)\n#de.create_meta_info(TABLE_NAME, FIL_4TH_NODE)\n#de.create_efts_metadata()'
            ],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "4fb0e9d9-b1e2-441b-a503-7ef9561bd4bd",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": [
                '#TABLE_NAME = "SAREA"\n#FIL_4TH_NODE = "SAM"\n \n#de.get_ann_count(TABLE_NAME)\n#de.create_meta_info(TABLE_NAME, FIL_4TH_NODE)\n#de.create_efts_metadata()'
            ],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "865d487e-b602-412c-8f50-9e6c989919f2",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": [
                '#TABLE_NAME = "ENRLMT"\n#FIL_4TH_NODE = "EPM"\n \n#de.get_ann_count(TABLE_NAME)\n#de.create_meta_info(TABLE_NAME, FIL_4TH_NODE)\n#de.create_efts_metadata()'
            ],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "35b3f4ba-8d68-42a5-b4c2-7aa92d52c1fd",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": [
                '#TABLE_NAME = "OA"\n#FIL_4TH_NODE = "OAM"\n \n#de.get_ann_count(TABLE_NAME)\n#de.create_meta_info(TABLE_NAME, FIL_4TH_NODE)\n#de.create_efts_metadata()'
            ],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "38c52914-564e-4f90-ac7a-c33e999cad64",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": [
                '#TABLE_NAME = "BASE"\n#FIL_4TH_NODE = "BSM"\n \n#de.get_ann_count(TABLE_NAME)\n#de.create_meta_info(TABLE_NAME, FIL_4TH_NODE)\n#de.create_efts_metadata()'
            ],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "07ae4d5b-d1cf-4bea-b5db-ded7cd973221",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": ["apl.job_control_updt2()"],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "fcc77258-41b6-4640-83e0-07c7fbd9e814",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
    ],
    "metadata": {
        "application/vnd.databricks.v1+notebook": {
            "notebookName": "APL Runner",
            "dashboards": [],
            "notebookMetadata": {"pythonIndentUnit": 2},
            "language": "python",
            "widgets": {},
            "notebookOrigID": 437000,
        }
    },
    "nbformat": 4,
    "nbformat_minor": 0,
}
