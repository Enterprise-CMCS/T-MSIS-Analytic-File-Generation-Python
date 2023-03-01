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
                    "nuid": "5389bd9c-44d1-44fe-be4f-7b48d633f04e",
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
                    "nuid": "84219b20-e4df-4f78-9ccc-71c301609b73",
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
                    "nuid": "a9549427-95ff-457d-835a-fe2df1463332",
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
                    "nuid": "f87a1ae2-af92-48b4-9b4c-d192691972b7",
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
                    "nuid": "80168bf3-a269-45e8-b508-f18c250feb37",
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
            "notebookOrigID": 606657,
        }
    },
    "nbformat": 4,
    "nbformat_minor": 0,
}
