{
    "cells": [
        {
            "cell_type": "code",
            "source": ["from taf.MCP.MCP_Runner import MCP_Runner"],
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
            "source": ["from taf.MCP.MCP_Metadata import MCP_Metadata"],
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
                'mcp = MCP_Runner(reporting_period = dbutils.widgets.get("reporting_period")\n                ,state_code       = dbutils.widgets.get("state_code")\n                ,run_id           = dbutils.widgets.get("run_id")\n                ,job_id           = dbutils.widgets.get("job_id"))'
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
            "source": ['mcp.job_control_wrt("MCP")'],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "cbe76768-6cd4-44e8-a706-ab06162a1fff",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": ["mcp.job_control_updt()"],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "5a67d274-1196-45da-876a-127b04abe4ea",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": ["mcp.print()"],
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
            "source": ["mcp.init()"],
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
            "source": ["mcp.run()   "],
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
            "source": ["mcp.view_plan()"],
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
            "source": ["display(mcp.audit())"],
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
            "source": ["mcp.job_control_updt2()"],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "7ba247e8-c895-4092-bdc9-ace0bff34b97",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": [
                'TABLE_NAME = "TAF_MCP"\nFIL_4TH_NODE = "MCP"\n \nmcp.get_cnt(TABLE_NAME)\nmcp.getcounts("101_mc_build.sas", "base_MCP")\nmcp.create_meta_info(TABLE_NAME, FIL_4TH_NODE)\nmcp.create_eftsmeta_info(TABLE_NAME, "101_mc_build.sas", "base_MCP", "MC02_Base", "submtg_state_cd")\nmcp.file_contents(TABLE_NAME)'
            ],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "8f98f62f-b125-42c0-9618-16863e912ca9",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": [
                'TABLE_NAME = "TAF_MCL"\nFIL_4TH_NODE = "MCL"\n\nmcp.get_cnt(TABLE_NAME)\nmcp.getcounts("101_mc_build.sas", "constructed_MC03")\nmcp.create_meta_info(TABLE_NAME, FIL_4TH_NODE)\nmcp.create_eftsmeta_info(TABLE_NAME, "101_mc_build.sas", "constructed_MC03", "MC03_Location_CNST", "submtg_state_cd")\nmcp.file_contents(TABLE_NAME)'
            ],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "83d4291a-ad8e-45fd-be13-1f3d2876dbc8",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": [
                'TABLE_NAME = "TAF_MCS"\nFIL_4TH_NODE = "MCS"\n\nmcp.get_cnt(TABLE_NAME)\nmcp.getcounts("101_mc_build.sas", "constructed_MC04")\nmcp.create_meta_info(TABLE_NAME, FIL_4TH_NODE)\nmcp.create_eftsmeta_info(TABLE_NAME, "101_mc_build.sas", "constructed_MC04", "MC04_Service_Area_CNST", "submtg_state_cd")\nmcp.file_contents(TABLE_NAME)'
            ],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "841d0060-ff82-4925-9996-9c931de0bd2c",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": [
                'TABLE_NAME = "TAF_MCE"\nFIL_4TH_NODE = "MCE"\n\nmcp.get_cnt(TABLE_NAME)\nmcp.getcounts("101_mc_build.sas", "segment_MC06")\nmcp.create_meta_info(TABLE_NAME, FIL_4TH_NODE)\nmcp.create_eftsmeta_info(TABLE_NAME, "101_mc_build.sas", "segment_MC06", "MC06_Population", "submtg_state_cd")\nmcp.file_contents(TABLE_NAME)'
            ],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "41e0abbd-e1cd-4b3f-b6f0-1a19c11ff08b",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
    ],
    "metadata": {
        "application/vnd.databricks.v1+notebook": {
            "notebookName": "MCP Runner",
            "dashboards": [],
            "notebookMetadata": {"pythonIndentUnit": 2},
            "language": "python",
            "widgets": {},
            "notebookOrigID": 434298,
        }
    },
    "nbformat": 4,
    "nbformat_minor": 0,
}
