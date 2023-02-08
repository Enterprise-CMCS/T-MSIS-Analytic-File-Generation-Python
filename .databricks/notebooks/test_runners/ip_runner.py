{
    "cells": [
        {
            "cell_type": "code",
            "source": ["from taf.IP.IP_Runner import IP_Runner"],
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
            "source": ["from taf.IP.IP_Metadata import IP_Metadata"],
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
                'ipr = IP_Runner(reporting_period = dbutils.widgets.get("reporting_period")\n               ,state_code       = dbutils.widgets.get("state_code")\n               ,run_id           = dbutils.widgets.get("run_id")\n               ,job_id           = dbutils.widgets.get("job_id"))'
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
            "source": ['ipr.job_control_wrt("TIP")'],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "5e9d7db2-1fcd-47c9-b301-dcf4b29f16c0",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": ["ipr.job_control_updt()"],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "d51a46a1-8904-4dd5-91d0-6dda2a621919",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": ["ipr.print()"],
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
            "source": ["ipr.init()"],
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
            "source": ["ipr.run()"],
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
            "source": ["ipr.view_plan()"],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "adeed909-62b1-43ff-a1f0-adcfa2dd67a3",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": ["display(ipr.audit())"],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "2602d2da-f7a7-40d2-8c80-3bb5d240fcd5",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": ["ipr.job_control_updt2()"],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "d44b8e16-e815-4ec5-8b6e-194f133d4b52",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": [
                'TABLE_NAME = "TAF_IPH"\nFIL_4TH_NODE = "IPH"\n \nipr.get_cnt(TABLE_NAME)\nipr.getcounts("AWS_IP_MACROS", "1.1 AWS_Extract_Line_IP")\nipr.create_meta_info(TABLE_NAME, FIL_4TH_NODE)\nipr.create_eftsmeta_info(TABLE_NAME, "AWS_IP_MACROS", "1.1 AWS_Extract_Line_IP", "IP_HEADER", "new_submtg_state_cd")\nipr.file_contents(TABLE_NAME)'
            ],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "4bae9bbf-6a59-47bb-be13-a20dc0bf41b6",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": [
                'TABLE_NAME = "TAF_IPL"\nFIL_4TH_NODE = "IPL"\n \nipr.get_cnt(TABLE_NAME)\nipr.getcounts("AWS_IP_MACROS", "1.1 AWS_Extract_Line_IP")\nipr.create_meta_info(TABLE_NAME, FIL_4TH_NODE)\nipr.create_eftsmeta_info(TABLE_NAME, "AWS_IP_MACROS", "1.1 AWS_Extract_Line_IP", "IP_LINE", "new_submtg_state_cd_line")\nipr.file_contents(TABLE_NAME)'
            ],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "ef55932d-ff19-421c-b9ce-1602b98ccc60",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
    ],
    "metadata": {
        "application/vnd.databricks.v1+notebook": {
            "notebookName": "IP Runner",
            "dashboards": [],
            "notebookMetadata": {"pythonIndentUnit": 2},
            "language": "python",
            "widgets": {},
            "notebookOrigID": 606720,
        }
    },
    "nbformat": 4,
    "nbformat_minor": 0,
}
