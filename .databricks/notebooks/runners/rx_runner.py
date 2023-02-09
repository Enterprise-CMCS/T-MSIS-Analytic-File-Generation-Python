{
    "cells": [
        {
            "cell_type": "code",
            "source": ["from taf.RX.RX_Runner import RX_Runner"],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "d48635fe-da1e-4651-a3df-fef860a67b37",
                }
            },
            "outputs": [
                {
                    "output_type": "display_data",
                    "metadata": {
                        "application/vnd.databricks.v1+output": {
                            "data": "",
                            "errorSummary": "",
                            "metadata": {},
                            "errorTraceType": null,
                            "type": "ipynbError",
                            "arguments": {},
                        }
                    },
                    "output_type": "display_data",
                    "data": {
                        "text/html": [
                            '<style scoped>\n  .ansiout {\n    display: block;\n    unicode-bidi: embed;\n    white-space: pre-wrap;\n    word-wrap: break-word;\n    word-break: break-all;\n    font-family: "Source Code Pro", "Menlo", monospace;;\n    font-size: 13px;\n    color: #555;\n    margin-left: 4px;\n    line-height: 19px;\n  }\n</style>'
                        ]
                    },
                }
            ],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": ["from taf.RX.RX_Metadata import RX_Metadata"],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "0bdb1278-48c8-4402-8983-bbb7b352247c",
                }
            },
            "outputs": [
                {
                    "output_type": "display_data",
                    "metadata": {
                        "application/vnd.databricks.v1+output": {
                            "data": "",
                            "errorSummary": "",
                            "metadata": {},
                            "errorTraceType": null,
                            "type": "ipynbError",
                            "arguments": {},
                        }
                    },
                    "output_type": "display_data",
                    "data": {
                        "text/html": [
                            '<style scoped>\n  .ansiout {\n    display: block;\n    unicode-bidi: embed;\n    white-space: pre-wrap;\n    word-wrap: break-word;\n    word-break: break-all;\n    font-family: "Source Code Pro", "Menlo", monospace;;\n    font-size: 13px;\n    color: #555;\n    margin-left: 4px;\n    line-height: 19px;\n  }\n</style>'
                        ]
                    },
                }
            ],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": [
                'rxr = RX_Runner(reporting_period = dbutils.widgets.get("reporting_period")\n               ,state_code       = dbutils.widgets.get("state_code")\n               ,run_id           = dbutils.widgets.get("run_id")\n               ,job_id           = dbutils.widgets.get("job_id"))'
            ],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "98f85947-1b54-4cf1-a852-46fc34f948d6",
                }
            },
            "outputs": [
                {
                    "output_type": "display_data",
                    "metadata": {
                        "application/vnd.databricks.v1+output": {
                            "data": "",
                            "errorSummary": "",
                            "metadata": {},
                            "errorTraceType": null,
                            "type": "ipynbError",
                            "arguments": {},
                        }
                    },
                    "output_type": "display_data",
                    "data": {
                        "text/html": [
                            '<style scoped>\n  .ansiout {\n    display: block;\n    unicode-bidi: embed;\n    white-space: pre-wrap;\n    word-wrap: break-word;\n    word-break: break-all;\n    font-family: "Source Code Pro", "Menlo", monospace;;\n    font-size: 13px;\n    color: #555;\n    margin-left: 4px;\n    line-height: 19px;\n  }\n</style>'
                        ]
                    },
                }
            ],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": ['rxr.job_control_wrt("TRX")'],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "f2e29f18-e8bf-4da8-a0a1-080adeade5d9",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": ["rxr.job_control_updt()"],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "d40eafab-b608-4b80-89e2-d4bbd14e8834",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": ["rxr.print()"],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "b6972483-0001-40d1-86c6-33c0f2f5dc18",
                }
            },
            "outputs": [
                {
                    "output_type": "display_data",
                    "metadata": {
                        "application/vnd.databricks.v1+output": {
                            "data": "",
                            "errorSummary": "",
                            "metadata": {},
                            "errorTraceType": null,
                            "type": "ipynbError",
                            "arguments": {},
                        }
                    },
                    "output_type": "display_data",
                    "data": {
                        "text/html": [
                            '<style scoped>\n  .ansiout {\n    display: block;\n    unicode-bidi: embed;\n    white-space: pre-wrap;\n    word-wrap: break-word;\n    word-break: break-all;\n    font-family: "Source Code Pro", "Menlo", monospace;;\n    font-size: 13px;\n    color: #555;\n    margin-left: 4px;\n    line-height: 19px;\n  }\n</style>'
                        ]
                    },
                }
            ],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": ["rxr.init()"],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "06ffa6d7-d11d-4911-8652-40912892c5f3",
                }
            },
            "outputs": [
                {
                    "output_type": "display_data",
                    "metadata": {
                        "application/vnd.databricks.v1+output": {
                            "data": "",
                            "errorSummary": "",
                            "metadata": {},
                            "errorTraceType": null,
                            "type": "ipynbError",
                            "arguments": {},
                        }
                    },
                    "output_type": "display_data",
                    "data": {
                        "text/html": [
                            '<style scoped>\n  .ansiout {\n    display: block;\n    unicode-bidi: embed;\n    white-space: pre-wrap;\n    word-wrap: break-word;\n    word-break: break-all;\n    font-family: "Source Code Pro", "Menlo", monospace;;\n    font-size: 13px;\n    color: #555;\n    margin-left: 4px;\n    line-height: 19px;\n  }\n</style>'
                        ]
                    },
                }
            ],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": ["rxr.run()"],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "21da3815-acbe-4a9b-bc26-54bdadf89720",
                }
            },
            "outputs": [
                {
                    "output_type": "display_data",
                    "metadata": {
                        "application/vnd.databricks.v1+output": {
                            "data": "",
                            "errorSummary": "",
                            "metadata": {},
                            "errorTraceType": null,
                            "type": "ipynbError",
                            "arguments": {},
                        }
                    },
                    "output_type": "display_data",
                    "data": {
                        "text/html": [
                            '<style scoped>\n  .ansiout {\n    display: block;\n    unicode-bidi: embed;\n    white-space: pre-wrap;\n    word-wrap: break-word;\n    word-break: break-all;\n    font-family: "Source Code Pro", "Menlo", monospace;;\n    font-size: 13px;\n    color: #555;\n    margin-left: 4px;\n    line-height: 19px;\n  }\n</style>'
                        ]
                    },
                }
            ],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": ["rxr.view_plan()"],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "def1c74a-d681-4451-8dba-0f126bf872e6",
                }
            },
            "outputs": [
                {
                    "output_type": "display_data",
                    "metadata": {
                        "application/vnd.databricks.v1+output": {
                            "data": "",
                            "errorSummary": "",
                            "metadata": {},
                            "errorTraceType": null,
                            "type": "ipynbError",
                            "arguments": {},
                        }
                    },
                    "output_type": "display_data",
                    "data": {
                        "text/html": [
                            '<style scoped>\n  .ansiout {\n    display: block;\n    unicode-bidi: embed;\n    white-space: pre-wrap;\n    word-wrap: break-word;\n    word-break: break-all;\n    font-family: "Source Code Pro", "Menlo", monospace;;\n    font-size: 13px;\n    color: #555;\n    margin-left: 4px;\n    line-height: 19px;\n  }\n</style>'
                        ]
                    },
                }
            ],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": ["display(rxr.audit())"],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "7e28d403-d71e-4402-9a8e-1592c84ae464",
                }
            },
            "outputs": [
                {
                    "output_type": "display_data",
                    "metadata": {
                        "application/vnd.databricks.v1+output": {
                            "data": "",
                            "errorSummary": "",
                            "metadata": {},
                            "errorTraceType": null,
                            "type": "ipynbError",
                            "arguments": {},
                        }
                    },
                    "output_type": "display_data",
                    "data": {
                        "text/html": [
                            '<style scoped>\n  .ansiout {\n    display: block;\n    unicode-bidi: embed;\n    white-space: pre-wrap;\n    word-wrap: break-word;\n    word-break: break-all;\n    font-family: "Source Code Pro", "Menlo", monospace;;\n    font-size: 13px;\n    color: #555;\n    margin-left: 4px;\n    line-height: 19px;\n  }\n</style>'
                        ]
                    },
                }
            ],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": ["rxr.job_control_updt2()"],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "eebea083-8c53-47a3-b56d-86962d87218a",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": [
                'TABLE_NAME = "TAF_RXH"\nFIL_4TH_NODE = "RXH"\n \nrxr.get_cnt(TABLE_NAME)\nrxr.getcounts("AWS_RX_MACROS", "1.1 AWS_Extract_Line_RX")\nrxr.create_meta_info(TABLE_NAME, FIL_4TH_NODE)\nrxr.create_eftsmeta_info(TABLE_NAME, "AWS_RX_MACROS", "1.1 AWS_Extract_Line_RX", "RX_HEADER", "new_submtg_state_cd")\nrxr.file_contents(TABLE_NAME)'
            ],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "efaf0cb3-10c9-4dcf-adf7-f4427610ee69",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": [
                'TABLE_NAME = "TAF_RXL"\nFIL_4TH_NODE = "RXL"\n \nrxr.get_cnt(TABLE_NAME)\nrxr.getcounts("AWS_RX_MACROS", "1.1 AWS_Extract_Line_RX")\nrxr.create_meta_info(TABLE_NAME, FIL_4TH_NODE)\nrxr.create_eftsmeta_info(TABLE_NAME, "AWS_RX_MACROS", "1.1 AWS_Extract_Line_RX", "RX_LINE", "new_submtg_state_cd_line")\nrxr.file_contents(TABLE_NAME)'
            ],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "4df5470e-6de8-4f6a-a33c-6857c0f9520c",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
    ],
    "metadata": {
        "application/vnd.databricks.v1+notebook": {
            "notebookName": "RX Runner",
            "dashboards": [],
            "notebookMetadata": {"pythonIndentUnit": 2},
            "language": "python",
            "widgets": {},
            "notebookOrigID": 430973,
        }
    },
    "nbformat": 4,
    "nbformat_minor": 0,
}
