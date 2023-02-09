{
    "cells": [
        {
            "cell_type": "code",
            "source": ["from taf.PRV.PRV_Runner import PRV_Runner"],
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
            "source": ["from taf.PRV.PRV_Metadata import PRV_Metadata"],
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
                'prv = PRV_Runner(reporting_period = dbutils.widgets.get("reporting_period")\n                ,state_code       = dbutils.widgets.get("state_code")\n                ,run_id           = dbutils.widgets.get("run_id")\n                ,job_id           = dbutils.widgets.get("job_id"))'
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
            "source": ['prv.job_control_wrt("PRV")'],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "2f96843a-60e6-49ab-bfdb-4bfbe5f319c7",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": ["prv.job_control_updt()"],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "5299c1f6-e065-4000-b6d9-5ab4888a9af7",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": ["prv.print()"],
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
            "source": ["prv.init()"],
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
            "source": ["prv.run()   "],
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
            "source": ["prv.view_plan()"],
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
            "source": ["display(prv.audit())"],
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
            "source": ["prv.job_control_updt2()"],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "eaf9b492-5461-4881-a7a3-32caf2278363",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": [
                'TABLE_NAME = "TAF_PRV"\nFIL_4TH_NODE = "PBS"\n \nprv.get_cnt(TABLE_NAME)\nprv.getcounts("101_prvdr_build.sas", "base_Prov")\nprv.create_meta_info(TABLE_NAME, FIL_4TH_NODE)\nprv.create_eftsmeta_info(TABLE_NAME, "101_prvdr_build.sas", "base_Prov", "Prov02_Base", "submtg_state_cd")\nprv.file_contents(TABLE_NAME)'
            ],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "41314a9a-0410-4539-afa3-60be5d3192b8",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": [
                'TABLE_NAME = "TAF_PRV_LOC"\nFIL_4TH_NODE = "PLO"\n \nprv.get_cnt(TABLE_NAME)\nprv.getcounts("101_prvdr_build.sas", "constructed_3_Prov03")\nprv.create_meta_info(TABLE_NAME, FIL_4TH_NODE)\nprv.create_eftsmeta_info(TABLE_NAME, "101_prvdr_build.sas", "constructed_3_Prov03", "Prov03_Location_CNST", "submtg_state_cd")\nprv.file_contents(TABLE_NAME)'
            ],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "b3584797-a260-4de1-a8b4-bb7392094447",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": [
                'TABLE_NAME = "TAF_PRV_LIC"\nFIL_4TH_NODE = "PLI"\n \nprv.get_cnt(TABLE_NAME)\nprv.getcounts("101_prvdr_build.sas", "constructed_Prov04")\nprv.create_meta_info(TABLE_NAME, FIL_4TH_NODE)\nprv.create_eftsmeta_info(TABLE_NAME, "101_prvdr_build.sas", "constructed_Prov04", "Prov04_Licensing_CNST", "submtg_state_cd")\nprv.file_contents(TABLE_NAME)'
            ],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "04780f0b-35c3-4b3d-b523-1d45ffebd077",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": [
                'TABLE_NAME = "TAF_PRV_IDT"\nFIL_4TH_NODE = "PID"\n \nprv.get_cnt(TABLE_NAME)\nprv.getcounts("101_prvdr_build.sas", "constructed_Prov05")\nprv.create_meta_info(TABLE_NAME, FIL_4TH_NODE)\nprv.create_eftsmeta_info(TABLE_NAME, "101_prvdr_build.sas", "constructed_Prov05", "Prov05_Identifiers_CNST", "submtg_state_cd")\nprv.file_contents(TABLE_NAME)'
            ],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "65ceac4b-19eb-4674-8f4b-a82b88ef3287",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": [
                'TABLE_NAME = "TAF_PRV_TAX"\nFIL_4TH_NODE = "PTX"\n \nprv.get_cnt(TABLE_NAME)\nprv.getcounts("101_prvdr_build.sas", "constructed_Prov06")\nprv.create_meta_info(TABLE_NAME, FIL_4TH_NODE)\nprv.create_eftsmeta_info(TABLE_NAME, "101_prvdr_build.sas", "constructed_Prov06", "Prov06_Taxonomies_seg", "submtg_state_cd")\nprv.file_contents(TABLE_NAME)'
            ],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "d73cd046-2685-4d20-8d3b-6dc3a5b719ca",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": [
                'TABLE_NAME = "TAF_PRV_ENR"\nFIL_4TH_NODE = "PEN"\n \nprv.get_cnt(TABLE_NAME)\nprv.getcounts("101_prvdr_build.sas", "segment_Prov07")\nprv.create_meta_info(TABLE_NAME, FIL_4TH_NODE)\nprv.create_eftsmeta_info(TABLE_NAME, "101_prvdr_build.sas", "segment_Prov07", "Prov07_Medicaid_ENROP", "submtg_state_cd")\nprv.file_contents(TABLE_NAME)'
            ],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "cfc90362-29ab-435c-9f42-4c05d2a173d8",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": [
                'TABLE_NAME = "TAF_PRV_GRP"\nFIL_4TH_NODE = "PAG"\n \nprv.get_cnt(TABLE_NAME)\nprv.getcounts("101_prvdr_build.sas", "constructed_Prov08")\nprv.create_meta_info(TABLE_NAME, FIL_4TH_NODE)\nprv.create_eftsmeta_info(TABLE_NAME, "101_prvdr_build.sas", "constructed_Prov08", "Prov08_Groups_CNST", "submtg_state_cd")\nprv.file_contents(TABLE_NAME)'
            ],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "8585d4c8-e761-450a-a649-7eb330ccf06c",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": [
                'TABLE_NAME = "TAF_PRV_PGM"\nFIL_4TH_NODE = "PAP"\n \nprv.get_cnt(TABLE_NAME)\nprv.getcounts("101_prvdr_build.sas", "constructed_Prov09")\nprv.create_meta_info(TABLE_NAME, FIL_4TH_NODE)\nprv.create_eftsmeta_info(TABLE_NAME, "101_prvdr_build.sas", "constructed_Prov09", "Prov09_AffPgms_CNST", "submtg_state_cd")\nprv.file_contents(TABLE_NAME)'
            ],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "e2643ae9-b94f-4f8f-9dfe-49e1ef12f7dc",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
        {
            "cell_type": "code",
            "source": [
                'TABLE_NAME = "TAF_PRV_BED"\nFIL_4TH_NODE = "PBT"\n \nprv.get_cnt(TABLE_NAME)\nprv.getcounts("101_prvdr_build.sas", "constructed_Prov10")\nprv.create_meta_info(TABLE_NAME, FIL_4TH_NODE)\nprv.create_eftsmeta_info(TABLE_NAME, "101_prvdr_build.sas", "constructed_Prov10", "Prov10_BedType_CNST", "submtg_state_cd")\nprv.file_contents(TABLE_NAME)'
            ],
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": false,
                    "inputWidgets": {},
                    "nuid": "0cd9ff46-28cc-4b56-b29f-2b33ade04d35",
                }
            },
            "outputs": [],
            "execution_count": 0,
        },
    ],
    "metadata": {
        "application/vnd.databricks.v1+notebook": {
            "notebookName": "PRV Runner",
            "dashboards": [],
            "notebookMetadata": {"pythonIndentUnit": 2},
            "language": "python",
            "widgets": {},
            "notebookOrigID": 428378,
        }
    },
    "nbformat": 4,
    "nbformat_minor": 0,
}
