Remove-Item -r -fo .\build; Remove-Item -r -fo .\*.egg-info
python setup.py bdist_wheel
databricks --profile val fs cp .\dist\taf-7.1.26-py3-none-any.whl dbfs:/FileStore/shared_uploads/ficu/lib/taf-latest-py3-none-any.whl --overwrite