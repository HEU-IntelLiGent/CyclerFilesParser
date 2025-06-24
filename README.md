# CyclerFilesParser
Scripts to load electrochemical time series exported from battery cyclers, and parse them into a parquet format compliant with the [Battery Data Format BDF](https://github.com/battery-data-alliance/battery-data-format).

# Use
* Clone this repository
  ```
  git clone https://github.com/HEU-IntelLiGent/CyclerFilesParser.git
  ```
* Create a Python virtual environment, activate it and install the required packages
    ```
    python -m venv venv
    source venv/Scripts/activate
    pip install -r requirements.txt
    ```
* Add the files you wish to parse into a single directory `./data/`.
* Run the `txt_to_parquet.py`file.

### Result
Your parsed files will be created next to the original ones, with the same name but with the `.parquet` extension. 