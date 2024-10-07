# Geocoding using CUZK

* This program performs geocoding of address points based on RUIAN data for the Czech Republic.
* Outputs are three CSV files: 
  - **output.csv** - all values with exactly one geocoded result per input row
    - Attributes are ID (provided by input.csv), x, y, address, adress type and score
  - **multivalue.csv** - all values with more than one result
  - **not_found.csv** - all values with no results

* Input is a CSV file **input.csv** places in the same folder as the .exe file.
* Tested for Python v. 3.12.7
* Python file tested on Ubuntu LTR 24.04 Noble Numbat
* Executable file tested on Windows 10 Pro



