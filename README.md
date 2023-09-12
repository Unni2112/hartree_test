## Getting Started

### Prerequisites

- Use Python3
- Create and activate the virtual environment by following the below steps:

``` bash
pip install virtualenv
virtualenv env

[In Mac]
source env/bin/activate

[In windows]
env/Scripts/activate
```

### We need to install required packages in our virtual environment

To install the requirements from the requirements.txt file, Use this command:

```
pip install -r "requirements.txt"
```

## Commands to Execute the files

```bash 
python3 apache_beam_sol.py
python3 pandas_sol.py
```

## Folder Structure

* [Input](./Input)
    * [dataset1.csv](./Input/dataset1.csv)
    * [dataset2.csv](./Input/dataset2.csv)
* [Output](./Output) (Note: Once output files are generated, access them here.)
    * [apache_beam_sol.csv](./Output/apache_beam_sol.csv)
    * [pandas_sol.csv](./Output/pandas_sol.csv)
* [ apache_beam_sol.py](./apache_beam_sol.py)
* [logs.log](./logs.log)
* [pandas_sol.py](./pandas_sol.py)
* [README.md](./README.md)
* [requirements.txt](./requirements.txt)
* [sample_test.txt](./sample_test.txt)


## Instructions

- Use the two input datasets, _dataset1_ and _dataset2_ from the Input folder.
- Applied transformations on _dataset1_ and _dataset2_ and merged them at the end to achieve the required result.
- Implementation involved various function like read_data(), transform_data(), process_data(), merge_data() to perform
  various operations.

## Output

The required output CSV files are [apache_beam_sol.csv](./Output/apache_beam_sol.csv)
and [pandas_sol.csv](./Output/pandas_sol.csv). (Once output files are generated, access them here.)




