# OTree Index Visualizer
Python visualizer for qbeast index, a tool to visually examine your index, get clues about tree balance, and gather information about sampling efficiency.

With a given qbeast table path and a target `RevisionID`, it scans the `_delta_log/` to find the `cubes` of the target `Revision` and builds the index structure for visual display.

One can also provide a sampling fraction and the selected nodes will be highlighted with a different color. Details about on the sampling efficiency will be shown in the terminal. 

### Installation

First of all, you need to make sure you have python installed in your pc and then install poetry in order to download all the dependencies.

```bash
# Clone qbeast-spark repo
git clone git@github.com:Qbeast-io/qbeast-spark.git
cd qbeast-spark/utils/visualizer

# Pyhton version should be 3.12
python3 --version

# Install required dependencies
poetry install 
```

### Usage

Launch a `Flask` serve with the following command and open the link with a browser:

```bash
# Run the tool on the test table
poetry run qviz tests/resources/test_table/

# optionally, specify the index revision(defaulted to 1)
# qviz <table-path> --revision-id=2
```

- Visualize cubes retrieved for a given sampling fraction.
![](docs/images/sampling-fraction.png)
- Sampling details: when a valid value for sampling fraction is given, a set of sampling metrics are displayed.
**Only the chosen revision is taken into account for computing sampling metrics.**
```
Sampling Info:        
        Disclaimer:
                The displayed sampling metrics are only for the chosen revisionId.
                The values will be different if the table contains multiple revisions.
        
        sample fraction: 0.02        
        number of rows: 751130/8000000, 9.39%        
        sample size: 0.04273/2.09944GB, 2.04%
```
- To visualize any table, point to the folder in which the target `_delta_log/` is contained.
- To visualize a remote table, download its `_delta_log/` and point to the folder location.

### Tests
- To run all tests:
```bash
python -m unittest discover -s tests -p "*_test.py"
```
