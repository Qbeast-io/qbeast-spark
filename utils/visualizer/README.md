# OTree Index Visualizer
Python visualizer for qbeast index, a tool to visually examine your index, get clues about tree balance, and gather information about sampling efficiency.

With a given qbeast table path and a target `RevisionID`, it scans the `_delta_log/` to find the `cubes` of the target `Revision` and builds the index structure for visual display.

One can also provide a sampling fraction and the selected nodes will be highlighted with a different color. Details about on the sampling efficiency will be shown in the terminal. 

### Installation

First of all, you need to make sure you have python installed in your pc and then install poetry in order to download all the dependencies.

```bash
# We will create a virtual environment in the visualization folder
git clone git@github.com:Qbeast-io/qbeast-spark.git
cd your-path-to-qbeast-spark-repository/utils/visualizer

# Pyhton version should be 3.12
python3 --version

# We will need poetry to create the environment
pip install poetry 

# Create a poetry shell (remember to use command exit when you are done with using this environment)
poetry shell

# Install all the required dependencies
poetry install --no-root

# Install qviz
poetry run pip install -e .

```

### Usage

Launch a `Flask` serve with the following command and open the link with a browser: 
```
qviz ./docs/test_table/

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
                The displayed sampling metrics are valid only for single revision indexes(excluding revision 0):
        
        sample fraction: 0.3
        number of cubes read:8/20, 40.00%
        number of rows: 41858/100000, 41.86%
        sample size: 0.00141/0.00324GB, 43.49%
```
- To visualize any table, point to the folder in which the target `_delta_log/` is contained.
- To visualize a remote table, download its `_delta_log/` and point to the folder location.

### Tests
- To run all tests: `make test`
