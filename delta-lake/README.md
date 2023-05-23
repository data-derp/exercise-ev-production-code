

Build:
```bash
pip install -r requirements.txt -e .
```

Install:
```bash
pip install git+https://github.com/data-derp/exercise_ev_databricks_unit_tests#egg=exercise_ev_databricks_unit_tests
```

```text
package-one==0.1.0
exercise_ev_production_code_delta_lake @ git+https://github.com/data-derp/exercise_ev_databricks_unit_tests/delta-lake
package-three==0.3.0
```

Importing into notebook (example)

```python
pip install git+https://github.com/data-derp/exercise_ev_databricks_unit_tests/delta-lake#egg=exercise_ev_production_code_delta_lake
```

```python
def my_awesome_function(input_df: DataFrame) -> DataFrame:
    return input_df
```

```python
from exercise_ev_production_code_delta_lake.bronze import Bronze

Bronze().set_partitioning_cols(input_df)
```