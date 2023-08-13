# spark-examples

spark-examples

## Running locally

```
docker run -t -v ${PWD}/pyspark:/opt/pyspark_app pyspark3.3 sample_pyspa
rk_app.py

docker run -t -v ${PWD}/pyspark:/opt/pyspark_app -v ${PWD}/resources/jars:/opt/pyspark_app/jars/ pyspark3.3 example_pyspark_write_file.py

```
