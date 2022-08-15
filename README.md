# Delta Lake and Soda 

This repository contains

* a Dockerfile based on [jupyter/pyspark-notebook](https://jupyter-docker-stacks.readthedocs.io/en/latest/using/selecting.html#jupyter-pyspark-notebook) and extended with
  * [Delta Lake](https://docs.delta.io/latest/index.html)
  * [Soda Spark](https://docs.soda.io/soda-spark/install-and-use.html) (deprecated)
  * [Soda Core](https://docs.soda.io/soda-core/overview-main.html)
  * [Faker](https://faker.readthedocs.io/en/master/)

* notebooks for learning and experimenting with the combination of these technologies.



## Usage



### Dockerfile

Build the image
```
docker build -t jupyter/delta-lake .
```

Run a container with the 
```
docker run -it --rm -p 8888:8888 -v "${PWD}":/home/jovyan/work jupyter/delta-lake
```



## Notebooks



### *00_test-delta-and-soda.ipynb*

Test if delta and soda are working. 



### *01_utils_data-generation.ipynb*

Intro in the `utils.data_generation` module.



### *02_soda-scan-with-checks-and-profiling-on-delta-table.ipynb*

WIP



### *11_pipeline_append-snapshots-with-schema-evolution.ipynb*

WIP
