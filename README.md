# Pravega Sumo Flink Demo

A Demo program using Pravega, Flink to analyze data generated on [Sumo](https://www.eclipse.org/sumo/)

## Prerequesites

1. Install [Sumo 1.12.0](https://sumo.dlr.de/releases/)
2. Download [Pravega 0.10.1](https://github.com/pravega/pravega/releases/download/v0.10.1/pravega-0.10.1.zip), [Elasticsearch 7.6.0](https://www.elastic.co/downloads/past-releases/elasticsearch-7-6-0) and [Kibana 7.6.0](https://www.elastic.co/downloads/past-releases/kibana-7-6-0)
3. Install [python3.8](https://www.python.org/downloads/release/python-380/), then create a virtual environment for the project.
4. Install the requirements: `pip install -r requirements.txt`.
5. Download [Pravega connector](https://github.com/pravega/flink-connectors/releases/download/v0.10.1/pravega-connectors-flink-1.13_2.12-0.10.1.jar) and [Elasticsearch connector](https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-elasticsearch6_2.12/1.13.2/flink-sql-connector-elasticsearch6_2.12-1.13.2.jar) jar files and copy them to the pyflink folder in your virtual environment(`VIRTUAL_ENV/Lib/site-packages/pyflink/lib`)


## Start the demo

1. Start Pravega, Elasticsearch and Kibana
2. Set your [`SUMO_HOME`](code/sumo_client.py:12)
3. Run `sumo_client.py`