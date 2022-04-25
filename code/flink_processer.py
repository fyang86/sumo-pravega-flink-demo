from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table.table_environment import StreamTableEnvironment


def speeding_vehicle():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)
    t_env.get_config().get_configuration().set_string(
        "pipeline.jars",
        "file:///C:/workplace/sumo-test/sumo-pravega-flink-demo/lib/pravega-connectors-flink-1.13_2.12-0.10.1.jar"
    )

    vehicle_source_ddl = f"""
                            CREATE TABLE VehicleTable (
                                timestep FLOAT,
                                id FLOAT,
                                `type` STRING,
                                waiting FLOAT,
                                lane STRING,
                                pos FLOAT,
                                speed FLOAT
                            ) WITH (
                                'connector' = 'pravega',
                                'controller-uri' = 'tcp://localhost:9090', 
                                'scope' = 'sumo',
                                'scan.execution.type' = 'streaming',
                                'scan.streams' = 'vehicleStream',
                                'format' = 'json'              
                            )
    """

    t_env.execute_sql(vehicle_source_ddl)

    query = f"""
                SELECT timestep, id, lane, pos, speed FROM VehicleTable
                WHERE speed > 14.0 AND (lane = 'A0A1_0' or lane = 'A1A0_0')
    """


    speeding_vehicles = t_env.sql_query(query)
    t_env.to_append_stream(
        speeding_vehicles,
        Types.ROW_NAMED(['timestep', 'id', 'lane', 'pos', 'speed'], [
            Types.FLOAT(),
            Types.FLOAT(),
            Types.STRING(),
            Types.FLOAT(),
            Types.FLOAT()
        ])).print()

    env.execute('Speeding vehicles')
