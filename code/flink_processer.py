from typing import Set

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table.table_environment import StreamTableEnvironment


def prepare_flink() -> StreamTableEnvironment:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)

    return t_env


def speeding_vehicle(step: int, t_env: StreamTableEnvironment) -> Set[str]:
    # you can also add your Pravega and ES connector jars here
    # t_env.get_config().get_configuration().set_string(
    #     "pipeline.jars",
    #     "file:///C:/workplace/sumo-test/sumo-pravega-flink-demo/lib/pravega-connectors-flink-1.13_2.12-0.10.1.jar"
    # )

    vehicle_source_ddl = f"""
                            CREATE TABLE VehicleTable (
                                timestep FLOAT,
                                id STRING,
                                `type` STRING,
                                waiting FLOAT,
                                lane STRING,
                                pos FLOAT,
                                speed FLOAT,
                                ts TIMESTAMP(3)
                            ) WITH (
                                'connector' = 'pravega',
                                'controller-uri' = 'tcp://localhost:9090', 
                                'scope' = 'sumo',
                                'scan.execution.type' = 'batch',
                                'scan.streams' = 'vehicleStream',
                                'format' = 'json'              
                            )
    """

    t_env.execute_sql(vehicle_source_ddl)

    query = f"""
                SELECT timestep, id, lane, pos, speed FROM VehicleTable
                WHERE speed > 14.0 AND timestep > {step - 10} AND timestep <= {step}
    """

    speeding_set = set()
    with t_env.execute_sql(query).collect() as results:
        for result in results:
            result.set_field_names(['timestep', 'id', 'lane', 'pos', 'speed'])
            vehicle_id = str(result.as_dict().get('id'))
            speeding_set.add(vehicle_id)

    return speeding_set


def closing_lane(step: int, t_env: StreamTableEnvironment) -> Set[str]:
    # you can also add your Pravega and ES connector jars here
    # t_env.get_config().get_configuration().set_string(
    #     "pipeline.jars",
    #     "file:///C:/workplace/sumo-test/sumo-pravega-flink-demo/lib/pravega-connectors-flink-1.13_2.12-0.10.1.jar"
    # )

    lane_source_ddl = f"""
                                CREATE TABLE LaneTable (
                                    timestep FLOAT,
                                    id STRING,
                                    maxspeed FLOAT,
                                    meanspeed FLOAT,
                                    occupancy FLOAT,
                                    vehicle_count FLOAT,
                                    ts TIMESTAMP(3)
                                ) WITH (
                                    'connector' = 'pravega',
                                    'controller-uri' = 'tcp://localhost:9090', 
                                    'scope' = 'sumo',
                                    'scan.execution.type' = 'batch',
                                    'scan.streams' = 'laneStream',
                                    'format' = 'json'              
                                )
        """

    t_env.execute_sql(lane_source_ddl)

    query = f"""
                    SELECT id, vehicle_count
                    FROM LaneTable
                    WHERE timestep = {step}
    """

    lane_list = set()
    max_veh_cnt = 0.0
    with t_env.execute_sql(query).collect() as results:
        for result in results:
            result.set_field_names(['id', 'vehicle_count'])
            lane_id = str(result.as_dict().get('id'))
            if result.as_dict().get('vehicle_count') > max_veh_cnt:
                lane_list.clear()
                lane_list.add(lane_id)
                max_veh_cnt = result.as_dict().get('vehicle_count')
            elif result.as_dict().get('vehicle_count') == max_veh_cnt:
                lane_list.add(lane_id)

    return lane_list


def connect_to_es(t_env: StreamTableEnvironment):
    es_table_veh_ddl = f"""
                        CREATE TABLE es_table_veh ( 
                            timestep FLOAT,
                            id STRING,
                            `type` STRING,
                            waiting FLOAT,
                            lane STRING,
                            pos FLOAT,
                            speed FLOAT,
                            ts TIMESTAMP(3)
                        ) WITH (
                            'connector.type' = 'elasticsearch', 
                            'connector.version' = '6',  
                            'connector.hosts' = 'http://localhost:9200',
                            'connector.index' = 'vehicle',
                            'connector.document-type' = 'VehicleTable', 
                            'connector.bulk-flush.max-actions' = '1',  
                            'format.type' = 'json',  
                            'update-mode' = 'append'
                        )
        """

    es_table_lane_ddl = f"""
                                    CREATE TABLE es_table_lane (
                                        timestep FLOAT,
                                        id STRING,
                                        maxspeed FLOAT,
                                        meanspeed FLOAT,
                                        occupancy FLOAT,
                                        vehicle_count FLOAT,
                                        ts TIMESTAMP(3)
                                    ) WITH (
                                        'connector.type' = 'elasticsearch', 
                                        'connector.version' = '6',  
                                        'connector.hosts' = 'http://localhost:9200',
                                        'connector.index' = 'lane',
                                        'connector.document-type' = 'LaneTable', 
                                        'connector.bulk-flush.max-actions' = '1',  
                                        'format.type' = 'json',  
                                        'update-mode' = 'append'            
                                    )
            """

    t_env.execute_sql(es_table_veh_ddl)
    t_env.execute_sql(es_table_lane_ddl)

    es_query_veh = f"""
                    INSERT INTO es_table_veh
                    SELECT * FROM VehicleTable
        """

    es_query_lane = f"""
                        INSERT INTO es_table_lane
                        SELECT * FROM LaneTable
        """

    t_env.execute_sql(es_query_veh)
    t_env.execute_sql(es_query_lane)
