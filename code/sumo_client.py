import json
import os
import sys
import time

import traci

from pravega_client import StreamManager
from pravega_client import StreamWriter

from flink_processer import speeding_vehicle

os.environ["SUMO_HOME"] = "C:/workplace/releases/sumo-1.12.0"

if 'SUMO_HOME' not in os.environ:
    sys.exit("please declare environment variable 'SUMO_HOME'")

SUMO_BIN = os.path.join(os.environ["SUMO_HOME"], "bin", "sumo-gui")

DEFAULT_CONTROLLER_URI = 'tcp://localhost:9090'
SCOPE = 'sumo'
VEHICLE_STREAM = 'vehicleStream'
LANE_STREAM = 'laneStream'


def start_sumo(sumo_config: str):
    sumo_cmd = [SUMO_BIN, "-c", sumo_config, "--start", "--step-length", "1"]
    traci.start(sumo_cmd)

def close_sumo() -> None:
    traci.close()

def run_simulation(
        steps: int, vehicle_writer: StreamWriter, lane_writer: StreamWriter) -> None:
    for step in range(steps):
        traci.simulationStep()

        #write vehicle data
        vehicle_list = traci.vehicle.getIDList()
        for vehicle_id in vehicle_list:
            event = {
                "timestep": float(step),
                "id": float(vehicle_id),
                "type": traci.vehicle.getTypeID(vehicle_id),
                "waiting": traci.vehicle.getWaitingTime(vehicle_id),
                "lane": traci.vehicle.getLaneID(vehicle_id),
                "pos": float(traci.vehicle.getLanePosition(vehicle_id)),
                "speed": float(traci.vehicle.getSpeed(vehicle_id))
            }
            # print(event)
            vehicle_writer.write_event(json.dumps(event),
                                       routing_key=str(step))

            # write lane data
            lane_list = traci.lane.getIDList()
            for lane_id in lane_list:
                event = {
                    "timestep": float(step),
                    "id": lane_id,
                    "maxspeed": float(traci.lane.getMaxSpeed(lane_id)),
                    "meanspeed": float(traci.lane.getLastStepMeanSpeed(lane_id)),
                    "occupancy": float(traci.lane.getLastStepOccupancy(lane_id)),
                    "vehicle_count": float(traci.lane.getLastStepVehicleNumber(lane_id))
                }
                # print(event)
                lane_writer.write_event(json.dumps(event),
                                        routing_key=str(step))
        # deal with speeding cars
        if step == 25:
            speeding_vehicle()


        # time.sleep(1)


def create_vehicle_writer(manager: StreamManager) -> StreamWriter:
    manager.create_stream(scope_name=SCOPE,
                          stream_name=VEHICLE_STREAM,
                          initial_segments=3)
    return manager.create_writer(SCOPE, VEHICLE_STREAM)


def create_lane_writer(manager: StreamManager) -> StreamWriter:
    manager.create_stream(scope_name=SCOPE,
                          stream_name=LANE_STREAM,
                          initial_segments=3)
    return manager.create_writer(SCOPE, LANE_STREAM)

def sumo_generator(sumo_cfg: str, steps: int) -> None:
    manager = StreamManager(DEFAULT_CONTROLLER_URI)
    manager.create_scope(SCOPE)

    vehicle_writer = create_vehicle_writer(manager)
    edge_writer = create_lane_writer(manager)

    start_sumo(sumo_cfg)
    try:
        run_simulation(steps, vehicle_writer, edge_writer)
    except KeyboardInterrupt:
        pass
    finally:
        close_sumo()

if __name__ == "__main__":
    base_path = os.path.dirname(os.path.realpath(__file__))

    SUMO_CFG = os.path.realpath("../config/manhattan.sumocfg")
    sumo_generator(SUMO_CFG, 10000)