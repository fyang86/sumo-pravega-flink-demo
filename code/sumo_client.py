import json
import os
import sys

import traci
from pravega_client import StreamManager
from pravega_client import StreamWriter

from flink_processer import speeding_vehicle, closing_lane

os.environ["SUMO_HOME"] = "C:/workplace/releases/sumo-1.12.0"

if 'SUMO_HOME' not in os.environ:
    sys.exit("please declare environment variable 'SUMO_HOME'")

SUMO_BIN = os.path.join(os.environ["SUMO_HOME"], "bin", "sumo-gui")

DEFAULT_CONTROLLER_URI = 'tcp://localhost:9090'
SCOPE = 'sumo'
VEHICLE_STREAM = 'vehicleStream'
LANE_STREAM = 'laneStream'


def start_sumo(sumo_config: str):
    sumo_cmd = [SUMO_BIN, "-c", sumo_config, "--start", "--step-length", "1", "--delay", "100"]
    traci.start(sumo_cmd)


def close_sumo() -> None:
    traci.close()


def run_simulation(
        steps: int, vehicle_writer: StreamWriter, lane_writer: StreamWriter) -> None:
    closing_lane_list = []
    for step in range(steps):
        traci.simulationStep()

        # write vehicle data
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
        # stop speeding cars within 10 timesteps for 10 steps
        if step == 25:
            speeding_set = speeding_vehicle(step)
            for vehicle_id in speeding_set:
                print(vehicle_id)
                traci.vehicle.setStop(vehID=vehicle_id,
                                      edgeID=traci.vehicle.getRoadID(vehicle_id),
                                      pos=traci.vehicle.getLanePosition(vehicle_id),
                                      duration=10.00)

        # close the busiest lane currently for 10 timesteps
        if step == 10:
            closing_lane_list = closing_lane(step)
            for lane_id in closing_lane_list:
                traci.lane.setDisallowed(lane_id, ['passenger'])

        if step == 20:
            for lane_id in closing_lane_list:
                traci.lane.setAllowed(lane_id, ['passenger'])


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
    lane_writer = create_lane_writer(manager)

    start_sumo(sumo_cfg)
    try:
        run_simulation(steps, vehicle_writer, lane_writer)
    except KeyboardInterrupt:
        pass
    finally:
        close_sumo()


if __name__ == "__main__":
    SUMO_CFG = os.path.realpath("../config/manhattan.sumocfg")
    sumo_generator(SUMO_CFG, 1500)