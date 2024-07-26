from opensky_api import OpenSkyApi
from kafka import KafkaProducer
import json
import datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
)

def get_departures_by_airport(airport_icao, begin, end):
    api = OpenSkyApi(username="rachit", password="rachitbits")
    begin_timestamp = int(begin.timestamp())
    end_timestamp = int(end.timestamp())
    departures = api.get_departures_by_airport(airport_icao, begin_timestamp, end_timestamp)
    return departures

def get_state_vectors(icao24_addresses):
    api = OpenSkyApi(username="rachit", password="rachitbits")
    states = api.get_states(icao24=icao24_addresses)
    return states

def get_all_state_vectors():
    api = OpenSkyApi()
    states = api.get_states()
    return states

if __name__ == "__main__":
    airport_icao = 'KDFW'
    end_time = datetime.datetime.utcnow()
    start_time = end_time - datetime.timedelta(hours=2)

    try:
        departures = get_departures_by_airport(airport_icao, start_time, end_time)
        print("Fetched departures:", departures)

        if departures:
            icao24_addresses = [flight.icao24 for flight in departures]
            print("ICAO24 addresses:", icao24_addresses)

            if not icao24_addresses:
                print("No ICAO24 addresses found.")
            else:
                states = get_state_vectors(icao24_addresses)
                print(f"States for departures from {airport_icao} between {start_time} and {end_time}:")
                print("Fetched states:", states)

                if states and states.states:
                    for state in states.states:
                        producer.send(airport_icao, value=state.__dict__)
                        print("Sent state:", state.__dict__)
                    producer.flush()
                    print("State vectors sent to Kafka.")
                else:
                    print("No state vectors found.")
        else:
            print("No departures found.")
    except ValueError as e:
        print("ValueError:", e)
    except Exception as e:
        print("Exception:", e)
    finally:
        producer.close()

