from opensky_api import OpenSkyApi

api = OpenSkyApi(username="rachit", password="rachitbits")
s = api.get_states()
print(s)

