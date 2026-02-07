import requests


BASE_URL = "https://www150.statcan.gc.ca/t1/wds/rest"


def post(endpoint, payload):
    response = requests.post(f"{BASE_URL}/{endpoint}", json=payload)
    response.raise_for_status()
    return response.json()


def get(endpoint):
    response = requests.get(f"{BASE_URL}/{endpoint}")
    response.raise_for_status()
    return response.json()
