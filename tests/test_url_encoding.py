import urllib.parse

import requests


def test_powerbi_activityevents_url_encoding():
    start_str = "2025-11-14T00:00:00.000000Z"
    end_str = "2025-11-14T23:59:59.999999Z"

    params = {
        "startDateTime": start_str,
        "endDateTime": end_str,
    }

    url = "https://api.powerbi.com/v1.0/myorg/admin/activityevents"
    req = requests.Request("GET", url, params=params)
    prepared = req.prepare()

    assert prepared.url
    assert prepared.url.startswith(url)

    start_encoded = urllib.parse.quote(start_str)
    end_encoded = urllib.parse.quote(end_str)

    assert f"startDateTime={start_encoded}" in prepared.url
    assert f"endDateTime={end_encoded}" in prepared.url
