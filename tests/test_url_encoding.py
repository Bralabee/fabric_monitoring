#!/usr/bin/env python3
"""
Quick test to debug Power BI API URL encoding issues.
"""
import requests
from datetime import datetime

# Test datetime parameters
start_str = "2025-11-14T00:00:00.000000Z"
end_str = "2025-11-14T23:59:59.999999Z"

print(f"Start string: {start_str}")
print(f"End string: {end_str}")

# Test params
params = {
    "startDateTime": start_str,
    "endDateTime": end_str
}

print(f"Params: {params}")

# Test URL construction
url = "https://api.powerbi.com/v1.0/myorg/admin/activityevents"

# Test with requests.Request to see what URL gets constructed
req = requests.Request('GET', url, params=params)
prepared = req.prepare()

print(f"Prepared URL: {prepared.url}")

# Show individual parameter encoding
import urllib.parse

print(f"Start encoded: {urllib.parse.quote(start_str)}")
print(f"End encoded: {urllib.parse.quote(end_str)}")