import urllib.request
import json
try:
    req = urllib.request.Request('http://localhost:19120/api/v1/trees/tree')
    with urllib.request.urlopen(req) as response:
        ref = json.loads(response.read())
        print('Nessie default branch:', ref)
except Exception as e:
    print('Error:', e)
