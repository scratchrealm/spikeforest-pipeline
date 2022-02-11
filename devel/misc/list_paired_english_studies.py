import kachery_client as kc

x = kc.load_json('sha1://8dad98cd35efe571bf10e8ccba1c8e082600de61/paired_english.json')

recordings = x['recordings']
for recording in recordings:
    print(recording['name'])