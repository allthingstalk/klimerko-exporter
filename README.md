# Klimerko exporter
An example exporter of ground data into a single CSV file.

```
$ python3 -m venv venv
$ source venv/bin/activate
$ pip install -r requirements.txt
$ USER=your_username PASSWORD=your_password GROUND=your_ground_id ./klimerko.py
```

The output will be in `export.csv`

You can add the optional parameter DAYS, to get less data.

```
$ DAYS=1 USER=your_username PASSWORD=your_password ./klimerko.py
```