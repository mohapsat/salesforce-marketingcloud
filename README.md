## Salesforce Marketing Cloud

Performs:
- Read from source database table
- Create Data Extension (DE) is SFMC in a specific folder
- Load data into the Data Extension

```Uses Fuel-SDK and PytTeradata```

``usage: deCreate.py [-h] [-s TABLE_NAME] [-t TARGET_DE]``
```
  SFMC Async Data Load

optional arguments:
  -h, --help     show this help message and exit
  -s TABLE_NAME  source teradata table
  -t TARGET_DE   target salesforce data extension
```


License: MIT
