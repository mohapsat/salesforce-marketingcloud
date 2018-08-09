## Salesforce Marketing Cloud - Async Data Load

Performs:
- Read from source database table (while persisting datatypes and col widths)
- Drop and Create Data Extension (DE) is SFMC in a target folder
- Load data into the Data Extension

-----
```

usage: deCreate.py [-h] [-s TABLE_NAME] [-t TARGET_DE] [-f FOLDERID] [-c CHUNK_SIZE]

SFMC Async Data Load

optional arguments:
  -h, --help     show this help message and exit
  -s TABLE_NAME  Source Teradata table
  -t TARGET_DE   target Salesforce data extension
  -f FOLDERID    target Salesforce folder ID
  -c CHUNK_SIZE  Batch size for Salesforce Load

```

License: MIT
