# Common Library
Library of modules used throughout the BugSwarm toolset

**This repository is deprecated. Please click [here](https://github.com/BugSwarm/bugswarm/tree/master/bugswarm/common) for the latest common library.**

## REST API Usage:
```
from bugswarm.common.rest_api.database_api import DatabaseAPI

bugswarmapi = DatabaseAPI(token='YOUR_TOKEN')
response = bugswarmapi.find_artifact("Abjad-abjad-289716771")
```
