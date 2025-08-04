#!/bin/bash
source /home/ghre/Desktop/weatherApp/.venv/bin/activate
python -u "/home/ghre/Desktop/weatherApp/etl/pipeline.py" "$@"
deactivate