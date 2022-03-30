#!/bin/bash

echo "Starting PubSub emulator.."
source /Users/andras.palfi/Dev/Projects/AndrisProjects/pubsub-emulator/venv/bin/activate
python /Users/andras.palfi/Dev/Projects/AndrisProjects/pubsub-emulator/main.py $1
deactivate
