#!/bin/bash

mkdir -p /logs/verifier
pytest /tests/test_outputs.py -rA
exit_code=$?

if [ $exit_code -eq 0 ]; then
    echo 1 > /logs/verifier/reward.txt
    echo "SUCCESS: All tests passed"
else
    echo 0 > /logs/verifier/reward.txt
    echo "FAILURE: Tests failed with exit code $exit_code"
fi