#!/usr/bin/env bash

cd error-capture-repository
echo Installing dependancies
serverless plugin install --name serverless-latest-layer-version
echo Destroying serverless bundle...
serverless remove --verbose;
