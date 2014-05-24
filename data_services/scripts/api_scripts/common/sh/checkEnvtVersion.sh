#!/bin/bash

url="https://${1}/version.txt"
ver=`curl -sS ${url} | head -1 | cut -d'=' -f2`

echo ${ver}