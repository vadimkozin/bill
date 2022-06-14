#!/bin/bash
docker build -q -t vadimkozin/bill .
docker push vadimkozin/bill
./_zip.sh