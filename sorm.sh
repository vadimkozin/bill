#!/bin/sh

year=$(date +%Y)
delimiter="---------------------------------------"

help="${delimiter}\nPrepare csv-file for SORM\nused: $0 year quarter email\nexample: $0 $year 1 test@mail.ru\n${delimiter}"

function isEmailValid() {
  regex="^([A-Za-z]+[A-Za-z0-9]*((\.|\-|\_)?[A-Za-z]+[A-Za-z0-9]*){1,})@(([A-Za-z]+[A-Za-z0-9]*)+((\.|\-|\_)?([A-Za-z]+[A-Za-z0-9]*)+){1,})+\.([A-Za-z]{2,})+$"
  [[ "${1}" =~ $regex ]]
}

function isYearValid() {
  regex="^202[0-9]$"
  [[ "${1}" =~ $regex ]]
}

function isQuarterValid() {
  regex="^[1234]$"
  [[ "${1}" =~ $regex ]]
}

if [ -n "$1" ] && [ -n "$2" ]  && [ -n "$3" ]
then
  if ! isYearValid $1 ;then
    echo "year: '${1}' invalid, must be: 2022, 2023, .."
    exit 1
  fi
  if ! isQuarterValid $2 ;then
    echo "quarter: '${2}' invalid, must be: 1,2,3 or 4"
    exit 1
  fi
  if ! isEmailValid $3 ;then
    echo "email: '${3}' invalid"
    exit 1
  fi

  DATA=$(pwd)/data

  if [ ! -d "$DATA" ]; then
    mkdir $DATA
  fi

  docker run --rm -v $(pwd)/cfg:/app/cfg -v $DATA:/app/data vadimkozin/sorm \
      python ./sorm.py --act=all --year=${1} --quarter=${2} --email=${3}

  docker run --rm \
  -v $(pwd)/cfg:/app/cfg \
  -v $(pwd)/result:/app/result \
  -v $(pwd)/log:/app/log \
  vadimkozin/bill /bin/bash
else
  echo $help
fi
