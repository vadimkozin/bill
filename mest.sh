#!/bin/sh
source ./func.sh

# $1=year, $2=month

echo $1 $2

# current year and month
year=$(date +%Y)
month=$(echo "$(date +%m)" | sed 's/^0*//')  # cuts out the leading zero, 05 -> 5

delimiter="---------------------------------------"

help="${delimiter}\nBilling city calls for period\nused: $0 year month\nexample: $0 ${year} ${month}\n${delimiter}"

if [ -n "$1" ] && [ -n "$2" ]
then
  if ! isYearValid $1 ;then
    echo "year: '${1}' invalid, must be: 2022, 2023, .."
    exit 1
  fi
  if ! isMonthValid $2 ;then
    echo "month: '${2}' invalid, must be: 1,2,3,..12"
    exit 1
  fi

#  DATA=$(pwd)/data
#  makeDir $DATA


#  docker run --rm -v $(pwd)/cfg:/app/cfg -v $DATA:/app/data vadimkozin/sorm \
#      python ./sorm.py --act=all --year=${1} --quarter=${2} --email=${3}
  /Users/vadim/PycharmProjects/bill/venv/bin/python /Users/vadim/PycharmProjects/bill/mest.py --year=${1} --month=${2}

else
  echo $help
  exit 1
fi

