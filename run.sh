#!/bin/bash

echo "$1 $2 $3"

# $1=year $2=month $3=action
# used:    ./run.sh year month action
# actions: info | load | bill | mts | local | mest | all
# example: ./run.sh 2022 6 load
# example: ./run.sh 2022 6 all  // all billing: load, bill, mts, local, mest

# [setting] ######################
YEAR=$1
MONTH=$2
ACTIONS="info load bill mts local mest all"
ACTION=$(echo "$3" | tr '[:upper:]' '[:lower:]')
SCRIPT="${ACTION}.py"

# current billing year and month
BILL_YEAR=$(date +%Y)
BILL_MONTH=$(date +%m | sed 's/^0*//')  # cuts out the leading zero, 05 -> 5

BILL_MONTH=$((BILL_MONTH-1))
if [ "${BILL_MONTH}" = "0" ] ; then
  BILL_YEAR=$((BILL_YEAR-1))
  BILL_MONTH=12
fi

DELIMITER="------------------------------------------"
HELP="${DELIMITER}
## Phone billing ##
used: $0 year month action
year: 2022, 2023, ...
month: 1,2...12
actions: ${ACTIONS}
example: $0 ${BILL_YEAR} ${BILL_MONTH} load | bill | mts | local | mest
example: $0 ${BILL_YEAR} ${BILL_MONTH} all  -- all billing
${DELIMITER}"

IMAGE=vadimkozin/bill

# [functions] ######################
function isEmailValid() {
  re="^([A-Za-z]+[A-Za-z0-9]*((\.|\-|\_)?[A-Za-z]+[A-Za-z0-9]*){1,})@(([A-Za-z]+[A-Za-z0-9]*)+((\.|\-|\_)?([A-Za-z]+[A-Za-z0-9]*)+){1,})+\.([A-Za-z]{2,})+$"
  [[ "${1}" =~ $re ]]
}

function isYearValid() {
  re="^202[0-9]$"
  [[ "${1}" =~ $re ]]
}

function isInteger() {
  re='^[0-9]+$'
  [[ "${1}" =~ $re ]]
}

function isMonthValid() {
  isInteger "$1" && [ "$1" -ge "1" ] && [ "$1" -le "12" ]
}

function makeDir() { if [ ! -d "$1" ]; then mkdir "$1";fi }


# [code] ######################

if [ -n "$ACTION" ] && [ -n "$YEAR" ] && [ -n "$MONTH" ] ; then
  action_exist=false
  for item in $ACTIONS
  do
    if [ "$item" = "$ACTION" ] ; then
      action_exist=true
      break
    fi
  done

  if [ "$action_exist" != true ] ; then
    echo -e "action: '${ACTION}' invalid,  must be one from: '${ACTIONS}'"
    exit 1
  fi

  if ! isYearValid "$YEAR" ;then
    echo "year: '${YEAR}' invalid, must be: 2022, 2023, .."
    exit 1
  fi

  if ! isMonthValid "$MONTH" ;then
    echo "month: '${MONTH}' invalid, must be: 1,2,3,..12"
    exit 1
  fi

  RESULT=$(pwd)/result
  LOG=$(pwd)/log
  CFG=$(pwd)/cfg
  makeDir "$RESULT"
  makeDir "$LOG"
  makeDir "$CFG"

  docker pull -q $IMAGE

  old_images=$(docker images | grep $IMAGE | grep none | awk '{print $3}')

  if [ -n "${old_images}" ] ; then
    echo "delete old image(s): ${old_images}"
    docker rmi ${old_images}
  fi

  docker run --rm \
    -v "${CFG}":/app/cfg \
    -v "${RESULT}":/app/result \
    -v "${LOG}":/app/log \
    ${IMAGE} \
    python ./"${SCRIPT}" --year="${YEAR}" --month="${MONTH}"
 
else
  echo -e "${HELP}"
  exit 1
fi

exit 0


