#!/bin/bash
source ./func.sh

echo $1 $2 $3 $4

#  $1=year $2=month $3=action [$4=reset]
# used:    ./run.sh year month action [reset]
# actions: load | bill | mts | local | mest
# example: ./run.sh 2022 4 load [--reset]
# ps. --reset only for action: load

YEAR=$1
MONTH=$2
ACTIONS="load bill mts local mest"
ACTION=$(echo $3 | tr '[:upper:]' '[:lower:]')
SCRIPT="${ACTION}.py"

RESET=$(echo $4 | tr '[:upper:]' '[:lower:]')

# current billing year and month
CURRENT_YEAR=$(date +%Y)
CURRENT_MONTH=$(echo "$(date +%m)" | sed 's/^0*//')  # cuts out the leading zero, 05 -> 5
CURRENT_MONTH=$((CURRENT_MONTH-1))
if [ "${CURRENT_MONTH}" = "0" ] ; then
  CURRENT_YEAR=$((CURRENT_YEAR-1))
  CURRENT_MONTH=12
fi

DELIMITER="------------------------------------------"
HELP="${DELIMITER}
## Phone billing ##\nused: $0 year month action [--reset]
year: 2022, 2023, ...\nmonth: 1,2...12
actions: ${ACTIONS}
example: $0 ${CURRENT_YEAR} ${CURRENT_MONTH} load [--reset]
ps. --reset only for action load.
${DELIMITER}"

IMAGE=vadimkozin/bill

if [ -n "$ACTION" ] && [ -n "$YEAR" ] && [ -n "$MONTH" ] ; then
  action_exist=false
  for item in $ACTIONS
  do
    if [ $item = $ACTION ] ; then
      action_exist=true
      break
    fi
  done

  if [ "$action_exist" != true ] ; then
    echo -e "action: '${ACTION}' invalid,  must be one from: '${ACTIONS}'"
    exit 1
  fi

  if ! isYearValid $YEAR ;then
    echo "year: '${YEAR}' invalid, must be: 2022, 2023, .."
    exit 1
  fi

  if ! isMonthValid $MONTH ;then
    echo "month: '${MONTH}' invalid, must be: 1,2,3,..12"
    exit 1
  fi

  RESULT=$(pwd)/result
  LOG=$(pwd)/log
  CFG=$(pwd)/cfg
  makeDir $RESULT
  makeDir $LOG
  makeDir $CFG

  docker pull -q $IMAGE

  old_images=`docker images | grep $IMAGE | grep none | awk '{print $3}'`

  if [ -n "${old_images}" ] ; then
    echo "delete old image(s): ${old_images}"
    docker rmi ${old_images}
  fi

  docker run --rm \
    -v $CFG:/app/cfg \
    -v $RESULT:/app/result \
    -v $LOG:/app/log \
    ${IMAGE} \
    python ./${SCRIPT} --year=${YEAR} --month=${MONTH} ${RESET}
 
else
  echo -e "${HELP}"
  exit 1
fi

exit 0


