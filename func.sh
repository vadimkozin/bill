#!/bin/bash

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
