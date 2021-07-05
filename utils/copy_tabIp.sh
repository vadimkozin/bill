#!/bin/sh
mysqldump --column-statistics=0 -h192.168.200.201 -ubilling -pxxx tabIP  | mysql -h192.168.200.201 -uwebrss -pwebrss459 webrss
