#!/usr/bin/env sh
rm /tmp/logdata
touch /tmp/logdata
tail -f /tmp/logdata | nc -lk 7777 &
TAIL_NC_PID=$!
cat ./log1.log >> /tmp/logdata
sleep 5
cat ./log2.log >> /tmp/logdata
sleep 1
cat ./log1.log >> /tmp/logdata
sleep 10
cat ./log1.log >> /tmp/logdata
sleep 3
cat ./log2.log >> /tmp/logdata
cat ./log1.log >> /tmp/logdata
cat ./log2.log >> /tmp/logdata
sleep 4
cat ./log2.log >> /tmp/logdata
cat ./log1.log >> /tmp/logdata
sleep 20
kill $TAIL_NC_PID

