#!/bin/bash

max=$1
if [[ $max == "" ]]
then
  max=128
fi

killall lua51
sleep 1

for (( n=1;n<=$max;n++ ))
do
  rm $n.log > /dev/null 2>&1
lua51 ./tkelips_resil.lua $n $max > $n.log 2>&1 &
done