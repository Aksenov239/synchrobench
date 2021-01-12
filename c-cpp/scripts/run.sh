#!/bin/bash

dir=..
output=${dir}/output
bin=${dir}/bin

threads="1 2 4 8 14 12 17 26 35 44 53 62 71"
sizes="25 100 1000 10000"

writes="0" # 20 100"
#writes="100"
duration="10000"
warmup="5"
snapshot="0"
writeall="0"
iterations="5"

if [ ! -d "${output}" ]; then
  mkdir $output
fi
if [ ! -d "${output}/log" ]; then
#  rm -rf ${output}/log
  mkdir ${output}/log
fi

for write in ${writes}; do
  for t in ${threads}; do
     for i in ${sizes}; do
       r=$((2*${i}))

#       rm "${output}/log/co-list-v2-${t}-${i}-${write}"
#       rm "${output}/log/lazy-list-v2-${t}-${i}-${write}"

       for (( j=1; j<=${iterations}; j++ )); do
         echo "CO List v2 ${write} ${t} ${i} ${j}"
         out="${output}/log/co-list-v2-${t}-${i}-${write}"
         numactl --interleave=all ${bin}/MUTEX-co-list-v2 -d ${duration} -i ${i} -r ${r} -t ${t} -u ${write} 2>&1 >> ${out}

         echo "Lazy List v2 ${write} ${t} ${i} ${j}"
         out="${output}/log/lazy-list-v2-${t}-${i}-${write}"
         numactl --interleave=all ${bin}/MUTEX-lazy-list-v2 -d ${duration} -i ${i} -r ${r} -t ${t} -u ${write} 2>&1 >> ${out}

         echo "Harris-Michael ${write} ${t} ${i} ${j}"
         out="${output}/log/harris-${t}-${i}-${write}"
         numactl --interleave=all ${bin}/lockfree-linkedlist -d ${duration} -i ${i} -r ${r} -t ${t} -u ${write} 2>&1 >> ${out}
       done
     done
  done
done
