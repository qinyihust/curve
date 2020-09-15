#!/bin/bash

echo case 1
echo "sudo ./lfs_read_write_test -path /data/chunkserver0/chunkfilepool/ -threadNum 10 -iocount 100000 -logtostderr -aio -usebthread -useepool"
sudo ./lfs_read_write_test -path /data/chunkserver0/chunkfilepool/ -threadNum 10 -iocount 100000 -logtostderr -aio -usebthread -useepool >> result.txt 2>&1
echo
echo case 2
echo "sudo ./lfs_read_write_test -path /data/chunkserver0/chunkfilepool/ -threadNum 10 -iocount 100000 -logtostderr -aio -usebthread"
sudo ./lfs_read_write_test -path /data/chunkserver0/chunkfilepool/ -threadNum 10 -iocount 100000 -logtostderr -aio -usebthread >> result.txt 2>&1
echo
echo case 3
echo "sudo ./lfs_read_write_test -path /data/chunkserver0/chunkfilepool/ -threadNum 10 -iocount 100000 -logtostderr -aio -useepool"
sudo ./lfs_read_write_test -path /data/chunkserver0/chunkfilepool/ -threadNum 10 -iocount 100000 -logtostderr -aio -useepool >> result.txt 2>&1
echo
echo case 4
echo "sudo ./lfs_read_write_test -path /data/chunkserver0/chunkfilepool/ -threadNum 10 -iocount 100000 -logtostderr -aio"
sudo ./lfs_read_write_test -path /data/chunkserver0/chunkfilepool/ -threadNum 10 -iocount 100000 -logtostderr -aio >> result.txt 2>&1
echo
echo case 5
echo "for i in {0..9}; do sudo ./lfs_read_write_test -path /data/chunkserver$i/chunkfilepool/  -threadNum 10 -iocount 100000 -logtostderr -aio -usebthread -useepool & done"
for i in {0..9}; do sudo ./lfs_read_write_test -path /data/chunkserver$i/chunkfilepool/  -threadNum 10 -iocount 100000 -logtostderr -aio -usebthread -useepool >> result.txt 2>&1 & done
count=`ps -aux | grep lfs_read_write_test | grep -v grep | wc -l`
echo ${count}
while [ ${count} -ne 0 ]
do
    sleep 5
    echo count = ${count} ,sleep 5
    count=`ps -aux | grep lfs_read_write_test | grep -v grep | wc -l`
done
echo
echo case 6
echo "for i in {0..9}; do sudo ./lfs_read_write_test -path /data/chunkserver$i/chunkfilepool/  -threadNum 10 -iocount 100000 -logtostderr -aio -usebthread & done"
for i in {0..9}; do sudo ./lfs_read_write_test -path /data/chunkserver$i/chunkfilepool/  -threadNum 10 -iocount 100000 -logtostderr -aio -usebthread >> result.txt 2>&1 & done
count=`ps -aux | grep lfs_read_write_test | grep -v grep | wc -l`
echo ${count}
while [ ${count} -ne 0 ]
do
    sleep 5
    echo count = ${count} ,sleep 5
    count=`ps -aux | grep lfs_read_write_test | grep -v grep | wc -l`
done
echo
echo case 7
echo "for i in {0..9}; do sudo ./lfs_read_write_test -path /data/chunkserver$i/chunkfilepool/  -threadNum 10 -iocount 100000 -logtostderr -aio -useepool & done"
for i in {0..9}; do sudo ./lfs_read_write_test -path /data/chunkserver$i/chunkfilepool/  -threadNum 10 -iocount 100000 -logtostderr -aio -useepool >> result.txt 2>&1 & done
count=`ps -aux | grep lfs_read_write_test | grep -v grep | wc -l`
echo ${count}
while [ ${count} -ne 0 ]
do
    sleep 5
    echo count = ${count} ,sleep 5
    count=`ps -aux | grep lfs_read_write_test | grep -v grep | wc -l`
done
echo
echo case 8
echo "for i in {0..9}; do sudo ./lfs_read_write_test -path /data/chunkserver$i/chunkfilepool/  -threadNum 10 -iocount 100000 -logtostderr -aio & done"
for i in {0..9}; do sudo ./lfs_read_write_test -path /data/chunkserver$i/chunkfilepool/  -threadNum 10 -iocount 100000 -logtostderr -aio >> result.txt 2>&1 & done
count=`ps -aux | grep lfs_read_write_test | grep -v grep | wc -l`
echo ${count}
while [ ${count} -ne 0 ]
do
    sleep 5
    echo count = ${count} ,sleep 5
    count=`ps -aux | grep lfs_read_write_test | grep -v grep | wc -l`
done
echo
echo case 9
echo "sudo ./lfs_read_write_test -path /data/chunkserver0/chunkfilepool/ -threadNum 10 -iocount 100000 -logtostderr -usebthread"
sudo ./lfs_read_write_test -path /data/chunkserver0/chunkfilepool/ -threadNum 10 -iocount 100000 -logtostderr -usebthread >> result.txt 2>&1
echo
echo case 10
echo "sudo ./lfs_read_write_test -path /data/chunkserver0/chunkfilepool/ -threadNum 10 -iocount 100000 -logtostderr"
sudo ./lfs_read_write_test -path /data/chunkserver0/chunkfilepool/ -threadNum 10 -iocount 100000 -logtostderr >> result.txt 2>&1
echo
echo case 11
echo "for i in {0..9}; do sudo ./lfs_read_write_test -path /data/chunkserver$i/chunkfilepool/  -threadNum 10 -iocount 100000 -logtostderr  -usebthread & done"
for i in {0..9}; do sudo ./lfs_read_write_test -path /data/chunkserver$i/chunkfilepool/  -threadNum 10 -iocount 100000 -logtostderr  -usebthread >> result.txt 2>&1 & done
count=`ps -aux | grep lfs_read_write_test | grep -v grep | wc -l`
echo ${count}
while [ ${count} -ne 0 ]
do
    sleep 5
    echo count = ${count} ,sleep 5
    count=`ps -aux | grep lfs_read_write_test | grep -v grep | wc -l`
done
echo
echo case 12
echo "for i in {0..9}; do sudo ./lfs_read_write_test -path /data/chunkserver$i/chunkfilepool/  -threadNum 10 -iocount 100000 -logtostderr & done"
for i in {0..9}; do sudo ./lfs_read_write_test -path /data/chunkserver$i/chunkfilepool/  -threadNum 10 -iocount 100000 -logtostderr >> result.txt 2>&1 & done
count=`ps -aux | grep lfs_read_write_test | grep -v grep | wc -l`
echo ${count}
while [ ${count} -ne 0 ]
do
    sleep 5
    echo count = ${count} ,sleep 5
    count=`ps -aux | grep lfs_read_write_test | grep -v grep | wc -l`
done
echo
echo case 13
echo "for i in {0..19}; do sudo ./lfs_read_write_test -path /data/chunkserver$i/chunkfilepool/  -threadNum 10 -iocount 100000 -logtostderr -aio -usebthread -useepool & done"
for i in {0..19}; do sudo ./lfs_read_write_test -path /data/chunkserver$i/chunkfilepool/  -threadNum 10 -iocount 100000 -logtostderr -aio -usebthread -useepool >> result.txt 2>&1 & done
count=`ps -aux | grep lfs_read_write_test | grep -v grep | wc -l`
echo ${count}
while [ ${count} -ne 0 ]
do
    sleep 5
    echo count = ${count} ,sleep 5
    count=`ps -aux | grep lfs_read_write_test | grep -v grep | wc -l`
done
echo
echo case 14
echo "for i in {0..19}; do sudo ./lfs_read_write_test -path /data/chunkserver$i/chunkfilepool/  -threadNum 10 -iocount 100000 -logtostderr -aio -usebthread & done"
for i in {0..19}; do sudo ./lfs_read_write_test -path /data/chunkserver$i/chunkfilepool/  -threadNum 10 -iocount 100000 -logtostderr -aio -usebthread >> result.txt 2>&1 & done
count=`ps -aux | grep lfs_read_write_test | grep -v grep | wc -l`
echo ${count}
while [ ${count} -ne 0 ]
do
    sleep 5
    echo count = ${count} ,sleep 5
    count=`ps -aux | grep lfs_read_write_test | grep -v grep | wc -l`
done
echo
echo case 15
echo "for i in {0..19}; do sudo ./lfs_read_write_test -path /data/chunkserver$i/chunkfilepool/  -threadNum 10 -iocount 100000 -logtostderr -aio -useepool & done"
for i in {0..19}; do sudo ./lfs_read_write_test -path /data/chunkserver$i/chunkfilepool/  -threadNum 10 -iocount 100000 -logtostderr -aio -useepool >> result.txt 2>&1 & done
count=`ps -aux | grep lfs_read_write_test | grep -v grep | wc -l`
echo ${count}
while [ ${count} -ne 0 ]
do
    sleep 5
    echo count = ${count} ,sleep 5
    count=`ps -aux | grep lfs_read_write_test | grep -v grep | wc -l`
done
echo
echo case 16
echo "for i in {0..19}; do sudo ./lfs_read_write_test -path /data/chunkserver$i/chunkfilepool/  -threadNum 10 -iocount 100000 -logtostderr -aio & done"
for i in {0..19}; do sudo ./lfs_read_write_test -path /data/chunkserver$i/chunkfilepool/  -threadNum 10 -iocount 100000 -logtostderr -aio >> result.txt 2>&1 & done
count=`ps -aux | grep lfs_read_write_test | grep -v grep | wc -l`
echo ${count}
while [ ${count} -ne 0 ]
do
    sleep 5
    echo count = ${count} ,sleep 5
    count=`ps -aux | grep lfs_read_write_test | grep -v grep | wc -l`
done
echo
echo case 17
echo "for i in {0..19}; do sudo ./lfs_read_write_test -path /data/chunkserver$i/chunkfilepool/  -threadNum 10 -iocount 100000 -logtostderr  -usebthread & done"
for i in {0..19}; do sudo ./lfs_read_write_test -path /data/chunkserver$i/chunkfilepool/  -threadNum 10 -iocount 100000 -logtostderr  -usebthread >> result.txt 2>&1 & done
count=`ps -aux | grep lfs_read_write_test | grep -v grep | wc -l`
echo ${count}
while [ ${count} -ne 0 ]
do
    sleep 5
    echo count = ${count} ,sleep 5
    count=`ps -aux | grep lfs_read_write_test | grep -v grep | wc -l`
done
echo
echo case 18
echo "for i in {0..19}; do sudo ./lfs_read_write_test -path /data/chunkserver$i/chunkfilepool/  -threadNum 10 -iocount 100000 -logtostderr & done"
for i in {0..19}; do sudo ./lfs_read_write_test -path /data/chunkserver$i/chunkfilepool/  -threadNum 10 -iocount 100000 -logtostderr >> result.txt 2>&1 & done
count=`ps -aux | grep lfs_read_write_test | grep -v grep | wc -l`
echo ${count}
while [ ${count} -ne 0 ]
do
    sleep 5
    echo count = ${count} ,sleep 5
    count=`ps -aux | grep lfs_read_write_test | grep -v grep | wc -l`
done
echo