# 1 thread 1G
cd /home/ec2-user/storage
time ./sortsmall.sh 1 20
time ./sortsmall.sh 8 1

#remove auxiliary files
cd /home/ec2-user/storage/small/final
for i in `seq 0 284`;
do
   rm $i.txt 
done 

cd /home/ec2-user/storage/small
for i in `seq 0 284`;
do
   rm -r $i/
done 
#varifies result
cd /home/ec2-user/storage/small/final
unix2dos result.txt

cd /home/ec2-user/storage/64
./valsort /home/ec2-user/storage/small/final/result.txt

cd /home/ec2-user/storage/small/
rm -r final/

#begin sort 1T data


# 1T 1 thread
cd /home/ec2-user/storage
time ./sortlarge.sh 4 250

#remove auxiliary files
cd /home/ec2-user/storage/large/final
for i in `seq 0 284`;
do
   rm $i.txt 
done 

cd /home/ec2-user/storage/large
for i in `seq 0 284`;
do
   rm -r $i/
done 

#varifies result
cd /home/ec2-user/storage/large/final
unix2dos result.txt

cd /home/ec2-user/storage/64
./valsort /home/ec2-user/storage/large/final/result.txt

cd /home/ec2-user/storage/large
rm -r final/

#create output
cd /home/ec2-user/
head -10 /home/ec2-user/storage/large/final/0.txt > terasort-shared-memory-1TB.txt
tail -10 /home/ec2-user/storage/large/final/0.txt >> terasort-shared-memory-1TB.txt
