cd /home/ec2-user/storage/

java -classpath . Sorting /home/ec2-user/storage/small/ $1 $2

#combine all files into one
cd /home/ec2-user/storage/small/final
for i in `seq 0 284`;
do
   cat $i.txt >> result.txt
done 

