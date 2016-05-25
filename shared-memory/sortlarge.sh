cd /home/ec2-user/storage/

java -Xmx28g Sorting /home/ec2-user/storage/large/ $1 $2

#combine all files into one
cd /home/ec2-user/storage/large/final
for i in `seq 0 284`;
do
   cat $i.txt >> result.txt
done 


