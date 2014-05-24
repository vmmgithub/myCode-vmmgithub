curr=`pwd`
npm install underscore async api_request colors
source /home/ec2-user/userdata
cd /
sudo wget http://$MASTER/packages/S3Cmd.tar.gz
sudo tar xvfz S3Cmd.tar.gz 
sudo chown ec2-user:ec2-user /home/ec2-user/.s3cfg 
cd $curr
