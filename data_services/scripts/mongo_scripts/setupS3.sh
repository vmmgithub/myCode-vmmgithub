echo ". scripts/aliases" >> .bash_profile

source /home/ec2-user/userdata
cd /
sudo wget http://$MASTER/packages/S3Cmd.tar.gz
sudo tar xvfz S3Cmd.tar.gz 
sudo chown ec2-user:ec2-user /home/ec2-user/.s3cfg 

cd
mkdir scripts
mkdir reports
mkdir temp

cd scripts
s3cmd sync -r s3://Renew-Dev-Workspace/nithin/scripts/ .

cd ../reports
echo "s3cmd sync -r . s3://Renew-Dev-Workspace/nithin/reports/ >> sync.log" > upload.sh
chmod +x upload.sh

cd ../temp
#s3cmd sync -r s3://Renew-Dev-Workspace/nithin/temp/ . >> sync.log
