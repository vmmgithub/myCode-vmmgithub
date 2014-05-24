cluster=$1
host=`curl http://master-us-east-1b-vpc-94c04efc.int.ssi-cloud.com/nginx-asg/as-app-production-${cluster}/nodeJS.conf 2>/dev/null | grep 'server' | tail -1 | cut -d'#' -f2`
echo "ssh ${host}.int.ssi-cloud.com"
