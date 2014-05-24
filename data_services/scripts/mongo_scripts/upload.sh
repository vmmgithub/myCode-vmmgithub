if [[ $1 == "force" ]]
then
s3cmd sync -r --delete-removed . s3://Renew-Dev-Workspace/nithin/scripts/ >> sync.log
else
s3cmd sync -r . s3://Renew-Dev-Workspace/nithin/scripts/ >> sync.log
fi
