tenant="$1";
logs="logs"

if [[ -z $tenant ]]
then
echo "Usage: $0 <tenant>"
exit 1
fi

ps -eaf | grep node | grep $tenant | awk '{ print $2 }' | xargs kill -9

if [[ -z $2 ]]
then
touch "${logs}/$1.stop"
fi
