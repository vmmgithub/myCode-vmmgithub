
echo "Setting up npm libraries ... "
npm install
sudo npm install -g jake

echo "Setting up aliases ... "

if [[ -z $1 ]]
then
HM='/data_raid10/software/Implementations/data_services'
fi

cat << EOF >> ~./bash_profile
export HM=${HM}
export AVA_HOME='\$HM/scripts/api_scripts/linker'

alias l='ls -altr';
alias ava='cd $AVA_HOME';
alias c='clear';
alias gs='git status -s';
alias ga='git add ';
alias gc='git checkout ';
alias gpl='git pull';
alias gps='git push';

alias ll='ls -rlt|more'
alias tl='tail  -f $1'

alias shs='cd \$HM/scripts/api_scripts/common/sh'
alias sjs='cd \$HM/scripts/api_scripts/common/js'
alias tnat='cd \$HM/scripts/api_scripts/tenants;cd $1'
alias jslib='cd \$HM/scripts/api_scripts/lib/helpers'
alias indir='cd \$HM/data/input'

alias dlog='cd \$HM/data/dev/logs'
alias darc='cd \$HM/data/dev/archive'
alias dsnap='cd \$HM/data/dev/snapshot'

alias slog='cd \$HM/data/stg/logs'
alias sarc='cd \$HM/data/stg/archive'
alias ssnap='cd \$HM/data/stg/snapshot'

alias plog='cd \$HM/data/prd/logs'
alias parc='cd \$HM/data/prd/archive'
alias psnap='cd \$HM/data/prd/snapshot'
EOF

echo "Setting up directories ... "
cd $HM

mkdir -p data/prd/logs
mkdir -p data/prd/archive
mkdir -p data/prd/snapshot

mkdir -p data/dev/logs
mkdir -p data/dev/archive
mkdir -p data/dev/snapshot

mkdir -p data/stg/logs
mkdir -p data/stg/archive
mkdir -p data/stg/snapshot

mkdir -p data/input/tenants