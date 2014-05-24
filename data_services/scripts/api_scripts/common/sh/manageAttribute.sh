#!/bin/bash 
#*************************************************************************#
#                                                                         #
#   Script  Name   : manageAttribute.sh                                   #
#   Author  Name   : vmahadik                                             #
#   Date           : Nov-02-2013                                          #
#   Date           : Nov-02-2013                                          #
#   Purpose        : Provide user interface to manageAttributes.js script #
#   Input File     : Data file contains two fields Name,Field Name        #
#                                                                         #
#                                                                         #
#   Change :                                                              #
#   Purpose        :                                                      #
#   Author         :                                                      #
#   Date           :                                                      #
#                                                                         #
#                                                                         #
#*************************************************************************#

LIB_DIR="../../lib/helpers"
SCRIPT_DIR="../js"
INPUT_DIR="../../../../data/input"
OUTPUT_DIR=""
LOG_DIR=""
SNAPSHOT_DIR=""
ARCHIVE_DIR=""

SCRIPT_NAME='manageAttributes.js'
USER_NAME=$USER

NORMAL=`echo "\033[m"`
BLUE=`echo "\033[36m"`        # Blue
NUMBER=`echo "\033[33m"`      # Yellow
FGRED=`echo "\033[41m"`
RED_TEXT=`echo "\033[31m"`    # Red
ENTER_LINE=`echo "\033[35m"`  # Purple


function option_picked() {
    COLOR='\033[01;31m' # bold red
    RESET='\033[00;00m' # normal white
    MESSAGE=${@:-"${RESET}Error: No message passed"}
    echo -e "${COLOR}${MESSAGE}${RESET}"
}

####   Environment details 

#  DEV = config-t2.ssi-cloud.com OR config.ssi-cloud.com
#  STG = stgcurrent.ssi-cloud.com OR stgnext.ssi-cloud.com
#  PRD = prod02dl-int.ssi-cloud.com

#  Dell 
#  DEV = dell-sit.ssi-cloud.com
#  STG = dellstgcurrent.ssi-cloud.com OR dellstgnext.ssi-cloud.com
#  PROD = dell-prd1DL4-int.ssi-cloud.com

#### Find Environment using host name

function env_setup() {
   if  [[ $host = "prod02dl-int.ssi-cloud.com" ]] || [[ $host = "dell-prd1DL4-int.ssi-cloud.com" ]] || [[ $host = "prod02-api.ssi-cloud.com" ]] || [[ $host = "prod02dl-api.ssi-cloud.com" ]] ; then
             OUTPUT_DIR="../../../../data/prd"
       elif [[ $host = "dellstgcurrent.ssi-cloud.com" ]] || [[ $host = "dellstgnext.ssi-cloud.com" ]] || [[ $host = "stgcurrent.ssi-cloud.com" ]] || [[ $host = "stgnext.ssi-cloud.com" ]]; then
             OUTPUT_DIR="../../../../data/stg"
   else [[ $host = "config-t2.ssi-cloud.com" ]] || [[ $host = "config.ssi-cloud.com" ]] || [[ $host = "dell-sit.ssi-cloud.com" ]] || [[ $host = "uat02.ssi-cloud.com" ]]
            OUTPUT_DIR="../../../../data/dev"
  fi
 
   LOG_DIR="$OUTPUT_DIR/logs"
   SNAPSHOT_DIR="$OUTPUT_DIR/snapshot"
   ARCHIVE_DIR="$OUTPUT_DIR/archive"
 }

function accept_parameter() {
      clear
      echo  -e "${BLUE}$STAR_LINE ${NORMAL}"
      echo  ""
      echo  -e "${BLUE}             Running $SCRIPT_NAME         ${NORMAL}" 
      echo  ""
      echo  -e "${BLUE}$STAR_LINE ${NORMAL}"
      echo  ""
      TITLE="Please press <Enter> to accept default value..."
      echo  -e "${BLUE}$TITLE         ${NORMAL}" 
      echo  ""
      echo  ""
      TITLE=""
      echo -e -n "${RED_TEXT} Tenant (Default:dell) :  ${NORMAL}"
      read  tenant  
      if [[ $tenant = "" ]]; then 
      	tenant='dell'
      fi 

      echo -e -n "${RED_TEXT} Host Name (Default:dellstgcurrent.ssi-cloud.com) :  ${NORMAL} "
      read host  
      if [[ $host = "" ]]; then 
      	host='dellstgcurrent.ssi-cloud.com'
      fi 

      env_setup

      echo -e -n "${RED_TEXT} Port (Default:443) :  ${NORMAL}"
      read port  
      
      echo -e -n "${RED_TEXT} User Name (Default:xyz@${tenant}.com) :  ${NORMAL}"
      read user_name  
      
     echo -e -n "${RED_TEXT} Password      :  ${NORMAL}"
     read -s password  
     echo 

      echo -e -n "${RED_TEXT} File Name To Process :  ${NORMAL}"
      read file_name  
      if [[ $file_name = "" ]]; then 
         echo;echo;echo 
         option_picked 'Invalid File Name, restarting again  ...'	
         sleep 5
         accept_parameter
      fi 

      echo -e -n "${RED_TEXT} Multiple Yes/No [Default: No ] :  ${NORMAL}"
      read multiple  
      if [[ $multiple = [Yy][Ee][Ss] ]]; then
         multiple='true'
      else 
      	 multiple='false'
      fi 
      # echo "multiple :  $multiple"
      
      echo -e -n "${RED_TEXT} Concurrent ThreadLimit [Default:1 , max=5] :  ${NORMAL}"
      read limit  
      if [[ $limit = ""  ]]; then 
      	 limit=1
      fi 

      echo -e -n "${RED_TEXT} Source Type [Default:app.opportunity] :  ${NORMAL}"
      read sourceType  
      if [[ $sourceType = ""  ]]; then 
      	 sourceType='app.opportunity'
      fi 

      echo -e -n "${RED_TEXT} Search By Source Type [Default:_id, displayName, externalId] :  ${NORMAL}"
      read srcSearchBy  
      if [[ $srcSearchBy = ""  ]]; then 
      	 srcSearchBy="_id"
      fi 

      echo -e -n "${RED_TEXT} Field to modify. Supports dot notation [Example:amount.amount] :  ${NORMAL}"
      read field  
      if [[ $field = "" ]]; then 
      	 field="_id"
      fi 

      schemeName=''
      if [[ $field = "externalIds.id" ]]; then 
      	echo -e -n "${RED_TEXT} Enter Scheme Name [Default:''] :  ${NORMAL}"
      	read schemeName
      fi 

      echo -e -n "${RED_TEXT} Datatype [Default:String, boolean, date, number] :  ${NORMAL}"
      read datatype  
      if [[ $datatype = "" ]]; then 
      	 datatype=String
      fi 

      echo -e -n "${RED_TEXT} Operation  [Default:update, delete] :  ${NORMAL}"
      read operation  
      if [[ $operation = ""  ]]; then 
      	 operation="log"
      fi 

      outFile=${tenant}.${file_name}.${operation}.`date +%Y%m%d%H%M%S`
    
      
      Cmd="$SCRIPT_DIR/$SCRIPT_NAME --tenant $tenant   
                                    --host $host 
                                    --port $port   
                                    --user $user_name  
                                    --password $password  
                                    --file $INPUT_DIR/$file_name  
                                    --multiple $multiple 
				    --limit $limit 
				    --source $sourceType 
				    --searchBy  $srcSearchBy  
                                    --schemeName $schemeName
				    --field $field 
				    --datatype  $datatype  
				    --operation $operation
				     >> $LOG_DIR/$outFile"

      if [[ $schemeName = "" ]]; then
      FinalCmd="nohup $SCRIPT_DIR/$SCRIPT_NAME --tenant $tenant   --host $host --port $port   --user $user_name  --password $password  --file $INPUT_DIR/$file_name  --multiple $multiple --limit $limit --source $sourceType  --searchBy  $srcSearchBy --field $field --datatype $datatype --operation $operation >> $LOG_DIR/$outFile &"
     else
      FinalCmd="nohup $SCRIPT_DIR/$SCRIPT_NAME --tenant $tenant   --host $host --port $port   --user $user_name  --password $password  --file $INPUT_DIR/$file_name  --multiple $multiple --limit $limit --source $sourceType  --searchBy  $srcSearchBy  --schemeName $schemeName --field $field --datatype $datatype --operation $operation >> $LOG_DIR/$outFile &"
     fi

      echo
      echo
      echo -e -n "${RED_TEXT}Command ==> $Cmd ${NORMAL}"
      echo " "
      echo " "
      
      echo -n -e "${BLUE} Please review your command before executing  [Enter : Yes] to proceed ... ${NORMAL}"
      
      read ans

      if [[ $ans = [Yy][Ee][Ss] ]]; then
         
         echo "$FinalCmd" > $LOG_DIR/$outFile
         ${FinalCmd} >> $LOG_DIR/$outFile
      
      	 echo;echo;echo 
     	 echo -e -n "${RED_TEXT} Please review : $LOG_DIR/$outFile.Failed and $ARCHIVE_DIR/$outFile.Processed ${NORMAL}"

         if [[ $operation != [Ll][Oo][Gg] ]] ; then
      	 	grep "FAILED" $LOG_DIR/$outFile | sed 's/FAILED ://g' > $LOG_DIR/${outFile}.Failed
      	 	grep "PROCESSED" $LOG_DIR/$outFile | sed -e 's/PROCESSED ://g' > $ARCHIVE_DIR/${outFile}.Processed
         fi

      	 grep "EXISTING" $LOG_DIR/$outFile | sed -e 's/EXISTING ://g' > $SNAPSHOT_DIR/${outFile}.Existing
      	 echo;echo;echo 

       fi
       exit   
}

# Options:Available for running script
#   -t, --tenant     Specify tenant                                                            [required]
#   -h, --host       Specify host                                                              [required]
#   -n, --port       Specify port                                                              [default: "443"]
#   -u, --user       Specify user                                                              [required]
#   -p, --password   Specify password                                                          
#   -f, --file       File to process                                                           [required]
#   -m, --multiple   Flag to indicate if updating all matching records or just the first       [default: false]
#   -l, --limit      Concurrent threads                                                        [default: 5]
#   -s, --source     Source type                                                               [default: "app.opportunity"]
#   -b, --searchBy   Search by attribute [_id, displayName, externalId]                        [default: "_id"]
#   -c, --schemeName  Name for scheme Id Ex. Dataload, Batchload                               [default: ""]
#   -e, --field      Field to modify. Supports dot notation                                    [default: "displayName"]
#   -d, --datatype   Data Type of the value being set ["boolean", "date", "string", "number"]  [default: "string"]
#   -o, --operation  Operation to perform [update, delete]                                     [default: "update"]

accept_parameter;
exit
