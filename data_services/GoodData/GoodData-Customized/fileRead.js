var fs = require("fs");
var line;
var csvFile;
var writeStream;
var createdOn = new Date();
var lastModifiedOn = new Date();
var head = '';
var fileIndex=1;
var jsonPathsFromFile;
lineCounter=0;
var tenant = '';
exports.convertToCsv = function convertToCsv(inFile,outFile,entity,logFolder,dateConfig,tenent)
{
tenant = tenent;
jsonPathsFromFile=new Array();
createdOn.setFullYear(1723,10,3);
lastModifiedOn.setFullYear(1723,10,3);
//console.log(dateConfig+" - - "+fs.exists('Config/dateLog_'+dateConfig+'.config'));
fs.exists(tenent+'/internal/'+dateConfig+'.config',function(e){
   if(e){    
 var dates = fs.readFileSync(tenent+'/internal/'+dateConfig+'.config','utf-8').replace('\n','').split(',');
         if(dates[0]!='')
                {
                                createdOn = new Date(dates[0]);
                                lastModifiedOn = new Date(dates[1]);
                }}
});

csvFile = outFile;

var header = getPathsAndHeader(tenent+'/internal/'+entity+'.map');
var logStream = fs.createWriteStream(logFolder, {flags: 'a'});
        writeStream = fs.createWriteStream(csvFile+'.csv', {flags: 'w'});
        writeStream.write(header);

    var stream = fs.createReadStream(inFile, {
      flags: 'r',
      encoding: 'utf-8',
      fd: null,
      mode: 0666,
      bufferSize: 1024
    });

        var temp='';
        var start=new Date();
    var fileData;
    stream.on('data', function(data){
      stream.pause();

          fileData = data.split('\n');
                if(temp!='')
                {
                        fileData[0] = temp+fileData[0];
                        temp='';
                }
                if(data.match("\n"+"$") != "\n")
                {
                        temp = fileData[fileData.length-1];
                        delete fileData[fileData.length-1]
                }
//console.log(fileData);
                transFormData(fileData);
      stream.resume();
        });

    stream.on('error', function(){

    });

    stream.on('end', function(){
                fs.writeFileSync(tenent+'/internal/dateLog_'+dateConfig+'.config',createdOn.toISOString()+','+lastModifiedOn.toISOString());
      var logText = "\n\n===============================================================";
          logText += "\nStatus Report From CSV Conversion : "+dateConfig;
          logText += "\n===============================================================";
          logText += "\nNumber Of Error :"+errors+"\nTotal Good Records :"+goodRecords;
          logText += "\nTime Taken :"+(new Date() - start)/1000+" seconds";
          var timeTaken = (new Date() - start)/1000;
          logText+="\nExecution Speed :"+goodRecords/timeTaken;
          logText += "\nCreated On : "+createdOn+", Last Modified On : "+lastModifiedOn+'\n\n';
          logStream.write(logText);
    });

}
/*function getPathsAndHeader(filename)
{
head='';
        var file = fs.readFileSync(filename, 'utf-8');
        file.split("\n").forEach(function(line){
                var temp = line.split("==");
if(tenant == "ibm" && (temp[0]=="ContractType" || temp[0]=="ServiceProduct")){
                head += '"'+temp[0]+'",';
                jsonPathsFromFile[jsonPathsFromFile.length] = temp[1];}
else {
                head += '"'+temp[0]+'",';
                jsonPathsFromFile[jsonPathsFromFile.length] = temp[1];
}
        });
        return head.slice(0,head.length-1)+"\n";
}*/
function getPathsAndHeader(filename)
{
head='';
        var file = fs.readFileSync(filename, 'utf-8');
        file.split("\n").forEach(function(line){
                var temp = line.split("==");
				/*if((temp[0]=="CustomField1" || temp[0]=="CustomField2")){
					if((tenant == "ibm" || tenant == "juniper")){
						head += '"'+temp[0]+'",';
						jsonPathsFromFile[jsonPathsFromFile.length] = temp[1];
					}
					else
					{
						head += '"'+temp[0]+'",';
                                                jsonPathsFromFile[jsonPathsFromFile.length] = '';
					}
}
				else
				{*/
					head += '"'+temp[0]+'",';
					jsonPathsFromFile[jsonPathsFromFile.length] = temp[1];
				//}
        });
        return head.slice(0,head.length-1)+"\n";
}
function transFormData(list)
{

        list.forEach(function(item) {
                                try {

                                        if(item!=''&&item.length>4){
                                                if(item.match(","+"$") == ",")
                                                {
                                                        record = JSON.parse(item.replace(/.$/g, ''));

                                                }
                                                else{
                                                        record = JSON.parse(item);
                                                }
                                          goodRecords++;

                                            line='"';
                                                getRelationIndex(record["relationships"]);
                                                line='"';
                                                checkDates(record["systemProperties"]);
                                                jsonPathsFromFile.forEach(function(head){
			                        if(head!==undefined)
                                                {
							if(head=="")
							line+='","';
							else{
switch(head)
{
	case "FirstContactDate":
		line += getToStateChangeDate(record,"contacted")+'","';
		break;
	case "SalesStageOrder":
		line += salesTage(record)+'","';
		break;
	case "FirstQuoteDate":
		line += getToStateChangeDate(record,"quoted")+'","';
		break;
	case "BookingDate":
		line += getToStateChangeDate(record,"closedSale")+'","';
		break;
	default :
		line += getValueFromJsonPath(record, head.replace("\r",''))+'","';
		break;
}
}
	                                            }
                                                });
                                                line = line.replace(/undefined/g, '').replace(/.$/g, '').replace(/.$/g, '').replace(/null/g,'').replace(/Null/g,'')+'\n';     
						writeStream.write(line);
                                                line='';
                                                lineCounter++;
                                                /*if(lineCounter>=100000)
                                                {
                                                        fileIndex++;
                                                        writeStream = fs.createWriteStream(csvFile+'_'+fileIndex+'.csv', {flags: 'w'});
                                                        writeStream.write(head+"\n");
                                                        lineCounter=0;
                                                }*/
                                        }
                                }
                                catch (e) {
//console.log(e.stack);                                
}
        });
}
function checkDates(data)
{
        try{
                if(new Date(data.createdOn)>createdOn)
                {
                        createdOn = new Date(data.createdOn);
                }
                if(new Date(data.lastModifiedOn)>lastModifiedOn)
                {
                        lastModifiedOn = new Date(data.lastModifiedOn);
                }
        }
        catch(e){}
}
var errors=0;
var goodRecords = 0;
function getMultiRelationValue(obj ,indexes, path)
{
        var value = "11";
        var tempObj;
if(indexes !==undefined)
	{
        indexes.replace('undefined,','').split(',').every(function(item){
                try{
                        if(path.indexOf('?')>-1)
			{
				value += getConditionalValue(obj[item],path);	
			}
			else{
			
			tempObj = obj[item];
                        path.split('/').forEach(function(data){
                                tempObj = tempObj[data];
			
                        });
                         value = tempObj;
}                        
}
                catch(ex)
                {
                }
        });
}
        return value;
}

function getToStateChangeDate(obj,comp)
{
	try{
	var data = obj["flows"]["salesStages"]["transitions"];
	if(data!==undefined && data.length>0)
	{
		for(var i=0;i<data.length;i++)
		{
			if(data[i]["toState"]!==undefined && data[i]["toState"]==comp)
			{
				return data[i]["changeDate"];
			}
		}
	}
	}
	catch(e){
		return '';
	}
	
}

function getValueFromJsonPath(obj,paths)
{
        var tempObj;
        var returnData='';
        var  realPath= '';
	var path='';
        var paths1 = paths.split('+');
        for(var index =0;index<paths1.length;index++)
        {
                path = paths1[index];
                try
                {
                        tempObj = obj;
                        realPath = path;

                        if(path.split('$').length>1)
                        {
                                returnData = getMultiRelationValue(obj["relationships"],relationIndexs[path.split('$')[0]],path.split('$')[1]);
                        }
                        else{

                        	if(path.indexOf('?')>-1)
                        	{
                        		returnData = getConditionalValue(obj,path);
                        	}
				else if(path.indexOf('#')>-1)
				{
					returnData = getTenantCustomValue(obj,path);
				}
                        	else{
		                        var jsonPath = realPath.split('/');
                		        jsonPath.forEach(function(item){
		                                	tempObj = tempObj[item];
					});
		                        returnData = tempObj;						
				}
                        }
			if(returnData!==undefined && returnData!=11)
                        {
					returnData = returnData+'';
                                        return returnData.replace(/"/g,'\"\"');
                        }

                }
                catch(ex)
                {
			returnData = "";
                }
        }
if(returnData===undefined)
{
	return "";
}
        return returnData.replace('11','');
}
function getTenantCustomValue(obj,path){
/*ibm#[displayName>"-Transitioned"]1:0*/
        try{
        var tempObj=obj;
        var varTenant = path.split("#")[0];

                if(varTenant==tenant)
                {
                var temppath = path.split("[");
                var realPath = temppath[1].split(">\"")[0];
                var compValue=temppath[1].split(">\"")[1].split("\"]")[0];
                var strActualValue=temppath[1].split("\"]")[1];
                var arrActualValue =  strActualValue.split(":");
                realPath.split("/").forEach(function(item){
                tempObj = tempObj[item];
        });
/*console.log("tempObj : "+tempObj+" CompValue : "+compValue);*/

                        if(tempObj.indexOf(compValue)>-1)
                        {
                                return arrActualValue[0];
                        }
                        else
                        {
                                return arrActualValue[1];
                        }
                }
                else
                {
                        return "";
                }
        }
        catch(ex){return;}
}

function getConditionalValue(obj,path)
{
	try{

		var tempObj = obj;
		var paths = path.split("]?");
		var realPath = paths[0].split('[');
		var jsonPath = realPath[0].split('/');
       jsonPath.forEach(function(item){
                tempObj = tempObj[item];
        });
       tempObj = JSON.stringify(tempObj);
       var checkValues = realPath[1].split('|');
       for(i=0;i<=checkValues.length;i++)
       {

       		if(tempObj.indexOf(checkValues[i])>-1)
       		{
       			tempObj=obj;
       			paths[1].split('/').forEach(function(item){
       				tempObj = tempObj[item];
       			});
       			return tempObj;
       		}
       }
       return '11';
	}
	catch(er){
		return '11';
}
}
var relationIndexs;
function getRelationIndex(obj)
{
 relationIndexs = new Array()
        obj.forEach(function(item,index) {
                relationIndexs[item.relation.name] += ','+index;
        });
}

function salesTage(obj)
{
	var state = getValueFromJsonPath(obj,"flows/salesStages/state/displayName");
	switch (state)
                {


		case "Not Contacted":
                        return "a. Not Contacted";
                        break;
		case "Contacted":
                        return "b. Contacted";
                        break;	
		case "Quote Requested":
                        return "c. Quote Requested";
                        break;	
		case "Quote Completed":
                        return "d. Quote Completed";
                        break;	
                case "Quote Delivered":
                        return "e. Quote Delivered";
                        break;
		case "Customer Commitment":
                        return "f. Customer Commitment";
                        break;
		case "PO Received":
                        return "g. PO Received";
                        break;
		case "Closed Sale":
                        return "h. Closed Sale";
                        break;
		case "No Service":
                        return "i. No Service";
                        break;
		case "House Account":
                        return "j. House Account";
                        break;
                case "Consolidated":
                        return "k. Consolidated";
                        break;
                case "Transitioned":
                        return "l. Transitioned";
                        break;
                default:
                        return "XX. Error - Unknown Sales Stage";
                        break;
                }
	
}

