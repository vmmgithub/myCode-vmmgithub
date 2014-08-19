DELIMITER ;;
CREATE DEFINER=`sdelosreyes`@`localhost` PROCEDURE `createJavascriptCommand`(tenantName varchar(50),datascrub varchar(50),jsName varchar(50), outputfilename varchar(50))
begin
  declare done  INT DEFAULT FALSE;
  declare var_field varchar(50);
  declare var_description varchar(50);  
  declare var_command varchar(255);
  declare cur_1 CURSOR FOR 
  	select distinct field, shellscriptoption 
  	from tenant_shell_script_config_meta 
  	where shellscriptname = jsName;

  declare continue handler for not found set done = TRUE;

set var_command :=concat('./',jsName);

drop table if exists tt_value;
create temporary table tt_value (`value` varchar(255));

  open cur_1;

  read_loop: loop
    fetch cur_1 into var_field, var_description;

    IF done THEN
      LEAVE read_loop;
    END IF;

    set @columnName = var_field;
    set @query = concat('insert into tt_value
    					select ',
    						@columnName,'
    					 from 
    						tenant_shell_script_config 
    					where 
    						tenant=\'',tenantName,'\' and ',
    						'datascrub=\'',datascrub,'\' and ',
    						'shellscriptname=\'',jsName,'\'');


	prepare stmt from @query;
	execute stmt;

	if (select `value` from tt_value) is not null then
		set var_command := concat(var_command,' --',var_description,' ', (select `value` from tt_value));
    end if;
    deallocate prepare stmt;
    
	truncate tt_value;
  END LOOP;

  CLOSE cur_1;
  
  select var_command;

    set @query2 = concat("select '", var_command, "' into outfile '",outputfilename,".sh';");
    prepare stmt2 from @query2;
    execute stmt2;
    deallocate prepare stmt2;


END;;
DELIMITER ;