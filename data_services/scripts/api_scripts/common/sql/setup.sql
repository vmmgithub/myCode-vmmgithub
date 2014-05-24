CREATE USER 'dataadmin'@'localhost' IDENTIFIED BY 'passwordone';

GRANT ALL PRIVILEGES ON *.* TO 'dataadmin'@'localhost' WITH GRANT OPTION;

CREATE DATABASE IF NOT EXISTS dataadmin;

use dataadmin;

delimiter //
DROP PROCEDURE IF EXISTS create_index_if_not_exists//
CREATE PROCEDURE `create_index_if_not_exists` (IN param_schema CHAR(255), IN param_table CHAR(255), IN param_column CHAR(255))
BEGIN

    SELECT @indexes := COUNT(*)
        FROM INFORMATION_SCHEMA.STATISTICS
        WHERE table_schema = param_schema
        AND table_name = param_table
        AND COLUMN_NAME = param_column;

    IF @indexes = 0 THEN
        SET @sql_cmd := CONCAT(
            'ALTER TABLE ',
            param_table,
            ' ADD INDEX ',
            '`', param_column, '` ',
            '(', param_column, ')');
        SELECT @sql_cmd;
        PREPARE stmt FROM @sql_cmd;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    END IF;

END//
delimiter ;

  drop TABLE if exists JOB_STATUSES;

  CREATE TABLE JOB_STATUSES (
    JOB varchar(250) DEFAULT NULL,
    TABLENAME varchar(250) DEFAULT NULL,
    STARTDATE timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    DESCRIPTION varchar(250) DEFAULT NULL,
    NUMBERRECORDS int(11) DEFAULT NULL,
    NUMBERERRORS int(11) DEFAULT NULL,
    STATUS varchar(250) DEFAULT NULL,
    MESSAGE text,
    ENDDATE timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
    UPDATEDATE timestamp NOT NULL DEFAULT '0000-00-00 00:00:00'
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

  