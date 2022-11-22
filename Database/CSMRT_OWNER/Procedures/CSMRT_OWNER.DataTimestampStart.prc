DROP PROCEDURE CSMRT_OWNER."DataTimestampStart"
/

--
-- "DataTimestampStart"  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."DataTimestampStart"

        (
               i_MartId                in  Varchar2
        )
 AUTHID CURRENT_USER
 IS
------------------------------------------------------------------------
-- Loads table COMMON_OWNER.DATA_TIMESTAMP
--
-- V01  SMT-7581 01/08/2018     Jim Doucette
--                              Converted from DATA_TIMESTAMP_START
--
------------------------------------------------------------------------

        strProcessName                  Varchar2(100)   := 'DataTimestampStart';
        dtProcessStart                  Date            := SYSDATE;
        strMessage01                    VARCHAR2(4000);
        strNewLine                      VARCHAR2(2)     := chr(13) || chr(10);
        strSqlCommand                   VARCHAR2(32767) := '';
        intRowCount                     INTEGER;



BEGIN
strSqlCommand   := 'DBMS_APPLICATION_INFO.SET_CLIENT_INFO';
DBMS_APPLICATION_INFO.SET_CLIENT_INFO (strProcessName);

strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_INIT';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_INIT
        (
                i_MartId                => i_MartId,
                i_ProcessName           => strProcessName,
                i_ProcessStartTime      => dtProcessStart
        );

strMessage01    := 'Procedure COMMON_OWNER."DataTimestampStart" arguments:'
                || strNewLine || '         i_MartId: ' || i_MartId;
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strMessage01     := 'Deleting DATA_TIMESTAMP rows.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);


strMessage01     := 'DELETE FROM DATA_TIMESTAMP';
DELETE
  FROM COMMON_OWNER.DATA_TIMESTAMP
 WHERE MART_ID        = i_MartId
   AND BUSINESS_UNIT  IS NULL
   AND COMPONENT      IS NULL
   AND SOURCE_SYSTEM   = 'SA90';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand   := 'COMMIT';
COMMIT;

strMessage01    := '# of rows deleted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);


strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'DATA_TIMESTAMP',
                i_Action            => 'DELETE',
                i_RowCount          => intRowCount
        );

strMessage01     := 'Inserting DATA_TIMESTAMP rows.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'DATA_TIMESTAMP INSERT';
INSERT
  INTO  COMMON_OWNER.DATA_TIMESTAMP
        (
        MART_ID,
        BUSINESS_UNIT,
        COMPONENT,
        SOURCE_SYSTEM,
        SOURCE_TIMESTAMP,
        SOURCE_DATE,
        LAST_UPDATE_TIMESTAMP,
        LAST_UPDATE_BY
        )	
VALUES (
       i_MartId,
	   '',
	   '',
	   'SA90'
	   ,sysdate,
	   trunc(sysdate),
	   to_date(to_char('01/01/1900'), 'MM/DD/YYYY' ),
	   'DataTimestampStart')	
;

intRowCount     := SQL%ROWCOUNT;

strSqlCommand   := 'COMMIT';
COMMIT;

strMessage01    := '# of rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'DATA_TIMESTAMP',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );


strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := strProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

EXCEPTION
    WHEN OTHERS THEN

        COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_EXCEPTION
                (
                        i_SqlCommand   => strSqlCommand,
                        i_SqlCode      => SQLCODE,
                        i_SqlErrm      => SQLERRM
                );


END "DataTimestampStart";
/
