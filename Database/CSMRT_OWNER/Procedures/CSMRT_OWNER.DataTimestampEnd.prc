DROP PROCEDURE CSMRT_OWNER."DataTimestampEnd"
/

--
-- "DataTimestampEnd"  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."DataTimestampEnd" 

        (
               i_MartId                in  Varchar2
        )
 AUTHID CURRENT_USER
 IS
------------------------------------------------------------------------
-- Loads table COMMON_OWNER.DATA_TIMESTAMP
--
-- V01  SMT-7581 01/09/2018     Jim Doucette
--                              Converted from DATA_TIMESTAMP_END
--
------------------------------------------------------------------------

        strProcessName                  Varchar2(100)   := 'DataTimestampEnd';
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

strMessage01    := 'Procedure COMMON_OWNER."DataTimestampEnd" arguments:'
                || strNewLine || '         i_MartId: ' || i_MartId;
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);


strMessage01     := 'Updating DATA_TIMESTAMP rows.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'DATA_TIMESTAMP UPDATE';
UPDATE COMMON_OWNER.DATA_TIMESTAMP
   SET LAST_UPDATE_TIMESTAMP = sysdate,
       LAST_UPDATE_BY = 'DataTimestampEnd'
 where MART_ID         = i_MartId
   AND BUSINESS_UNIT  IS NULL
   AND COMPONENT      IS NULL
   AND SOURCE_SYSTEM   = 'SA90';	

intRowCount     := SQL%ROWCOUNT;

strSqlCommand   := 'COMMIT';
COMMIT;

strMessage01    := '# of rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'DATA_TIMESTAMP',
                i_Action            => 'UPDATE',
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


END "DataTimestampEnd";
/
