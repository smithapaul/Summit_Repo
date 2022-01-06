CREATE OR REPLACE PROCEDURE             UM_DDL_P AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- James Doucette
--
-- Loads table CSSTG_OWNER.UM_DDL.
--
-- V01  SMT-xxxx 05/28/2019,    James Doucette
--                              Converted from script
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_DDL';
        intProcessSid                   Integer;
        dtProcessStart                  Date            := SYSDATE;
        strMessage01                    Varchar2(4000);
        strMessage02                    Varchar2(512);
        strMessage03                    Varchar2(512)   :='';
        strNewLine                      Varchar2(2)     := chr(13) || chr(10);
        strSqlCommand                   Varchar2(32767) :='';
        strSqlDynamic                   Varchar2(32767) :='';
        strClientInfo                   Varchar2(100);
        intRowCount                     Integer;
        intTotalRowCount                Integer         := 0;
        numSqlCode                      Number;
        strSqlErrm                      Varchar2(4000);
        intTries                        Integer;

BEGIN

strSqlCommand := 'DBMS_APPLICATION_INFO.SET_CLIENT_INFO';
DBMS_APPLICATION_INFO.SET_CLIENT_INFO (strProcessName);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_INIT';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_INIT
        (
                i_MartId                => strMartId,
                i_ProcessName           => strProcessName,
                i_ProcessStartTime      => dtProcessStart,
                o_ProcessSid            => intProcessSid
        );

strMessage01    := 'Deleting from table CSSTG_OWNER.UM_DDL where DDL_DT = '  || TO_CHAR(trunc(SYSDATE));
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'delete CSSTG_OWNER.UM_DDL where DDL_DT = trunc(SYSDATE-90)';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
				

strMessage01    := 'Inserting data into CSSTG_OWNER.UM_DDL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSSTG_OWNER.UM_DDL';	
insert into CSSTG_OWNER.UM_DDL
select SYSDATE DDL_DT, upper(sys_context('USERENV','DB_NAME')) DDL_INSTANCE, 
       OWNER, OBJECT_TYPE, OBJECT_NAME, DBMS_METADATA.GET_DDL(OBJECT_TYPE, OBJECT_NAME, OWNER) OBJECT_DDL 
  from ALL_OBJECTS
 where OWNER in ('CSSTG_OWNER','CSMRT_OWNER')
   and OBJECT_TYPE in ('FUNCTION','INDEX','PROCEDURE','TABLE','TRIGGER','VIEW')
   and OBJECT_NAME not like '%NEW%'
   and OBJECT_NAME not like '%OLD%'
--   and length(trim(OBJECT_NAME)) <= 30
;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_DDL rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_DDL',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
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

END UM_DDL_P;
/
