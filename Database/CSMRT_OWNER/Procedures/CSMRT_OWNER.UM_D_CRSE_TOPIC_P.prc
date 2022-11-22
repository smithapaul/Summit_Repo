DROP PROCEDURE CSMRT_OWNER.UM_D_CRSE_TOPIC_P
/

--
-- UM_D_CRSE_TOPIC_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_D_CRSE_TOPIC_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table UM_D_CRSE_TOPIC from PeopleSoft table PS_CRSE_TOPICS.
--
 --V01  SMT-xxxx 12/06/2017,    James Doucette
--                              Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_D_CRSE_TOPIC';
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

strMessage01    := 'Truncating table CSMRT_OWNER.UM_D_CRSE_TOPIC';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_D_CRSE_TOPIC';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_D_CRSE_TOPIC';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_D_CRSE_TOPIC');

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_D_CRSE_TOPIC disable constraint PK_UM_D_CRSE_TOPIC';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
strMessage01    := 'Inserting data into CSMRT_OWNER.UM_D_CRSE_TOPIC';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_D_CRSE_TOPIC';				
insert /*+ append parallel(8) enable_parallel_dml */ into CSMRT_OWNER.UM_D_CRSE_TOPIC
  with Q1 as (  
select /*+ parallel(8) inline */ 
       CRSE_ID CRSE_CD, CRS_TOPIC_ID, SRC_SYS_ID, EFFDT, 
       DESCRSHORT, DESCR, DESCRFORMAL, 
       CRSE_REPEATABLE CRSE_REPEATABLE_FLG, CRSE_REPEAT_LIMIT, 
       CRS_TOPIC_LINK, UNITS_REPEAT_LIMIT 
  from CSSTG_OWNER.PS_CRSE_TOPICS 
 where DATA_ORIGIN <> 'D'), 
       S as (
select /*+ parallel(8) inline */ 
       C.CRSE_CD, C.CRSE_OFFER_NUM, nvl(Q1.CRS_TOPIC_ID,0) CRSE_TOPIC_ID, C.SRC_SYS_ID, 
       nvl(Q1.EFFDT,to_date('01-JAN-1900')) EFFDT, C.CRSE_SID,      -- Sept 2019 
       nvl(Q1.DESCRSHORT,'-') DESCRSHORT, nvl(Q1.DESCR,'-') DESCR, nvl(Q1.DESCRFORMAL,'-') DESCRFORMAL,
       nvl(Q1.CRSE_REPEATABLE_FLG,'-') CRSE_REPEATABLE_FLG, nvl(Q1.CRSE_REPEAT_LIMIT,0) CRSE_REPEAT_LIMIT, 
       nvl(Q1.CRS_TOPIC_LINK,0) CRS_TOPIC_LINK, nvl(Q1.UNITS_REPEAT_LIMIT,0) UNITS_REPEAT_LIMIT 
  from CSMRT_OWNER.UM_D_CRSE C
  left outer join Q1
    on C.CRSE_CD = Q1.CRSE_CD
   and C.EFFDT = Q1.EFFDT  
   and C.SRC_SYS_ID = Q1.SRC_SYS_ID
   )                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            
select /*+ parallel(8) */ 
       ROWNUM CRSE_TOPIC_SID, CRSE_CD, CRSE_OFFER_NUM, CRSE_TOPIC_ID, SRC_SYS_ID, 
       EFFDT, DESCRSHORT, DESCR, DESCRFORMAL, CRSE_SID, CRSE_REPEATABLE_FLG, CRSE_REPEAT_LIMIT, 
       CRS_TOPIC_LINK, UNITS_REPEAT_LIMIT, 
       'S' DATA_ORIGIN, SYSDATE CREATED_EW_DTTM, SYSDATE LASTUPD_EW_DTTM
  from S
;  

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_D_CRSE_TOPIC rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);


strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_D_CRSE_TOPIC';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_D_CRSE_TOPIC enable constraint PK_UM_D_CRSE_TOPIC';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_D_CRSE_TOPIC');

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

END UM_D_CRSE_TOPIC_P;
/
