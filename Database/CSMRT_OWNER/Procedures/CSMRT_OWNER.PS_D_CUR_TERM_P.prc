CREATE OR REPLACE PROCEDURE             "PS_D_CUR_TERM_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- James Doucette
-- Loads target table       -- PS_D_CUR_TERM
-- PS_D_CUR_TERM            -- Dependent on PS_D_TERM
-- V01 7/22/2019            -- doucette ,james new procedure.
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_D_CUR_TERM';
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

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.PS_D_CUR_TERM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','PS_D_CUR_TERM');

strSqlDynamic   := 'alter table CSMRT_OWNER.PS_D_CUR_TERM disable constraint PK_PS_D_CUR_TERM';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
strMessage01    := 'Truncating table CSMRT_OWNER.PS_D_CUR_TERM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.PS_D_CUR_TERM';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSMRT_OWNER.PS_D_CUR_TERM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.PS_D_CUR_TERM';				
insert into CSMRT_OWNER.PS_D_CUR_TERM 
select INSTITUTION_CD, 
       ACAD_CAR_CD, 
       TERM_CD, 
       SRC_SYS_ID, 
       decode(PREV_WINTER,'-','',PREV_WINTER) PREVIOUS_WINTER_TERM, 
       decode(PREV_FALL,'-','',PREV_FALL) PREVIOUS_FALL_TERM, 
       decode(PREV_SPRING,'-','',PREV_SPRING) PREVIOUS_SPRING_TERM, 
       decode(PREV_SUMMER,'-','',PREV_SUMMER) PREVIOUS_SUMMER_TERM, 
       decode(PREV_SUMMER_2,'-','',PREV_SUMMER_2) PREVIOUS_TRI_TERM, 
       decode(PREV_TERM,'-','',PREV_TERM) PREVIOUS_TERM,
       decode(NEXT_TERM,'-','',NEXT_TERM) NEXT_TERM, 
       decode(NEXT_WINTER,'-','',NEXT_WINTER) NEXT_WINTER_TERM, 
       decode(NEXT_FALL,'-','',NEXT_FALL) NEXT_FALL_TERM, 
       decode(NEXT_SPRING,'-','',NEXT_SPRING) NEXT_SPRING_TERM, 
       decode(NEXT_SUMMER,'-','',NEXT_SUMMER) NEXT_SUMMER_TERM, 
       decode(NEXT_SUMMER_2,'-','',NEXT_SUMMER_2) NEXT_TRI_TERM,
       'S' DATA_ORIGIN, 
       SYSDATE CREATED_EW_DTTM, 
       SYSDATE LASTUPD_EW_DTTM
  from PS_D_TERM
 where CURRENT_TERM_FLG = 'Y'
   and TERM_CD not like '%90'
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;
 
strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_D_CUR_TERM rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_D_CUR_TERM',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.PS_D_CUR_TERM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'alter table CSMRT_OWNER.PS_D_CUR_TERM enable constraint PK_PS_D_CUR_TERM';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','PS_D_CUR_TERM');

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

END PS_D_CUR_TERM_P;
/
