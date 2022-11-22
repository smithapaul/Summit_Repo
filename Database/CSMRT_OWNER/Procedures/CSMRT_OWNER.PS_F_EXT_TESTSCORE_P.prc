DROP PROCEDURE CSMRT_OWNER.PS_F_EXT_TESTSCORE_P
/

--
-- PS_F_EXT_TESTSCORE_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_F_EXT_TESTSCORE_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_F_EXT_TESTSCORE from PeopleSoft table PS_F_EXT_TESTSCORE.
--
-- V01  SMT-xxxx 06/15/2018,    James Doucette
--                              Converted from SQL Script
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_F_EXT_TESTSCORE';
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

strMessage01    := 'Truncating table CSMRT_OWNER.PS_F_EXT_TESTSCORE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.PS_F_EXT_TESTSCORE';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.PS_F_EXT_TESTSCORE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','PS_F_EXT_TESTSCORE');

--strSqlDynamic   := 'alter table CSMRT_OWNER.PS_F_EXT_TESTSCORE disable constraint PK_PS_F_EXT_TESTSCORE';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );
				
strMessage01    := 'Inserting data into CSMRT_OWNER.PS_F_EXT_TESTSCORE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.PS_F_EXT_TESTSCORE';				
insert /*+ append enable_parallel_dml parallel(8) */ into CSMRT_OWNER.PS_F_EXT_TESTSCORE
with T1 as (
select /*+ parallel(8) inline */
       EMPLID, TEST_ID, TEST_COMPONENT, TEST_DT, LS_DATA_SOURCE, SRC_SYS_ID, 
       SCORE, SCORE_LETTER, EXT_ACAD_LEVEL, DATE_LOADED, PERCENTILE, TEST_ADMIN, TEST_INDEX,
       'N' CONV_FLG, 
       0 TEST_CNT
  from CSSTG_OWNER.PS_STDNT_TEST_COMP 
 where DATA_ORIGIN <> 'D'
--   and TEST_ID <> 'SAT C'       -- Removed July 2019  
),
T2 as (
select /*+ parallel(8) inline */
       T.EMPLID, T.TEST_ID, T.TEST_COMPONENT, T.TEST_DT, T.LS_DATA_SOURCE, T.SRC_SYS_ID, 
       T.SCORE, T.SCORE_LETTER, T.EXT_ACAD_LEVEL, T.DATE_LOADED, T.PERCENTILE, T.TEST_ADMIN, T.TEST_INDEX, T.CONV_FLG, 
       C.MAX_SCORE, C.MIN_SCORE, 
       row_number() over (partition by T.EMPLID, T.TEST_ID, T.TEST_COMPONENT, T.TEST_DT, T.LS_DATA_SOURCE, T.SRC_SYS_ID
                              order by C.EFFDT desc) COMP_ORDER
  from T1 T
  left outer join CSSTG_OWNER.PS_SA_TCMP_REL_TBL C 
    on T.TEST_ID = C.TEST_ID
   and T.TEST_COMPONENT = C.TEST_COMPONENT
   and T.TEST_DT >= C.EFFDT
   and T.SRC_SYS_ID = C.SRC_SYS_ID
   and C.DATA_ORIGIN <> 'D'
 where T.TEST_CNT <> 1
)
select /*+ parallel(8) */ 
        P.PERSON_SID,
        C.EXT_TST_CMPNT_SID,
        T2.TEST_DT EXT_TST_DT,
        S.TST_DATA_SRC_SID, 
        T2.SRC_SYS_ID, 
        T2.EMPLID, 
        T2.TEST_ID EXT_TST_ID, 
        T2.TEST_COMPONENT EXT_TST_CMPNT_ID,
        T2.LS_DATA_SOURCE TST_DATA_SRC_ID,
        nvl(L.EXT_ACAD_LVL_SID,2147483646) EXT_ACAD_LVL_SID,  
        T2.SCORE NUMERIC_SCORE, 
        T2.SCORE_LETTER LETTER_SCORE, 
        T2.PERCENTILE SCORE_PERCENTILE,
        T2.DATE_LOADED LOAD_DT,
        T2.TEST_ADMIN,
        T2.TEST_INDEX,
        nvl(T2.MAX_SCORE,0) MAX_SCORE, 
        nvl(T2.MIN_SCORE,0) MIN_SCORE,
        T2.CONV_FLG, 
        'N' LOAD_ERROR, 
        'S' DATA_ORIGIN, 
        SYSDATE CREATED_EW_DTTM, 
        SYSDATE LASTUPD_EW_DTTM, 
        1234 BATCH_SID
  from T2
  join PS_D_PERSON P
    on T2.EMPLID = P.PERSON_ID
   and T2.SRC_SYS_ID = P.SRC_SYS_ID
  join PS_D_EXT_TST_CMPNT C
    on T2.TEST_ID = C.EXT_TST_ID 
   and T2.TEST_COMPONENT = C.EXT_TST_CMPNT_ID 
   and T2.SRC_SYS_ID = C.SRC_SYS_ID
  join PS_D_TST_DATA_SRC S
    on T2.LS_DATA_SOURCE = S.TST_DATA_SRC_ID
   and T2.SRC_SYS_ID = S.SRC_SYS_ID
  left outer join PS_D_EXT_ACAD_LVL L
    on T2.EXT_ACAD_LEVEL = L.EXT_ACAD_LVL_ID  
   and T2.SRC_SYS_ID = L.SRC_SYS_ID
 where COMP_ORDER = 1
-- order by CONV_FLG desc
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_F_EXT_TESTSCORE rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_F_EXT_TESTSCORE',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.PS_F_EXT_TESTSCORE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

--strSqlDynamic   := 'alter table CSMRT_OWNER.PS_F_EXT_TESTSCORE enable constraint PK_PS_F_EXT_TESTSCORE';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','PS_F_EXT_TESTSCORE');

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

END PS_F_EXT_TESTSCORE_P;
/
