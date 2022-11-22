DROP PROCEDURE CSMRT_OWNER.AM_PS_RQ_MAIN_TBL_P
/

--
-- AM_PS_RQ_MAIN_TBL_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER.AM_PS_RQ_MAIN_TBL_P IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_RQ_MAIN_TBL from PeopleSoft table PS_RQ_MAIN_TBL.
--
-- V01  SMT-xxxx 05/30/2017,    Jim Doucette
--                              Converted from PS_RQ_MAIN_TBL.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'AM_PS_RQ_MAIN_TBL';
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

strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);


strSqlCommand   := 'update START_DT on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Reading',
       START_DT = SYSDATE,
       END_DT = NULL
 where TABLE_NAME = 'PS_RQ_MAIN_TBL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Truncating',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_RQ_MAIN_TBL@AMSOURCE S)
 where TABLE_NAME = 'PS_RQ_MAIN_TBL'
;

strSqlCommand := 'commit';
commit;


strSqlDynamic   := 'truncate table AMSTG_OWNER.PS_T_RQ_MAIN_TBL';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );


strSqlCommand   := 'Loading temp table for AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Loading'
 where TABLE_NAME = 'PS_RQ_MAIN_TBL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand := 'insert';
insert /*+ append */  into AMSTG_OWNER.PS_T_RQ_MAIN_TBL
select REQUIREMENT, 
       EFFDT, 
       'CS90' 
       SRC_SYS_ID, 
       EFF_STATUS, 
       DESCR, 
       DESCRSHORT, 
       RQRMNT_USEAGE, 
       INSTITUTION, 
       ACAD_CAREER, 
       ACAD_GROUP, 
       ACAD_PROG, 
       ACAD_PLAN, 
       ACAD_SUB_PLAN, 
       SUBJECT, 
       CATALOG_NBR, 
       RQRMNT_LIST_SEQ, 
       RQ_CONNECT_TYPE, 
       SPECIAL_PROCESSING, 
       MIN_UNITS_REQD, 
       MIN_CRSES_REQD, 
       GRADE_POINTS_MIN, 
       GPA_REQUIRED, 
       REQ_CRSSELECT_METH, 
       CREDIT_INCL_MODE, 
       RQ_REPORTING, 
       SAA_DISPLAY_GPA, 
       SAA_DISPLAY_UNITS, 
       SAA_DISPLAY_CRSCNT, 
       CONDITION_CODE, 
       CONDITION_OPERATOR, 
       CONDITION_DATA, 
       REQCH_RESOLV_METH, 
       REQCH_STOP_RULE, 
       RQ_MIN_LINES, 
       RQ_MAX_LINES, 
       RQ_PARTITION_SHR, 
       RQ_PRINT_CNTL, 
       TEST_ID, 
       TEST_COMPONENT, 
       SCORE, 
       SAA_MAX_VALID_AGE, 
       SAA_BEST_TEST_OPT, 
       SAA_HIDE_STATUS, 
       SAA_DESCR80, 
       DESCR254A, 
       to_char(substr(trim(SAA_DESCRLONG), 1, 4000)) SAA_DESCRLONG,
       to_number(ORA_ROWSCN) SRC_SCN
  from SYSADM.PS_RQ_MAIN_TBL@AMSOURCE
  ;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging'
 where TABLE_NAME = 'PS_RQ_MAIN_TBL'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into AMSTG_OWNER.PS_RQ_MAIN_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into AMSTG_OWNER.PS_RQ_MAIN_TBL';
merge /*+ use_hash(S,T) */ into AMSTG_OWNER.PS_RQ_MAIN_TBL T
using (select /*+ full(S) */
    nvl(trim(REQUIREMENT),'-') REQUIREMENT,
    to_date(to_char(EFFDT,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EFFDT,
    EFF_STATUS EFF_STATUS,
    DESCR DESCR,
    DESCRSHORT DESCRSHORT,
    RQRMNT_USEAGE RQRMNT_USEAGE,
    INSTITUTION INSTITUTION,
    ACAD_CAREER ACAD_CAREER,
    ACAD_GROUP ACAD_GROUP,
    ACAD_PROG ACAD_PROG,
    ACAD_PLAN ACAD_PLAN,
    ACAD_SUB_PLAN ACAD_SUB_PLAN,
    SUBJECT SUBJECT,
    CATALOG_NBR CATALOG_NBR,
    RQRMNT_LIST_SEQ RQRMNT_LIST_SEQ,
    RQ_CONNECT_TYPE RQ_CONNECT_TYPE,
    SPECIAL_PROCESSING SPECIAL_PROCESSING,
    MIN_UNITS_REQD MIN_UNITS_REQD,
    MIN_CRSES_REQD MIN_CRSES_REQD,
    GRADE_POINTS_MIN GRADE_POINTS_MIN,
    GPA_REQUIRED GPA_REQUIRED,
    REQ_CRSSELECT_METH REQ_CRSSELECT_METH,
    CREDIT_INCL_MODE CREDIT_INCL_MODE,
    RQ_REPORTING RQ_REPORTING,
    SAA_DISPLAY_GPA SAA_DISPLAY_GPA,
    SAA_DISPLAY_UNITS SAA_DISPLAY_UNITS,
    SAA_DISPLAY_CRSCNT SAA_DISPLAY_CRSCNT,
    CONDITION_CODE CONDITION_CODE,
    CONDITION_OPERATOR CONDITION_OPERATOR,
    CONDITION_DATA CONDITION_DATA,
    REQCH_RESOLV_METH REQCH_RESOLV_METH,
    REQCH_STOP_RULE REQCH_STOP_RULE,
    RQ_MIN_LINES RQ_MIN_LINES,
    RQ_MAX_LINES RQ_MAX_LINES,
    RQ_PARTITION_SHR RQ_PARTITION_SHR,
    RQ_PRINT_CNTL RQ_PRINT_CNTL,
    TEST_ID TEST_ID,
    TEST_COMPONENT TEST_COMPONENT,
    SCORE SCORE,
    SAA_MAX_VALID_AGE SAA_MAX_VALID_AGE,
    SAA_BEST_TEST_OPT SAA_BEST_TEST_OPT,
    SAA_HIDE_STATUS SAA_HIDE_STATUS,
    SAA_DESCR80 SAA_DESCR80,
    DESCR254A DESCR254A,
    SAA_DESCRLONG SAA_DESCRLONG
from AMSTG_OWNER.PS_T_RQ_MAIN_TBL S
where SRC_SCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_RQ_MAIN_TBL')
) S
   on (
    T.REQUIREMENT = S.REQUIREMENT and
    T.EFFDT = S.EFFDT and
    T.SRC_SYS_ID = 'CS90')
    when matched then update set
    T.EFF_STATUS = S.EFF_STATUS,
    T.DESCR = S.DESCR,
    T.DESCRSHORT = S.DESCRSHORT,
    T.RQRMNT_USEAGE = S.RQRMNT_USEAGE,
    T.INSTITUTION = S.INSTITUTION,
    T.ACAD_CAREER = S.ACAD_CAREER,
    T.ACAD_GROUP = S.ACAD_GROUP,
    T.ACAD_PROG = S.ACAD_PROG,
    T.ACAD_PLAN = S.ACAD_PLAN,
    T.ACAD_SUB_PLAN = S.ACAD_SUB_PLAN,
    T.SUBJECT = S.SUBJECT,
    T.CATALOG_NBR = S.CATALOG_NBR,
    T.RQRMNT_LIST_SEQ = S.RQRMNT_LIST_SEQ,
    T.RQ_CONNECT_TYPE = S.RQ_CONNECT_TYPE,
    T.SPECIAL_PROCESSING = S.SPECIAL_PROCESSING,
    T.MIN_UNITS_REQD = S.MIN_UNITS_REQD,
    T.MIN_CRSES_REQD = S.MIN_CRSES_REQD,
    T.GRADE_POINTS_MIN = S.GRADE_POINTS_MIN,
    T.GPA_REQUIRED = S.GPA_REQUIRED,
    T.REQ_CRSSELECT_METH = S.REQ_CRSSELECT_METH,
    T.CREDIT_INCL_MODE = S.CREDIT_INCL_MODE,
    T.RQ_REPORTING = S.RQ_REPORTING,
    T.SAA_DISPLAY_GPA = S.SAA_DISPLAY_GPA,
    T.SAA_DISPLAY_UNITS = S.SAA_DISPLAY_UNITS,
    T.SAA_DISPLAY_CRSCNT = S.SAA_DISPLAY_CRSCNT,
    T.CONDITION_CODE = S.CONDITION_CODE,
    T.CONDITION_OPERATOR = S.CONDITION_OPERATOR,
    T.CONDITION_DATA = S.CONDITION_DATA,
    T.REQCH_RESOLV_METH = S.REQCH_RESOLV_METH,
    T.REQCH_STOP_RULE = S.REQCH_STOP_RULE,
    T.RQ_MIN_LINES = S.RQ_MIN_LINES,
    T.RQ_MAX_LINES = S.RQ_MAX_LINES,
    T.RQ_PARTITION_SHR = S.RQ_PARTITION_SHR,
    T.RQ_PRINT_CNTL = S.RQ_PRINT_CNTL,
    T.TEST_ID = S.TEST_ID,
    T.TEST_COMPONENT = S.TEST_COMPONENT,
    T.SCORE = S.SCORE,
    T.SAA_MAX_VALID_AGE = S.SAA_MAX_VALID_AGE,
    T.SAA_BEST_TEST_OPT = S.SAA_BEST_TEST_OPT,
    T.SAA_HIDE_STATUS = S.SAA_HIDE_STATUS,
    T.SAA_DESCR80 = S.SAA_DESCR80,
    T.DESCR254A = S.DESCR254A,
    T.SAA_DESCRLONG = S.SAA_DESCRLONG,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID   = 1234
where
    nvl(trim(T.EFF_STATUS),0) <> nvl(trim(S.EFF_STATUS),0) or
    nvl(trim(T.DESCR),0) <> nvl(trim(S.DESCR),0) or
    nvl(trim(T.DESCRSHORT),0) <> nvl(trim(S.DESCRSHORT),0) or
    nvl(trim(T.RQRMNT_USEAGE),0) <> nvl(trim(S.RQRMNT_USEAGE),0) or
    nvl(trim(T.INSTITUTION),0) <> nvl(trim(S.INSTITUTION),0) or
    nvl(trim(T.ACAD_CAREER),0) <> nvl(trim(S.ACAD_CAREER),0) or
    nvl(trim(T.ACAD_GROUP),0) <> nvl(trim(S.ACAD_GROUP),0) or
    nvl(trim(T.ACAD_PROG),0) <> nvl(trim(S.ACAD_PROG),0) or
    nvl(trim(T.ACAD_PLAN),0) <> nvl(trim(S.ACAD_PLAN),0) or
    nvl(trim(T.ACAD_SUB_PLAN),0) <> nvl(trim(S.ACAD_SUB_PLAN),0) or
    nvl(trim(T.SUBJECT),0) <> nvl(trim(S.SUBJECT),0) or
    nvl(trim(T.CATALOG_NBR),0) <> nvl(trim(S.CATALOG_NBR),0) or
    nvl(trim(T.RQRMNT_LIST_SEQ),0) <> nvl(trim(S.RQRMNT_LIST_SEQ),0) or
    nvl(trim(T.RQ_CONNECT_TYPE),0) <> nvl(trim(S.RQ_CONNECT_TYPE),0) or
    nvl(trim(T.SPECIAL_PROCESSING),0) <> nvl(trim(S.SPECIAL_PROCESSING),0) or
    nvl(trim(T.MIN_UNITS_REQD),0) <> nvl(trim(S.MIN_UNITS_REQD),0) or
    nvl(trim(T.MIN_CRSES_REQD),0) <> nvl(trim(S.MIN_CRSES_REQD),0) or
    nvl(trim(T.GRADE_POINTS_MIN),0) <> nvl(trim(S.GRADE_POINTS_MIN),0) or
    nvl(trim(T.GPA_REQUIRED),0) <> nvl(trim(S.GPA_REQUIRED),0) or
    nvl(trim(T.REQ_CRSSELECT_METH),0) <> nvl(trim(S.REQ_CRSSELECT_METH),0) or
    nvl(trim(T.CREDIT_INCL_MODE),0) <> nvl(trim(S.CREDIT_INCL_MODE),0) or
    nvl(trim(T.RQ_REPORTING),0) <> nvl(trim(S.RQ_REPORTING),0) or
    nvl(trim(T.SAA_DISPLAY_GPA),0) <> nvl(trim(S.SAA_DISPLAY_GPA),0) or
    nvl(trim(T.SAA_DISPLAY_UNITS),0) <> nvl(trim(S.SAA_DISPLAY_UNITS),0) or
    nvl(trim(T.SAA_DISPLAY_CRSCNT),0) <> nvl(trim(S.SAA_DISPLAY_CRSCNT),0) or
    nvl(trim(T.CONDITION_CODE),0) <> nvl(trim(S.CONDITION_CODE),0) or
    nvl(trim(T.CONDITION_OPERATOR),0) <> nvl(trim(S.CONDITION_OPERATOR),0) or
    nvl(trim(T.CONDITION_DATA),0) <> nvl(trim(S.CONDITION_DATA),0) or
    nvl(trim(T.REQCH_RESOLV_METH),0) <> nvl(trim(S.REQCH_RESOLV_METH),0) or
    nvl(trim(T.REQCH_STOP_RULE),0) <> nvl(trim(S.REQCH_STOP_RULE),0) or
    nvl(trim(T.RQ_MIN_LINES),0) <> nvl(trim(S.RQ_MIN_LINES),0) or
    nvl(trim(T.RQ_MAX_LINES),0) <> nvl(trim(S.RQ_MAX_LINES),0) or
    nvl(trim(T.RQ_PARTITION_SHR),0) <> nvl(trim(S.RQ_PARTITION_SHR),0) or
    nvl(trim(T.RQ_PRINT_CNTL),0) <> nvl(trim(S.RQ_PRINT_CNTL),0) or
    nvl(trim(T.TEST_ID),0) <> nvl(trim(S.TEST_ID),0) or
    nvl(trim(T.TEST_COMPONENT),0) <> nvl(trim(S.TEST_COMPONENT),0) or
    nvl(trim(T.SCORE),0) <> nvl(trim(S.SCORE),0) or
    nvl(trim(T.SAA_MAX_VALID_AGE),0) <> nvl(trim(S.SAA_MAX_VALID_AGE),0) or
    nvl(trim(T.SAA_BEST_TEST_OPT),0) <> nvl(trim(S.SAA_BEST_TEST_OPT),0) or
    nvl(trim(T.SAA_HIDE_STATUS),0) <> nvl(trim(S.SAA_HIDE_STATUS),0) or
    nvl(trim(T.SAA_DESCR80),0) <> nvl(trim(S.SAA_DESCR80),0) or
    nvl(trim(T.DESCR254A),0) <> nvl(trim(S.DESCR254A),0) or
    nvl(trim(T.SAA_DESCRLONG),0) <> nvl(trim(S.SAA_DESCRLONG),0) or
    T.DATA_ORIGIN = 'D'
when not matched then
insert (
    T.REQUIREMENT,
    T.EFFDT,
    T.SRC_SYS_ID,
    T.EFF_STATUS,
    T.DESCR,
    T.DESCRSHORT,
    T.RQRMNT_USEAGE,
    T.INSTITUTION,
    T.ACAD_CAREER,
    T.ACAD_GROUP,
    T.ACAD_PROG,
    T.ACAD_PLAN,
    T.ACAD_SUB_PLAN,
    T.SUBJECT,
    T.CATALOG_NBR,
    T.RQRMNT_LIST_SEQ,
    T.RQ_CONNECT_TYPE,
    T.SPECIAL_PROCESSING,
    T.MIN_UNITS_REQD,
    T.MIN_CRSES_REQD,
    T.GRADE_POINTS_MIN,
    T.GPA_REQUIRED,
    T.REQ_CRSSELECT_METH,
    T.CREDIT_INCL_MODE,
    T.RQ_REPORTING,
    T.SAA_DISPLAY_GPA,
    T.SAA_DISPLAY_UNITS,
    T.SAA_DISPLAY_CRSCNT,
    T.CONDITION_CODE,
    T.CONDITION_OPERATOR,
    T.CONDITION_DATA,
    T.REQCH_RESOLV_METH,
    T.REQCH_STOP_RULE,
    T.RQ_MIN_LINES,
    T.RQ_MAX_LINES,
    T.RQ_PARTITION_SHR,
    T.RQ_PRINT_CNTL,
    T.TEST_ID,
    T.TEST_COMPONENT,
    T.SCORE,
    T.SAA_MAX_VALID_AGE,
    T.SAA_BEST_TEST_OPT,
    T.SAA_HIDE_STATUS,
    T.SAA_DESCR80,
    T.DESCR254A,
    T.SAA_DESCRLONG,
    T.LOAD_ERROR,
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
    )
values (
    S.REQUIREMENT,
    S.EFFDT,
    'CS90',
    S.EFF_STATUS,
    S.DESCR,
    S.DESCRSHORT,
    S.RQRMNT_USEAGE,
    S.INSTITUTION,
    S.ACAD_CAREER,
    S.ACAD_GROUP,
    S.ACAD_PROG,
    S.ACAD_PLAN,
    S.ACAD_SUB_PLAN,
    S.SUBJECT,
    S.CATALOG_NBR,
    S.RQRMNT_LIST_SEQ,
    S.RQ_CONNECT_TYPE,
    S.SPECIAL_PROCESSING,
    S.MIN_UNITS_REQD,
    S.MIN_CRSES_REQD,
    S.GRADE_POINTS_MIN,
    S.GPA_REQUIRED,
    S.REQ_CRSSELECT_METH,
    S.CREDIT_INCL_MODE,
    S.RQ_REPORTING,
    S.SAA_DISPLAY_GPA,
    S.SAA_DISPLAY_UNITS,
    S.SAA_DISPLAY_CRSCNT,
    S.CONDITION_CODE,
    S.CONDITION_OPERATOR,
    S.CONDITION_DATA,
    S.REQCH_RESOLV_METH,
    S.REQCH_STOP_RULE,
    S.RQ_MIN_LINES,
    S.RQ_MAX_LINES,
    S.RQ_PARTITION_SHR,
    S.RQ_PRINT_CNTL,
    S.TEST_ID,
    S.TEST_COMPONENT,
    S.SCORE,
    S.SAA_MAX_VALID_AGE,
    S.SAA_BEST_TEST_OPT,
    S.SAA_HIDE_STATUS,
    S.SAA_DESCR80,
    S.DESCR254A,
    S.SAA_DESCRLONG,
    'N',
    'S',
    sysdate,
    sysdate,
    1234);

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;


strMessage01    := '# of PS_RQ_MAIN_TBL rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_RQ_MAIN_TBL',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_RQ_MAIN_TBL';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_RQ_MAIN_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on AMSTG_OWNER.PS_RQ_MAIN_TBL';
update AMSTG_OWNER.PS_RQ_MAIN_TBL T
        set T.DATA_ORIGIN = 'D',
               T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select REQUIREMENT, EFFDT
   from AMSTG_OWNER.PS_RQ_MAIN_TBL T2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_RQ_MAIN_TBL') = 'Y'
  minus
 select REQUIREMENT, EFFDT
   from SYSADM.PS_RQ_MAIN_TBL@AMSOURCE S2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_RQ_MAIN_TBL') = 'Y'
   ) S
 where T.REQUIREMENT = S.REQUIREMENT
   and T.EFFDT = S.EFFDT
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_RQ_MAIN_TBL rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_RQ_MAIN_TBL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_RQ_MAIN_TBL'
;

strSqlCommand := 'commit';
commit;


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

END AM_PS_RQ_MAIN_TBL_P;
/
