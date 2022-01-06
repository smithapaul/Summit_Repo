CREATE OR REPLACE PROCEDURE             PS_RQ_LINE_TBL_P AUTHID CURRENT_USER IS
/*
-- Run before the first time
DELETE
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_RQ_LINE_TBL'

INSERT INTO CSSTG_OWNER.UM_STAGE_JOBS
(TABLE_NAME, DELETE_FLG)
VALUES
('PS_RQ_LINE_TBL', 'Y')

SELECT *
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_RQ_LINE_TBL'
*/

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_RQ_LINE_TBL from PeopleSoft table PS_RQ_LINE_TBL.
--
-- V01  SMT-xxxx 05/15/2017,    Jim Doucette
--                              Converted from PS_RQ_LINE_TBL.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_RQ_LINE_TBL';
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

strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);


strSqlCommand   := 'update START_DT on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Reading',
       START_DT = SYSDATE,
       END_DT = NULL
 where TABLE_NAME = 'PS_RQ_LINE_TBL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Truncating',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_RQ_LINE_TBL@SASOURCE S)
 where TABLE_NAME = 'PS_RQ_LINE_TBL'
;

strSqlCommand := 'commit';
commit;


strSqlDynamic   := 'truncate table CSSTG_OWNER.PS_T_RQ_LINE_TBL';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );


strSqlCommand   := 'Loading temp table for CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Loading'
 where TABLE_NAME = 'PS_RQ_LINE_TBL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand := 'insert';
insert /*+ append */  into CSSTG_OWNER.PS_T_RQ_LINE_TBL
select /*+ full(S) */
    nvl(trim(REQUIREMENT),'-') REQUIREMENT, 
    nvl(EFFDT, to_date('01-JAN-1900')) EFFDT,
    nvl(trim(RQ_LINE_KEY_NBR),'-') RQ_LINE_KEY_NBR, 
    'CS90' SRC_SYS_ID, 
    RQ_LINE_NBR, 
    DESCR, 
    DESCRSHORT, 
    FULL_SET_RPN, 
    SPECIAL_PROCESSING, 
    REQ_LINE_TYPE, 
    RQ_CONNECT, 
    CREDIT_INCL_MODE, 
    REQ_CRSSELECT_METH, 
    CT_COND_COMPLEMENT, 
    MIN_UNITS_REQD, 
    MIN_CRSES_REQD, 
    MAX_UNITS_ALLOWD, 
    MAX_CRSES_ALLOWD, 
    GRADE_POINTS_MIN, 
    GPA_REQUIRED, 
    GPA_MAXIMUM, 
    RQ_REPORTING, 
    SAA_DISPLAY_GPA, 
    SAA_DISPLAY_UNITS, 
    SAA_DISPLAY_CRSCNT, 
    CONDITION_CODE, 
    CONDITION_OPERATOR, 
    CONDITION_DATA, 
    COUNT_ATTEMPTS, 
    DISP_SELECT_LINE, 
    ENABLE_SPLITTING, 
    RQ_PRINT_CNTL, 
    PARENTHESIS, 
    SAA_COMPLEX_RQ_LN, 
    TEST_ID, 
    TEST_COMPONENT, 
    SCORE, 
    SAA_MAX_VALID_AGE, 
    SAA_BEST_TEST_OPT, 
    SAA_HIDE_STATUS, 
    SAA_DESCR80, 
    DESCR254A, 
    SAA_DESCR254, 
    substr(to_char(trim(SAA_DESCRLONG)),1,4000) SAA_DESCRLONG,
    to_number(ORA_ROWSCN) SRC_SCN
  from SYSADM.PS_RQ_LINE_TBL@SASOURCE S
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging'
 where TABLE_NAME = 'PS_RQ_LINE_TBL'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_RQ_LINE_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_RQ_LINE_TBL';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_RQ_LINE_TBL T
using (select /*+ full(S) */
    nvl(trim(REQUIREMENT),'-') REQUIREMENT,
    to_date(to_char(EFFDT,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EFFDT,
    nvl(trim(RQ_LINE_KEY_NBR),'-') RQ_LINE_KEY_NBR,
    RQ_LINE_NBR RQ_LINE_NBR,
    DESCR DESCR,
    DESCRSHORT DESCRSHORT,
    FULL_SET_RPN FULL_SET_RPN,
    SPECIAL_PROCESSING SPECIAL_PROCESSING,
    REQ_LINE_TYPE REQ_LINE_TYPE,
    RQ_CONNECT RQ_CONNECT,
    CREDIT_INCL_MODE CREDIT_INCL_MODE,
    REQ_CRSSELECT_METH REQ_CRSSELECT_METH,
    CT_COND_COMPLEMENT CT_COND_COMPLEMENT,
    MIN_UNITS_REQD MIN_UNITS_REQD,
    MIN_CRSES_REQD MIN_CRSES_REQD,
    MAX_UNITS_ALLOWD MAX_UNITS_ALLOWD,
    MAX_CRSES_ALLOWD MAX_CRSES_ALLOWD,
    GRADE_POINTS_MIN GRADE_POINTS_MIN,
    GPA_REQUIRED GPA_REQUIRED,
    GPA_MAXIMUM GPA_MAXIMUM,
    RQ_REPORTING RQ_REPORTING,
    SAA_DISPLAY_GPA SAA_DISPLAY_GPA,
    SAA_DISPLAY_UNITS SAA_DISPLAY_UNITS,
    SAA_DISPLAY_CRSCNT SAA_DISPLAY_CRSCNT,
    CONDITION_CODE CONDITION_CODE,
    CONDITION_OPERATOR CONDITION_OPERATOR,
    CONDITION_DATA CONDITION_DATA,
    COUNT_ATTEMPTS COUNT_ATTEMPTS,
    DISP_SELECT_LINE DISP_SELECT_LINE,
    ENABLE_SPLITTING ENABLE_SPLITTING,
    RQ_PRINT_CNTL RQ_PRINT_CNTL,
    PARENTHESIS PARENTHESIS,
    SAA_COMPLEX_RQ_LN SAA_COMPLEX_RQ_LN,
    TEST_ID TEST_ID,
    TEST_COMPONENT TEST_COMPONENT,
    SCORE SCORE,
    SAA_MAX_VALID_AGE SAA_MAX_VALID_AGE,
    SAA_BEST_TEST_OPT SAA_BEST_TEST_OPT,
    SAA_HIDE_STATUS SAA_HIDE_STATUS,
    SAA_DESCR80 SAA_DESCR80,
    DESCR254A DESCR254A,
    SAA_DESCR254 SAA_DESCR254,
    SAA_DESCRLONG SAA_DESCRLONG
from CSSTG_OWNER.PS_T_RQ_LINE_TBL S
where SRC_SCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_RQ_LINE_TBL')
) S
   on (
    T.REQUIREMENT = S.REQUIREMENT and
    T.EFFDT = S.EFFDT and
    T.RQ_LINE_KEY_NBR = S.RQ_LINE_KEY_NBR and
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.RQ_LINE_NBR = S.RQ_LINE_NBR,
    T.DESCR = S.DESCR,
    T.DESCRSHORT = S.DESCRSHORT,
    T.FULL_SET_RPN = S.FULL_SET_RPN,
    T.SPECIAL_PROCESSING = S.SPECIAL_PROCESSING,
    T.REQ_LINE_TYPE = S.REQ_LINE_TYPE,
    T.RQ_CONNECT = S.RQ_CONNECT,
    T.CREDIT_INCL_MODE = S.CREDIT_INCL_MODE,
    T.REQ_CRSSELECT_METH = S.REQ_CRSSELECT_METH,
    T.CT_COND_COMPLEMENT = S.CT_COND_COMPLEMENT,
    T.MIN_UNITS_REQD = S.MIN_UNITS_REQD,
    T.MIN_CRSES_REQD = S.MIN_CRSES_REQD,
    T.MAX_UNITS_ALLOWD = S.MAX_UNITS_ALLOWD,
    T.MAX_CRSES_ALLOWD = S.MAX_CRSES_ALLOWD,
    T.GRADE_POINTS_MIN = S.GRADE_POINTS_MIN,
    T.GPA_REQUIRED = S.GPA_REQUIRED,
    T.GPA_MAXIMUM = S.GPA_MAXIMUM,
    T.RQ_REPORTING = S.RQ_REPORTING,
    T.SAA_DISPLAY_GPA = S.SAA_DISPLAY_GPA,
    T.SAA_DISPLAY_UNITS = S.SAA_DISPLAY_UNITS,
    T.SAA_DISPLAY_CRSCNT = S.SAA_DISPLAY_CRSCNT,
    T.CONDITION_CODE = S.CONDITION_CODE,
    T.CONDITION_OPERATOR = S.CONDITION_OPERATOR,
    T.CONDITION_DATA = S.CONDITION_DATA,
    T.COUNT_ATTEMPTS = S.COUNT_ATTEMPTS,
    T.DISP_SELECT_LINE = S.DISP_SELECT_LINE,
    T.ENABLE_SPLITTING = S.ENABLE_SPLITTING,
    T.RQ_PRINT_CNTL = S.RQ_PRINT_CNTL,
    T.PARENTHESIS = S.PARENTHESIS,
    T.SAA_COMPLEX_RQ_LN = S.SAA_COMPLEX_RQ_LN,
    T.TEST_ID = S.TEST_ID,
    T.TEST_COMPONENT = S.TEST_COMPONENT,
    T.SCORE = S.SCORE,
    T.SAA_MAX_VALID_AGE = S.SAA_MAX_VALID_AGE,
    T.SAA_BEST_TEST_OPT = S.SAA_BEST_TEST_OPT,
    T.SAA_HIDE_STATUS = S.SAA_HIDE_STATUS,
    T.SAA_DESCR80 = S.SAA_DESCR80,
    T.DESCR254A = S.DESCR254A,
    T.SAA_DESCR254 = S.SAA_DESCR254,
    T.SAA_DESCRLONG = S.SAA_DESCRLONG,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID   = 1234
where
    nvl(trim(T.RQ_LINE_NBR),0) <> nvl(trim(S.RQ_LINE_NBR),0) or
    nvl(trim(T.DESCR),0) <> nvl(trim(S.DESCR),0) or
    nvl(trim(T.DESCRSHORT),0) <> nvl(trim(S.DESCRSHORT),0) or
    nvl(trim(T.FULL_SET_RPN),0) <> nvl(trim(S.FULL_SET_RPN),0) or
    nvl(trim(T.SPECIAL_PROCESSING),0) <> nvl(trim(S.SPECIAL_PROCESSING),0) or
    nvl(trim(T.REQ_LINE_TYPE),0) <> nvl(trim(S.REQ_LINE_TYPE),0) or
    nvl(trim(T.RQ_CONNECT),0) <> nvl(trim(S.RQ_CONNECT),0) or
    nvl(trim(T.CREDIT_INCL_MODE),0) <> nvl(trim(S.CREDIT_INCL_MODE),0) or
    nvl(trim(T.REQ_CRSSELECT_METH),0) <> nvl(trim(S.REQ_CRSSELECT_METH),0) or
    nvl(trim(T.CT_COND_COMPLEMENT),0) <> nvl(trim(S.CT_COND_COMPLEMENT),0) or
    nvl(trim(T.MIN_UNITS_REQD),0) <> nvl(trim(S.MIN_UNITS_REQD),0) or
    nvl(trim(T.MIN_CRSES_REQD),0) <> nvl(trim(S.MIN_CRSES_REQD),0) or
    nvl(trim(T.MAX_UNITS_ALLOWD),0) <> nvl(trim(S.MAX_UNITS_ALLOWD),0) or
    nvl(trim(T.MAX_CRSES_ALLOWD),0) <> nvl(trim(S.MAX_CRSES_ALLOWD),0) or
    nvl(trim(T.GRADE_POINTS_MIN),0) <> nvl(trim(S.GRADE_POINTS_MIN),0) or
    nvl(trim(T.GPA_REQUIRED),0) <> nvl(trim(S.GPA_REQUIRED),0) or
    nvl(trim(T.GPA_MAXIMUM),0) <> nvl(trim(S.GPA_MAXIMUM),0) or
    nvl(trim(T.RQ_REPORTING),0) <> nvl(trim(S.RQ_REPORTING),0) or
    nvl(trim(T.SAA_DISPLAY_GPA),0) <> nvl(trim(S.SAA_DISPLAY_GPA),0) or
    nvl(trim(T.SAA_DISPLAY_UNITS),0) <> nvl(trim(S.SAA_DISPLAY_UNITS),0) or
    nvl(trim(T.SAA_DISPLAY_CRSCNT),0) <> nvl(trim(S.SAA_DISPLAY_CRSCNT),0) or
    nvl(trim(T.CONDITION_CODE),0) <> nvl(trim(S.CONDITION_CODE),0) or
    nvl(trim(T.CONDITION_OPERATOR),0) <> nvl(trim(S.CONDITION_OPERATOR),0) or
    nvl(trim(T.CONDITION_DATA),0) <> nvl(trim(S.CONDITION_DATA),0) or
    nvl(trim(T.COUNT_ATTEMPTS),0) <> nvl(trim(S.COUNT_ATTEMPTS),0) or
    nvl(trim(T.DISP_SELECT_LINE),0) <> nvl(trim(S.DISP_SELECT_LINE),0) or
    nvl(trim(T.ENABLE_SPLITTING),0) <> nvl(trim(S.ENABLE_SPLITTING),0) or
    nvl(trim(T.RQ_PRINT_CNTL),0) <> nvl(trim(S.RQ_PRINT_CNTL),0) or
    nvl(trim(T.PARENTHESIS),0) <> nvl(trim(S.PARENTHESIS),0) or
    nvl(trim(T.SAA_COMPLEX_RQ_LN),0) <> nvl(trim(S.SAA_COMPLEX_RQ_LN),0) or
    nvl(trim(T.TEST_ID),0) <> nvl(trim(S.TEST_ID),0) or
    nvl(trim(T.TEST_COMPONENT),0) <> nvl(trim(S.TEST_COMPONENT),0) or
    nvl(trim(T.SCORE),0) <> nvl(trim(S.SCORE),0) or
    nvl(trim(T.SAA_MAX_VALID_AGE),0) <> nvl(trim(S.SAA_MAX_VALID_AGE),0) or
    nvl(trim(T.SAA_BEST_TEST_OPT),0) <> nvl(trim(S.SAA_BEST_TEST_OPT),0) or
    nvl(trim(T.SAA_HIDE_STATUS),0) <> nvl(trim(S.SAA_HIDE_STATUS),0) or
    nvl(trim(T.SAA_DESCR80),0) <> nvl(trim(S.SAA_DESCR80),0) or
    nvl(trim(T.DESCR254A),0) <> nvl(trim(S.DESCR254A),0) or
    nvl(trim(T.SAA_DESCR254),0) <> nvl(trim(S.SAA_DESCR254),0) or
    nvl(trim(T.SAA_DESCRLONG),0) <> nvl(trim(S.SAA_DESCRLONG),0) or
    T.DATA_ORIGIN = 'D'
when not matched then
insert (
    T.REQUIREMENT,
    T.EFFDT,
    T.RQ_LINE_KEY_NBR,
    T.SRC_SYS_ID,
    T.RQ_LINE_NBR,
    T.DESCR,
    T.DESCRSHORT,
    T.FULL_SET_RPN,
    T.SPECIAL_PROCESSING,
    T.REQ_LINE_TYPE,
    T.RQ_CONNECT,
    T.CREDIT_INCL_MODE,
    T.REQ_CRSSELECT_METH,
    T.CT_COND_COMPLEMENT,
    T.MIN_UNITS_REQD,
    T.MIN_CRSES_REQD,
    T.MAX_UNITS_ALLOWD,
    T.MAX_CRSES_ALLOWD,
    T.GRADE_POINTS_MIN,
    T.GPA_REQUIRED,
    T.GPA_MAXIMUM,
    T.RQ_REPORTING,
    T.SAA_DISPLAY_GPA,
    T.SAA_DISPLAY_UNITS,
    T.SAA_DISPLAY_CRSCNT,
    T.CONDITION_CODE,
    T.CONDITION_OPERATOR,
    T.CONDITION_DATA,
    T.COUNT_ATTEMPTS,
    T.DISP_SELECT_LINE,
    T.ENABLE_SPLITTING,
    T.RQ_PRINT_CNTL,
    T.PARENTHESIS,
    T.SAA_COMPLEX_RQ_LN,
    T.TEST_ID,
    T.TEST_COMPONENT,
    T.SCORE,
    T.SAA_MAX_VALID_AGE,
    T.SAA_BEST_TEST_OPT,
    T.SAA_HIDE_STATUS,
    T.SAA_DESCR80,
    T.DESCR254A,
    T.SAA_DESCR254,
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
    S.RQ_LINE_KEY_NBR,
    'CS90',
    S.RQ_LINE_NBR,
    S.DESCR,
    S.DESCRSHORT,
    S.FULL_SET_RPN,
    S.SPECIAL_PROCESSING,
    S.REQ_LINE_TYPE,
    S.RQ_CONNECT,
    S.CREDIT_INCL_MODE,
    S.REQ_CRSSELECT_METH,
    S.CT_COND_COMPLEMENT,
    S.MIN_UNITS_REQD,
    S.MIN_CRSES_REQD,
    S.MAX_UNITS_ALLOWD,
    S.MAX_CRSES_ALLOWD,
    S.GRADE_POINTS_MIN,
    S.GPA_REQUIRED,
    S.GPA_MAXIMUM,
    S.RQ_REPORTING,
    S.SAA_DISPLAY_GPA,
    S.SAA_DISPLAY_UNITS,
    S.SAA_DISPLAY_CRSCNT,
    S.CONDITION_CODE,
    S.CONDITION_OPERATOR,
    S.CONDITION_DATA,
    S.COUNT_ATTEMPTS,
    S.DISP_SELECT_LINE,
    S.ENABLE_SPLITTING,
    S.RQ_PRINT_CNTL,
    S.PARENTHESIS,
    S.SAA_COMPLEX_RQ_LN,
    S.TEST_ID,
    S.TEST_COMPONENT,
    S.SCORE,
    S.SAA_MAX_VALID_AGE,
    S.SAA_BEST_TEST_OPT,
    S.SAA_HIDE_STATUS,
    S.SAA_DESCR80,
    S.DESCR254A,
    S.SAA_DESCR254,
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


strMessage01    := '# of PS_RQ_LINE_TBL rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_RQ_LINE_TBL',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_RQ_LINE_TBL';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_RQ_LINE_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_RQ_LINE_TBL';
update CSSTG_OWNER.PS_RQ_LINE_TBL T
        set T.DATA_ORIGIN = 'D',
               T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select REQUIREMENT, EFFDT, RQ_LINE_KEY_NBR
   from CSSTG_OWNER.PS_RQ_LINE_TBL T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_RQ_LINE_TBL') = 'Y'
  minus
 select REQUIREMENT, EFFDT, RQ_LINE_KEY_NBR
   from SYSADM.PS_RQ_LINE_TBL@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_RQ_LINE_TBL') = 'Y'
   ) S
 where T.REQUIREMENT = S.REQUIREMENT
   and T.EFFDT = S.EFFDT
   and T.RQ_LINE_KEY_NBR = S.RQ_LINE_KEY_NBR
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_RQ_LINE_TBL rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_RQ_LINE_TBL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_RQ_LINE_TBL'
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

END PS_RQ_LINE_TBL_P;
/
