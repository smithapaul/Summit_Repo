CREATE OR REPLACE PROCEDURE             "PS_RQ_GRP_DETL_TBL_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_RQ_GRP_DETL_TBL from PeopleSoft table PS_RQ_GRP_DETL_TBL.
--
-- V01  SMT-xxxx 05/15/2017,    Jim Doucette
--                              Converted from PS_RQ_GRP_DETL_TBL.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_RQ_GRP_DETL_TBL';
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
       START_DT = sysdate,
       END_DT = NULL
 where TABLE_NAME = 'PS_RQ_GRP_DETL_TBL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_RQ_GRP_DETL_TBL@SASOURCE S)
 where TABLE_NAME = 'PS_RQ_GRP_DETL_TBL'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_RQ_GRP_DETL_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_RQ_GRP_DETL_TBL';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_RQ_GRP_DETL_TBL T
using (select /*+ full(S) */
    nvl(trim(RQRMNT_GROUP),'-') RQRMNT_GROUP,
    to_date(to_char(case when EFFDT < '01-JAN-1800' then NULL else EFFDT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EFFDT,
    nvl(trim(RQ_LINE_KEY_NBR),'-') RQ_LINE_KEY_NBR,
    nvl(trim(RQ_GRP_LINE_NBR),'-') RQ_GRP_LINE_NBR,
    nvl(trim(RQ_GRP_LINE_TYPE),'-') RQ_GRP_LINE_TYPE,
    MIN_UNITS_REQD MIN_UNITS_REQD,
    MIN_CRSES_REQD MIN_CRSES_REQD,
    REQUISITE_TYPE REQUISITE_TYPE,
    REQUIREMENT REQUIREMENT,
    CONDITION_CODE CONDITION_CODE,
    CONDITION_OPERATOR CONDITION_OPERATOR,
    CONDITION_DATA CONDITION_DATA,
    INSTITUTION INSTITUTION,
    ACAD_GROUP ACAD_GROUP,
    SUBJECT SUBJECT,
    CATALOG_NBR CATALOG_NBR,
    WILD_PATTERN_TYPE WILD_PATTERN_TYPE,
    CRSE_ID CRSE_ID,
    TRNSFR_LVL_ALLOWD TRNSFR_LVL_ALLOWD,
    TEST_CRDT_ALLOWD TEST_CRDT_ALLOWD,
    OTHR_CRDT_ALLOWD OTHR_CRDT_ALLOWD,
    INCL_GPA_REQ INCL_GPA_REQ,
    EXCL_IP_CREDIT EXCL_IP_CREDIT,
    GRADE_POINTS_MIN GRADE_POINTS_MIN,
    UNITS_MINIMUM UNITS_MINIMUM,
    INCLUDE_EQUIVALENT INCLUDE_EQUIVALENT,
    to_date(to_char(case when CRSVALID_BEGIN < '01-JAN-1800' then NULL else CRSVALID_BEGIN end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') CRSVALID_BEGIN,
    to_date(to_char(case when CRSVALID_END < '01-JAN-1800' then NULL else CRSVALID_END end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') CRSVALID_END,
    STRM STRM,
    ASSOCIATED_CLASS ASSOCIATED_CLASS,
    CRS_TOPIC_ID CRS_TOPIC_ID,
    RQMNT_DESIGNTN RQMNT_DESIGNTN,
    RQ_CONNECT RQ_CONNECT,
    PARENTHESIS PARENTHESIS,
    TEST_ID TEST_ID,
    TEST_COMPONENT TEST_COMPONENT,
    SCORE SCORE,
    SAA_MAX_VALID_AGE SAA_MAX_VALID_AGE,
    SAA_BEST_TEST_OPT SAA_BEST_TEST_OPT,
    SSR_DESCR80 SSR_DESCR80
from SYSADM.PS_RQ_GRP_DETL_TBL@SASOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_RQ_GRP_DETL_TBL') ) S
   on (
    T.RQRMNT_GROUP = S.RQRMNT_GROUP and
    T.EFFDT = S.EFFDT and
    T.RQ_LINE_KEY_NBR = S.RQ_LINE_KEY_NBR and
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.RQ_GRP_LINE_NBR = S.RQ_GRP_LINE_NBR,
    T.RQ_GRP_LINE_TYPE = S.RQ_GRP_LINE_TYPE,
    T.MIN_UNITS_REQD = S.MIN_UNITS_REQD,
    T.MIN_CRSES_REQD = S.MIN_CRSES_REQD,
    T.REQUISITE_TYPE = S.REQUISITE_TYPE,
    T.REQUIREMENT = S.REQUIREMENT,
    T.CONDITION_CODE = S.CONDITION_CODE,
    T.CONDITION_OPERATOR = S.CONDITION_OPERATOR,
    T.CONDITION_DATA = S.CONDITION_DATA,
    T.INSTITUTION = S.INSTITUTION,
    T.ACAD_GROUP = S.ACAD_GROUP,
    T.SUBJECT = S.SUBJECT,
    T.CATALOG_NBR = S.CATALOG_NBR,
    T.WILD_PATTERN_TYPE = S.WILD_PATTERN_TYPE,
    T.CRSE_ID = S.CRSE_ID,
    T.TRNSFR_LVL_ALLOWD = S.TRNSFR_LVL_ALLOWD,
    T.TEST_CRDT_ALLOWD = S.TEST_CRDT_ALLOWD,
    T.OTHR_CRDT_ALLOWD = S.OTHR_CRDT_ALLOWD,
    T.INCL_GPA_REQ = S.INCL_GPA_REQ,
    T.EXCL_IP_CREDIT = S.EXCL_IP_CREDIT,
    T.GRADE_POINTS_MIN = S.GRADE_POINTS_MIN,
    T.UNITS_MINIMUM = S.UNITS_MINIMUM,
    T.INCLUDE_EQUIVALENT = S.INCLUDE_EQUIVALENT,
    T.CRSVALID_BEGIN = S.CRSVALID_BEGIN,
    T.CRSVALID_END = S.CRSVALID_END,
    T.STRM = S.STRM,
    T.ASSOCIATED_CLASS = S.ASSOCIATED_CLASS,
    T.CRS_TOPIC_ID = S.CRS_TOPIC_ID,
    T.RQMNT_DESIGNTN = S.RQMNT_DESIGNTN,
    T.RQ_CONNECT = S.RQ_CONNECT,
    T.PARENTHESIS = S.PARENTHESIS,
    T.TEST_ID = S.TEST_ID,
    T.TEST_COMPONENT = S.TEST_COMPONENT,
    T.SCORE = S.SCORE,
    T.SAA_MAX_VALID_AGE = S.SAA_MAX_VALID_AGE,
    T.SAA_BEST_TEST_OPT = S.SAA_BEST_TEST_OPT,
    T.SSR_DESCR80 = S.SSR_DESCR80,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID   = 1234
where
    T.RQ_GRP_LINE_NBR <> S.RQ_GRP_LINE_NBR or
    T.RQ_GRP_LINE_TYPE <> S.RQ_GRP_LINE_TYPE or
    nvl(trim(T.MIN_UNITS_REQD),0) <> nvl(trim(S.MIN_UNITS_REQD),0) or
    nvl(trim(T.MIN_CRSES_REQD),0) <> nvl(trim(S.MIN_CRSES_REQD),0) or
    nvl(trim(T.REQUISITE_TYPE),0) <> nvl(trim(S.REQUISITE_TYPE),0) or
    nvl(trim(T.REQUIREMENT),0) <> nvl(trim(S.REQUIREMENT),0) or
    nvl(trim(T.CONDITION_CODE),0) <> nvl(trim(S.CONDITION_CODE),0) or
    nvl(trim(T.CONDITION_OPERATOR),0) <> nvl(trim(S.CONDITION_OPERATOR),0) or
    nvl(trim(T.CONDITION_DATA),0) <> nvl(trim(S.CONDITION_DATA),0) or
    nvl(trim(T.INSTITUTION),0) <> nvl(trim(S.INSTITUTION),0) or
    nvl(trim(T.ACAD_GROUP),0) <> nvl(trim(S.ACAD_GROUP),0) or
    nvl(trim(T.SUBJECT),0) <> nvl(trim(S.SUBJECT),0) or
    nvl(trim(T.CATALOG_NBR),0) <> nvl(trim(S.CATALOG_NBR),0) or
    nvl(trim(T.WILD_PATTERN_TYPE),0) <> nvl(trim(S.WILD_PATTERN_TYPE),0) or
    nvl(trim(T.CRSE_ID),0) <> nvl(trim(S.CRSE_ID),0) or
    nvl(trim(T.TRNSFR_LVL_ALLOWD),0) <> nvl(trim(S.TRNSFR_LVL_ALLOWD),0) or
    nvl(trim(T.TEST_CRDT_ALLOWD),0) <> nvl(trim(S.TEST_CRDT_ALLOWD),0) or
    nvl(trim(T.OTHR_CRDT_ALLOWD),0) <> nvl(trim(S.OTHR_CRDT_ALLOWD),0) or
    nvl(trim(T.INCL_GPA_REQ),0) <> nvl(trim(S.INCL_GPA_REQ),0) or
    nvl(trim(T.EXCL_IP_CREDIT),0) <> nvl(trim(S.EXCL_IP_CREDIT),0) or
    nvl(trim(T.GRADE_POINTS_MIN),0) <> nvl(trim(S.GRADE_POINTS_MIN),0) or
    nvl(trim(T.UNITS_MINIMUM),0) <> nvl(trim(S.UNITS_MINIMUM),0) or
    nvl(trim(T.INCLUDE_EQUIVALENT),0) <> nvl(trim(S.INCLUDE_EQUIVALENT),0) or
    nvl(trim(T.CRSVALID_BEGIN),0) <> nvl(trim(S.CRSVALID_BEGIN),0) or
    nvl(trim(T.CRSVALID_END),0) <> nvl(trim(S.CRSVALID_END),0) or
    nvl(trim(T.STRM),0) <> nvl(trim(S.STRM),0) or
    nvl(trim(T.ASSOCIATED_CLASS),0) <> nvl(trim(S.ASSOCIATED_CLASS),0) or
    nvl(trim(T.CRS_TOPIC_ID),0) <> nvl(trim(S.CRS_TOPIC_ID),0) or
    nvl(trim(T.RQMNT_DESIGNTN),0) <> nvl(trim(S.RQMNT_DESIGNTN),0) or
    nvl(trim(T.RQ_CONNECT),0) <> nvl(trim(S.RQ_CONNECT),0) or
    nvl(trim(T.PARENTHESIS),0) <> nvl(trim(S.PARENTHESIS),0) or
    nvl(trim(T.TEST_ID),0) <> nvl(trim(S.TEST_ID),0) or
    nvl(trim(T.TEST_COMPONENT),0) <> nvl(trim(S.TEST_COMPONENT),0) or
    nvl(trim(T.SCORE),0) <> nvl(trim(S.SCORE),0) or
    nvl(trim(T.SAA_MAX_VALID_AGE),0) <> nvl(trim(S.SAA_MAX_VALID_AGE),0) or
    nvl(trim(T.SAA_BEST_TEST_OPT),0) <> nvl(trim(S.SAA_BEST_TEST_OPT),0) or
    nvl(trim(T.SSR_DESCR80),0) <> nvl(trim(S.SSR_DESCR80),0) or
    T.DATA_ORIGIN = 'D'
when not matched then
insert (
    T.RQRMNT_GROUP,
    T.EFFDT,
    T.RQ_LINE_KEY_NBR,
    T.SRC_SYS_ID,
    T.RQ_GRP_LINE_NBR,
    T.RQ_GRP_LINE_TYPE,
    T.MIN_UNITS_REQD,
    T.MIN_CRSES_REQD,
    T.REQUISITE_TYPE,
    T.REQUIREMENT,
    T.CONDITION_CODE,
    T.CONDITION_OPERATOR,
    T.CONDITION_DATA,
    T.INSTITUTION,
    T.ACAD_GROUP,
    T.SUBJECT,
    T.CATALOG_NBR,
    T.WILD_PATTERN_TYPE,
    T.CRSE_ID,
    T.TRNSFR_LVL_ALLOWD,
    T.TEST_CRDT_ALLOWD,
    T.OTHR_CRDT_ALLOWD,
    T.INCL_GPA_REQ,
    T.EXCL_IP_CREDIT,
    T.GRADE_POINTS_MIN,
    T.UNITS_MINIMUM,
    T.INCLUDE_EQUIVALENT,
    T.CRSVALID_BEGIN,
    T.CRSVALID_END,
    T.STRM,
    T.ASSOCIATED_CLASS,
    T.CRS_TOPIC_ID,
    T.RQMNT_DESIGNTN,
    T.RQ_CONNECT,
    T.PARENTHESIS,
    T.TEST_ID,
    T.TEST_COMPONENT,
    T.SCORE,
    T.SAA_MAX_VALID_AGE,
    T.SAA_BEST_TEST_OPT,
    T.SSR_DESCR80,
    T.LOAD_ERROR,
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
    )
values (
    S.RQRMNT_GROUP,
    S.EFFDT,
    S.RQ_LINE_KEY_NBR,
    'CS90',
    S.RQ_GRP_LINE_NBR,
    S.RQ_GRP_LINE_TYPE,
    S.MIN_UNITS_REQD,
    S.MIN_CRSES_REQD,
    S.REQUISITE_TYPE,
    S.REQUIREMENT,
    S.CONDITION_CODE,
    S.CONDITION_OPERATOR,
    S.CONDITION_DATA,
    S.INSTITUTION,
    S.ACAD_GROUP,
    S.SUBJECT,
    S.CATALOG_NBR,
    S.WILD_PATTERN_TYPE,
    S.CRSE_ID,
    S.TRNSFR_LVL_ALLOWD,
    S.TEST_CRDT_ALLOWD,
    S.OTHR_CRDT_ALLOWD,
    S.INCL_GPA_REQ,
    S.EXCL_IP_CREDIT,
    S.GRADE_POINTS_MIN,
    S.UNITS_MINIMUM,
    S.INCLUDE_EQUIVALENT,
    S.CRSVALID_BEGIN,
    S.CRSVALID_END,
    S.STRM,
    S.ASSOCIATED_CLASS,
    S.CRS_TOPIC_ID,
    S.RQMNT_DESIGNTN,
    S.RQ_CONNECT,
    S.PARENTHESIS,
    S.TEST_ID,
    S.TEST_COMPONENT,
    S.SCORE,
    S.SAA_MAX_VALID_AGE,
    S.SAA_BEST_TEST_OPT,
    S.SSR_DESCR80,
    'N',
    'S',
    sysdate,
    sysdate,
    1234);


strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_RQ_GRP_DETL_TBL rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_RQ_GRP_DETL_TBL',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_RQ_GRP_DETL_TBL';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_RQ_GRP_DETL_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_RQ_GRP_DETL_TBL';
update CSSTG_OWNER.PS_RQ_GRP_DETL_TBL T
        set T.DATA_ORIGIN = 'D',
               T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select RQRMNT_GROUP, EFFDT, RQ_LINE_KEY_NBR
   from CSSTG_OWNER.PS_RQ_GRP_DETL_TBL T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_RQ_GRP_DETL_TBL') = 'Y'
  minus
 select RQRMNT_GROUP, EFFDT, RQ_LINE_KEY_NBR
   from SYSADM.PS_RQ_GRP_DETL_TBL@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_RQ_GRP_DETL_TBL') = 'Y'
   ) S
 where T.RQRMNT_GROUP = S.RQRMNT_GROUP
   and T.EFFDT = S.EFFDT
   and T.RQ_LINE_KEY_NBR = S.RQ_LINE_KEY_NBR
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_RQ_GRP_DETL_TBL rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_RQ_GRP_DETL_TBL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_RQ_GRP_DETL_TBL'
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

END PS_RQ_GRP_DETL_TBL_P;
/
