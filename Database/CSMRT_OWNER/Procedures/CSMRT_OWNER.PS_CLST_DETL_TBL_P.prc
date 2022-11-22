DROP PROCEDURE CSMRT_OWNER.PS_CLST_DETL_TBL_P
/

--
-- PS_CLST_DETL_TBL_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_CLST_DETL_TBL_P" AUTHID CURRENT_USER IS

/*
-- Run before the first time
DELETE
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_CLST_DETL_TBL'

INSERT INTO CSSTG_OWNER.UM_STAGE_JOBS
(TABLE_NAME, DELETE_FLG)
VALUES
('PS_CLST_DETL_TBL', 'Y')

SELECT *
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_CLST_DETL_TBL'
*/


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_CLST_DETL_TBL from PeopleSoft table PS_CLST_DETL_TBL.
--
-- V01  SMT-xxxx 05/16/2017,    Jim Doucette
--                              Converted from PS_CLST_DETL_TBL.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_CLST_DETL_TBL';
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
 where TABLE_NAME = 'PS_CLST_DETL_TBL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_CLST_DETL_TBL@SASOURCE S)
 where TABLE_NAME = 'PS_CLST_DETL_TBL'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_CLST_DETL_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_CLST_DETL_TBL';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_CLST_DETL_TBL T
using (select /*+ full(S) */
    nvl(trim(COURSE_LIST),'-') COURSE_LIST, 
    to_date(to_char(case when EFFDT < '01-JAN-1800' then NULL else EFFDT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EFFDT, 
    nvl(R_COURSE_SEQUENCE,0) R_COURSE_SEQUENCE, 
    WILDCARD_IND WILDCARD_IND,
    to_date(to_char(case when CRSVALID_BEGIN < '01-JAN-1800' then NULL else CRSVALID_BEGIN end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') CRSVALID_BEGIN,
    to_date(to_char(case when CRSVALID_END < '01-JAN-1800' then NULL else CRSVALID_END end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') CRSVALID_END,
    TRNSFR_LVL_ALLOWD TRNSFR_LVL_ALLOWD,
    TEST_CRDT_ALLOWD TEST_CRDT_ALLOWD,
    OTHR_CRDT_ALLOWD OTHR_CRDT_ALLOWD,
    INCL_GPA_REQ INCL_GPA_REQ,
    EXCL_IP_CREDIT EXCL_IP_CREDIT,
    GRADE_POINTS_MIN GRADE_POINTS_MIN,
    UNITS_MINIMUM UNITS_MINIMUM,
    INSTITUTION INSTITUTION,
    ACAD_GROUP ACAD_GROUP,
    SUBJECT SUBJECT,
    CATALOG_NBR CATALOG_NBR,
    WILD_PATTERN_TYPE WILD_PATTERN_TYPE,
    CRSE_ID CRSE_ID,
    INCLUDE_EQUIVALENT INCLUDE_EQUIVALENT,
    STRM STRM,
    ASSOCIATED_CLASS ASSOCIATED_CLASS,
    CRS_TOPIC_ID CRS_TOPIC_ID,
    RQMNT_DESIGNTN RQMNT_DESIGNTN,
    SAA_DSP_WILD_CRSES SAA_DSP_WILD_CRSES,
    SAA_WILDCARD_XLIST SAA_WILDCARD_XLIST,
    DESCR DESCR,
    SAA_DESCR254 SAA_DESCR254
from SYSADM.PS_CLST_DETL_TBL@SASOURCE S 
where ORA_ROWSCN > (select OLD_MAX_SCN 
                      from CSSTG_OWNER.UM_STAGE_JOBS 
                     where TABLE_NAME = 'PS_CLST_DETL_TBL') ) S
 on (T.COURSE_LIST = S.COURSE_LIST 
 and T.EFFDT = S.EFFDT 
 and T.R_COURSE_SEQUENCE = S.R_COURSE_SEQUENCE 
 and T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.WILDCARD_IND = S.WILDCARD_IND,
    T.CRSVALID_BEGIN = S.CRSVALID_BEGIN,
    T.CRSVALID_END = S.CRSVALID_END,
    T.TRNSFR_LVL_ALLOWD = S.TRNSFR_LVL_ALLOWD,
    T.TEST_CRDT_ALLOWD = S.TEST_CRDT_ALLOWD,
    T.OTHR_CRDT_ALLOWD = S.OTHR_CRDT_ALLOWD,
    T.INCL_GPA_REQ = S.INCL_GPA_REQ,
    T.EXCL_IP_CREDIT = S.EXCL_IP_CREDIT,
    T.GRADE_POINTS_MIN = S.GRADE_POINTS_MIN,
    T.UNITS_MINIMUM = S.UNITS_MINIMUM,
    T.INSTITUTION = S.INSTITUTION,
    T.ACAD_GROUP = S.ACAD_GROUP,
    T.SUBJECT = S.SUBJECT,
    T.CATALOG_NBR = S.CATALOG_NBR,
    T.WILD_PATTERN_TYPE = S.WILD_PATTERN_TYPE,
    T.CRSE_ID = S.CRSE_ID,
    T.INCLUDE_EQUIVALENT = S.INCLUDE_EQUIVALENT,
    T.STRM = S.STRM,
    T.ASSOCIATED_CLASS = S.ASSOCIATED_CLASS,
    T.CRS_TOPIC_ID = S.CRS_TOPIC_ID,
    T.RQMNT_DESIGNTN = S.RQMNT_DESIGNTN,
    T.SAA_DSP_WILD_CRSES = S.SAA_DSP_WILD_CRSES,
    T.SAA_WILDCARD_XLIST = S.SAA_WILDCARD_XLIST,
    T.DESCR = S.DESCR,
    T.SAA_DESCR254 = S.SAA_DESCR254,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    nvl(trim(T.WILDCARD_IND),0) <> nvl(trim(S.WILDCARD_IND),0) or 
    nvl(trim(T.CRSVALID_BEGIN),0) <> nvl(trim(S.CRSVALID_BEGIN),0) or 
    nvl(trim(T.CRSVALID_END),0) <> nvl(trim(S.CRSVALID_END),0) or 
    nvl(trim(T.TRNSFR_LVL_ALLOWD),0) <> nvl(trim(S.TRNSFR_LVL_ALLOWD),0) or 
    nvl(trim(T.TEST_CRDT_ALLOWD),0) <> nvl(trim(S.TEST_CRDT_ALLOWD),0) or 
    nvl(trim(T.OTHR_CRDT_ALLOWD),0) <> nvl(trim(S.OTHR_CRDT_ALLOWD),0) or 
    nvl(trim(T.INCL_GPA_REQ),0) <> nvl(trim(S.INCL_GPA_REQ),0) or 
    nvl(trim(T.EXCL_IP_CREDIT),0) <> nvl(trim(S.EXCL_IP_CREDIT),0) or 
    nvl(trim(T.GRADE_POINTS_MIN),0) <> nvl(trim(S.GRADE_POINTS_MIN),0) or 
    nvl(trim(T.UNITS_MINIMUM),0) <> nvl(trim(S.UNITS_MINIMUM),0) or 
    nvl(trim(T.INSTITUTION),0) <> nvl(trim(S.INSTITUTION),0) or 
    nvl(trim(T.ACAD_GROUP),0) <> nvl(trim(S.ACAD_GROUP),0) or 
    nvl(trim(T.SUBJECT),0) <> nvl(trim(S.SUBJECT),0) or 
    nvl(trim(T.CATALOG_NBR),0) <> nvl(trim(S.CATALOG_NBR),0) or 
    nvl(trim(T.WILD_PATTERN_TYPE),0) <> nvl(trim(S.WILD_PATTERN_TYPE),0) or 
    nvl(trim(T.CRSE_ID),0) <> nvl(trim(S.CRSE_ID),0) or 
    nvl(trim(T.INCLUDE_EQUIVALENT),0) <> nvl(trim(S.INCLUDE_EQUIVALENT),0) or 
    nvl(trim(T.STRM),0) <> nvl(trim(S.STRM),0) or 
    nvl(trim(T.ASSOCIATED_CLASS),0) <> nvl(trim(S.ASSOCIATED_CLASS),0) or 
    nvl(trim(T.CRS_TOPIC_ID),0) <> nvl(trim(S.CRS_TOPIC_ID),0) or 
    nvl(trim(T.RQMNT_DESIGNTN),0) <> nvl(trim(S.RQMNT_DESIGNTN),0) or 
    nvl(trim(T.SAA_DSP_WILD_CRSES),0) <> nvl(trim(S.SAA_DSP_WILD_CRSES),0) or 
    nvl(trim(T.SAA_WILDCARD_XLIST),0) <> nvl(trim(S.SAA_WILDCARD_XLIST),0) or 
    nvl(trim(T.DESCR),0) <> nvl(trim(S.DESCR),0) or 
    nvl(trim(T.SAA_DESCR254),0) <> nvl(trim(S.SAA_DESCR254),0) or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.COURSE_LIST,
    T.EFFDT,
    T.R_COURSE_SEQUENCE,
    T.SRC_SYS_ID, 
    T.WILDCARD_IND, 
    T.CRSVALID_BEGIN, 
    T.CRSVALID_END, 
    T.TRNSFR_LVL_ALLOWD,
    T.TEST_CRDT_ALLOWD, 
    T.OTHR_CRDT_ALLOWD, 
    T.INCL_GPA_REQ, 
    T.EXCL_IP_CREDIT, 
    T.GRADE_POINTS_MIN, 
    T.UNITS_MINIMUM,
    T.INSTITUTION,
    T.ACAD_GROUP, 
    T.SUBJECT,
    T.CATALOG_NBR,
    T.WILD_PATTERN_TYPE,
    T.CRSE_ID,
    T.INCLUDE_EQUIVALENT, 
    T.STRM, 
    T.ASSOCIATED_CLASS, 
    T.CRS_TOPIC_ID, 
    T.RQMNT_DESIGNTN, 
    T.SAA_DSP_WILD_CRSES, 
    T.SAA_WILDCARD_XLIST, 
    T.DESCR,
    T.SAA_DESCR254, 
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
) 
values (
    S.COURSE_LIST,
    S.EFFDT,
    S.R_COURSE_SEQUENCE,
    'CS90', 
    S.WILDCARD_IND, 
    S.CRSVALID_BEGIN, 
    S.CRSVALID_END, 
    S.TRNSFR_LVL_ALLOWD,
    S.TEST_CRDT_ALLOWD, 
    S.OTHR_CRDT_ALLOWD, 
    S.INCL_GPA_REQ, 
    S.EXCL_IP_CREDIT, 
    S.GRADE_POINTS_MIN, 
    S.UNITS_MINIMUM,
    S.INSTITUTION,
    S.ACAD_GROUP, 
    S.SUBJECT,
    S.CATALOG_NBR,
    S.WILD_PATTERN_TYPE,
    S.CRSE_ID,
    S.INCLUDE_EQUIVALENT, 
    S.STRM, 
    S.ASSOCIATED_CLASS, 
    S.CRS_TOPIC_ID, 
    S.RQMNT_DESIGNTN, 
    S.SAA_DSP_WILD_CRSES, 
    S.SAA_WILDCARD_XLIST, 
    S.DESCR,
    S.SAA_DESCR254, 
    'N',
    'S',
    sysdate,
    sysdate,
    1234);


strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_CLST_DETL_TBL rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_CLST_DETL_TBL',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_CLST_DETL_TBL';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_CLST_DETL_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_CLST_DETL_TBL';
update CSSTG_OWNER.PS_CLST_DETL_TBL T
        set T.DATA_ORIGIN = 'D',
               T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select COURSE_LIST, EFFDT, R_COURSE_SEQUENCE
   from CSSTG_OWNER.PS_CLST_DETL_TBL T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CLST_DETL_TBL') = 'Y'
  minus
 select COURSE_LIST, EFFDT, R_COURSE_SEQUENCE
   from SYSADM.PS_CLST_DETL_TBL@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CLST_DETL_TBL') = 'Y'
   ) S
 where T.COURSE_LIST = S.COURSE_LIST
   and T.EFFDT = S.EFFDT
   and T.R_COURSE_SEQUENCE = S.R_COURSE_SEQUENCE
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_CLST_DETL_TBL rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_CLST_DETL_TBL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_CLST_DETL_TBL'
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

END PS_CLST_DETL_TBL_P;
/
