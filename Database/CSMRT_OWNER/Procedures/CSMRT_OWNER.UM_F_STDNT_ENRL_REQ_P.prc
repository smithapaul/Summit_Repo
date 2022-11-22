DROP PROCEDURE CSMRT_OWNER.UM_F_STDNT_ENRL_REQ_P
/

--
-- UM_F_STDNT_ENRL_REQ_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_F_STDNT_ENRL_REQ_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table UM_F_STDNT_ENRL_REQ from PeopleSoft table UM_F_STDNT_ENRL_REQ.
--
 --V01  SMT-xxxx 07/02/2018,    James Doucette
--                              Converted from SQL Script
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_STDNT_ENRL_REQ';
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

strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_STDNT_ENRL_REQ';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_STDNT_ENRL_REQ';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_STDNT_ENRL_REQ';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_STDNT_ENRL_REQ');

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_STDNT_ENRL_REQ disable constraint PK_UM_F_STDNT_ENRL_REQ';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_STDNT_ENRL_REQ';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_STDNT_ENRL_REQ';
insert /*+ append enable_parallel_dml parallel(8) */ into UM_F_STDNT_ENRL_REQ
with CAR1 as (
select /*+ inline parallel(8) */
INSTITUTION, ACAD_CAREER, SRC_SYS_ID, GRADING_SCHEME, REPEAT_SCHEME,
row_number() over (partition by INSTITUTION, ACAD_CAREER, SRC_SYS_ID
                       order by EFFDT desc) CAR_ORDER
from CSSTG_OWNER.PS_ACAD_CAR_TBL
where DATA_ORIGIN <> 'D'),
CAR2 as (
select /*+ inline parallel(8) */
INSTITUTION, ACAD_CAREER, SRC_SYS_ID, GRADING_SCHEME, REPEAT_SCHEME
from CAR1
where CAR_ORDER = 1),
CLASS as (
select /*+ inline parallel(8) */
INSTITUTION_CD, TERM_CD, CLASS_NUM, SRC_SYS_ID, CLASS_SID,
row_number() over (partition by INSTITUTION_CD, TERM_CD, CLASS_NUM, SRC_SYS_ID
                       order by SESSION_CD, CLASS_SECTION_CD) CLASS_ORDER
from UM_D_CLASS L
where DATA_ORIGIN <> 'D'
)
SELECT /*+ parallel(8) */
R.ENRL_REQUEST_ID,
R.ENRL_REQ_DETL_SEQ,
R.SRC_SYS_ID,
nvl(I.INSTITUTION_SID,2147483646) INSTITUTION_SID,
nvl(C.ACAD_CAR_SID,2147483646) ACAD_CAR_SID,
nvl(T.TERM_SID,2147483646) TERM_SID,
nvl(P.PERSON_SID,2147483646) PERSON_SID,
nvl(L.CLASS_SID,2147483646) CLASS_SID,
R.INSTITUTION INSTITUTION_CD,
R.ACAD_CAREER ACAD_CAR_CD,
R.STRM TERM_CD,
R.EMPLID PERSON_ID,
R.CLASS_NBR,
R.ENRL_REQ_ACTION,
nvl((SELECT XLATLONGNAME
       FROM UM_D_XLATITEM_VW X
      WHERE FIELDNAME = 'ENRL_REQ_ACTION' AND R.ENRL_REQ_ACTION = X.FIELDVALUE),'-') ENRL_REQ_ACTION_LD,
R.ENRL_ACTION_REASON,
nvl(D.ENRL_ACT_RSN_LD,'-') ENRL_ACTION_REASON_LD,
R.ENRL_ACTION_DT,
R.UNT_TAKEN,
R.UNT_EARNED,
R.CRSE_COUNT,
R.REPEAT_CODE,
nvl(R2.REPEAT_SID,2147483646) REPEAT_SID,
R.CRSE_GRADE_INPUT,
R.GRADING_BASIS_ENRL,
nvl((SELECT XLATLONGNAME
       FROM UM_D_XLATITEM_VW X
      WHERE FIELDNAME = 'GRADING_BASIS' AND R.GRADING_BASIS_ENRL = X.FIELDVALUE),'-') GRADING_BASIS_ENRL_LD,
R.CLASS_PRMSN_NBR,
R.CLASS_NBR_CHG_TO,
R.DROP_CLASS_IF_ENRL,
R.CHG_TO_WL_NUM,
R.RELATE_CLASS_NBR_1,
R.RELATE_CLASS_NBR_2,
R.OVRD_CLASS_LIMIT,
R.OVRD_GRADING_BASIS,
R.OVRD_CLASS_UNITS,
R.OVRD_UNIT_LOAD,
R.OVRD_CLASS_LINKS,
R.OVRD_CLASS_PRMSN,
R.OVRD_REQUISITES,
R.OVRD_TIME_CNFLCT,
R.OVRD_CAREER,
R.WAIT_LIST_OKAY,
R.OVRD_ENRL_ACTN_DT,
R.OVRD_RQMNT_DESIG,
R.OVRD_SRVC_INDIC,
R.OVRD_APPT,
R.INSTRUCTOR_ID,
R.ENRL_REQ_DETL_STAT,
nvl((SELECT XLATLONGNAME
       FROM UM_D_XLATITEM_VW X
      WHERE FIELDNAME = 'ENRL_REQ_DETL_STAT' AND R.ENRL_REQ_DETL_STAT = X.FIELDVALUE),'-') ENRL_REQ_DETL_STAT_LD,
R.RQMNT_DESIGNTN,
R.RQMNT_DESIGNTN_OPT,
R.RQMNT_DESIGNTN_GRD,
R.TSCRPT_NOTE_ID,
R.TSCRPT_NOTE_EXISTS,
R.OPRID,
R.DTTM_STAMP_SEC,
R.START_DT,
R.ACAD_PROG,
nvl(H.ENRL_REQ_SOURCE,'-') ENRL_REQ_SOURCE,
nvl((SELECT XLATLONGNAME
       FROM UM_D_XLATITEM_VW X
      WHERE FIELDNAME = 'ENRL_REQ_SOURCE' AND H.ENRL_REQ_SOURCE = X.FIELDVALUE),'-') ENRL_REQ_SOURCE_LD,
'N' LOAD_ERROR,
'S' DATA_ORIGIN,
sysdate CREATED_EW_DTTM,
sysdate LASTUPD_EW_DTTM,
1234 BATCH_SID
FROM CSSTG_OWNER.PS_ENRL_REQ_DETAIL R
left outer join CSSTG_OWNER.PS_ENRL_REQ_HEADER H
  on R.ENRL_REQUEST_ID = H.ENRL_REQUEST_ID
 and R.SRC_SYS_ID = H.SRC_SYS_ID
 and H.DATA_ORIGIN <> 'D'
left outer join CSMRT_OWNER.PS_D_INSTITUTION I
  on R.INSTITUTION = I.INSTITUTION_CD
 and R.SRC_SYS_ID = I.SRC_SYS_ID
left outer join CSMRT_OWNER.PS_D_ACAD_CAR C
  on R.INSTITUTION = C.INSTITUTION_CD
 and R.ACAD_CAREER = C.ACAD_CAR_CD
 and R.SRC_SYS_ID = C.SRC_SYS_ID
left outer join CSMRT_OWNER.PS_D_TERM T
  on R.INSTITUTION = T.INSTITUTION_CD
 and R.ACAD_CAREER = T.ACAD_CAR_CD
 and R.STRM = T.TERM_CD
 and R.SRC_SYS_ID = T.SRC_SYS_ID
--left outer join UM_D_PERSON_AGG P     -- Does not have XXX people!!!
left outer join CSMRT_OWNER.PS_D_PERSON P
  on R.EMPLID = P.PERSON_ID
 and R.SRC_SYS_ID = P.SRC_SYS_ID
--left outer join UM_D_CLASS L
--  on R.INSTITUTION = L.INSTITUTION_CD
-- and R.STRM = L.TERM_CD
-- and R.CLASS_NBR = L.CLASS_NUM
-- and R.SRC_SYS_ID = L.SRC_SYS_ID
-- and L.DATA_ORIGIN <> 'D'
left outer join CLASS L
  on R.INSTITUTION = L.INSTITUTION_CD
 and R.STRM = L.TERM_CD
 and R.CLASS_NBR = L.CLASS_NUM
 and R.SRC_SYS_ID = L.SRC_SYS_ID
 and L.CLASS_ORDER = 1
left outer join CSMRT_OWNER.PS_D_ENRL_RSN D
  on R.INSTITUTION = D.SETID
 and R.ACAD_CAREER = D.ACAD_CAR_CD
 and R.ENRL_REQ_ACTION = D.ENRL_ACTION
 and R.ENRL_ACTION_REASON = D.ENRL_ACT_RSN
left outer join CAR2
  on R.INSTITUTION = CAR2.INSTITUTION
 and R.ACAD_CAREER = CAR2.ACAD_CAREER
 and R.SRC_SYS_ID = CAR2.SRC_SYS_ID
left outer join CSMRT_OWNER.PS_D_REPEAT R2
  on R.INSTITUTION = R2.SETID
 and CAR2.REPEAT_SCHEME = R2.REPEAT_SCHEME_CD
 and R.REPEAT_CODE = R2.REPEAT_CD
 and R.SRC_SYS_ID = R2.SRC_SYS_ID
where R.DATA_ORIGIN <> 'D'
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_STDNT_ENRL_REQ rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_STDNT_ENRL_REQ',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_STDNT_ENRL_REQ',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_STDNT_ENRL_REQ';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_STDNT_ENRL_REQ enable constraint PK_UM_F_STDNT_ENRL_REQ';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_STDNT_ENRL_REQ');

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

END UM_F_STDNT_ENRL_REQ_P;
/
