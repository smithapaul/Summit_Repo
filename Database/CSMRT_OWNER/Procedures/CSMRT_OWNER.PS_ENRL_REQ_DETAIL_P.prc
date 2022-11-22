DROP PROCEDURE CSMRT_OWNER.PS_ENRL_REQ_DETAIL_P
/

--
-- PS_ENRL_REQ_DETAIL_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_ENRL_REQ_DETAIL_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_ENRL_REQ_DETAIL from PeopleSoft table PS_ENRL_REQ_DETAIL.
--
-- V01  SMT-xxxx 05/11/2017,    Jim Doucette
--                              Converted from PS_ENRL_REQ_DETAIL.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_ENRL_REQ_DETAIL';
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
 where TABLE_NAME = 'PS_ENRL_REQ_DETAIL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_ENRL_REQ_DETAIL@SASOURCE S)
 where TABLE_NAME = 'PS_ENRL_REQ_DETAIL'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_ENRL_REQ_DETAIL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_ENRL_REQ_DETAIL';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_ENRL_REQ_DETAIL T
using (select /*+ full(S) */
    nvl(trim(ENRL_REQUEST_ID),'-') ENRL_REQUEST_ID,
    nvl(ENRL_REQ_DETL_SEQ,0) ENRL_REQ_DETL_SEQ,
    nvl(trim(EMPLID),'-') EMPLID,
    nvl(trim(ACAD_CAREER),'-') ACAD_CAREER,
    nvl(trim(INSTITUTION),'-') INSTITUTION,
    nvl(trim(STRM),'-') STRM,
    nvl(CLASS_NBR,0) CLASS_NBR,
    nvl(trim(ENRL_REQ_ACTION),'-') ENRL_REQ_ACTION,
    nvl(trim(ENRL_ACTION_REASON),'-') ENRL_ACTION_REASON,
    to_date(to_char(case when ENRL_ACTION_DT < '01-JAN-1800' then NULL else ENRL_ACTION_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') ENRL_ACTION_DT,
    nvl(UNT_TAKEN,0) UNT_TAKEN,
    nvl(UNT_EARNED,0) UNT_EARNED,
    nvl(CRSE_COUNT,0) CRSE_COUNT,
    nvl(trim(REPEAT_CODE),'-') REPEAT_CODE,
    nvl(trim(CRSE_GRADE_INPUT),'-') CRSE_GRADE_INPUT,
    nvl(trim(GRADING_BASIS_ENRL),'-') GRADING_BASIS_ENRL,
    nvl(CLASS_PRMSN_NBR,0) CLASS_PRMSN_NBR,
    nvl(CLASS_NBR_CHG_TO,0) CLASS_NBR_CHG_TO,
    nvl(DROP_CLASS_IF_ENRL,0) DROP_CLASS_IF_ENRL,
    nvl(CHG_TO_WL_NUM,0) CHG_TO_WL_NUM,
    nvl(RELATE_CLASS_NBR_1,0) RELATE_CLASS_NBR_1,
    nvl(RELATE_CLASS_NBR_2,0) RELATE_CLASS_NBR_2,
    nvl(trim(OVRD_CLASS_LIMIT),'-') OVRD_CLASS_LIMIT,
    nvl(trim(OVRD_GRADING_BASIS),'-') OVRD_GRADING_BASIS,
    nvl(trim(OVRD_CLASS_UNITS),'-') OVRD_CLASS_UNITS,
    nvl(trim(OVRD_UNIT_LOAD),'-') OVRD_UNIT_LOAD,
    nvl(trim(OVRD_CLASS_LINKS),'-') OVRD_CLASS_LINKS,
    nvl(trim(OVRD_CLASS_PRMSN),'-') OVRD_CLASS_PRMSN,
    nvl(trim(OVRD_REQUISITES),'-') OVRD_REQUISITES,
    nvl(trim(OVRD_TIME_CNFLCT),'-') OVRD_TIME_CNFLCT,
    nvl(trim(OVRD_CAREER),'-') OVRD_CAREER,
    nvl(trim(WAIT_LIST_OKAY),'-') WAIT_LIST_OKAY,
    nvl(trim(OVRD_ENRL_ACTN_DT),'-') OVRD_ENRL_ACTN_DT,
    nvl(trim(OVRD_RQMNT_DESIG),'-') OVRD_RQMNT_DESIG,
    nvl(trim(OVRD_SRVC_INDIC),'-') OVRD_SRVC_INDIC,
    nvl(trim(OVRD_APPT),'-') OVRD_APPT,
    nvl(trim(INSTRUCTOR_ID),'-') INSTRUCTOR_ID,
    nvl(trim(ENRL_REQ_DETL_STAT),'-') ENRL_REQ_DETL_STAT,
    nvl(trim(RQMNT_DESIGNTN),'-') RQMNT_DESIGNTN,
    nvl(trim(RQMNT_DESIGNTN_OPT),'-') RQMNT_DESIGNTN_OPT,
    nvl(trim(RQMNT_DESIGNTN_GRD),'-') RQMNT_DESIGNTN_GRD,
    nvl(trim(TSCRPT_NOTE_ID),'-') TSCRPT_NOTE_ID,
    nvl(trim(TSCRPT_NOTE_EXISTS),'-') TSCRPT_NOTE_EXISTS,
    nvl(trim(OPRID),'-') OPRID,
    to_date(to_char(case when DTTM_STAMP_SEC < '01-JAN-1800' then NULL else DTTM_STAMP_SEC end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') DTTM_STAMP_SEC,
    to_date(to_char(case when START_DT < '01-JAN-1800' then NULL else START_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') START_DT,
    nvl(trim(ACAD_PROG),'-') ACAD_PROG
from SYSADM.PS_ENRL_REQ_DETAIL@SASOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ENRL_REQ_DETAIL') 
  and STRM >= '2010'
  and ENRL_REQ_ACTION <> 'F'
  and EMPLID BETWEEN '00000000' AND '99999999'
  and length(EMPLID) = 8
) S
   on (
    T.ENRL_REQUEST_ID = S.ENRL_REQUEST_ID and
    T.ENRL_REQ_DETL_SEQ = S.ENRL_REQ_DETL_SEQ and
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.EMPLID = S.EMPLID,
    T.ACAD_CAREER = S.ACAD_CAREER,
    T.INSTITUTION = S.INSTITUTION,
    T.STRM = S.STRM,
    T.CLASS_NBR = S.CLASS_NBR,
    T.ENRL_REQ_ACTION = S.ENRL_REQ_ACTION,
    T.ENRL_ACTION_REASON = S.ENRL_ACTION_REASON,
    T.ENRL_ACTION_DT = S.ENRL_ACTION_DT,
    T.UNT_TAKEN = S.UNT_TAKEN,
    T.UNT_EARNED = S.UNT_EARNED,
    T.CRSE_COUNT = S.CRSE_COUNT,
    T.REPEAT_CODE = S.REPEAT_CODE,
    T.CRSE_GRADE_INPUT = S.CRSE_GRADE_INPUT,
    T.GRADING_BASIS_ENRL = S.GRADING_BASIS_ENRL,
    T.CLASS_PRMSN_NBR = S.CLASS_PRMSN_NBR,
    T.CLASS_NBR_CHG_TO = S.CLASS_NBR_CHG_TO,
    T.DROP_CLASS_IF_ENRL = S.DROP_CLASS_IF_ENRL,
    T.CHG_TO_WL_NUM = S.CHG_TO_WL_NUM,
    T.RELATE_CLASS_NBR_1 = S.RELATE_CLASS_NBR_1,
    T.RELATE_CLASS_NBR_2 = S.RELATE_CLASS_NBR_2,
    T.OVRD_CLASS_LIMIT = S.OVRD_CLASS_LIMIT,
    T.OVRD_GRADING_BASIS = S.OVRD_GRADING_BASIS,
    T.OVRD_CLASS_UNITS = S.OVRD_CLASS_UNITS,
    T.OVRD_UNIT_LOAD = S.OVRD_UNIT_LOAD,
    T.OVRD_CLASS_LINKS = S.OVRD_CLASS_LINKS,
    T.OVRD_CLASS_PRMSN = S.OVRD_CLASS_PRMSN,
    T.OVRD_REQUISITES = S.OVRD_REQUISITES,
    T.OVRD_TIME_CNFLCT = S.OVRD_TIME_CNFLCT,
    T.OVRD_CAREER = S.OVRD_CAREER,
    T.WAIT_LIST_OKAY = S.WAIT_LIST_OKAY,
    T.OVRD_ENRL_ACTN_DT = S.OVRD_ENRL_ACTN_DT,
    T.OVRD_RQMNT_DESIG = S.OVRD_RQMNT_DESIG,
    T.OVRD_SRVC_INDIC = S.OVRD_SRVC_INDIC,
    T.OVRD_APPT = S.OVRD_APPT,
    T.INSTRUCTOR_ID = S.INSTRUCTOR_ID,
    T.ENRL_REQ_DETL_STAT = S.ENRL_REQ_DETL_STAT,
    T.RQMNT_DESIGNTN = S.RQMNT_DESIGNTN,
    T.RQMNT_DESIGNTN_OPT = S.RQMNT_DESIGNTN_OPT,
    T.RQMNT_DESIGNTN_GRD = S.RQMNT_DESIGNTN_GRD,
    T.TSCRPT_NOTE_ID = S.TSCRPT_NOTE_ID,
    T.TSCRPT_NOTE_EXISTS = S.TSCRPT_NOTE_EXISTS,
    T.OPRID = S.OPRID,
    T.DTTM_STAMP_SEC = S.DTTM_STAMP_SEC,
    T.START_DT = S.START_DT,
    T.ACAD_PROG = S.ACAD_PROG,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID   = 1234
where
    T.EMPLID <> S.EMPLID or
    T.ACAD_CAREER <> S.ACAD_CAREER or
    T.INSTITUTION <> S.INSTITUTION or
    T.STRM <> S.STRM or
    T.CLASS_NBR <> S.CLASS_NBR or
    T.ENRL_REQ_ACTION <> S.ENRL_REQ_ACTION or
    T.ENRL_ACTION_REASON <> S.ENRL_ACTION_REASON or
    nvl(trim(T.ENRL_ACTION_DT),0) <> nvl(trim(S.ENRL_ACTION_DT),0) or
    T.UNT_TAKEN <> S.UNT_TAKEN or
    T.UNT_EARNED <> S.UNT_EARNED or
    T.CRSE_COUNT <> S.CRSE_COUNT or
    T.REPEAT_CODE <> S.REPEAT_CODE or
    T.CRSE_GRADE_INPUT <> S.CRSE_GRADE_INPUT or
    T.GRADING_BASIS_ENRL <> S.GRADING_BASIS_ENRL or
    T.CLASS_PRMSN_NBR <> S.CLASS_PRMSN_NBR or
    T.CLASS_NBR_CHG_TO <> S.CLASS_NBR_CHG_TO or
    T.DROP_CLASS_IF_ENRL <> S.DROP_CLASS_IF_ENRL or
    T.CHG_TO_WL_NUM <> S.CHG_TO_WL_NUM or
    T.RELATE_CLASS_NBR_1 <> S.RELATE_CLASS_NBR_1 or
    T.RELATE_CLASS_NBR_2 <> S.RELATE_CLASS_NBR_2 or
    T.OVRD_CLASS_LIMIT <> S.OVRD_CLASS_LIMIT or
    T.OVRD_GRADING_BASIS <> S.OVRD_GRADING_BASIS or
    T.OVRD_CLASS_UNITS <> S.OVRD_CLASS_UNITS or
    T.OVRD_UNIT_LOAD <> S.OVRD_UNIT_LOAD or
    T.OVRD_CLASS_LINKS <> S.OVRD_CLASS_LINKS or
    T.OVRD_CLASS_PRMSN <> S.OVRD_CLASS_PRMSN or
    T.OVRD_REQUISITES <> S.OVRD_REQUISITES or
    T.OVRD_TIME_CNFLCT <> S.OVRD_TIME_CNFLCT or
    T.OVRD_CAREER <> S.OVRD_CAREER or
    T.WAIT_LIST_OKAY <> S.WAIT_LIST_OKAY or
    T.OVRD_ENRL_ACTN_DT <> S.OVRD_ENRL_ACTN_DT or
    T.OVRD_RQMNT_DESIG <> S.OVRD_RQMNT_DESIG or
    T.OVRD_SRVC_INDIC <> S.OVRD_SRVC_INDIC or
    T.OVRD_APPT <> S.OVRD_APPT or
    T.INSTRUCTOR_ID <> S.INSTRUCTOR_ID or
    T.ENRL_REQ_DETL_STAT <> S.ENRL_REQ_DETL_STAT or
    T.RQMNT_DESIGNTN <> S.RQMNT_DESIGNTN or
    T.RQMNT_DESIGNTN_OPT <> S.RQMNT_DESIGNTN_OPT or
    T.RQMNT_DESIGNTN_GRD <> S.RQMNT_DESIGNTN_GRD or
    T.TSCRPT_NOTE_ID <> S.TSCRPT_NOTE_ID or
    T.TSCRPT_NOTE_EXISTS <> S.TSCRPT_NOTE_EXISTS or
    T.OPRID <> S.OPRID or
    nvl(trim(T.DTTM_STAMP_SEC),0) <> nvl(trim(S.DTTM_STAMP_SEC),0) or
    nvl(trim(T.START_DT),0) <> nvl(trim(S.START_DT),0) or
    T.ACAD_PROG <> S.ACAD_PROG or
    T.DATA_ORIGIN = 'D'
when not matched then
insert (
    T.ENRL_REQUEST_ID,
    T.ENRL_REQ_DETL_SEQ,
    T.SRC_SYS_ID,
    T.EMPLID,
    T.ACAD_CAREER,
    T.INSTITUTION,
    T.STRM,
    T.CLASS_NBR,
    T.ENRL_REQ_ACTION,
    T.ENRL_ACTION_REASON,
    T.ENRL_ACTION_DT,
    T.UNT_TAKEN,
    T.UNT_EARNED,
    T.CRSE_COUNT,
    T.REPEAT_CODE,
    T.CRSE_GRADE_INPUT,
    T.GRADING_BASIS_ENRL,
    T.CLASS_PRMSN_NBR,
    T.CLASS_NBR_CHG_TO,
    T.DROP_CLASS_IF_ENRL,
    T.CHG_TO_WL_NUM,
    T.RELATE_CLASS_NBR_1,
    T.RELATE_CLASS_NBR_2,
    T.OVRD_CLASS_LIMIT,
    T.OVRD_GRADING_BASIS,
    T.OVRD_CLASS_UNITS,
    T.OVRD_UNIT_LOAD,
    T.OVRD_CLASS_LINKS,
    T.OVRD_CLASS_PRMSN,
    T.OVRD_REQUISITES,
    T.OVRD_TIME_CNFLCT,
    T.OVRD_CAREER,
    T.WAIT_LIST_OKAY,
    T.OVRD_ENRL_ACTN_DT,
    T.OVRD_RQMNT_DESIG,
    T.OVRD_SRVC_INDIC,
    T.OVRD_APPT,
    T.INSTRUCTOR_ID,
    T.ENRL_REQ_DETL_STAT,
    T.RQMNT_DESIGNTN,
    T.RQMNT_DESIGNTN_OPT,
    T.RQMNT_DESIGNTN_GRD,
    T.TSCRPT_NOTE_ID,
    T.TSCRPT_NOTE_EXISTS,
    T.OPRID,
    T.DTTM_STAMP_SEC,
    T.START_DT,
    T.ACAD_PROG,
    T.LOAD_ERROR,
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
    )
values (
    S.ENRL_REQUEST_ID,
    S.ENRL_REQ_DETL_SEQ,
    'CS90',
    S.EMPLID,
    S.ACAD_CAREER,
    S.INSTITUTION,
    S.STRM,
    S.CLASS_NBR,
    S.ENRL_REQ_ACTION,
    S.ENRL_ACTION_REASON,
    S.ENRL_ACTION_DT,
    S.UNT_TAKEN,
    S.UNT_EARNED,
    S.CRSE_COUNT,
    S.REPEAT_CODE,
    S.CRSE_GRADE_INPUT,
    S.GRADING_BASIS_ENRL,
    S.CLASS_PRMSN_NBR,
    S.CLASS_NBR_CHG_TO,
    S.DROP_CLASS_IF_ENRL,
    S.CHG_TO_WL_NUM,
    S.RELATE_CLASS_NBR_1,
    S.RELATE_CLASS_NBR_2,
    S.OVRD_CLASS_LIMIT,
    S.OVRD_GRADING_BASIS,
    S.OVRD_CLASS_UNITS,
    S.OVRD_UNIT_LOAD,
    S.OVRD_CLASS_LINKS,
    S.OVRD_CLASS_PRMSN,
    S.OVRD_REQUISITES,
    S.OVRD_TIME_CNFLCT,
    S.OVRD_CAREER,
    S.WAIT_LIST_OKAY,
    S.OVRD_ENRL_ACTN_DT,
    S.OVRD_RQMNT_DESIG,
    S.OVRD_SRVC_INDIC,
    S.OVRD_APPT,
    S.INSTRUCTOR_ID,
    S.ENRL_REQ_DETL_STAT,
    S.RQMNT_DESIGNTN,
    S.RQMNT_DESIGNTN_OPT,
    S.RQMNT_DESIGNTN_GRD,
    S.TSCRPT_NOTE_ID,
    S.TSCRPT_NOTE_EXISTS,
    S.OPRID,
    S.DTTM_STAMP_SEC,
    S.START_DT,
    S.ACAD_PROG,
    'N',
    'S',
    sysdate,
    sysdate,
    1234);

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_ENRL_REQ_DETAIL rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ENRL_REQ_DETAIL',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_ENRL_REQ_DETAIL';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_ENRL_REQ_DETAIL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_ENRL_REQ_DETAIL';
update CSSTG_OWNER.PS_ENRL_REQ_DETAIL T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select ENRL_REQUEST_ID, ENRL_REQ_DETL_SEQ
   from CSSTG_OWNER.PS_ENRL_REQ_DETAIL T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ENRL_REQ_DETAIL') = 'Y'
  minus
 select ENRL_REQUEST_ID, ENRL_REQ_DETL_SEQ
   from SYSADM.PS_ENRL_REQ_DETAIL@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ENRL_REQ_DETAIL') = 'Y'
   ) S
 where T.ENRL_REQUEST_ID = S.ENRL_REQUEST_ID
   and T.ENRL_REQ_DETL_SEQ = S.ENRL_REQ_DETL_SEQ
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_ENRL_REQ_DETAIL rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ENRL_REQ_DETAIL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_ENRL_REQ_DETAIL'
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

END PS_ENRL_REQ_DETAIL_P;
/
