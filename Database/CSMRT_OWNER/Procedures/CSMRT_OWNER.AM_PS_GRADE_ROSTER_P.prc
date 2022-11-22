DROP PROCEDURE CSMRT_OWNER.AM_PS_GRADE_ROSTER_P
/

--
-- AM_PS_GRADE_ROSTER_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."AM_PS_GRADE_ROSTER_P" IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_GRADE_ROSTER from PeopleSoft table PS_GRADE_ROSTER.
--
-- V01  SMT-xxxx 05/15/2017,    Jim Doucette
--                              Converted from PS_GRADE_ROSTER.SQL
--VXX    07/06/2021,            Kieu ,Srikanth - Added EMPLID or COMMON_ID additional filter logic 
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'AM_PS_GRADE_ROSTER';
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
       START_DT = sysdate,
       END_DT = NULL
 where TABLE_NAME = 'PS_GRADE_ROSTER'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_GRADE_ROSTER@AMSOURCE S)
 where TABLE_NAME = 'PS_GRADE_ROSTER'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into AMSTG_OWNER.PS_GRADE_ROSTER';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into AMSTG_OWNER.PS_GRADE_ROSTER';
merge /*+ use_hash(S,T) */ into AMSTG_OWNER.PS_GRADE_ROSTER T
using (select /*+ full(S) */
    nvl(trim(STRM),'-') STRM,
    nvl(CLASS_NBR,0) CLASS_NBR,
    nvl(GRD_RSTR_TYPE_SEQ,0) GRD_RSTR_TYPE_SEQ,
    nvl(trim(EMPLID),'-') EMPLID,
    nvl(trim(ACAD_CAREER),'-') ACAD_CAREER,
    nvl(BLIND_GRADING_ID,0) BLIND_GRADING_ID,
    nvl(trim(LAST_NAME_SRCH),'-') LAST_NAME_SRCH,
    nvl(trim(FIRST_NAME_SRCH),'-') FIRST_NAME_SRCH,
    nvl(trim(INSTITUTION),'-') INSTITUTION,
    nvl(trim(CRSE_GRADE_INPUT),'-') CRSE_GRADE_INPUT,
    nvl(trim(RQMNT_DESIGNTN_GRD),'-') RQMNT_DESIGNTN_GRD,
    nvl(trim(TSCRPT_NOTE_ID),'-') TSCRPT_NOTE_ID,
    nvl(trim(TSCRPT_NOTE_EXISTS),'-') TSCRPT_NOTE_EXISTS,
    nvl(trim(GRADE_ROSTER_STAT),'-') GRADE_ROSTER_STAT,
    nvl(trim(INSTRUCTOR_ID),'-') INSTRUCTOR_ID,
    nvl(trim(GRADING_SCHEME),'-') GRADING_SCHEME,
    nvl(trim(GRADING_BASIS_ENRL),'-') GRADING_BASIS_ENRL,
    nvl(DYN_CLASS_NBR,0) DYN_CLASS_NBR
from SYSADM.PS_GRADE_ROSTER@AMSOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_GRADE_ROSTER') 
   and EMPLID between '00000000' and '99999999'
   and length(EMPLID) = 8) S
   on (
    T.STRM = S.STRM and
    T.CLASS_NBR = S.CLASS_NBR and
    T.GRD_RSTR_TYPE_SEQ = S.GRD_RSTR_TYPE_SEQ and
    T.EMPLID = S.EMPLID and
    T.ACAD_CAREER = S.ACAD_CAREER and
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.BLIND_GRADING_ID = S.BLIND_GRADING_ID,
    T.LAST_NAME_SRCH = S.LAST_NAME_SRCH,
    T.FIRST_NAME_SRCH = S.FIRST_NAME_SRCH,
    T.INSTITUTION = S.INSTITUTION,
    T.CRSE_GRADE_INPUT = S.CRSE_GRADE_INPUT,
    T.RQMNT_DESIGNTN_GRD = S.RQMNT_DESIGNTN_GRD,
    T.TSCRPT_NOTE_ID = S.TSCRPT_NOTE_ID,
    T.TSCRPT_NOTE_EXISTS = S.TSCRPT_NOTE_EXISTS,
    T.GRADE_ROSTER_STAT = S.GRADE_ROSTER_STAT,
    T.INSTRUCTOR_ID = S.INSTRUCTOR_ID,
    T.GRADING_SCHEME = S.GRADING_SCHEME,
    T.GRADING_BASIS_ENRL = S.GRADING_BASIS_ENRL,
    T.DYN_CLASS_NBR = S.DYN_CLASS_NBR,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID   = 1234
where
    T.BLIND_GRADING_ID <> S.BLIND_GRADING_ID or
    T.LAST_NAME_SRCH <> S.LAST_NAME_SRCH or
    T.FIRST_NAME_SRCH <> S.FIRST_NAME_SRCH or
    T.INSTITUTION <> S.INSTITUTION or
    T.CRSE_GRADE_INPUT <> S.CRSE_GRADE_INPUT or
    T.RQMNT_DESIGNTN_GRD <> S.RQMNT_DESIGNTN_GRD or
    T.TSCRPT_NOTE_ID <> S.TSCRPT_NOTE_ID or
    T.TSCRPT_NOTE_EXISTS <> S.TSCRPT_NOTE_EXISTS or
    T.GRADE_ROSTER_STAT <> S.GRADE_ROSTER_STAT or
    T.INSTRUCTOR_ID <> S.INSTRUCTOR_ID or
    T.GRADING_SCHEME <> S.GRADING_SCHEME or
    T.GRADING_BASIS_ENRL <> S.GRADING_BASIS_ENRL or
    T.DYN_CLASS_NBR <> S.DYN_CLASS_NBR or
    T.DATA_ORIGIN = 'D'
when not matched then
insert (
    T.STRM,
    T.CLASS_NBR,
    T.GRD_RSTR_TYPE_SEQ,
    T.EMPLID,
    T.ACAD_CAREER,
    T.SRC_SYS_ID,
    T.BLIND_GRADING_ID,
    T.LAST_NAME_SRCH,
    T.FIRST_NAME_SRCH,
    T.INSTITUTION,
    T.CRSE_GRADE_INPUT,
    T.RQMNT_DESIGNTN_GRD,
    T.TSCRPT_NOTE_ID,
    T.TSCRPT_NOTE_EXISTS,
    T.GRADE_ROSTER_STAT,
    T.INSTRUCTOR_ID,
    T.GRADING_SCHEME,
    T.GRADING_BASIS_ENRL,
    T.DYN_CLASS_NBR,
    T.LOAD_ERROR,
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
    )
values (
    S.STRM,
    S.CLASS_NBR,
    S.GRD_RSTR_TYPE_SEQ,
    S.EMPLID,
    S.ACAD_CAREER,
    'CS90',
    S.BLIND_GRADING_ID,
    S.LAST_NAME_SRCH,
    S.FIRST_NAME_SRCH,
    S.INSTITUTION,
    S.CRSE_GRADE_INPUT,
    S.RQMNT_DESIGNTN_GRD,
    S.TSCRPT_NOTE_ID,
    S.TSCRPT_NOTE_EXISTS,
    S.GRADE_ROSTER_STAT,
    S.INSTRUCTOR_ID,
    S.GRADING_SCHEME,
    S.GRADING_BASIS_ENRL,
    S.DYN_CLASS_NBR,
    'N',
    'S',
    sysdate,
    sysdate,
    1234);

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_GRADE_ROSTER rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_GRADE_ROSTER',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_GRADE_ROSTER';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_GRADE_ROSTER';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on AMSTG_OWNER.PS_GRADE_ROSTER';
update AMSTG_OWNER.PS_GRADE_ROSTER T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select STRM, CLASS_NBR, GRD_RSTR_TYPE_SEQ, EMPLID, ACAD_CAREER
   from AMSTG_OWNER.PS_GRADE_ROSTER T2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_GRADE_ROSTER') = 'Y'
  minus
 select STRM, CLASS_NBR, GRD_RSTR_TYPE_SEQ, EMPLID, ACAD_CAREER
   from SYSADM.PS_GRADE_ROSTER@AMSOURCE S2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_GRADE_ROSTER') = 'Y'
   ) S
 where T.STRM = S.STRM
   and T.CLASS_NBR = S.CLASS_NBR
   and T.GRD_RSTR_TYPE_SEQ = S.GRD_RSTR_TYPE_SEQ
   and T.EMPLID = S.EMPLID
   and T.ACAD_CAREER = S.ACAD_CAREER
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_GRADE_ROSTER rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_GRADE_ROSTER',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_GRADE_ROSTER'
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

END AM_PS_GRADE_ROSTER_P;
/
