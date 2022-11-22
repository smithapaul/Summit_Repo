DROP PROCEDURE CSMRT_OWNER.PS_EXT_COURSE_P
/

--
-- PS_EXT_COURSE_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_EXT_COURSE_P" AUTHID CURRENT_USER IS


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_EXT_COURSE from PeopleSoft table PS_EXT_COURSE.
--
 --V01  SMT-xxxx 10/03/2017,    James Doucette
--                              Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_EXT_COURSE';
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
 where TABLE_NAME = 'PS_EXT_COURSE'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_EXT_COURSE@SASOURCE S)
 where TABLE_NAME = 'PS_EXT_COURSE'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_EXT_COURSE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_EXT_COURSE';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_EXT_COURSE T 
using (select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(EXT_ORG_ID),'-') EXT_ORG_ID, 
    nvl(EXT_COURSE_NBR,0) EXT_COURSE_NBR, 
    nvl(trim(EXT_CRSE_TYPE),'-') EXT_CRSE_TYPE, 
    nvl(trim(LS_DATA_SOURCE),'-') LS_DATA_SOURCE, 
    nvl(EXT_DATA_NBR,0) EXT_DATA_NBR, 
    NVL(BEGIN_DT, to_date(to_char('01/01/1900'), 'MM/DD/YYYY' )) BEGIN_DT,
    NVL(END_DT, to_date(to_char('01/01/1900'), 'MM/DD/YYYY' )) END_DT,
    nvl(trim(EXT_TERM_TYPE),'-') EXT_TERM_TYPE, 
    nvl(UNT_TAKEN,0) UNT_TAKEN, 
    nvl(trim(GRADING_SCHEME),'-') GRADING_SCHEME, 
    nvl(trim(GRADING_BASIS),'-') GRADING_BASIS, 
    nvl(trim(COURSE_LEVEL),'-') COURSE_LEVEL, 
    nvl(trim(CRSE_GRADE_INPUT),'-') CRSE_GRADE_INPUT, 
    nvl(trim(CRSE_GRADE_OFF),'-') CRSE_GRADE_OFF, 
    nvl(trim(SCHOOL_SUBJECT),'-') SCHOOL_SUBJECT, 
    nvl(trim(SCHOOL_CRSE_NBR),'-') SCHOOL_CRSE_NBR, 
    nvl(trim(EXT_SUBJECT_AREA),'-') EXT_SUBJECT_AREA, 
    nvl(trim(EXT_CAREER),'-') EXT_CAREER, 
    nvl(trim(EXT_TERM),'-') EXT_TERM, 
    nvl(TERM_YEAR,0) TERM_YEAR, 
    nvl(trim(DESCR),'-') DESCR, 
    nvl(trim(UNT_TYPE),'-') UNT_TYPE, 
    nvl(trim(INSTITUTION),'-') INSTITUTION, 
    nvl(trim(EXT_ACAD_LEVEL),'-') EXT_ACAD_LEVEL, 
    nvl(trim(TRANS_CREDIT_FLAG),'-') TRANS_CREDIT_FLAG, 
    nvl(trim(CAN_TRNS_TYPE),'-') CAN_TRNS_TYPE, 
    LASTUPDDTTM, 
    nvl(trim(LASTUPDOPRID),'-') LASTUPDOPRID
from SYSADM.PS_EXT_COURSE@SASOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_EXT_COURSE')
  and EMPLID between '00000000' and '99999999'
  and length(EMPLID) = 8  ) S 
 on ( 
    T.EMPLID = S.EMPLID and 
    T.EXT_ORG_ID = S.EXT_ORG_ID and 
    T.EXT_COURSE_NBR = S.EXT_COURSE_NBR and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.EXT_CRSE_TYPE = S.EXT_CRSE_TYPE,
    T.LS_DATA_SOURCE = S.LS_DATA_SOURCE,
    T.EXT_DATA_NBR = S.EXT_DATA_NBR,
    T.BEGIN_DT = S.BEGIN_DT,
    T.END_DT = S.END_DT,
    T.EXT_TERM_TYPE = S.EXT_TERM_TYPE,
    T.UNT_TAKEN = S.UNT_TAKEN,
    T.GRADING_SCHEME = S.GRADING_SCHEME,
    T.GRADING_BASIS = S.GRADING_BASIS,
    T.COURSE_LEVEL = S.COURSE_LEVEL,
    T.CRSE_GRADE_INPUT = S.CRSE_GRADE_INPUT,
    T.CRSE_GRADE_OFF = S.CRSE_GRADE_OFF,
    T.SCHOOL_SUBJECT = S.SCHOOL_SUBJECT,
    T.SCHOOL_CRSE_NBR = S.SCHOOL_CRSE_NBR,
    T.EXT_SUBJECT_AREA = S.EXT_SUBJECT_AREA,
    T.EXT_CAREER = S.EXT_CAREER,
    T.EXT_TERM = S.EXT_TERM,
    T.TERM_YEAR = S.TERM_YEAR,
    T.DESCR = S.DESCR,
    T.UNT_TYPE = S.UNT_TYPE,
    T.INSTITUTION = S.INSTITUTION,
    T.EXT_ACAD_LEVEL = S.EXT_ACAD_LEVEL,
    T.TRANS_CREDIT_FLAG = S.TRANS_CREDIT_FLAG,
    T.CAN_TRNS_TYPE = S.CAN_TRNS_TYPE,
    T.LASTUPDDTTM = S.LASTUPDDTTM,
    T.LASTUPDOPRID = S.LASTUPDOPRID,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.EXT_CRSE_TYPE <> S.EXT_CRSE_TYPE or 
    T.LS_DATA_SOURCE <> S.LS_DATA_SOURCE or 
    T.EXT_DATA_NBR <> S.EXT_DATA_NBR or 
    nvl(trim(T.BEGIN_DT),0) <> nvl(trim(S.BEGIN_DT),0) or 
    nvl(trim(T.END_DT),0) <> nvl(trim(S.END_DT),0) or 
    T.EXT_TERM_TYPE <> S.EXT_TERM_TYPE or 
    T.UNT_TAKEN <> S.UNT_TAKEN or 
    T.GRADING_SCHEME <> S.GRADING_SCHEME or 
    T.GRADING_BASIS <> S.GRADING_BASIS or 
    T.COURSE_LEVEL <> S.COURSE_LEVEL or 
    T.CRSE_GRADE_INPUT <> S.CRSE_GRADE_INPUT or 
    T.CRSE_GRADE_OFF <> S.CRSE_GRADE_OFF or 
    T.SCHOOL_SUBJECT <> S.SCHOOL_SUBJECT or 
    T.SCHOOL_CRSE_NBR <> S.SCHOOL_CRSE_NBR or 
    T.EXT_SUBJECT_AREA <> S.EXT_SUBJECT_AREA or 
    T.EXT_CAREER <> S.EXT_CAREER or 
    T.EXT_TERM <> S.EXT_TERM or 
    T.TERM_YEAR <> S.TERM_YEAR or 
    T.DESCR <> S.DESCR or 
    T.UNT_TYPE <> S.UNT_TYPE or 
    T.INSTITUTION <> S.INSTITUTION or 
    T.EXT_ACAD_LEVEL <> S.EXT_ACAD_LEVEL or 
    T.TRANS_CREDIT_FLAG <> S.TRANS_CREDIT_FLAG or 
    T.CAN_TRNS_TYPE <> S.CAN_TRNS_TYPE or 
    nvl(trim(T.LASTUPDDTTM),0) <> nvl(trim(S.LASTUPDDTTM),0) or 
    T.LASTUPDOPRID <> S.LASTUPDOPRID or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.EMPLID, 
    T.EXT_ORG_ID, 
    T.EXT_COURSE_NBR, 
    T.SRC_SYS_ID, 
    T.EXT_CRSE_TYPE,
    T.LS_DATA_SOURCE, 
    T.EXT_DATA_NBR, 
    T.BEGIN_DT, 
    T.END_DT, 
    T.EXT_TERM_TYPE,
    T.UNT_TAKEN,
    T.GRADING_SCHEME, 
    T.GRADING_BASIS,
    T.COURSE_LEVEL, 
    T.CRSE_GRADE_INPUT, 
    T.CRSE_GRADE_OFF, 
    T.SCHOOL_SUBJECT, 
    T.SCHOOL_CRSE_NBR,
    T.EXT_SUBJECT_AREA, 
    T.EXT_CAREER, 
    T.EXT_TERM, 
    T.TERM_YEAR,
    T.DESCR,
    T.UNT_TYPE, 
    T.INSTITUTION,
    T.EXT_ACAD_LEVEL, 
    T.TRANS_CREDIT_FLAG,
    T.CAN_TRNS_TYPE,
    T.LASTUPDDTTM,
    T.LASTUPDOPRID, 
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
    ) 
values (
    S.EMPLID, 
    S.EXT_ORG_ID, 
    S.EXT_COURSE_NBR, 
    'CS90', 
    S.EXT_CRSE_TYPE,
    S.LS_DATA_SOURCE, 
    S.EXT_DATA_NBR, 
    S.BEGIN_DT, 
    S.END_DT, 
    S.EXT_TERM_TYPE,
    S.UNT_TAKEN,
    S.GRADING_SCHEME, 
    S.GRADING_BASIS,
    S.COURSE_LEVEL, 
    S.CRSE_GRADE_INPUT, 
    S.CRSE_GRADE_OFF, 
    S.SCHOOL_SUBJECT, 
    S.SCHOOL_CRSE_NBR,
    S.EXT_SUBJECT_AREA, 
    S.EXT_CAREER, 
    S.EXT_TERM, 
    S.TERM_YEAR,
    S.DESCR,
    S.UNT_TYPE, 
    S.INSTITUTION,
    S.EXT_ACAD_LEVEL, 
    S.TRANS_CREDIT_FLAG,
    S.CAN_TRNS_TYPE,
    S.LASTUPDDTTM,
    S.LASTUPDOPRID, 
    'N',
    'S',
    sysdate,
    sysdate,
    1234)
;
strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_EXT_COURSE rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_EXT_COURSE',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_EXT_COURSE';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_EXT_COURSE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_EXT_COURSE';
update CSSTG_OWNER.PS_EXT_COURSE T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, EXT_ORG_ID, EXT_COURSE_NBR
   from CSSTG_OWNER.PS_EXT_COURSE T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_EXT_COURSE') = 'Y'
  minus
 select EMPLID, EXT_ORG_ID, EXT_COURSE_NBR
   from SYSADM.PS_EXT_COURSE@SASOURCE
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_EXT_COURSE') = 'Y' 
   ) S
 where T.EMPLID = S.EMPLID
   AND T.EXT_ORG_ID = S.EXT_ORG_ID
   AND T.EXT_COURSE_NBR = S.EXT_COURSE_NBR
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_EXT_COURSE rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_EXT_COURSE',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_EXT_COURSE'
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

END PS_EXT_COURSE_P;
/
