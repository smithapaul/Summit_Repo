DROP PROCEDURE CSMRT_OWNER.PS_APPOINTMENT_TBL_P
/

--
-- PS_APPOINTMENT_TBL_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_APPOINTMENT_TBL_P" AUTHID CURRENT_USER IS


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_APPOINTMENT_TBL from PeopleSoft table PS_APPOINTMENT_TBL.
--
 --V01  SMT-xxxx 10/03/2017,    James Doucette
--                              Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_APPOINTMENT_TBL';
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
 where TABLE_NAME = 'PS_APPOINTMENT_TBL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_APPOINTMENT_TBL@SASOURCE S)
 where TABLE_NAME = 'PS_APPOINTMENT_TBL'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_APPOINTMENT_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_APPOINTMENT_TBL';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_APPOINTMENT_TBL T
using (select /*+ full(S) */
    nvl(trim(INSTITUTION),'-') INSTITUTION, 
    nvl(trim(ACAD_CAREER),'-') ACAD_CAREER, 
    nvl(trim(STRM),'-') STRM, 
    nvl(trim(SESSION_CODE),'-') SESSION_CODE, 
    nvl(trim(SSR_APPT_BLOCK),'-') SSR_APPT_BLOCK, 
    nvl(trim(APPOINTMENT_NBR),'-') APPOINTMENT_NBR, 
    APPT_START_DATE, 
    APPT_START_TIME, 
    APPT_END_DATE, 
    APPT_END_TIME, 
    nvl(SSR_APPT_NBR_STDNT,0) SSR_APPT_NBR_STDNT, 
    nvl(SSR_APPT_STD_ASGN,0) SSR_APPT_STD_ASGN
from SYSADM.PS_APPOINTMENT_TBL@SASOURCE S 
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_APPOINTMENT_TBL') ) S
 on ( 
    T.INSTITUTION = S.INSTITUTION and 
    T.ACAD_CAREER = S.ACAD_CAREER and 
    T.STRM = S.STRM and 
    T.SESSION_CODE = S.SESSION_CODE and 
    T.SSR_APPT_BLOCK = S.SSR_APPT_BLOCK and 
    T.APPOINTMENT_NBR = S.APPOINTMENT_NBR and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.APPT_START_DATE = S.APPT_START_DATE,
    T.APPT_START_TIME = S.APPT_START_TIME,
    T.APPT_END_DATE = S.APPT_END_DATE,
    T.APPT_END_TIME = S.APPT_END_TIME,
    T.SSR_APPT_NBR_STDNT = S.SSR_APPT_NBR_STDNT,
    T.SSR_APPT_STD_ASGN = S.SSR_APPT_STD_ASGN,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.APPT_START_DATE <> S.APPT_START_DATE or 
    T.APPT_START_TIME <> S.APPT_START_TIME or 
    T.APPT_END_DATE <> S.APPT_END_DATE or 
    T.APPT_END_TIME <> S.APPT_END_TIME or 
    T.SSR_APPT_NBR_STDNT <> S.SSR_APPT_NBR_STDNT or 
    T.SSR_APPT_STD_ASGN <> S.SSR_APPT_STD_ASGN or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.INSTITUTION,
    T.ACAD_CAREER,
    T.STRM, 
    T.SESSION_CODE, 
    T.SSR_APPT_BLOCK, 
    T.APPOINTMENT_NBR,
    T.SRC_SYS_ID, 
    T.APPT_START_DATE,
    T.APPT_START_TIME,
    T.APPT_END_DATE,
    T.APPT_END_TIME,
    T.SSR_APPT_NBR_STDNT, 
    T.SSR_APPT_STD_ASGN,
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
    ) 
values (
    S.INSTITUTION,
    S.ACAD_CAREER,
    S.STRM, 
    S.SESSION_CODE, 
    S.SSR_APPT_BLOCK, 
    S.APPOINTMENT_NBR,
    'CS90', 
    S.APPT_START_DATE,
    S.APPT_START_TIME,
    S.APPT_END_DATE,
    S.APPT_END_TIME,
    S.SSR_APPT_NBR_STDNT, 
    S.SSR_APPT_STD_ASGN,
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

strMessage01    := '# of PS_APPOINTMENT_TBL rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_APPOINTMENT_TBL',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_APPOINTMENT_TBL';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_APPOINTMENT_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_APPOINTMENT_TBL';
update CSSTG_OWNER.PS_APPOINTMENT_TBL T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select INSTITUTION, ACAD_CAREER, STRM, SESSION_CODE, SSR_APPT_BLOCK, APPOINTMENT_NBR
   from CSSTG_OWNER.PS_APPOINTMENT_TBL T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_APPOINTMENT_TBL') = 'Y'
  minus
 select INSTITUTION, ACAD_CAREER, STRM, SESSION_CODE, SSR_APPT_BLOCK, APPOINTMENT_NBR
   from SYSADM.PS_APPOINTMENT_TBL@SASOURCE
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_APPOINTMENT_TBL') = 'Y' 
   ) S
 where T.INSTITUTION = S.INSTITUTION
    AND T.ACAD_CAREER = S.ACAD_CAREER
    AND T.STRM = S.STRM
    AND T.SESSION_CODE = S.SESSION_CODE
    AND T.SSR_APPT_BLOCK = S.SSR_APPT_BLOCK
    AND T.APPOINTMENT_NBR = S.APPOINTMENT_NBR
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_APPOINTMENT_TBL rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_APPOINTMENT_TBL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_APPOINTMENT_TBL'
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

END PS_APPOINTMENT_TBL_P;
/
