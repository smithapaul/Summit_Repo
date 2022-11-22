DROP PROCEDURE CSMRT_OWNER.PS_SESSION_TBL_P
/

--
-- PS_SESSION_TBL_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_SESSION_TBL_P" AUTHID CURRENT_USER IS


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_SESSION_TBL from PeopleSoft table PS_SESSION_TBL.
--
 --V01  SMT-xxxx 09/11/2017,    James Doucette
--                              Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_SESSION_TBL';
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
 where TABLE_NAME = 'PS_SESSION_TBL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_SESSION_TBL@SASOURCE S)
 where TABLE_NAME = 'PS_SESSION_TBL'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_SESSION_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_SESSION_TBL';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_SESSION_TBL T
using (select /*+ full(S) */
    nvl(trim(INSTITUTION),'-') INSTITUTION, 
    nvl(trim(ACAD_CAREER),'-') ACAD_CAREER, 
    nvl(trim(STRM),'-') STRM, 
    nvl(trim(SESSION_CODE),'-') SESSION_CODE, 
    SESS_BEGIN_DT, 
    SESS_END_DT, 
    ENROLL_OPEN_DT,
    nvl(trim(SESSN_ENRL_CNTL),'-') SESSN_ENRL_CNTL, 
    nvl(trim(SESSN_APPT_CNTL),'-') SESSN_APPT_CNTL, 
    FIRST_ENRL_DT, 
    LAST_ENRL_DT,
    LAST_WAIT_DT,
    nvl(trim(HOLIDAY_SCHEDULE),'-') HOLIDAY_SCHEDULE, 
    nvl(WEEKS_OF_INSTRUCT,0) WEEKS_OF_INSTRUCT, 
    CENSUS_DT, 
    nvl(trim(USE_DYN_CLASS_DATE),'-') USE_DYN_CLASS_DATE, 
    SIXTY_PCT_DT,
    FACILITY_ASSIGNMNT,
    nvl(trim(SSR_ENR_APT_APPROV),'-') SSR_ENR_APT_APPROV, 
    nvl(trim(SSR_VAL_APT_APPROV),'-') SSR_VAL_APT_APPROV
from SYSADM.PS_SESSION_TBL@SASOURCE S 
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_SESSION_TBL') ) S
 on ( 
    T.INSTITUTION = S.INSTITUTION and 
    T.ACAD_CAREER = S.ACAD_CAREER and 
    T.STRM = S.STRM and 
    T.SESSION_CODE = S.SESSION_CODE and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.SESS_BEGIN_DT = S.SESS_BEGIN_DT,
    T.SESS_END_DT = S.SESS_END_DT,
    T.ENROLL_OPEN_DT = S.ENROLL_OPEN_DT,
    T.SESSN_ENRL_CNTL = S.SESSN_ENRL_CNTL,
    T.SESSN_APPT_CNTL = S.SESSN_APPT_CNTL,
    T.FIRST_ENRL_DT = S.FIRST_ENRL_DT,
    T.LAST_ENRL_DT = S.LAST_ENRL_DT,
    T.LAST_WAIT_DT = S.LAST_WAIT_DT,
    T.HOLIDAY_SCHEDULE = S.HOLIDAY_SCHEDULE,
    T.WEEKS_OF_INSTRUCT = S.WEEKS_OF_INSTRUCT,
    T.CENSUS_DT = S.CENSUS_DT,
    T.USE_DYN_CLASS_DATE = S.USE_DYN_CLASS_DATE,
    T.SIXTY_PCT_DT = S.SIXTY_PCT_DT,
    T.FACILITY_ASSIGNMNT = S.FACILITY_ASSIGNMNT,
    T.SSR_ENR_APT_APPROV = S.SSR_ENR_APT_APPROV,
    T.SSR_VAL_APT_APPROV = S.SSR_VAL_APT_APPROV,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    nvl(trim(T.SESS_BEGIN_DT),0) <> nvl(trim(S.SESS_BEGIN_DT),0) or 
    nvl(trim(T.SESS_END_DT),0) <> nvl(trim(S.SESS_END_DT),0) or 
    nvl(trim(T.ENROLL_OPEN_DT),0) <> nvl(trim(S.ENROLL_OPEN_DT),0) or 
    T.SESSN_ENRL_CNTL <> S.SESSN_ENRL_CNTL or 
    T.SESSN_APPT_CNTL <> S.SESSN_APPT_CNTL or 
    nvl(trim(T.FIRST_ENRL_DT),0) <> nvl(trim(S.FIRST_ENRL_DT),0) or 
    nvl(trim(T.LAST_ENRL_DT),0) <> nvl(trim(S.LAST_ENRL_DT),0) or 
    nvl(trim(T.LAST_WAIT_DT),0) <> nvl(trim(S.LAST_WAIT_DT),0) or 
    T.HOLIDAY_SCHEDULE <> S.HOLIDAY_SCHEDULE or 
    T.WEEKS_OF_INSTRUCT <> S.WEEKS_OF_INSTRUCT or 
    nvl(trim(T.CENSUS_DT),0) <> nvl(trim(S.CENSUS_DT),0) or 
    T.USE_DYN_CLASS_DATE <> S.USE_DYN_CLASS_DATE or 
    nvl(trim(T.SIXTY_PCT_DT),0) <> nvl(trim(S.SIXTY_PCT_DT),0) or 
    nvl(trim(T.FACILITY_ASSIGNMNT),0) <> nvl(trim(S.FACILITY_ASSIGNMNT),0) or 
    T.SSR_ENR_APT_APPROV <> S.SSR_ENR_APT_APPROV or 
    T.SSR_VAL_APT_APPROV <> S.SSR_VAL_APT_APPROV or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.INSTITUTION,
    T.ACAD_CAREER,
    T.STRM, 
    T.SESSION_CODE, 
    T.SRC_SYS_ID, 
    T.SESS_BEGIN_DT,
    T.SESS_END_DT,
    T.ENROLL_OPEN_DT, 
    T.SESSN_ENRL_CNTL,
    T.SESSN_APPT_CNTL,
    T.FIRST_ENRL_DT,
    T.LAST_ENRL_DT, 
    T.LAST_WAIT_DT, 
    T.HOLIDAY_SCHEDULE, 
    T.WEEKS_OF_INSTRUCT,
    T.CENSUS_DT,
    T.USE_DYN_CLASS_DATE, 
    T.SIXTY_PCT_DT, 
    T.FACILITY_ASSIGNMNT, 
    T.SSR_ENR_APT_APPROV, 
    T.SSR_VAL_APT_APPROV, 
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
    'CS90', 
    S.SESS_BEGIN_DT,
    S.SESS_END_DT,
    S.ENROLL_OPEN_DT, 
    S.SESSN_ENRL_CNTL,
    S.SESSN_APPT_CNTL,
    S.FIRST_ENRL_DT,
    S.LAST_ENRL_DT, 
    S.LAST_WAIT_DT, 
    S.HOLIDAY_SCHEDULE, 
    S.WEEKS_OF_INSTRUCT,
    S.CENSUS_DT,
    S.USE_DYN_CLASS_DATE, 
    S.SIXTY_PCT_DT, 
    S.FACILITY_ASSIGNMNT, 
    S.SSR_ENR_APT_APPROV, 
    S.SSR_VAL_APT_APPROV, 
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

strMessage01    := '# of PS_SESSION_TBL rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_SESSION_TBL',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_SESSION_TBL';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_SESSION_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_SESSION_TBL';
update CSSTG_OWNER.PS_SESSION_TBL T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select INSTITUTION, ACAD_CAREER, STRM, SESSION_CODE
   from CSSTG_OWNER.PS_SESSION_TBL T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_SESSION_TBL') = 'Y'
  minus
 select INSTITUTION, ACAD_CAREER, STRM, SESSION_CODE
   from SYSADM.PS_SESSION_TBL@SASOURCE
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_SESSION_TBL') = 'Y'
   ) S
 where T.INSTITUTION = S.INSTITUTION
   and T.ACAD_CAREER = S.ACAD_CAREER
   and T.STRM = S.STRM
   and T.SESSION_CODE = S.SESSION_CODE
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_SESSION_TBL rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_SESSION_TBL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_SESSION_TBL'
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

END PS_SESSION_TBL_P;
/
