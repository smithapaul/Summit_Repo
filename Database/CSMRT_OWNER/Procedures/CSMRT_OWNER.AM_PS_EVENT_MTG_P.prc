DROP PROCEDURE CSMRT_OWNER.AM_PS_EVENT_MTG_P
/

--
-- AM_PS_EVENT_MTG_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER.AM_PS_EVENT_MTG_P IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_EVENT_MTG from PeopleSoft table PS_EVENT_MTG.
--
-- V01  SMT-xxxx 8/31/2017,    James Doucette
--                             Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'AM_PS_EVENT_MTG';
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
 where TABLE_NAME = 'PS_EVENT_MTG'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Truncating',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_EVENT_MTG@AMSOURCE S)
 where TABLE_NAME = 'PS_EVENT_MTG'
;

strSqlCommand := 'commit';
commit;


strSqlDynamic   := 'truncate table AMSTG_OWNER.PS_T_EVENT_MTG';
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
 where TABLE_NAME = 'PS_EVENT_MTG'
;

strSqlCommand := 'commit';
commit;


strSqlCommand := 'insert';
INSERT /*+ append */
      INTO  AMSTG_OWNER.PS_T_EVENT_MTG
select /*+ full(S) */
    nvl(trim(CAMPUS_EVENT_NBR),'-') CAMPUS_EVENT_NBR, 
    nvl(EVENT_MTG_NBR,0) EVENT_MTG_NBR, 
    0 EVENT_MTG_ID, 
    nvl(trim(FACILITY_ID),'-') FACILITY_ID, 
    MEETING_DT,
    nvl(trim(DAY_OF_WK),'-') DAY_OF_WK, 
    MEETING_TIME_START,
    MEETING_TIME_END,
    nvl(CONTACT_MINUTES,0) CONTACT_MINUTES, 
    nvl(CLASS_MTG_NBR,0) CLASS_MTG_NBR, 
    nvl(trim(CAMPUS_MTG_TYPE),'-') CAMPUS_MTG_TYPE, 
    nvl(trim(DESCR),'-') DESCR, 
    nvl(trim(DESCRSHORT),'-') DESCRSHORT, 
    nvl(trim(COORDINATOR_ID),'-') COORDINATOR_ID, 
    nvl(trim(COORDNTR_OTR_ID),'-') COORDNTR_OTR_ID, 
    nvl(trim(EXT_ORG_ID),'-') EXT_ORG_ID, 
    nvl(ORG_LOCATION,0) ORG_LOCATION, 
    nvl(ORG_CONTACT,0) ORG_CONTACT, 
    nvl(trim(COUNTRY_CODE),'-') COUNTRY_CODE, 
    nvl(trim(PHONE),'-') PHONE, 
    nvl(trim(NAME),'-') NAME, 
    nvl(trim(DEPTID),'-') DEPTID, 
    nvl(PROJECTED_ATTEND,0) PROJECTED_ATTEND, 
    nvl(trim(CAMPUS_MTG_LOC),'-') CAMPUS_MTG_LOC, 
    nvl(MAX_ATTENDEE,0) MAX_ATTENDEE, 
    to_char(substr(trim(COMMENTS), 1, 4000)) COMMENTS,
    to_number(ORA_ROWSCN) SRC_SCN
from SYSADM.PS_EVENT_MTG@AMSOURCE S 
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging'
 where TABLE_NAME = 'PS_EVENT_MTG'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into AMSTG_OWNER.PS_EVENT_MTG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into AMSTG_OWNER.PS_EVENT_MTG';
merge /*+ use_hash(S,T) */ into AMSTG_OWNER.PS_EVENT_MTG T 
using (select /*+ full(S) */
    nvl(trim(CAMPUS_EVENT_NBR),'-') CAMPUS_EVENT_NBR, 
    nvl(EVENT_MTG_NBR,0) EVENT_MTG_NBR,
    'CS90' SRC_SYS_ID,    
    0 EVENT_MTG_ID, 
    nvl(trim(FACILITY_ID),'-') FACILITY_ID, 
    to_date(to_char(case when MEETING_DT < '01-JAN-1800' then NULL else MEETING_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') MEETING_DT,
    nvl(trim(DAY_OF_WK),'-') DAY_OF_WK, 
    to_date(to_char(case when MEETING_TIME_START < '01-JAN-1800' then NULL else MEETING_TIME_START end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') MEETING_TIME_START,
    to_date(to_char(case when MEETING_TIME_END < '01-JAN-1800' then NULL else MEETING_TIME_END end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') MEETING_TIME_END,
    nvl(CONTACT_MINUTES,0) CONTACT_MINUTES, 
    nvl(CLASS_MTG_NBR,0) CLASS_MTG_NBR, 
    nvl(trim(CAMPUS_MTG_TYPE),'-') CAMPUS_MTG_TYPE, 
    nvl(trim(DESCR),'-') DESCR, 
    nvl(trim(DESCRSHORT),'-') DESCRSHORT, 
    nvl(trim(COORDINATOR_ID),'-') COORDINATOR_ID, 
    nvl(trim(COORDNTR_OTR_ID),'-') COORDNTR_OTR_ID, 
    nvl(trim(EXT_ORG_ID),'-') EXT_ORG_ID, 
    nvl(ORG_LOCATION,0) ORG_LOCATION, 
    nvl(ORG_CONTACT,0) ORG_CONTACT, 
    nvl(trim(COUNTRY_CODE),'-') COUNTRY_CODE, 
    nvl(trim(PHONE),'-') PHONE, 
    nvl(trim(NAME),'-') NAME, 
    nvl(trim(DEPTID),'-') DEPTID, 
    nvl(PROJECTED_ATTEND,0) PROJECTED_ATTEND, 
    nvl(trim(CAMPUS_MTG_LOC),'-') CAMPUS_MTG_LOC, 
    nvl(MAX_ATTENDEE,0) MAX_ATTENDEE, 
    to_char(substr(trim(COMMENTS), 1, 4000)) COMMENTS,
    1234 BATCH_SID
from AMSTG_OWNER.PS_T_EVENT_MTG 
where SRC_SCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_EVENT_MTG') 
 ) S 
 on ( 
    T.CAMPUS_EVENT_NBR = S.CAMPUS_EVENT_NBR and 
    T.EVENT_MTG_NBR = S.EVENT_MTG_NBR and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.EVENT_MTG_ID = S.EVENT_MTG_ID,
    T.FACILITY_ID = S.FACILITY_ID,
    T.MEETING_DT = S.MEETING_DT,
    T.DAY_OF_WK = S.DAY_OF_WK,
    T.MEETING_TIME_START = S.MEETING_TIME_START,
    T.MEETING_TIME_END = S.MEETING_TIME_END,
    T.CONTACT_MINUTES = S.CONTACT_MINUTES,
    T.CLASS_MTG_NBR = S.CLASS_MTG_NBR,
    T.CAMPUS_MTG_TYPE = S.CAMPUS_MTG_TYPE,
    T.DESCR = S.DESCR,
    T.DESCRSHORT = S.DESCRSHORT,
    T.COORDINATOR_ID = S.COORDINATOR_ID,
    T.COORDNTR_OTR_ID = S.COORDNTR_OTR_ID,
    T.EXT_ORG_ID = S.EXT_ORG_ID,
    T.ORG_LOCATION = S.ORG_LOCATION,
    T.ORG_CONTACT = S.ORG_CONTACT,
    T.COUNTRY_CODE = S.COUNTRY_CODE,
    T.PHONE = S.PHONE,
    T.NAME = S.NAME,
    T.DEPTID = S.DEPTID,
    T.PROJECTED_ATTEND = S.PROJECTED_ATTEND,
    T.CAMPUS_MTG_LOC = S.CAMPUS_MTG_LOC,
    T.MAX_ATTENDEE = S.MAX_ATTENDEE,
    T.COMMENTS = S.COMMENTS,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.EVENT_MTG_ID <> S.EVENT_MTG_ID or 
    T.FACILITY_ID <> S.FACILITY_ID or 
    nvl(trim(T.MEETING_DT),0) <> nvl(trim(S.MEETING_DT),0) or 
    T.DAY_OF_WK <> S.DAY_OF_WK or 
    nvl(trim(T.MEETING_TIME_START),0) <> nvl(trim(S.MEETING_TIME_START),0) or 
    nvl(trim(T.MEETING_TIME_END),0) <> nvl(trim(S.MEETING_TIME_END),0) or 
    T.CONTACT_MINUTES <> S.CONTACT_MINUTES or 
    T.CLASS_MTG_NBR <> S.CLASS_MTG_NBR or 
    T.CAMPUS_MTG_TYPE <> S.CAMPUS_MTG_TYPE or 
    T.DESCR <> S.DESCR or 
    T.DESCRSHORT <> S.DESCRSHORT or 
    T.COORDINATOR_ID <> S.COORDINATOR_ID or 
    T.COORDNTR_OTR_ID <> S.COORDNTR_OTR_ID or 
    T.EXT_ORG_ID <> S.EXT_ORG_ID or 
    T.ORG_LOCATION <> S.ORG_LOCATION or 
    T.ORG_CONTACT <> S.ORG_CONTACT or 
    T.COUNTRY_CODE <> S.COUNTRY_CODE or 
    T.PHONE <> S.PHONE or 
    T.NAME <> S.NAME or 
    T.DEPTID <> S.DEPTID or 
    T.PROJECTED_ATTEND <> S.PROJECTED_ATTEND or 
    T.CAMPUS_MTG_LOC <> S.CAMPUS_MTG_LOC or 
    T.MAX_ATTENDEE <> S.MAX_ATTENDEE or 
    nvl(trim(T.COMMENTS),0) <> nvl(trim(S.COMMENTS),0) or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.CAMPUS_EVENT_NBR, 
    T.EVENT_MTG_NBR,
    T.SRC_SYS_ID, 
    T.EVENT_MTG_ID, 
    T.FACILITY_ID,
    T.MEETING_DT, 
    T.DAY_OF_WK,
    T.MEETING_TIME_START, 
    T.MEETING_TIME_END, 
    T.CONTACT_MINUTES,
    T.CLASS_MTG_NBR,
    T.CAMPUS_MTG_TYPE,
    T.DESCR,
    T.DESCRSHORT, 
    T.COORDINATOR_ID, 
    T.COORDNTR_OTR_ID,
    T.EXT_ORG_ID, 
    T.ORG_LOCATION, 
    T.ORG_CONTACT,
    T.COUNTRY_CODE, 
    T.PHONE,
    T.NAME, 
    T.DEPTID, 
    T.PROJECTED_ATTEND, 
    T.CAMPUS_MTG_LOC, 
    T.MAX_ATTENDEE, 
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID,
    T.COMMENTS
    ) 
values (
    S.CAMPUS_EVENT_NBR, 
    S.EVENT_MTG_NBR,
    'CS90', 
    S.EVENT_MTG_ID, 
    S.FACILITY_ID,
    S.MEETING_DT, 
    S.DAY_OF_WK,
    S.MEETING_TIME_START, 
    S.MEETING_TIME_END, 
    S.CONTACT_MINUTES,
    S.CLASS_MTG_NBR,
    S.CAMPUS_MTG_TYPE,
    S.DESCR,
    S.DESCRSHORT, 
    S.COORDINATOR_ID, 
    S.COORDNTR_OTR_ID,
    S.EXT_ORG_ID, 
    S.ORG_LOCATION, 
    S.ORG_CONTACT,
    S.COUNTRY_CODE, 
    S.PHONE,
    S.NAME, 
    S.DEPTID, 
    S.PROJECTED_ATTEND, 
    S.CAMPUS_MTG_LOC, 
    S.MAX_ATTENDEE,  
    'N',
    'S',
    sysdate,
    sysdate,
    1234,
    S.COMMENTS)
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;


strMessage01    := '# of PS_EVENT_MTG rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_EVENT_MTG',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_EVENT_MTG';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_EVENT_MTG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on AMSTG_OWNER.PS_EVENT_MTG';
update AMSTG_OWNER.PS_EVENT_MTG T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select CAMPUS_EVENT_NBR, EVENT_MTG_NBR
   from AMSTG_OWNER.PS_EVENT_MTG T2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_EVENT_MTG') = 'Y'
  minus
 select CAMPUS_EVENT_NBR, EVENT_MTG_NBR
   from SYSADM.PS_EVENT_MTG@AMSOURCE S2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_EVENT_MTG') = 'Y'
   ) S
 where T.CAMPUS_EVENT_NBR = S.CAMPUS_EVENT_NBR
   and T.EVENT_MTG_NBR = S.EVENT_MTG_NBR
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_EVENT_MTG rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_EVENT_MTG',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_EVENT_MTG'
;

strSqlCommand := 'commit';
commit;


strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := strProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);


EXCEPTION
   WHEN OTHERS
   THEN
      COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_EXCEPTION (
         i_SqlCommand   => strSqlCommand,
         i_SqlCode      => SQLCODE,
         i_SqlErrm      => SQLERRM);

END AM_PS_EVENT_MTG_P;
/
