CREATE OR REPLACE PROCEDURE             "PS_CLASS_PRMSN_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_CLASS_PRMSN from PeopleSoft table PS_CLASS_PRMSN.
--
 --V01  SMT-xxxx 08/17/2017,    James Doucette
--                              Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_CLASS_PRMSN';
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
 where TABLE_NAME = 'PS_CLASS_PRMSN'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_CLASS_PRMSN@SASOURCE S)
 where TABLE_NAME = 'PS_CLASS_PRMSN'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_CLASS_PRMSN';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_CLASS_PRMSN';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_CLASS_PRMSN T
using (select /*+ full(S) */
    nvl(trim(CRSE_ID),'-') CRSE_ID, 
    nvl(CRSE_OFFER_NBR,0) CRSE_OFFER_NBR, 
    nvl(trim(STRM),'-') STRM, 
    nvl(trim(SESSION_CODE),'-') SESSION_CODE, 
    nvl(trim(CLASS_SECTION),'-') CLASS_SECTION, 
    nvl(trim(PERMISSION_TYPE),'-') PERMISSION_TYPE, 
    nvl(CLASS_PRMSN_SEQ,0) CLASS_PRMSN_SEQ, 
    nvl(CLASS_PRMSN_NBR,0) CLASS_PRMSN_NBR, 
    nvl(trim(PERMISSION_USED),'-') PERMISSION_USED, 
    nvl(trim(EMPLID),'-') EMPLID, 
    to_date(to_char(case when PERMISSION_USE_DT < '01-JAN-1800' then NULL else PERMISSION_USE_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') PERMISSION_USE_DT, 
    to_date(to_char(case when PRMSN_EXPIRE_DT < '01-JAN-1800' then NULL else PRMSN_EXPIRE_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') PRMSN_EXPIRE_DT, 
    nvl(trim(OPRID),'-') OPRID, 
    to_date(to_char(case when CREATION_DT < '01-JAN-1800' then NULL else CREATION_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') CREATION_DT, 
    to_date(to_char(case when CREATION_TIME < '01-JAN-1800' then NULL else CREATION_TIME end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') CREATION_TIME, 
    nvl(trim(OPRID_LAST_UPDT),'-') OPRID_LAST_UPDT, 
    to_date(to_char(case when LAST_UPD_DT_STMP < '01-JAN-1800' then NULL else LAST_UPD_DT_STMP end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') LAST_UPD_DT_STMP,
    to_date(to_char(case when LAST_UPD_TM_STMP < '01-JAN-1800' then NULL else LAST_UPD_TM_STMP end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') LAST_UPD_TM_STMP,
    nvl(trim(SSR_ISSUE_FL),'-') SSR_ISSUE_FL, 
    nvl(trim(SSR_ISSUE_OPRID),'-') SSR_ISSUE_OPRID, 
    to_date(to_char(case when SSR_ISSUE_DT < '01-JAN-1800' then NULL else SSR_ISSUE_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') SSR_ISSUE_DT,
    to_date(to_char(case when SSR_ISSUE_TIME < '01-JAN-1800' then NULL else SSR_ISSUE_TIME end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') SSR_ISSUE_TIME,
    nvl(trim(OVRD_CLASS_LIMIT),'-') OVRD_CLASS_LIMIT, 
    nvl(trim(SSR_OVRD_REQ),'-') SSR_OVRD_REQ, 
    nvl(trim(SSR_OVRD_CONSENT),'-') SSR_OVRD_CONSENT, 
    nvl(trim(OVRD_CAREER),'-') OVRD_CAREER, 
    nvl(trim(SSR_OVRD_TIME_PERD),'-') SSR_OVRD_TIME_PERD, 
    nvl(trim(DESCR50),'-') DESCR50
from SYSADM.PS_CLASS_PRMSN@SASOURCE S 
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CLASS_PRMSN') ) S
 on ( 
    T.CRSE_ID = S.CRSE_ID and 
    T.CRSE_OFFER_NBR = S.CRSE_OFFER_NBR and 
    T.STRM = S.STRM and 
    T.SESSION_CODE = S.SESSION_CODE and 
    T.CLASS_SECTION = S.CLASS_SECTION and 
    T.PERMISSION_TYPE = S.PERMISSION_TYPE and 
    T.CLASS_PRMSN_SEQ = S.CLASS_PRMSN_SEQ and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.CLASS_PRMSN_NBR = S.CLASS_PRMSN_NBR,
    T.PERMISSION_USED = S.PERMISSION_USED,
    T.EMPLID = S.EMPLID,
    T.PERMISSION_USE_DT = S.PERMISSION_USE_DT,
    T.PRMSN_EXPIRE_DT = S.PRMSN_EXPIRE_DT,
    T.OPRID = S.OPRID,
    T.CREATION_DT = S.CREATION_DT,
    T.CREATION_TIME = S.CREATION_TIME,
    T.OPRID_LAST_UPDT = S.OPRID_LAST_UPDT,
    T.LAST_UPD_DT_STMP = S.LAST_UPD_DT_STMP,
    T.LAST_UPD_TM_STMP = S.LAST_UPD_TM_STMP,
    T.SSR_ISSUE_FL = S.SSR_ISSUE_FL,
    T.SSR_ISSUE_OPRID = S.SSR_ISSUE_OPRID,
    T.SSR_ISSUE_DT = S.SSR_ISSUE_DT,
    T.SSR_ISSUE_TIME = S.SSR_ISSUE_TIME,
    T.OVRD_CLASS_LIMIT = S.OVRD_CLASS_LIMIT,
    T.SSR_OVRD_REQ = S.SSR_OVRD_REQ,
    T.SSR_OVRD_CONSENT = S.SSR_OVRD_CONSENT,
    T.OVRD_CAREER = S.OVRD_CAREER,
    T.SSR_OVRD_TIME_PERD = S.SSR_OVRD_TIME_PERD,
    T.DESCR50 = S.DESCR50,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.CLASS_PRMSN_NBR <> S.CLASS_PRMSN_NBR or 
    T.PERMISSION_USED <> S.PERMISSION_USED or 
    T.EMPLID <> S.EMPLID or 
    nvl(trim(T.PERMISSION_USE_DT),0) <> nvl(trim(S.PERMISSION_USE_DT),0) or 
    nvl(trim(T.PRMSN_EXPIRE_DT),0) <> nvl(trim(S.PRMSN_EXPIRE_DT),0) or 
    T.OPRID <> S.OPRID or 
    nvl(trim(T.CREATION_DT),0) <> nvl(trim(S.CREATION_DT),0) or 
    nvl(trim(T.CREATION_TIME),0) <> nvl(trim(S.CREATION_TIME),0) or 
    T.OPRID_LAST_UPDT <> S.OPRID_LAST_UPDT or 
    nvl(trim(T.LAST_UPD_DT_STMP),0) <> nvl(trim(S.LAST_UPD_DT_STMP),0) or 
    nvl(trim(T.LAST_UPD_TM_STMP),0) <> nvl(trim(S.LAST_UPD_TM_STMP),0) or 
    T.SSR_ISSUE_FL <> S.SSR_ISSUE_FL or 
    T.SSR_ISSUE_OPRID <> S.SSR_ISSUE_OPRID or 
    nvl(trim(T.SSR_ISSUE_DT),0) <> nvl(trim(S.SSR_ISSUE_DT),0) or 
    nvl(trim(T.SSR_ISSUE_TIME),0) <> nvl(trim(S.SSR_ISSUE_TIME),0) or 
    T.OVRD_CLASS_LIMIT <> S.OVRD_CLASS_LIMIT or 
    T.SSR_OVRD_REQ <> S.SSR_OVRD_REQ or 
    T.SSR_OVRD_CONSENT <> S.SSR_OVRD_CONSENT or 
    T.OVRD_CAREER <> S.OVRD_CAREER or 
    T.SSR_OVRD_TIME_PERD <> S.SSR_OVRD_TIME_PERD or 
    T.DESCR50 <> S.DESCR50 or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.CRSE_ID,
    T.CRSE_OFFER_NBR, 
    T.STRM, 
    T.SESSION_CODE, 
    T.CLASS_SECTION,
    T.PERMISSION_TYPE,
    T.CLASS_PRMSN_SEQ,
    T.SRC_SYS_ID, 
    T.CLASS_PRMSN_NBR,
    T.PERMISSION_USED,
    T.EMPLID, 
    T.PERMISSION_USE_DT,
    T.PRMSN_EXPIRE_DT,
    T.OPRID,
    T.CREATION_DT,
    T.CREATION_TIME,
    T.OPRID_LAST_UPDT,
    T.LAST_UPD_DT_STMP, 
    T.LAST_UPD_TM_STMP, 
    T.SSR_ISSUE_FL, 
    T.SSR_ISSUE_OPRID,
    T.SSR_ISSUE_DT, 
    T.SSR_ISSUE_TIME, 
    T.OVRD_CLASS_LIMIT, 
    T.SSR_OVRD_REQ, 
    T.SSR_OVRD_CONSENT, 
    T.OVRD_CAREER,
    T.SSR_OVRD_TIME_PERD, 
    T.DESCR50,
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
    ) 
values (
    S.CRSE_ID,
    S.CRSE_OFFER_NBR, 
    S.STRM, 
    S.SESSION_CODE, 
    S.CLASS_SECTION,
    S.PERMISSION_TYPE,
    S.CLASS_PRMSN_SEQ,
    'CS90', 
    S.CLASS_PRMSN_NBR,
    S.PERMISSION_USED,
    S.EMPLID, 
    S.PERMISSION_USE_DT,
    S.PRMSN_EXPIRE_DT,
    S.OPRID,
    S.CREATION_DT,
    S.CREATION_TIME,
    S.OPRID_LAST_UPDT,
    S.LAST_UPD_DT_STMP, 
    S.LAST_UPD_TM_STMP, 
    S.SSR_ISSUE_FL, 
    S.SSR_ISSUE_OPRID,
    S.SSR_ISSUE_DT, 
    S.SSR_ISSUE_TIME, 
    S.OVRD_CLASS_LIMIT, 
    S.SSR_OVRD_REQ, 
    S.SSR_OVRD_CONSENT, 
    S.OVRD_CAREER,
    S.SSR_OVRD_TIME_PERD, 
    S.DESCR50,
    'N',
    'S',
    sysdate,
    sysdate,
    1234)
;

commit;


strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_CLASS_PRMSN rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_CLASS_PRMSN',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_CLASS_PRMSN';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_CLASS_PRMSN';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_CLASS_PRMSN';
update CSSTG_OWNER.PS_CLASS_PRMSN T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select CRSE_ID, CRSE_OFFER_NBR, STRM, SESSION_CODE, CLASS_SECTION, PERMISSION_TYPE, CLASS_PRMSN_SEQ
   from CSSTG_OWNER.PS_CLASS_PRMSN T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CLASS_PRMSN') = 'Y'
  minus
 select CRSE_ID, CRSE_OFFER_NBR, STRM, SESSION_CODE, CLASS_SECTION, PERMISSION_TYPE, CLASS_PRMSN_SEQ
   from SYSADM.PS_CLASS_PRMSN@SASOURCE
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CLASS_PRMSN') = 'Y'
   ) S
 where T.CRSE_ID = S.CRSE_ID
   and T.CRSE_OFFER_NBR = S.CRSE_OFFER_NBR
   and T.STRM = S.STRM
   and T.SESSION_CODE = S.SESSION_CODE
   and T.CLASS_SECTION = S.CLASS_SECTION
   and T.PERMISSION_TYPE = S.PERMISSION_TYPE
   and T.CLASS_PRMSN_SEQ = S.CLASS_PRMSN_SEQ
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_CLASS_PRMSN rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_CLASS_PRMSN',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_CLASS_PRMSN'
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

END PS_CLASS_PRMSN_P;
/
