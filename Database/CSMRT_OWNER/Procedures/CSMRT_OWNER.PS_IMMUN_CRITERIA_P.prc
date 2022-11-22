DROP PROCEDURE CSMRT_OWNER.PS_IMMUN_CRITERIA_P
/

--
-- PS_IMMUN_CRITERIA_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_IMMUN_CRITERIA_P" AUTHID CURRENT_USER IS


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_IMMUN_CRITERIA from PeopleSoft table PS_IMMUN_CRITERIA.
--
 --V01  SMT-xxxx 09/05/2017,    James Doucette
--                              Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_IMMUN_CRITERIA';
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
 where TABLE_NAME = 'PS_IMMUN_CRITERIA'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_IMMUN_CRITERIA@SASOURCE S)
 where TABLE_NAME = 'PS_IMMUN_CRITERIA'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_IMMUN_CRITERIA';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_IMMUN_CRITERIA';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_IMMUN_CRITERIA T 
using (select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(IMMUNIZATION),'-') IMMUNIZATION, 
    nvl(IMMUN_SEQ,0) IMMUN_SEQ, 
    nvl(CRITERIA_SEQ,0) CRITERIA_SEQ, 
    to_date(to_char(case when DATE_TAKEN < '01-JAN-1800' then NULL else DATE_TAKEN end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') DATE_TAKEN,
    to_date(to_char(case when EXPIRATION_DT < '01-JAN-1800' then NULL else EXPIRATION_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EXPIRATION_DT, 
    DESCR, 
    TEST_STATUS, 
    STATUS_IMMUN, 
    to_date(to_char(case when DATE_RECEIVED < '01-JAN-1800' then NULL else DATE_RECEIVED end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') DATE_RECEIVED
from SYSADM.PS_IMMUN_CRITERIA@SASOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_IMMUN_CRITERIA') 
  and EMPLID between '00000000' and '99999999'
  and length(EMPLID) = 8) S 
 on ( 
    T.EMPLID = S.EMPLID and 
    T.IMMUNIZATION = S.IMMUNIZATION and 
    T.IMMUN_SEQ = S.IMMUN_SEQ and 
    T.CRITERIA_SEQ = S.CRITERIA_SEQ and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.DATE_TAKEN = S.DATE_TAKEN,
    T.EXPIRATION_DT = S.EXPIRATION_DT,
    T.DESCR = S.DESCR,
    T.TEST_STATUS = S.TEST_STATUS,
    T.STATUS_IMMUN = S.STATUS_IMMUN,
    T.DATE_RECEIVED = S.DATE_RECEIVED,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    nvl(trim(T.DATE_TAKEN),0) <> nvl(trim(S.DATE_TAKEN),0) or 
    nvl(trim(T.EXPIRATION_DT),0) <> nvl(trim(S.EXPIRATION_DT),0) or 
    T.DESCR <> S.DESCR or 
    T.TEST_STATUS <> S.TEST_STATUS or 
    T.STATUS_IMMUN <> S.STATUS_IMMUN or 
    nvl(trim(T.DATE_RECEIVED),0) <> nvl(trim(S.DATE_RECEIVED),0) or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.EMPLID, 
    T.IMMUNIZATION, 
    T.IMMUN_SEQ,
    T.CRITERIA_SEQ, 
    T.SRC_SYS_ID, 
    T.DATE_TAKEN, 
    T.EXPIRATION_DT,
    T.DESCR,
    T.TEST_STATUS,
    T.STATUS_IMMUN, 
    T.DATE_RECEIVED,
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
    ) 
values (
    S.EMPLID, 
    S.IMMUNIZATION, 
    S.IMMUN_SEQ,
    S.CRITERIA_SEQ, 
    'CS90', 
    S.DATE_TAKEN, 
    S.EXPIRATION_DT,
    S.DESCR,
    S.TEST_STATUS,
    S.STATUS_IMMUN, 
    S.DATE_RECEIVED,
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

strMessage01    := '# of PS_IMMUN_CRITERIA rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_IMMUN_CRITERIA',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_IMMUN_CRITERIA';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_IMMUN_CRITERIA';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_IMMUN_CRITERIA';
update CSSTG_OWNER.PS_IMMUN_CRITERIA T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, IMMUNIZATION, IMMUN_SEQ, CRITERIA_SEQ
   from CSSTG_OWNER.PS_IMMUN_CRITERIA T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_IMMUN_CRITERIA') = 'Y'
  minus
 select EMPLID, IMMUNIZATION, IMMUN_SEQ, CRITERIA_SEQ
   from SYSADM.PS_IMMUN_CRITERIA@SASOURCE
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_IMMUN_CRITERIA') = 'Y'
            and EMPLID between '00000000' and '99999999'
            and length(EMPLID) = 8  
   ) S
 where T.EMPLID = S.EMPLID   
   and T.IMMUNIZATION = S.IMMUNIZATION
   and T.IMMUN_SEQ = S.IMMUN_SEQ
   and T.CRITERIA_SEQ = S.CRITERIA_SEQ
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_IMMUN_CRITERIA rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_IMMUN_CRITERIA',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_IMMUN_CRITERIA'
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

END PS_IMMUN_CRITERIA_P;
/
