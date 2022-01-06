CREATE OR REPLACE PROCEDURE             PS_ACCOM_REQUEST_P AUTHID CURRENT_USER IS

/*
-- Run before the first time

DELETE
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_ACCOM_REQUEST'

INSERT INTO CSSTG_OWNER.UM_STAGE_JOBS
(TABLE_NAME, DELETE_FLG)
VALUES
('PS_ACCOM_REQUEST', 'Y')

SELECT *
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_ACCOM_REQUEST'

*/ 

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_ACCOM_REQUEST from PeopleSoft table PS_ACCOM_REQUEST.
--
-- V01  SMT-xxxx 8/18/2017,    Preethi Lodha
--                             Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_ACCOM_REQUEST';
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
       START_DT = SYSDATE,
       END_DT = NULL
 where TABLE_NAME = 'PS_ACCOM_REQUEST'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Truncating',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_ACCOM_REQUEST@SASOURCE S)
 where TABLE_NAME = 'PS_ACCOM_REQUEST'
;

strSqlCommand := 'commit';
commit;


strSqlDynamic   := 'truncate table CSSTG_OWNER.PS_T_ACCOM_REQUEST';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );


strSqlCommand   := 'Loading temp table for CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Loading'
 where TABLE_NAME = 'PS_ACCOM_REQUEST'
;

strSqlCommand := 'commit';
commit;


strSqlCommand := 'insert';
INSERT /*+ append */
      INTO  CSSTG_OWNER.PS_T_ACCOM_REQUEST
   SELECT /*+ full(S) */
         EMPLID,
          EMPL_RCD,
          ACCOMMODATION_ID,
          'CS90' SRC_SYS_ID,
          DT_REQUESTED,
          RESPONSIBLE_ID,
          REQUEST_STATUS,
          STATUS_DT,
          '1234' BATCH_SID,
          TO_CHAR (SUBSTR (TRIM (COMMENTS), 1, 4000)) COMMENTS,
          TO_NUMBER (ORA_ROWSCN) SRC_SCN
     FROM SYSADM.PS_ACCOM_REQUEST@SASOURCE;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging'
 where TABLE_NAME = 'PS_ACCOM_REQUEST'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_ACCOM_REQUEST';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_ACCOM_REQUEST';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_ACCOM_REQUEST T
using (select /*+ full(S) */
nvl(trim(EMPLID),'-') EMPLID,
nvl(EMPL_RCD,0) EMPL_RCD,
nvl(ACCOMMODATION_ID,0) ACCOMMODATION_ID,
to_date(to_char(case when DT_REQUESTED < '01-JAN-1800' then NULL else DT_REQUESTED end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') DT_REQUESTED,
nvl(trim(RESPONSIBLE_ID),'-') RESPONSIBLE_ID,
nvl(trim(REQUEST_STATUS),'-') REQUEST_STATUS,
to_date(to_char(case when STATUS_DT < '01-JAN-1800' then NULL else STATUS_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') STATUS_DT,
COMMENTS COMMENTS
from CSSTG_OWNER.PS_T_ACCOM_REQUEST S
where SRC_SCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ACCOM_REQUEST') ) S
   on (
T.EMPLID = S.EMPLID and
T.EMPL_RCD = S.EMPL_RCD and
T.ACCOMMODATION_ID = S.ACCOMMODATION_ID and
T.SRC_SYS_ID = 'CS90')
when matched then update set
T.DT_REQUESTED = S.DT_REQUESTED,
T.RESPONSIBLE_ID = S.RESPONSIBLE_ID,
T.REQUEST_STATUS = S.REQUEST_STATUS,
T.STATUS_DT = S.STATUS_DT,
T.COMMENTS = S.COMMENTS,
T.DATA_ORIGIN = 'S',
T.LASTUPD_EW_DTTM = sysdate,
T.BATCH_SID   = 1234
where
T.DT_REQUESTED <> S.DT_REQUESTED or
T.RESPONSIBLE_ID <> S.RESPONSIBLE_ID or
T.REQUEST_STATUS <> S.REQUEST_STATUS or
T.STATUS_DT <> S.STATUS_DT or
nvl(trim(T.COMMENTS),0) <> nvl(trim(S.COMMENTS),0) or
T.DATA_ORIGIN = 'D'
when not matched then
insert (
T.EMPLID,
T.EMPL_RCD,
T.ACCOMMODATION_ID,
T.SRC_SYS_ID,
T.DT_REQUESTED,
T.RESPONSIBLE_ID,
T.REQUEST_STATUS,
T.STATUS_DT,
T.LOAD_ERROR,
T.DATA_ORIGIN,
T.CREATED_EW_DTTM,
T.LASTUPD_EW_DTTM,
T.BATCH_SID,
T.COMMENTS
)
values (
S.EMPLID,
S.EMPL_RCD,
S.ACCOMMODATION_ID,
'CS90',
S.DT_REQUESTED,
S.RESPONSIBLE_ID,
S.REQUEST_STATUS,
S.STATUS_DT,
'N',
'S',
sysdate,
sysdate,
1234,
S.COMMENTS);


strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;


strMessage01    := '# of PS_ACCOM_REQUEST rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ACCOM_REQUEST',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_ACCOM_REQUEST';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_ACCOM_REQUEST';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_ACCOM_REQUEST';
update CSSTG_OWNER.PS_ACCOM_REQUEST T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, EMPL_RCD, ACCOMMODATION_ID
   from CSSTG_OWNER.PS_ACCOM_REQUEST T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ACCOM_REQUEST') = 'Y'
  minus
 select EMPLID, EMPL_RCD, ACCOMMODATION_ID
   from SYSADM.PS_ACCOM_REQUEST@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ACCOM_REQUEST') = 'Y'
   ) S
 where T.EMPLID = S.EMPLID
   and T.EMPL_RCD = S.EMPL_RCD
   and T.ACCOMMODATION_ID = S.ACCOMMODATION_ID
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;
strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_ACCOM_REQUEST rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ACCOM_REQUEST',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_ACCOM_REQUEST'
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

END PS_ACCOM_REQUEST_P;
/
