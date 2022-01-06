CREATE OR REPLACE PROCEDURE             PS_ACCOM_OPTION_P AUTHID CURRENT_USER IS

/*
-- Run before the first time

DELETE
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_ACCOM_OPTION'

INSERT INTO CSSTG_OWNER.UM_STAGE_JOBS
(TABLE_NAME, DELETE_FLG)
VALUES
('PS_ACCOM_OPTION', 'Y')

SELECT *
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_ACCOM_OPTION'

*/ 

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_ACCOM_OPTION from PeopleSoft table PS_ACCOM_OPTION.
--
-- V01  SMT-xxxx 8/18/2017,    Preethi Lodha
--                             Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_ACCOM_OPTION';
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
 where TABLE_NAME = 'PS_ACCOM_OPTION'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Truncating',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_ACCOM_OPTION@SASOURCE S)
 where TABLE_NAME = 'PS_ACCOM_OPTION'
;

strSqlCommand := 'commit';
commit;


strSqlDynamic   := 'truncate table CSSTG_OWNER.PS_T_ACCOM_OPTION';
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
 where TABLE_NAME = 'PS_ACCOM_OPTION'
;

strSqlCommand := 'commit';
commit;


strSqlCommand := 'insert';
INSERT /*+ append */
      INTO  CSSTG_OWNER.PS_T_ACCOM_OPTION
   SELECT /*+ full(S) */
         EMPLID,
          EMPL_RCD,
          ACCOMMODATION_ID,
          ACCOMMODATION_OPT,
          'CS90' SRC_SYS_ID,
          ACCOMMODATION_TYPE,
          CURRENCY_CD,
          ACCOM_COST,
          EMPLOYER_SUGGESTED,
          ACCOM_STATUS,
          STATUS_DT,
          '1234' BATCH_SID,
          TO_CHAR (SUBSTR (TRIM (DESCRLONG), 1, 4000)) DESCRLONG,
          to_number(ORA_ROWSCN) SRC_SCN
from SYSADM.PS_ACCOM_OPTION@SASOURCE S;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging'
 where TABLE_NAME = 'PS_ACCOM_OPTION'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_ACCOM_OPTION';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_ACCOM_OPTION';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_ACCOM_OPTION T
using (select /*+ full(S) */
nvl(trim(EMPLID),'-') EMPLID,
nvl(EMPL_RCD,0) EMPL_RCD,
nvl(ACCOMMODATION_ID,0) ACCOMMODATION_ID,
nvl(ACCOMMODATION_OPT,0) ACCOMMODATION_OPT,
nvl(trim(ACCOMMODATION_TYPE),'-') ACCOMMODATION_TYPE,
nvl(trim(CURRENCY_CD),'-') CURRENCY_CD,
nvl(ACCOM_COST,0) ACCOM_COST,
nvl(trim(EMPLOYER_SUGGESTED),'-') EMPLOYER_SUGGESTED,
nvl(trim(ACCOM_STATUS),'-') ACCOM_STATUS,
to_date(to_char(case when STATUS_DT < '01-JAN-1800' then NULL else STATUS_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') STATUS_DT,
DESCRLONG DESCRLONG
from CSSTG_OWNER.PS_T_ACCOM_OPTION S
where SRC_SCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ACCOM_OPTION') ) S
   on (
T.EMPLID = S.EMPLID and
T.EMPL_RCD = S.EMPL_RCD and
T.ACCOMMODATION_ID = S.ACCOMMODATION_ID and
T.ACCOMMODATION_OPT = S.ACCOMMODATION_OPT and
T.SRC_SYS_ID = 'CS90')
when matched then update set
T.ACCOMMODATION_TYPE = S.ACCOMMODATION_TYPE,
T.CURRENCY_CD = S.CURRENCY_CD,
T.ACCOM_COST = S.ACCOM_COST,
T.EMPLOYER_SUGGESTED = S.EMPLOYER_SUGGESTED,
T.ACCOM_STATUS = S.ACCOM_STATUS,
T.STATUS_DT = S.STATUS_DT,
T.DESCRLONG = S.DESCRLONG,
T.DATA_ORIGIN = 'S',
T.LASTUPD_EW_DTTM = sysdate,
T.BATCH_SID   = 1234
where
T.ACCOMMODATION_TYPE <> S.ACCOMMODATION_TYPE or
T.CURRENCY_CD <> S.CURRENCY_CD or
T.ACCOM_COST <> S.ACCOM_COST or
T.EMPLOYER_SUGGESTED <> S.EMPLOYER_SUGGESTED or
T.ACCOM_STATUS <> S.ACCOM_STATUS or
T.STATUS_DT <> S.STATUS_DT or
nvl(trim(T.DESCRLONG),0) <> nvl(trim(S.DESCRLONG),0) or
T.DATA_ORIGIN = 'D'
when not matched then
insert (
T.EMPLID,
T.EMPL_RCD,
T.ACCOMMODATION_ID,
T.ACCOMMODATION_OPT,
T.SRC_SYS_ID,
T.ACCOMMODATION_TYPE,
T.CURRENCY_CD,
T.ACCOM_COST,
T.EMPLOYER_SUGGESTED,
T.ACCOM_STATUS,
T.STATUS_DT,
T.LOAD_ERROR,
T.DATA_ORIGIN,
T.CREATED_EW_DTTM,
T.LASTUPD_EW_DTTM,
T.BATCH_SID,
T.DESCRLONG
)
values (
S.EMPLID,
S.EMPL_RCD,
S.ACCOMMODATION_ID,
S.ACCOMMODATION_OPT,
'CS90',
S.ACCOMMODATION_TYPE,
S.CURRENCY_CD,
S.ACCOM_COST,
S.EMPLOYER_SUGGESTED,
S.ACCOM_STATUS,
S.STATUS_DT,
'N',
'S',
sysdate,
sysdate,
1234,
S.DESCRLONG);


strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;


strMessage01    := '# of PS_ACCOM_OPTION rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ACCOM_OPTION',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_ACCOM_OPTION';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_ACCOM_OPTION';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_ACCOM_OPTION';
update CSSTG_OWNER.PS_ACCOM_OPTION T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, EMPL_RCD, ACCOMMODATION_ID, ACCOMMODATION_OPT
   from CSSTG_OWNER.PS_ACCOM_OPTION T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ACCOM_OPTION') = 'Y'
  minus
 select EMPLID, EMPL_RCD, ACCOMMODATION_ID, ACCOMMODATION_OPT
   from SYSADM.PS_ACCOM_OPTION@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ACCOM_OPTION') = 'Y'
   ) S
 where T.EMPLID = S.EMPLID
   and T.EMPL_RCD = S.EMPL_RCD
   and T.ACCOMMODATION_ID = S.ACCOMMODATION_ID
   and T.ACCOMMODATION_OPT = S.ACCOMMODATION_OPT
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_ACCOM_OPTION rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ACCOM_OPTION',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_ACCOM_OPTION'
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

END PS_ACCOM_OPTION_P;
/
