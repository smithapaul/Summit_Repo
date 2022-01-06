CREATE OR REPLACE PROCEDURE             "PS_PERSON_CHK_ITEM_P" AUTHID CURRENT_USER IS

/*
-- Run before the first time
DELETE
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_PERSON_CHK_ITEM'

INSERT INTO CSSTG_OWNER.UM_STAGE_JOBS
(TABLE_NAME, DELETE_FLG)
VALUES
('PS_PERSON_CHK_ITEM', 'Y')

SELECT *
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_PERSON_CHK_ITEM'
*/


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_PERSON_CHK_ITEM from PeopleSoft table PS_PERSON_CHK_ITEM.
--
-- V01  SMT-xxxx 05/16/2017,    Jim Doucette
--                              Converted from PS_PERSON_CHK_ITEM.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_PERSON_CHK_ITEM';
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
 where TABLE_NAME = 'PS_PERSON_CHK_ITEM'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_PERSON_CHK_ITEM@SASOURCE S)
 where TABLE_NAME = 'PS_PERSON_CHK_ITEM'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_PERSON_CHK_ITEM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_PERSON_CHK_ITEM';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_PERSON_CHK_ITEM T
using (select /*+ full(S) */
    nvl(trim(COMMON_ID),'-') COMMON_ID,
    nvl(SEQ_3C,0) SEQ_3C,
    nvl(CHECKLIST_SEQ,0) CHECKLIST_SEQ,
    nvl(trim(CHKLST_ITEM_CD),'-') CHKLST_ITEM_CD,
    nvl(trim(ITEM_STATUS),'-') ITEM_STATUS,
    to_date(to_char(case when STATUS_DT < '01-JAN-1800' then NULL else STATUS_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') STATUS_DT,
    nvl(trim(STATUS_CHANGE_ID),'-') STATUS_CHANGE_ID,
    to_date(to_char(case when DUE_DT < '01-JAN-1800' then NULL else DUE_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') DUE_DT,
    nvl(trim(CURRENCY_CD),'-') CURRENCY_CD,
    nvl(DUE_AMT,0) DUE_AMT,
    nvl(trim(RESPONSIBLE_ID),'-') RESPONSIBLE_ID,
    nvl(trim(ASSOC_ID),'-') ASSOC_ID,
    nvl(trim(NAME),'-') NAME,
    nvl(trim(COMM_KEY),'-') COMM_KEY
from SYSADM.PS_PERSON_CHK_ITEM@SASOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_PERSON_CHK_ITEM')
  and COMMON_ID between '00000000' and '99999999'
  and length(COMMON_ID) = 8
 ) S
   on (
    T.COMMON_ID = S.COMMON_ID and
    T.SEQ_3C = S.SEQ_3C and
    T.CHECKLIST_SEQ = S.CHECKLIST_SEQ and
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.CHKLST_ITEM_CD = S.CHKLST_ITEM_CD,
    T.ITEM_STATUS = S.ITEM_STATUS,
    T.STATUS_DT = S.STATUS_DT,
    T.STATUS_CHANGE_ID = S.STATUS_CHANGE_ID,
    T.DUE_DT = S.DUE_DT,
    T.CURRENCY_CD = S.CURRENCY_CD,
    T.DUE_AMT = S.DUE_AMT,
    T.RESPONSIBLE_ID = S.RESPONSIBLE_ID,
    T.ASSOC_ID = S.ASSOC_ID,
    T.NAME = S.NAME,
    T.COMM_KEY = S.COMM_KEY,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID   = 1234
where
    T.CHKLST_ITEM_CD <> S.CHKLST_ITEM_CD or
    T.ITEM_STATUS <> S.ITEM_STATUS or
    T.STATUS_DT <> S.STATUS_DT or
    T.STATUS_CHANGE_ID <> S.STATUS_CHANGE_ID or
    T.DUE_DT <> S.DUE_DT or
    T.CURRENCY_CD <> S.CURRENCY_CD or
    T.DUE_AMT <> S.DUE_AMT or
    T.RESPONSIBLE_ID <> S.RESPONSIBLE_ID or
    T.ASSOC_ID <> S.ASSOC_ID or
    T.NAME <> S.NAME or
    T.COMM_KEY <> S.COMM_KEY or
    T.DATA_ORIGIN = 'D'
when not matched then
insert (
    T.COMMON_ID,
    T.SEQ_3C,
    T.CHECKLIST_SEQ,
    T.SRC_SYS_ID,
    T.CHKLST_ITEM_CD,
    T.ITEM_STATUS,
    T.STATUS_DT,
    T.STATUS_CHANGE_ID,
    T.DUE_DT,
    T.CURRENCY_CD,
    T.DUE_AMT,
    T.RESPONSIBLE_ID,
    T.ASSOC_ID,
    T.NAME,
    T.COMM_KEY,
    T.LOAD_ERROR,
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
    )
values (
    S.COMMON_ID,
    S.SEQ_3C,
    S.CHECKLIST_SEQ,
    'CS90',
    S.CHKLST_ITEM_CD,
    S.ITEM_STATUS,
    S.STATUS_DT,
    S.STATUS_CHANGE_ID,
    S.DUE_DT,
    S.CURRENCY_CD,
    S.DUE_AMT,
    S.RESPONSIBLE_ID,
    S.ASSOC_ID,
    S.NAME,
    S.COMM_KEY,
    'N',
    'S',
    sysdate,
    sysdate,
    1234);


strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_PERSON_CHK_ITEM rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_PERSON_CHK_ITEM',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_PERSON_CHK_ITEM';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_PERSON_CHK_ITEM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_PERSON_CHK_ITEM';
update CSSTG_OWNER.PS_PERSON_CHK_ITEM T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select COMMON_ID, SEQ_3C, CHECKLIST_SEQ
   from CSSTG_OWNER.PS_PERSON_CHK_ITEM T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_PERSON_CHK_ITEM') = 'Y'
  minus
 select COMMON_ID, SEQ_3C, CHECKLIST_SEQ
   from SYSADM.PS_PERSON_CHK_ITEM@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_PERSON_CHK_ITEM') = 'Y'
   ) S
 where T.COMMON_ID = S.COMMON_ID
   and T.SEQ_3C = S.SEQ_3C
   and T.CHECKLIST_SEQ = S.CHECKLIST_SEQ
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_PERSON_CHK_ITEM rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_PERSON_CHK_ITEM',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_PERSON_CHK_ITEM'
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

END PS_PERSON_CHK_ITEM_P;
/
