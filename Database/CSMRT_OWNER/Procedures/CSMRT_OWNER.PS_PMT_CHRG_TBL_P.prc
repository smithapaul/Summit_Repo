CREATE OR REPLACE PROCEDURE             "PS_PMT_CHRG_TBL_P" AUTHID CURRENT_USER IS

/*
-- Run before the first time
DELETE
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_PMT_CHRG_TBL'

INSERT INTO CSSTG_OWNER.UM_STAGE_JOBS
(TABLE_NAME, DELETE_FLG)
VALUES
('PS_PMT_CHRG_TBL', 'Y')

SELECT *
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_PMT_CHRG_TBL'
*/


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_PMT_CHRG_TBL from PeopleSoft table PS_PMT_CHRG_TBL.
--
 --V01  SMT-xxxx 08/09/2017,    James Doucette
--                              Converted from PS_PMT_CHRG_TBL.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_PMT_CHRG_TBL';
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
 where TABLE_NAME = 'PS_PMT_CHRG_TBL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_PMT_CHRG_TBL@SASOURCE S)
 where TABLE_NAME = 'PS_PMT_CHRG_TBL'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_PMT_CHRG_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_PMT_CHRG_TBL';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_PMT_CHRG_TBL T 
    using (select /*+ full(S) */
    nvl(trim(SETID),'-') SETID, 
    nvl(trim(CHARGE_PRIORITY),'-') CHARGE_PRIORITY, 
    NVL(S.EFFDT, to_date(to_char('01/01/1900'), 'MM/DD/YYYY' ))  EFFDT, 
    nvl(trim(EFF_STATUS),'-') EFF_STATUS, 
    nvl(trim(DESCR),'-') DESCR, 
    nvl(trim(DESCRSHORT),'-') DESCRSHORT, 
    nvl(trim(TREE_NAME),'-') TREE_NAME, 
    nvl(trim(DESCR254A),'-') DESCR254A, 
    nvl(trim(WAIVER_FORM),'-') WAIVER_FORM, 
    nvl(trim(SSF_USE_AID_YEAR),'-') SSF_USE_AID_YEAR
    from SYSADM.PS_PMT_CHRG_TBL@SASOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_PMT_CHRG_TBL') ) S 
 on ( 
    T.SETID = S.SETID and 
    T.CHARGE_PRIORITY = S.CHARGE_PRIORITY and 
    T.EFFDT = S.EFFDT and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.EFF_STATUS = S.EFF_STATUS,
    T.DESCR = S.DESCR,
    T.DESCRSHORT = S.DESCRSHORT,
    T.TREE_NAME = S.TREE_NAME,
    T.DESCR254A = S.DESCR254A,
    T.WAIVER_FORM = S.WAIVER_FORM,
    T.SSF_USE_AID_YEAR = S.SSF_USE_AID_YEAR,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.EFF_STATUS <> S.EFF_STATUS or 
    T.DESCR <> S.DESCR or 
    T.DESCRSHORT <> S.DESCRSHORT or 
    T.TREE_NAME <> S.TREE_NAME or 
    T.DESCR254A <> S.DESCR254A or 
    T.WAIVER_FORM <> S.WAIVER_FORM or 
    T.SSF_USE_AID_YEAR <> S.SSF_USE_AID_YEAR or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.SETID,
    T.CHARGE_PRIORITY,
    T.EFFDT,
    T.SRC_SYS_ID, 
    T.EFF_STATUS, 
    T.DESCR,
    T.DESCRSHORT, 
    T.TREE_NAME,
    T.DESCR254A,
    T.WAIVER_FORM,
    T.SSF_USE_AID_YEAR, 
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
    ) 
values (
    S.SETID,
    S.CHARGE_PRIORITY,
    S.EFFDT,
    'CS90', 
    S.EFF_STATUS, 
    S.DESCR,
    S.DESCRSHORT, 
    S.TREE_NAME,
    S.DESCR254A,
    S.WAIVER_FORM,
    S.SSF_USE_AID_YEAR, 
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

strMessage01    := '# of PS_PMT_CHRG_TBL rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_PMT_CHRG_TBL',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_PMT_CHRG_TBL';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_PMT_CHRG_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_PMT_CHRG_TBL';
update CSSTG_OWNER.PS_PMT_CHRG_TBL T
  set T.DATA_ORIGIN = 'D',
      T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select SETID, CHARGE_PRIORITY, EFFDT
   from CSSTG_OWNER.PS_PMT_CHRG_TBL T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_PMT_CHRG_TBL') = 'Y'
  minus
 select SETID, CHARGE_PRIORITY, EFFDT
   from SYSADM.PS_PMT_CHRG_TBL@SASOURCE
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_PMT_CHRG_TBL') = 'Y' 
-- AND EMPLID <>'00386824'
   ) S
 where T.SETID = S.SETID
   and T.CHARGE_PRIORITY = S.CHARGE_PRIORITY
   AND T.EFFDT = S.EFFDT
   AND T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_PMT_CHRG_TBL rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_PMT_CHRG_TBL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_PMT_CHRG_TBL'
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

END PS_PMT_CHRG_TBL_P;
/
