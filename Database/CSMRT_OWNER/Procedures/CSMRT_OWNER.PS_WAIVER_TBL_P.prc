CREATE OR REPLACE PROCEDURE             PS_WAIVER_TBL_P AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_WAIVER_TBL from PeopleSoft table PS_WAIVER_TBL.
--
-- V01  SMT-xxxx 04/04/2017,    Jim Doucette
--                              Converted from PS_WAIVER_TBL.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_WAIVER_TBL';
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
 where TABLE_NAME = 'PS_WAIVER_TBL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_WAIVER_TBL@SASOURCE S)
 where TABLE_NAME = 'PS_WAIVER_TBL'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_WAIVER_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_WAIVER_TBL';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_WAIVER_TBL T 
using (select /*+ full(S) */
    nvl(trim(SETID),'-') SETID, 
    nvl(trim(WAIVER_CODE),'-') WAIVER_CODE, 
    to_date(to_char(case when EFFDT < '01-JAN-1800' then NULL else EFFDT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EFFDT, 
    nvl(trim(EFF_STATUS),'-') EFF_STATUS, 
    nvl(trim(ACCOUNT_TYPE_SF),'-') ACCOUNT_TYPE_SF, 
    nvl(trim(ITEM_TYPE),'-') ITEM_TYPE, 
    nvl(trim(SSF_CRITR_EQUTN_SW),'-') SSF_CRITR_EQUTN_SW, 
    nvl(trim(CRITERIA),'-') CRITERIA, 
    to_date(to_char(case when ADJUST_UNTIL_DATE < '01-JAN-1800' then NULL 
                    else ADJUST_UNTIL_DATE end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') ADJUST_UNTIL_DATE, 
    nvl(AMT_PER_UNIT,0) AMT_PER_UNIT, 
    nvl(FLAT_AMT,0) FLAT_AMT, 
    nvl(WAIVE_PCT,0) WAIVE_PCT, 
    nvl(trim(ITEM_TYPE_GROUP),'-') ITEM_TYPE_GROUP, 
    nvl(WAIVER_OFFSET,0) WAIVER_OFFSET, 
    nvl(MAX_AMOUNT,0) MAX_AMOUNT, 
    nvl(trim(CURRENCY_CD),'-') CURRENCY_CD, 
    nvl(trim(EXC_ACCT_TYPE_FLG),'-') EXC_ACCT_TYPE_FLG, 
    nvl(trim(SF_INCREASE_WVR_TX),'-') SF_INCREASE_WVR_TX, 
    nvl(trim(SF_WAIVE_TAX),'-') SF_WAIVE_TAX, 
    nvl(trim(SF_TX_WVR_ACCT_TYP),'-') SF_TX_WVR_ACCT_TYP, 
    nvl(trim(SF_TX_WVR_ITEM_TYP),'-') SF_TX_WVR_ITEM_TYP, 
    nvl(trim(SSF_STDNT_WVR_FLG),'-') SSF_STDNT_WVR_FLG, 
    nvl(trim(SSF_WAIVER_TYPE),'-') SSF_WAIVER_TYPE, 
    nvl(trim(SSF_PRORTE_DRP_FLG),'-') SSF_PRORTE_DRP_FLG
from SYSADM.PS_WAIVER_TBL@SASOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_WAIVER_TBL') ) S 
 on ( 
    T.SETID = S.SETID and 
    T.WAIVER_CODE = S.WAIVER_CODE and 
    T.EFFDT = S.EFFDT and 
    T.EFF_STATUS = S.EFF_STATUS and
    T.SRC_SYS_ID = 'CS90')    
when matched then update set
    T.ACCOUNT_TYPE_SF = S.ACCOUNT_TYPE_SF,
    T.ITEM_TYPE = S.ITEM_TYPE,
    T.SSF_CRITR_EQUTN_SW = S.SSF_CRITR_EQUTN_SW,
    T.CRITERIA = S.CRITERIA,
    T.ADJUST_UNTIL_DATE = S.ADJUST_UNTIL_DATE,
    T.AMT_PER_UNIT = S.AMT_PER_UNIT,
    T.FLAT_AMT = S.FLAT_AMT,
    T.WAIVE_PCT = S.WAIVE_PCT,
    T.ITEM_TYPE_GROUP = S.ITEM_TYPE_GROUP,
    T.WAIVER_OFFSET = S.WAIVER_OFFSET,
    T.MAX_AMOUNT = S.MAX_AMOUNT,
    T.CURRENCY_CD = S.CURRENCY_CD,
    T.EXC_ACCT_TYPE_FLG = S.EXC_ACCT_TYPE_FLG,
    T.SF_INCREASE_WVR_TX = S.SF_INCREASE_WVR_TX,
    T.SF_WAIVE_TAX = S.SF_WAIVE_TAX,
    T.SF_TX_WVR_ACCT_TYP = S.SF_TX_WVR_ACCT_TYP,
    T.SF_TX_WVR_ITEM_TYP = S.SF_TX_WVR_ITEM_TYP,
    T.SSF_STDNT_WVR_FLG = S.SSF_STDNT_WVR_FLG,
    T.SSF_WAIVER_TYPE = S.SSF_WAIVER_TYPE,
    T.SSF_PRORTE_DRP_FLG = S.SSF_PRORTE_DRP_FLG,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.ACCOUNT_TYPE_SF <> S.ACCOUNT_TYPE_SF or 
    T.ITEM_TYPE <> S.ITEM_TYPE or 
    T.SSF_CRITR_EQUTN_SW <> S.SSF_CRITR_EQUTN_SW or 
    T.CRITERIA <> S.CRITERIA or 
    nvl(trim(T.ADJUST_UNTIL_DATE),0) <> nvl(trim(S.ADJUST_UNTIL_DATE),0) or 
    T.AMT_PER_UNIT <> S.AMT_PER_UNIT or 
    T.FLAT_AMT <> S.FLAT_AMT or 
    T.WAIVE_PCT <> S.WAIVE_PCT or 
    T.ITEM_TYPE_GROUP <> S.ITEM_TYPE_GROUP or 
    T.WAIVER_OFFSET <> S.WAIVER_OFFSET or 
    T.MAX_AMOUNT <> S.MAX_AMOUNT or 
    T.CURRENCY_CD <> S.CURRENCY_CD or 
    T.EXC_ACCT_TYPE_FLG <> S.EXC_ACCT_TYPE_FLG or 
    T.SF_INCREASE_WVR_TX <> S.SF_INCREASE_WVR_TX or 
    T.SF_WAIVE_TAX <> S.SF_WAIVE_TAX or 
    T.SF_TX_WVR_ACCT_TYP <> S.SF_TX_WVR_ACCT_TYP or 
    T.SF_TX_WVR_ITEM_TYP <> S.SF_TX_WVR_ITEM_TYP or 
    T.SSF_STDNT_WVR_FLG <> S.SSF_STDNT_WVR_FLG or 
    T.SSF_WAIVER_TYPE <> S.SSF_WAIVER_TYPE or 
    T.SSF_PRORTE_DRP_FLG <> S.SSF_PRORTE_DRP_FLG or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.SETID,
    T.WAIVER_CODE,
    T.EFFDT,
    T.EFF_STATUS, 
    T.SRC_SYS_ID,
    T.ACCOUNT_TYPE_SF,
    T.ITEM_TYPE,
    T.SSF_CRITR_EQUTN_SW, 
    T.CRITERIA, 
    T.ADJUST_UNTIL_DATE,
    T.AMT_PER_UNIT, 
    T.FLAT_AMT, 
    T.WAIVE_PCT,
    T.ITEM_TYPE_GROUP,
    T.WAIVER_OFFSET,
    T.MAX_AMOUNT, 
    T.CURRENCY_CD,
    T.EXC_ACCT_TYPE_FLG,
    T.SF_INCREASE_WVR_TX, 
    T.SF_WAIVE_TAX, 
    T.SF_TX_WVR_ACCT_TYP, 
    T.SF_TX_WVR_ITEM_TYP, 
    T.SSF_STDNT_WVR_FLG,
    T.SSF_WAIVER_TYPE,
    T.SSF_PRORTE_DRP_FLG, 
    T.LOAD_ERROR, 
    T.DATA_ORIGIN, 
    T.CREATED_EW_DTTM, 
    T.LASTUPD_EW_DTTM, 
    T.BATCH_SID
    ) 
values (
    S.SETID,
    S.WAIVER_CODE,
    S.EFFDT,
    S.EFF_STATUS, 
    'CS90',
    S.ACCOUNT_TYPE_SF,
    S.ITEM_TYPE,
    S.SSF_CRITR_EQUTN_SW, 
    S.CRITERIA, 
    S.ADJUST_UNTIL_DATE,
    S.AMT_PER_UNIT, 
    S.FLAT_AMT, 
    S.WAIVE_PCT,
    S.ITEM_TYPE_GROUP,
    S.WAIVER_OFFSET,
    S.MAX_AMOUNT, 
    S.CURRENCY_CD,
    S.EXC_ACCT_TYPE_FLG,
    S.SF_INCREASE_WVR_TX, 
    S.SF_WAIVE_TAX, 
    S.SF_TX_WVR_ACCT_TYP, 
    S.SF_TX_WVR_ITEM_TYP, 
    S.SSF_STDNT_WVR_FLG,
    S.SSF_WAIVER_TYPE,
    S.SSF_PRORTE_DRP_FLG, 
    'N',
    'S',
    sysdate,
    sysdate,
    1234);


strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_WAIVER_TBL rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_WAIVER_TBL',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_WAIVER_TBL';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_WAIVER_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_WAIVER_TBL';
update CSSTG_OWNER.PS_WAIVER_TBL T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select nvl(trim(SETID),'-') SETID, 
        nvl(trim(WAIVER_CODE),'-') WAIVER_CODE, 
        to_date(to_char(case when EFFDT < '01-JAN-1800' then NULL else EFFDT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EFFDT, 
        nvl(trim(EFF_STATUS),'-') EFF_STATUS
   from CSSTG_OWNER.PS_WAIVER_TBL T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_WAIVER_TBL') = 'Y'
  minus
 select nvl(trim(SETID),'-') SETID, 
        nvl(trim(WAIVER_CODE),'-') WAIVER_CODE, 
        to_date(to_char(case when EFFDT < '01-JAN-1800' then NULL else EFFDT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EFFDT, 
        nvl(trim(EFF_STATUS),'-') EFF_STATUS
   from SYSADM.PS_WAIVER_TBL@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_WAIVER_TBL') = 'Y'
   ) S
 where T.SETID = S.SETID
   and T.WAIVER_CODE = S.WAIVER_CODE
   and T.EFFDT = S.EFFDT
   and T.EFF_STATUS = S.EFF_STATUS
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_WAIVER_TBL rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_WAIVER_TBL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_WAIVER_TBL'
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

END PS_WAIVER_TBL_P;
/
