DROP PROCEDURE CSMRT_OWNER.AM_PS_CLASS_SBFEE_TBL_P
/

--
-- AM_PS_CLASS_SBFEE_TBL_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER.AM_PS_CLASS_SBFEE_TBL_P IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_CLASS_SBFEE_TBL from PeopleSoft table PS_CLASS_SBFEE_TBL.
--
-- V01  SMT-xxxx 04/04/2017,    Jim Doucette
--                              Converted from PS_CLASS_SBFEE_TBL.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'AM_PS_CLASS_SBFEE_TBL';
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
       START_DT = sysdate,
       END_DT = NULL
 where TABLE_NAME = 'PS_CLASS_SBFEE_TBL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_CLASS_SBFEE_TBL@AMSOURCE S)
 where TABLE_NAME = 'PS_CLASS_SBFEE_TBL'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into AMSTG_OWNER.PS_CLASS_SBFEE_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into AMSTG_OWNER.PS_CLASS_SBFEE_TBL';
merge /*+ use_hash(S,T) */ into AMSTG_OWNER.PS_CLASS_SBFEE_TBL T
using (select /*+ full(S) */
    nvl(trim(SETID),'-') SETID, 
    nvl(trim(CRSE_ID),'-') CRSE_ID, 
    nvl(CRSE_OFFER_NBR,0) CRSE_OFFER_NBR, 
    nvl(trim(STRM),'-') STRM, 
    nvl(trim(SESSION_CODE),'-')  SESSION_CODE,
    nvl(trim(CLASS_SECTION),'-')  CLASS_SECTION,
    nvl(trim(SSR_COMPONENT),'-')  SSR_COMPONENT,
    nvl(trim(ACCOUNT_TYPE_SF),'-')  ACCOUNT_TYPE_SF,
    nvl(trim(ITEM_TYPE),'-')  ITEM_TYPE,
    SSF_CRITR_EQUTN_SW,
    FEE_TRIGGER,
    EQUATION_NAME,
    CRSE_RATE_ID,
    AMT_PER_UNIT,
    FLAT_AMT,
    AMT_PER_UNIT_AUDIT,
    FLAT_AMT_AUDIT,
    MIN_AMOUNT,
    MAX_AMOUNT,
    CURRENCY_CD,
    ADJ_TERM_CD,
    DUE_DATE_CODE,
    WAIVER_GROUP,
    DYNAMIC_ORG,
    SSF_EXCLUDE_HECS
  from SYSADM.PS_CLASS_SBFEE_TBL@AMSOURCE S 
 where ORA_ROWSCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CLASS_SBFEE_TBL') 
    )S
 on ( 
    T.SETID = S.SETID and 
    T.CRSE_ID = S.CRSE_ID and 
    T.CRSE_OFFER_NBR = S.CRSE_OFFER_NBR and 
    T.STRM = S.STRM and 
    T.SESSION_CODE = S.SESSION_CODE and 
    T.CLASS_SECTION = S.CLASS_SECTION and 
    T.SSR_COMPONENT = S.SSR_COMPONENT and 
    T.ACCOUNT_TYPE_SF = S.ACCOUNT_TYPE_SF and 
    T.ITEM_TYPE = S.ITEM_TYPE and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.SSF_CRITR_EQUTN_SW = S.SSF_CRITR_EQUTN_SW,
    T.FEE_TRIGGER = S.FEE_TRIGGER,
    T.EQUATION_NAME = S.EQUATION_NAME,
    T.CRSE_RATE_ID = S.CRSE_RATE_ID,
    T.AMT_PER_UNIT = S.AMT_PER_UNIT,
    T.FLAT_AMT = S.FLAT_AMT,
    T.AMT_PER_UNIT_AUDIT = S.AMT_PER_UNIT_AUDIT,
    T.FLAT_AMT_AUDIT = S.FLAT_AMT_AUDIT,
    T.MIN_AMOUNT = S.MIN_AMOUNT,
    T.MAX_AMOUNT = S.MAX_AMOUNT,
    T.CURRENCY_CD = S.CURRENCY_CD,
    T.ADJ_TERM_CD = S.ADJ_TERM_CD,
    T.DUE_DATE_CODE = S.DUE_DATE_CODE,
    T.WAIVER_GROUP = S.WAIVER_GROUP,
    T.DYNAMIC_ORG = S.DYNAMIC_ORG,
    T.SSF_EXCLUDE_HECS = S.SSF_EXCLUDE_HECS,    
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    nvl(trim(T.SSF_CRITR_EQUTN_SW),0) <> nvl(trim(S.SSF_CRITR_EQUTN_SW),0) or
    nvl(trim(T.FEE_TRIGGER),0) <> nvl(trim(S.FEE_TRIGGER),0) or
    nvl(trim(T.EQUATION_NAME),0) <> nvl(trim(S.EQUATION_NAME),0) or
    nvl(trim(T.CRSE_RATE_ID),0) <> nvl(trim(S.CRSE_RATE_ID),0) or
    nvl(trim(T.AMT_PER_UNIT),0) <> nvl(trim(S.AMT_PER_UNIT),0) or
    nvl(trim(T.FLAT_AMT),0) <> nvl(trim(S.FLAT_AMT),0) or
    nvl(trim(T.AMT_PER_UNIT_AUDIT),0) <> nvl(trim(S.AMT_PER_UNIT_AUDIT),0) or
    nvl(trim(T.FLAT_AMT_AUDIT),0) <> nvl(trim(S.FLAT_AMT_AUDIT),0) or
    nvl(trim(T.MIN_AMOUNT),0) <> nvl(trim(S.MIN_AMOUNT),0) or
    nvl(trim(T.MAX_AMOUNT),0) <> nvl(trim(S.MAX_AMOUNT),0) or
    nvl(trim(T.CURRENCY_CD),0) <> nvl(trim(S.CURRENCY_CD),0) or
    nvl(trim(T.ADJ_TERM_CD),0) <> nvl(trim(S.ADJ_TERM_CD),0) or
    nvl(trim(T.DUE_DATE_CODE),0) <> nvl(trim(S.DUE_DATE_CODE),0) or
    nvl(trim(T.WAIVER_GROUP),0) <> nvl(trim(S.WAIVER_GROUP),0) or
    nvl(trim(T.DYNAMIC_ORG),0) <> nvl(trim(S.DYNAMIC_ORG),0) or
    nvl(trim(T.SSF_EXCLUDE_HECS),0) <> nvl(trim(S.SSF_EXCLUDE_HECS),0) or
    T.DATA_ORIGIN = 'D'
when not matched then 
insert (
    T.SETID, 
    T.CRSE_ID, 
    T.CRSE_OFFER_NBR, 
    T.STRM, 
    T.SESSION_CODE, 
    T.CLASS_SECTION, 
    T.SSR_COMPONENT, 
    T.ACCOUNT_TYPE_SF, 
    T.ITEM_TYPE, 
    T.SRC_SYS_ID, 
    T.SSF_CRITR_EQUTN_SW, 
    T.FEE_TRIGGER, 
    T.EQUATION_NAME, 
    T.CRSE_RATE_ID, 
    T.AMT_PER_UNIT, 
    T.FLAT_AMT, 
    T.AMT_PER_UNIT_AUDIT, 
    T.FLAT_AMT_AUDIT, 
    T.MIN_AMOUNT, 
    T.MAX_AMOUNT, 
    T.CURRENCY_CD, 
    T.ADJ_TERM_CD, 
    T.DUE_DATE_CODE, 
    T.WAIVER_GROUP, 
    T.DYNAMIC_ORG, 
    T.SSF_EXCLUDE_HECS, 
    T.LOAD_ERROR, 
    T.DATA_ORIGIN, 
    T.CREATED_EW_DTTM, 
    T.LASTUPD_EW_DTTM, 
    T.BATCH_SID
    ) 
values (
    S.SETID, 
    S.CRSE_ID, 
    S.CRSE_OFFER_NBR, 
    S.STRM, 
    S.SESSION_CODE, 
    S.CLASS_SECTION, 
    S.SSR_COMPONENT, 
    S.ACCOUNT_TYPE_SF, 
    S.ITEM_TYPE, 
    'CS90', 
    S.SSF_CRITR_EQUTN_SW, 
    S.FEE_TRIGGER, 
    S.EQUATION_NAME, 
    S.CRSE_RATE_ID, 
    S.AMT_PER_UNIT, 
    S.FLAT_AMT, 
    S.AMT_PER_UNIT_AUDIT, 
    S.FLAT_AMT_AUDIT, 
    S.MIN_AMOUNT, 
    S.MAX_AMOUNT, 
    S.CURRENCY_CD, 
    S.ADJ_TERM_CD, 
    S.DUE_DATE_CODE, 
    S.WAIVER_GROUP, 
    S.DYNAMIC_ORG, 
    S.SSF_EXCLUDE_HECS, 
    'N',
    'S',
    sysdate,
    sysdate,
    1234);


strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_CLASS_SBFEE_TBL rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_CLASS_SBFEE_TBL',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_CLASS_SBFEE_TBL';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_CLASS_SBFEE_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on AMSTG_OWNER.PS_CLASS_SBFEE_TBL';
update AMSTG_OWNER.PS_CLASS_SBFEE_TBL T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select nvl(trim(SETID),'-') SETID, nvl(trim(CRSE_ID),'-') CRSE_ID, nvl(CRSE_OFFER_NBR,0) CRSE_OFFER_NBR, 
        nvl(trim(STRM),'-') STRM, nvl(trim(SESSION_CODE),'-') SESSION_CODE, nvl(trim(CLASS_SECTION),'-') CLASS_SECTION, 
        nvl(trim(SSR_COMPONENT),'-') SSR_COMPONENT, nvl(trim(ACCOUNT_TYPE_SF),'-') ACCOUNT_TYPE_SF, nvl(trim(ITEM_TYPE),'-') ITEM_TYPE
   from AMSTG_OWNER.PS_CLASS_SBFEE_TBL T2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CLASS_SBFEE_TBL') = 'Y'
  minus
 select nvl(trim(SETID),'-') SETID, nvl(trim(CRSE_ID),'-') CRSE_ID, nvl(CRSE_OFFER_NBR,0) CRSE_OFFER_NBR, 
        nvl(trim(STRM),'-') STRM, nvl(trim(SESSION_CODE),'-') SESSION_CODE, nvl(trim(CLASS_SECTION),'-') CLASS_SECTION, 
        nvl(trim(SSR_COMPONENT),'-') SSR_COMPONENT, nvl(trim(ACCOUNT_TYPE_SF),'-') ACCOUNT_TYPE_SF, nvl(trim(ITEM_TYPE),'-') ITEM_TYPE
   from SYSADM.PS_CLASS_SBFEE_TBL@AMSOURCE S2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CLASS_SBFEE_TBL') = 'Y'
   ) S
 where T.SETID = S.SETID
   and T.CRSE_ID = S.CRSE_ID
   and T.CRSE_OFFER_NBR = S.CRSE_OFFER_NBR
   and T.STRM = S.STRM
   and T.SESSION_CODE = S.SESSION_CODE
   and T.CLASS_SECTION = S.CLASS_SECTION
   and T.SSR_COMPONENT = S.SSR_COMPONENT
   and T.ACCOUNT_TYPE_SF = S.ACCOUNT_TYPE_SF
   and T.ITEM_TYPE = S.ITEM_TYPE
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_CLASS_SBFEE_TBL rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_CLASS_SBFEE_TBL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_CLASS_SBFEE_TBL'
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

END AM_PS_CLASS_SBFEE_TBL_P;
/
