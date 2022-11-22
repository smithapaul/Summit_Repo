DROP PROCEDURE CSMRT_OWNER.AM_PS_STDNT_AWRD_ACTV_P_TEST
/

--
-- AM_PS_STDNT_AWRD_ACTV_P_TEST  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."AM_PS_STDNT_AWRD_ACTV_P_TEST" IS

------------------------------------------------------------------------
--
-- Loads stage table PS_STDNT_AWRD_ACTV 
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'AM_PS_STDNT_AWRD_ACTV_P_TEST';
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
        intMaxSCN                       Integer;

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
   set TABLE_STATUS = 'Merging',
       START_DT = sysdate,
       END_DT = NULL
 where TABLE_NAME = 'PS_STDNT_AWRD_ACTV'
;

strSqlCommand := 'commit';
commit;

--strSqlCommand   := 'update NEW_MAX_SCN on AMSTG_OWNER.UM_STAGE_JOBS';
--update AMSTG_OWNER.UM_STAGE_JOBS
--   set TABLE_STATUS = 'Merging',
--       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_STDNT_AWRD_ACTV@AMSOURCE S)
-- where TABLE_NAME = 'PS_STDNT_AWRD_ACTV'
--;

--strSqlCommand := 'commit';
--commit;

strMessage01    := 'Merging data into AMSTG_OWNER.PS_STDNT_AWRD_ACTV';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into AMSTG_OWNER.PS_STDNT_AWRD_ACTV';
merge /*+ use_hash(S,T) parallel(8) enable_parallel_dml */ into AMSTG_OWNER.PS_STDNT_AWRD_ACTV T
using (select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(INSTITUTION),'-') INSTITUTION, 
    nvl(trim(AID_YEAR),'-') AID_YEAR, 
    nvl(trim(ITEM_TYPE),'-') ITEM_TYPE, 
    nvl(trim(ACAD_CAREER),'-') ACAD_CAREER, 
--    TO_CHAR(ACTION_DTTM, 'YYYY-MM-DD HH24:MI:SS.FF') ACTION_DTTM, 
    nvl(ACTION_DTTM, to_timestamp(to_date('01-JAN-1900'))) ACTION_DTTM,  -- APR 2018     
    nvl(trim(DISBURSEMENT_PLAN),'-') DISBURSEMENT_PLAN, 
    nvl(trim(SPLIT_CODE),'-') SPLIT_CODE, 
    nvl(trim(DISBURSEMENT_ID),'-') DISBURSEMENT_ID, 
    nvl(trim(OPRID),'-') OPRID, 
    nvl(trim(AWARD_DISB_ACTION),'-') AWARD_DISB_ACTION, 
    nvl(OFFER_AMOUNT,0) OFFER_AMOUNT, 
    nvl(ACCEPT_AMOUNT,0) ACCEPT_AMOUNT, 
    nvl(AUTHORIZED_AMOUNT,0) AUTHORIZED_AMOUNT, 
    nvl(DISB_AMOUNT,0) DISB_AMOUNT, 
    nvl(trim(CURRENCY_CD),'-') CURRENCY_CD, 
    nvl(trim(BUSINESS_UNIT),'-') BUSINESS_UNIT, 
    nvl(trim(ADJUST_REASON_CD),'-') ADJUST_REASON_CD, 
    nvl(ADJUST_AMOUNT,0) ADJUST_AMOUNT, 
    nvl(trim(LOAN_ADJUST_CD),'-') LOAN_ADJUST_CD, 
    nvl(DISB_TO_DATE,0) DISB_TO_DATE, 
    nvl(AUTH_TO_DATE,0) AUTH_TO_DATE, 
    nvl(trim(PKG_APP_DATA_USED),'-') PKG_APP_DATA_USED
--  from SYSADM.PS_STDNT_AWRD_ACTV@AMSOURCE S 
-- where ORA_ROWSCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_STDNT_AWRD_ACTV')
--   and EMPLID between '00000000' and '99999999'
--   and length(EMPLID) = 8 
  from AMSTG_OWNER.PS_STDNT_AWRD_ACTV_S1 S ) S
 on ( 
    T.EMPLID = S.EMPLID and 
    T.INSTITUTION = S.INSTITUTION and 
    T.AID_YEAR = S.AID_YEAR and 
    T.ITEM_TYPE = S.ITEM_TYPE and 
    T.ACAD_CAREER = S.ACAD_CAREER and 
    T.ACTION_DTTM = S.ACTION_DTTM and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.DISBURSEMENT_PLAN = S.DISBURSEMENT_PLAN,
    T.SPLIT_CODE = S.SPLIT_CODE,
    T.DISBURSEMENT_ID = S.DISBURSEMENT_ID,
    T.OPRID = S.OPRID,
    T.AWARD_DISB_ACTION = S.AWARD_DISB_ACTION,
    T.OFFER_AMOUNT = S.OFFER_AMOUNT,
    T.ACCEPT_AMOUNT = S.ACCEPT_AMOUNT,
    T.AUTHORIZED_AMOUNT = S.AUTHORIZED_AMOUNT,
    T.DISB_AMOUNT = S.DISB_AMOUNT,
    T.CURRENCY_CD = S.CURRENCY_CD,
    T.BUSINESS_UNIT = S.BUSINESS_UNIT,
    T.ADJUST_REASON_CD = S.ADJUST_REASON_CD,
    T.ADJUST_AMOUNT = S.ADJUST_AMOUNT,
    T.LOAN_ADJUST_CD = S.LOAN_ADJUST_CD,
    T.DISB_TO_DATE = S.DISB_TO_DATE,
    T.AUTH_TO_DATE = S.AUTH_TO_DATE,
    T.PKG_APP_DATA_USED = S.PKG_APP_DATA_USED,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.DISBURSEMENT_PLAN <> S.DISBURSEMENT_PLAN or 
    T.SPLIT_CODE <> S.SPLIT_CODE or 
    T.DISBURSEMENT_ID <> S.DISBURSEMENT_ID or 
    T.OPRID <> S.OPRID or 
    T.AWARD_DISB_ACTION <> S.AWARD_DISB_ACTION or 
    T.OFFER_AMOUNT <> S.OFFER_AMOUNT or 
    T.ACCEPT_AMOUNT <> S.ACCEPT_AMOUNT or 
    T.AUTHORIZED_AMOUNT <> S.AUTHORIZED_AMOUNT or 
    T.DISB_AMOUNT <> S.DISB_AMOUNT or 
    T.CURRENCY_CD <> S.CURRENCY_CD or 
    T.BUSINESS_UNIT <> S.BUSINESS_UNIT or 
    T.ADJUST_REASON_CD <> S.ADJUST_REASON_CD or 
    T.ADJUST_AMOUNT <> S.ADJUST_AMOUNT or 
    T.LOAN_ADJUST_CD <> S.LOAN_ADJUST_CD or 
    T.DISB_TO_DATE <> S.DISB_TO_DATE or 
    T.AUTH_TO_DATE <> S.AUTH_TO_DATE or 
    T.PKG_APP_DATA_USED <> S.PKG_APP_DATA_USED or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.EMPLID, 
    T.INSTITUTION,
    T.AID_YEAR, 
    T.ITEM_TYPE,
    T.ACAD_CAREER,
    T.ACTION_DTTM,
    T.SRC_SYS_ID, 
    T.DISBURSEMENT_PLAN,
    T.SPLIT_CODE, 
    T.DISBURSEMENT_ID,
    T.OPRID,
    T.AWARD_DISB_ACTION,
    T.OFFER_AMOUNT, 
    T.ACCEPT_AMOUNT,
    T.AUTHORIZED_AMOUNT,
    T.DISB_AMOUNT,
    T.CURRENCY_CD,
    T.BUSINESS_UNIT,
    T.ADJUST_REASON_CD, 
    T.ADJUST_AMOUNT,
    T.LOAN_ADJUST_CD, 
    T.DISB_TO_DATE, 
    T.AUTH_TO_DATE, 
    T.PKG_APP_DATA_USED,
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
) 
values (
    S.EMPLID, 
    S.INSTITUTION,
    S.AID_YEAR, 
    S.ITEM_TYPE,
    S.ACAD_CAREER,
    S.ACTION_DTTM,
    'CS90', 
    S.DISBURSEMENT_PLAN,
    S.SPLIT_CODE, 
    S.DISBURSEMENT_ID,
    S.OPRID,
    S.AWARD_DISB_ACTION,
    S.OFFER_AMOUNT, 
    S.ACCEPT_AMOUNT,
    S.AUTHORIZED_AMOUNT,
    S.DISB_AMOUNT,
    S.CURRENCY_CD,
    S.BUSINESS_UNIT,
    S.ADJUST_REASON_CD, 
    S.ADJUST_AMOUNT,
    S.LOAN_ADJUST_CD, 
    S.DISB_TO_DATE, 
    S.AUTH_TO_DATE, 
    S.PKG_APP_DATA_USED,
    'N',
    'S',
    sysdate,
    sysdate,
    1234);

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_STDNT_AWRD_ACTV rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_STDNT_AWRD_ACTV',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       OLD_MAX_SCN = NEW_MAX_SCN,
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_STDNT_AWRD_ACTV'
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

END AM_PS_STDNT_AWRD_ACTV_P_TEST;
/
