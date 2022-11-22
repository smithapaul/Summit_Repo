DROP PROCEDURE CSMRT_OWNER.AM_PS_ITEM_TYPE_TBL_P
/

--
-- AM_PS_ITEM_TYPE_TBL_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER.AM_PS_ITEM_TYPE_TBL_P IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_ITEM_TYPE_TBL from PeopleSoft table PS_ITEM_TYPE_TBL.
--
-- V01  SMT-xxxx 03/29/2017,    George Adams
--                              Converted from PS_ITEM_TYPE_TBL.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'AM_PS_ITEM_TYPE_TBL';
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
 where TABLE_NAME = 'PS_ITEM_TYPE_TBL'
;

strSqlCommand := 'commit';
commit;

strSqlCommand   := 'update NEW_MAX_SCN on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_ITEM_TYPE_TBL@AMSOURCE S)
 where TABLE_NAME = 'PS_ITEM_TYPE_TBL'
;

strSqlCommand := 'commit';
commit;

strMessage01    := 'Merging data into AMSTG_OWNER.PS_ITEM_TYPE_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into AMSTG_OWNER.PS_ITEM_TYPE_TBL';
merge /*+ use_hash(S,T) */ into AMSTG_OWNER.PS_ITEM_TYPE_TBL T
using (select /*+ full(S) */
    nvl(trim(SETID),'-') SETID, 
    nvl(trim(ITEM_TYPE),'-') ITEM_TYPE, 
    to_date(to_char(case when EFFDT < '01-JAN-1800' then NULL else EFFDT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EFFDT, 
    nvl(trim(EFF_STATUS),'-') EFF_STATUS, 
    nvl(trim(DESCR),'-') DESCR, 
    nvl(trim(DESCRSHORT),'-') DESCRSHORT, 
    nvl(trim(CURRENCY_CD),'-') CURRENCY_CD, 
    nvl(MINIMUM_AMT,0) MINIMUM_AMT, 
    nvl(MAXIMUM_AMT,0) MAXIMUM_AMT, 
    nvl(trim(ITEM_TYPE_CD),'-') ITEM_TYPE_CD, 
    nvl(trim(SECURITY_GROUP),'-') SECURITY_GROUP, 
    nvl(trim(ADJUSTMENT_CAL_SF),'-') ADJUSTMENT_CAL_SF, 
    nvl(trim(PAYMENT_TERMS),'-') PAYMENT_TERMS, 
    nvl(trim(PAYMENT_PRIORITY),'-') PAYMENT_PRIORITY, 
    nvl(trim(CHARGE_PRIORITY),'-') CHARGE_PRIORITY, 
    nvl(ENCUMBRANCE_DAYS,0) ENCUMBRANCE_DAYS, 
    nvl(ENCUMBRANCE_PCT,0) ENCUMBRANCE_PCT, 
    nvl(trim(SSF_STATE_FLAG),'-') SSF_STATE_FLAG, 
    nvl(trim(TENDER_SPEC),'-') TENDER_SPEC, 
    nvl(trim(TENDER_CATEGORY),'-') TENDER_CATEGORY, 
    nvl(trim(ALL_OR_NONE),'-') ALL_OR_NONE, 
    nvl(trim(ENROL_FLAG),'-') ENROL_FLAG, 
    nvl(trim(KEYWORD1),'-') KEYWORD1, 
    nvl(trim(KEYWORD2),'-') KEYWORD2, 
    nvl(trim(KEYWORD3),'-') KEYWORD3, 
    nvl(DAYS_SINCE_EFFDT,0) DAYS_SINCE_EFFDT, 
    nvl(DAYS_TO_EFFDT,0) DAYS_TO_EFFDT, 
    nvl(DUEDAYS_PAST_EFFDT,0) DUEDAYS_PAST_EFFDT, 
    nvl(DUEDAYS_PRIOR_EFFD,0) DUEDAYS_PRIOR_EFFD, 
    nvl(trim(TERM_ENROLL_REQ),'-') TERM_ENROLL_REQ, 
    nvl(trim(REFUNDABLE_IND),'-') REFUNDABLE_IND, 
    nvl(trim(ERNCD),'-') ERNCD, 
    nvl(trim(TAXABLE_Y_N),'-') TAXABLE_Y_N, 
    nvl(trim(TUITION_DEPOSIT),'-') TUITION_DEPOSIT, 
    nvl(trim(GL_INTERFACE_REQ),'-') GL_INTERFACE_REQ, 
    nvl(DEFAULT_AMT,0) DEFAULT_AMT, 
    nvl(trim(ERNCD_NOTAX),'-') ERNCD_NOTAX, 
    nvl(trim(RECVABLE_FROM_CHRG),'-') RECVABLE_FROM_CHRG, 
    nvl(PRIORITY,0) PRIORITY, 
    nvl(trim(PRIORITY_PMT_FLG),'-') PRIORITY_PMT_FLG, 
    nvl(trim(NRA_CREDIT_TAX_FLG),'-') NRA_CREDIT_TAX_FLG, 
    nvl(trim(NRA_DEBIT_TAX_FLG),'-') NRA_DEBIT_TAX_FLG, 
    nvl(trim(MATCH_WRITEOFF),'-') MATCH_WRITEOFF, 
    nvl(trim(LOCAL_TAX_OFFSET),'-') LOCAL_TAX_OFFSET, 
    nvl(trim(LOCAL_TAX_PMT),'-') LOCAL_TAX_PMT, 
    nvl(trim(STATE_TAX_OFFSET),'-') STATE_TAX_OFFSET, 
    nvl(trim(STATE_TAX_PMT),'-') STATE_TAX_PMT, 
    nvl(trim(GL_CRSE_CLASS_SPC),'-') GL_CRSE_CLASS_SPC, 
    nvl(trim(TAX_CD),'-') TAX_CD, 
    nvl(trim(T4_INCOME),'-') T4_INCOME, 
    nvl(trim(WAGE_LOSS_PLAN),'-') WAGE_LOSS_PLAN, 
    nvl(trim(T2202A_FLG),'-') T2202A_FLG, 
    nvl(T2202A_PCT,0) T2202A_PCT, 
    nvl(trim(T2202A_OFFSET_FLG),'-') T2202A_OFFSET_FLG, 
    nvl(trim(PAY_PRIORITY_ID),'-') PAY_PRIORITY_ID, 
    nvl(trim(ACCTG_DT_CNTL),'-') ACCTG_DT_CNTL, 
    nvl(trim(SF_1098_FLG),'-') SF_1098_FLG, 
    nvl(trim(SSF_DEST_TUT_TYPE),'-') SSF_DEST_TUT_TYPE
from SYSADM.PS_ITEM_TYPE_TBL@AMSOURCE S 
where ORA_ROWSCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ITEM_TYPE_TBL') ) S
 on ( 
    T.SETID = S.SETID and 
    T.ITEM_TYPE = S.ITEM_TYPE and 
    T.EFFDT = S.EFFDT and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.EFF_STATUS = S.EFF_STATUS,
    T.DESCR = S.DESCR,
    T.DESCRSHORT = S.DESCRSHORT,
    T.CURRENCY_CD = S.CURRENCY_CD,
    T.MINIMUM_AMT = S.MINIMUM_AMT,
    T.MAXIMUM_AMT = S.MAXIMUM_AMT,
    T.ITEM_TYPE_CD = S.ITEM_TYPE_CD,
    T.SECURITY_GROUP = S.SECURITY_GROUP,
    T.ADJUSTMENT_CAL_SF = S.ADJUSTMENT_CAL_SF,
    T.PAYMENT_TERMS = S.PAYMENT_TERMS,
    T.PAYMENT_PRIORITY = S.PAYMENT_PRIORITY,
    T.CHARGE_PRIORITY = S.CHARGE_PRIORITY,
    T.ENCUMBRANCE_DAYS = S.ENCUMBRANCE_DAYS,
    T.ENCUMBRANCE_PCT = S.ENCUMBRANCE_PCT,
    T.SSF_STATE_FLAG = S.SSF_STATE_FLAG,
    T.TENDER_SPEC = S.TENDER_SPEC,
    T.TENDER_CATEGORY = S.TENDER_CATEGORY,
    T.ALL_OR_NONE = S.ALL_OR_NONE,
    T.ENROL_FLAG = S.ENROL_FLAG,
    T.KEYWORD1 = S.KEYWORD1,
    T.KEYWORD2 = S.KEYWORD2,
    T.KEYWORD3 = S.KEYWORD3,
    T.DAYS_SINCE_EFFDT = S.DAYS_SINCE_EFFDT,
    T.DAYS_TO_EFFDT = S.DAYS_TO_EFFDT,
    T.DUEDAYS_PAST_EFFDT = S.DUEDAYS_PAST_EFFDT,
    T.DUEDAYS_PRIOR_EFFD = S.DUEDAYS_PRIOR_EFFD,
    T.TERM_ENROLL_REQ = S.TERM_ENROLL_REQ,
    T.REFUNDABLE_IND = S.REFUNDABLE_IND,
    T.ERNCD = S.ERNCD,
    T.TAXABLE_Y_N = S.TAXABLE_Y_N,
    T.TUITION_DEPOSIT = S.TUITION_DEPOSIT,
    T.GL_INTERFACE_REQ = S.GL_INTERFACE_REQ,
    T.DEFAULT_AMT = S.DEFAULT_AMT,
    T.ERNCD_NOTAX = S.ERNCD_NOTAX,
    T.RECVABLE_FROM_CHRG = S.RECVABLE_FROM_CHRG,
    T.PRIORITY = S.PRIORITY,
    T.PRIORITY_PMT_FLG = S.PRIORITY_PMT_FLG,
    T.NRA_CREDIT_TAX_FLG = S.NRA_CREDIT_TAX_FLG,
    T.NRA_DEBIT_TAX_FLG = S.NRA_DEBIT_TAX_FLG,
    T.MATCH_WRITEOFF = S.MATCH_WRITEOFF,
    T.LOCAL_TAX_OFFSET = S.LOCAL_TAX_OFFSET,
    T.LOCAL_TAX_PMT = S.LOCAL_TAX_PMT,
    T.STATE_TAX_OFFSET = S.STATE_TAX_OFFSET,
    T.STATE_TAX_PMT = S.STATE_TAX_PMT,
    T.GL_CRSE_CLASS_SPC = S.GL_CRSE_CLASS_SPC,
    T.TAX_CD = S.TAX_CD,
    T.T4_INCOME = S.T4_INCOME,
    T.WAGE_LOSS_PLAN = S.WAGE_LOSS_PLAN,
    T.T2202A_FLG = S.T2202A_FLG,
    T.T2202A_PCT = S.T2202A_PCT,
    T.T2202A_OFFSET_FLG = S.T2202A_OFFSET_FLG,
    T.PAY_PRIORITY_ID = S.PAY_PRIORITY_ID,
    T.ACCTG_DT_CNTL = S.ACCTG_DT_CNTL,
    T.SF_1098_FLG = S.SF_1098_FLG,
    T.SSF_DEST_TUT_TYPE = S.SSF_DEST_TUT_TYPE,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.EFF_STATUS <> S.EFF_STATUS or 
    T.DESCR <> S.DESCR or 
    T.DESCRSHORT <> S.DESCRSHORT or 
    T.CURRENCY_CD <> S.CURRENCY_CD or 
    T.MINIMUM_AMT <> S.MINIMUM_AMT or 
    T.MAXIMUM_AMT <> S.MAXIMUM_AMT or 
    T.ITEM_TYPE_CD <> S.ITEM_TYPE_CD or 
    T.SECURITY_GROUP <> S.SECURITY_GROUP or 
    T.ADJUSTMENT_CAL_SF <> S.ADJUSTMENT_CAL_SF or 
    T.PAYMENT_TERMS <> S.PAYMENT_TERMS or 
    T.PAYMENT_PRIORITY <> S.PAYMENT_PRIORITY or 
    T.CHARGE_PRIORITY <> S.CHARGE_PRIORITY or 
    T.ENCUMBRANCE_DAYS <> S.ENCUMBRANCE_DAYS or 
    T.ENCUMBRANCE_PCT <> S.ENCUMBRANCE_PCT or 
    T.SSF_STATE_FLAG <> S.SSF_STATE_FLAG or 
    T.TENDER_SPEC <> S.TENDER_SPEC or 
    T.TENDER_CATEGORY <> S.TENDER_CATEGORY or 
    T.ALL_OR_NONE <> S.ALL_OR_NONE or 
    T.ENROL_FLAG <> S.ENROL_FLAG or 
    T.KEYWORD1 <> S.KEYWORD1 or 
    T.KEYWORD2 <> S.KEYWORD2 or 
    T.KEYWORD3 <> S.KEYWORD3 or 
    T.DAYS_SINCE_EFFDT <> S.DAYS_SINCE_EFFDT or 
    T.DAYS_TO_EFFDT <> S.DAYS_TO_EFFDT or 
    T.DUEDAYS_PAST_EFFDT <> S.DUEDAYS_PAST_EFFDT or 
    T.DUEDAYS_PRIOR_EFFD <> S.DUEDAYS_PRIOR_EFFD or 
    T.TERM_ENROLL_REQ <> S.TERM_ENROLL_REQ or 
    T.REFUNDABLE_IND <> S.REFUNDABLE_IND or 
    T.ERNCD <> S.ERNCD or 
    T.TAXABLE_Y_N <> S.TAXABLE_Y_N or 
    T.TUITION_DEPOSIT <> S.TUITION_DEPOSIT or 
    T.GL_INTERFACE_REQ <> S.GL_INTERFACE_REQ or 
    T.DEFAULT_AMT <> S.DEFAULT_AMT or 
    T.ERNCD_NOTAX <> S.ERNCD_NOTAX or 
    T.RECVABLE_FROM_CHRG <> S.RECVABLE_FROM_CHRG or 
    T.PRIORITY <> S.PRIORITY or 
    T.PRIORITY_PMT_FLG <> S.PRIORITY_PMT_FLG or 
    T.NRA_CREDIT_TAX_FLG <> S.NRA_CREDIT_TAX_FLG or 
    T.NRA_DEBIT_TAX_FLG <> S.NRA_DEBIT_TAX_FLG or 
    T.MATCH_WRITEOFF <> S.MATCH_WRITEOFF or 
    T.LOCAL_TAX_OFFSET <> S.LOCAL_TAX_OFFSET or 
    T.LOCAL_TAX_PMT <> S.LOCAL_TAX_PMT or 
    T.STATE_TAX_OFFSET <> S.STATE_TAX_OFFSET or 
    T.STATE_TAX_PMT <> S.STATE_TAX_PMT or 
    T.GL_CRSE_CLASS_SPC <> S.GL_CRSE_CLASS_SPC or 
    T.TAX_CD <> S.TAX_CD or 
    T.T4_INCOME <> S.T4_INCOME or 
    T.WAGE_LOSS_PLAN <> S.WAGE_LOSS_PLAN or 
    T.T2202A_FLG <> S.T2202A_FLG or 
    T.T2202A_PCT <> S.T2202A_PCT or 
    T.T2202A_OFFSET_FLG <> S.T2202A_OFFSET_FLG or 
    T.PAY_PRIORITY_ID <> S.PAY_PRIORITY_ID or 
    T.ACCTG_DT_CNTL <> S.ACCTG_DT_CNTL or 
    T.SF_1098_FLG <> S.SF_1098_FLG or 
    T.SSF_DEST_TUT_TYPE <> S.SSF_DEST_TUT_TYPE or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.SETID,
    T.ITEM_TYPE,
    T.EFFDT,
    T.SRC_SYS_ID, 
    T.EFF_STATUS, 
    T.DESCR,
    T.DESCRSHORT, 
    T.CURRENCY_CD,
    T.MINIMUM_AMT,
    T.MAXIMUM_AMT,
    T.ITEM_TYPE_CD, 
    T.SECURITY_GROUP, 
    T.ADJUSTMENT_CAL_SF,
    T.PAYMENT_TERMS,
    T.PAYMENT_PRIORITY, 
    T.CHARGE_PRIORITY,
    T.ENCUMBRANCE_DAYS, 
    T.ENCUMBRANCE_PCT,
    T.SSF_STATE_FLAG, 
    T.TENDER_SPEC,
    T.TENDER_CATEGORY,
    T.ALL_OR_NONE,
    T.ENROL_FLAG, 
    T.KEYWORD1, 
    T.KEYWORD2, 
    T.KEYWORD3, 
    T.DAYS_SINCE_EFFDT, 
    T.DAYS_TO_EFFDT,
    T.DUEDAYS_PAST_EFFDT, 
    T.DUEDAYS_PRIOR_EFFD, 
    T.TERM_ENROLL_REQ,
    T.REFUNDABLE_IND, 
    T.ERNCD,
    T.TAXABLE_Y_N,
    T.TUITION_DEPOSIT,
    T.GL_INTERFACE_REQ, 
    T.DEFAULT_AMT,
    T.ERNCD_NOTAX,
    T.RECVABLE_FROM_CHRG, 
    T.PRIORITY, 
    T.PRIORITY_PMT_FLG, 
    T.NRA_CREDIT_TAX_FLG, 
    T.NRA_DEBIT_TAX_FLG,
    T.MATCH_WRITEOFF, 
    T.LOCAL_TAX_OFFSET, 
    T.LOCAL_TAX_PMT,
    T.STATE_TAX_OFFSET, 
    T.STATE_TAX_PMT,
    T.GL_CRSE_CLASS_SPC,
    T.TAX_CD, 
    T.T4_INCOME,
    T.WAGE_LOSS_PLAN, 
    T.T2202A_FLG, 
    T.T2202A_PCT, 
    T.T2202A_OFFSET_FLG,
    T.PAY_PRIORITY_ID,
    T.ACCTG_DT_CNTL,
    T.SF_1098_FLG,
    T.SSF_DEST_TUT_TYPE,
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
) 
values (
    S.SETID,
    S.ITEM_TYPE,
    S.EFFDT,
    'CS90', 
    S.EFF_STATUS, 
    S.DESCR,
    S.DESCRSHORT, 
    S.CURRENCY_CD,
    S.MINIMUM_AMT,
    S.MAXIMUM_AMT,
    S.ITEM_TYPE_CD, 
    S.SECURITY_GROUP, 
    S.ADJUSTMENT_CAL_SF,
    S.PAYMENT_TERMS,
    S.PAYMENT_PRIORITY, 
    S.CHARGE_PRIORITY,
    S.ENCUMBRANCE_DAYS, 
    S.ENCUMBRANCE_PCT,
    S.SSF_STATE_FLAG, 
    S.TENDER_SPEC,
    S.TENDER_CATEGORY,
    S.ALL_OR_NONE,
    S.ENROL_FLAG, 
    S.KEYWORD1, 
    S.KEYWORD2, 
    S.KEYWORD3, 
    S.DAYS_SINCE_EFFDT, 
    S.DAYS_TO_EFFDT,
    S.DUEDAYS_PAST_EFFDT, 
    S.DUEDAYS_PRIOR_EFFD, 
    S.TERM_ENROLL_REQ,
    S.REFUNDABLE_IND, 
    S.ERNCD,
    S.TAXABLE_Y_N,
    S.TUITION_DEPOSIT,
    S.GL_INTERFACE_REQ, 
    S.DEFAULT_AMT,
    S.ERNCD_NOTAX,
    S.RECVABLE_FROM_CHRG, 
    S.PRIORITY, 
    S.PRIORITY_PMT_FLG, 
    S.NRA_CREDIT_TAX_FLG, 
    S.NRA_DEBIT_TAX_FLG,
    S.MATCH_WRITEOFF, 
    S.LOCAL_TAX_OFFSET, 
    S.LOCAL_TAX_PMT,
    S.STATE_TAX_OFFSET, 
    S.STATE_TAX_PMT,
    S.GL_CRSE_CLASS_SPC,
    S.TAX_CD, 
    S.T4_INCOME,
    S.WAGE_LOSS_PLAN, 
    S.T2202A_FLG, 
    S.T2202A_PCT, 
    S.T2202A_OFFSET_FLG,
    S.PAY_PRIORITY_ID,
    S.ACCTG_DT_CNTL,
    S.SF_1098_FLG,
    S.SSF_DEST_TUT_TYPE,
    'N',
    'S',
    sysdate,
    sysdate,
    1234);

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_ITEM_TYPE_TBL rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ITEM_TYPE_TBL',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_ITEM_TYPE_TBL';

strSqlCommand := 'commit';
commit;

strMessage01    := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_ITEM_TYPE_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on AMSTG_OWNER.PS_ITEM_TYPE_TBL';
update AMSTG_OWNER.PS_ITEM_TYPE_TBL T
        set T.DATA_ORIGIN = 'D',
            T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and T.SETID in ('UMBOS','UMDAR','UMLOW')
   and exists 
(select 1 from
(select SETID, ITEM_TYPE, EFFDT
   from AMSTG_OWNER.PS_ITEM_TYPE_TBL T2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ITEM_TYPE_TBL') = 'Y'
  minus
 select SETID, ITEM_TYPE, EFFDT
   from SYSADM.PS_ITEM_TYPE_TBL@AMSOURCE S2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ITEM_TYPE_TBL') = 'Y'
   ) S
 where T.SETID = S.SETID
   and T.ITEM_TYPE = S.ITEM_TYPE
   and T.EFFDT = S.EFFDT
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_ITEM_TYPE_TBL rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ITEM_TYPE_TBL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_ITEM_TYPE_TBL'
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

END AM_PS_ITEM_TYPE_TBL_P;
/
