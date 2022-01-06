CREATE OR REPLACE PROCEDURE             PS_ITEM_TYPE_TBL_AMH_P AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_ITEM_TYPE_TBL.
--
-- V01  SMT-xxxx 03/29/2017,    George Adams
--                              Converted from PS_ITEM_TYPE_TBL.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_ITEM_TYPE_TBL_AMH_P';
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
 where TABLE_NAME = 'PS_ITEM_TYPE_TBL'
;

strSqlCommand := 'commit';
commit;

strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging'
 where TABLE_NAME = 'PS_ITEM_TYPE_TBL'
;

strSqlCommand := 'commit';
commit;

strMessage01    := 'Merging data into CSSTG_OWNER.PS_ITEM_TYPE_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_ITEM_TYPE_TBL';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_ITEM_TYPE_TBL T
using (with Q1 as (  
select COLUMN_001 SETID, 
       to_char(replace(COLUMN_002,CHR(9),'')) ITEM_TYPE, 
       to_date(COLUMN_003,'DD-MON-YYYY') EFFDT, 
       COLUMN_004 EFF_STATUS, 
       COLUMN_005 DESCR, 
       COLUMN_006 DESCRSHORT, 
       COLUMN_007 CURRENCY_CD, 
       COLUMN_008 MINIMUM_AMT, 
       COLUMN_009 MAXIMUM_AMT, 
       COLUMN_010 ITEM_TYPE_CD, 
       COLUMN_011 SECURITY_GROUP, 
       COLUMN_012 ADJUSTMENT_CAL_SF, 
       COLUMN_013 PAYMENT_TERMS, 
       COLUMN_014 PAYMENT_PRIORITY, 
       COLUMN_015 CHARGE_PRIORITY, 
       COLUMN_016 ENCUMBRANCE_DAYS, 
       COLUMN_017 ENCUMBRANCE_PCT, 
       COLUMN_018 SSF_STATE_FLAG, 
       COLUMN_019 TENDER_SPEC, 
       COLUMN_020 TENDER_CATEGORY, 
       COLUMN_020 ALL_OR_NONE, 
       COLUMN_021 ENROL_FLAG, 
       COLUMN_022 KEYWORD1, 
       COLUMN_023 KEYWORD2, 
       COLUMN_024 KEYWORD3, 
       COLUMN_026 DAYS_SINCE_EFFDT,     -- COLUMN_025 missing???  
       COLUMN_027 DAYS_TO_EFFDT, 
       COLUMN_028 DUEDAYS_PAST_EFFDT, 
       COLUMN_029 DUEDAYS_PRIOR_EFFD, 
       COLUMN_030 TERM_ENROLL_REQ, 
       COLUMN_031 REFUNDABLE_IND, 
       COLUMN_032 ERNCD, 
       COLUMN_033 TAXABLE_Y_N, 
       COLUMN_034 TUITION_DEPOSIT, 
       COLUMN_035 GL_INTERFACE_REQ, 
       COLUMN_036 DEFAULT_AMT, 
       COLUMN_037 ERNCD_NOTAX, 
       COLUMN_038 RECVABLE_FROM_CHRG, 
       COLUMN_039 PRIORITY, 
       COLUMN_040 PRIORITY_PMT_FLG, 
       COLUMN_041 NRA_CREDIT_TAX_FLG, 
       COLUMN_042 NRA_DEBIT_TAX_FLG, 
       COLUMN_043 MATCH_WRITEOFF, 
       COLUMN_044 LOCAL_TAX_OFFSET, 
       COLUMN_045 LOCAL_TAX_PMT, 
       COLUMN_046 STATE_TAX_OFFSET, 
       COLUMN_047 STATE_TAX_PMT, 
       COLUMN_048 GL_CRSE_CLASS_SPC, 
       COLUMN_049 TAX_CD, 
       COLUMN_050 T4_INCOME, 
       COLUMN_051 WAGE_LOSS_PLAN, 
       COLUMN_052 T2202A_FLG, 
       COLUMN_053 T2202A_PCT, 
       COLUMN_054 T2202A_OFFSET_FLG, 
       COLUMN_055 PAY_PRIORITY_ID, 
       COLUMN_056 ACCTG_DT_CNTL, 
       COLUMN_057 SF_1098_FLG, 
       COLUMN_058 SSF_DEST_TUT_TYPE, 
       'S' DATA_ORIGIN
  from COMMON_OWNER.UPLOAD_S1_VW    -- Reads from apprpriate database with DB link 
-- where UPLOAD_ID = i_UploadId ITEM_TYPE_TBL_AMH
 where UPLOAD_ID = 'ITEM_TYPE_TBL_AMH'
   and COLUMN_001 = 'UMAMH')
select Q1.SETID, 
       Q1.ITEM_TYPE, 
       Q1.EFFDT, 
       Q1.EFF_STATUS, 
       Q1.DESCR, 
       nvl(Q1.DESCRSHORT,'-') DESCRSHORT, 
       Q1.CURRENCY_CD, 
       to_number(Q1.MINIMUM_AMT) MINIMUM_AMT, 
       to_number(Q1.MAXIMUM_AMT) MAXIMUM_AMT,
       Q1.ITEM_TYPE_CD,
       Q1.SECURITY_GROUP,
       Q1.ADJUSTMENT_CAL_SF,
       Q1.PAYMENT_TERMS,
       Q1.PAYMENT_PRIORITY,
       Q1.CHARGE_PRIORITY,
       to_number(Q1.ENCUMBRANCE_DAYS) ENCUMBRANCE_DAYS, 
       to_number(Q1.ENCUMBRANCE_PCT) ENCUMBRANCE_PCT, 
       Q1.SSF_STATE_FLAG, 
       Q1.TENDER_SPEC, 
       Q1.TENDER_CATEGORY, 
       Q1.ALL_OR_NONE, 
       Q1.ENROL_FLAG, 
       Q1.KEYWORD1, 
       Q1.KEYWORD2, 
       Q1.KEYWORD3, 
       to_number(Q1.DAYS_SINCE_EFFDT) DAYS_SINCE_EFFDT, 
       to_number(Q1.DAYS_TO_EFFDT) DAYS_TO_EFFDT, 
       to_number(Q1.DUEDAYS_PAST_EFFDT) DUEDAYS_PAST_EFFDT, 
       to_number(Q1.DUEDAYS_PRIOR_EFFD) DUEDAYS_PRIOR_EFFD, 
       Q1.TERM_ENROLL_REQ, 
       Q1.REFUNDABLE_IND, 
       Q1.ERNCD, 
       Q1.TAXABLE_Y_N, 
       Q1.TUITION_DEPOSIT, 
       Q1.GL_INTERFACE_REQ, 
       to_number(Q1.DEFAULT_AMT) DEFAULT_AMT, 
       Q1.ERNCD_NOTAX, 
       Q1.RECVABLE_FROM_CHRG, 
       to_number(Q1.PRIORITY) PRIORITY, 
       Q1.PRIORITY_PMT_FLG, 
       Q1.NRA_CREDIT_TAX_FLG, 
       Q1.NRA_DEBIT_TAX_FLG, 
       Q1.MATCH_WRITEOFF, 
       Q1.LOCAL_TAX_OFFSET, 
       Q1.LOCAL_TAX_PMT, 
       Q1.STATE_TAX_OFFSET, 
       Q1.STATE_TAX_PMT, 
       Q1.GL_CRSE_CLASS_SPC, 
       Q1.TAX_CD, 
       Q1.T4_INCOME, 
       Q1.WAGE_LOSS_PLAN, 
       Q1.T2202A_FLG, 
       to_number(Q1.T2202A_PCT) T2202A_PCT, 
       Q1.T2202A_OFFSET_FLG, 
       Q1.PAY_PRIORITY_ID, 
       Q1.ACCTG_DT_CNTL, 
       Q1.SF_1098_FLG, 
       Q1.SSF_DEST_TUT_TYPE, 
       Q1.DATA_ORIGIN  
  from Q1) S
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
    T.LASTUPD_EW_DTTM = SYSDATE,
    T.BATCH_SID = 1234
 where 
    decode(T.EFF_STATUS,S.EFF_STATUS,1,0) = 0 or
    decode(T.DESCR,S.DESCR,1,0) = 0 or
    decode(T.DESCRSHORT,S.DESCRSHORT,1,0) = 0 or
    decode(T.CURRENCY_CD,S.CURRENCY_CD,1,0) = 0 or
    decode(T.MINIMUM_AMT,S.MINIMUM_AMT,1,0) = 0 or
    decode(T.MAXIMUM_AMT,S.MAXIMUM_AMT,1,0) = 0 or
    decode(T.ITEM_TYPE_CD,S.ITEM_TYPE_CD,1,0) = 0 or
    decode(T.SECURITY_GROUP,S.SECURITY_GROUP,1,0) = 0 or
    decode(T.ADJUSTMENT_CAL_SF,S.ADJUSTMENT_CAL_SF,1,0) = 0 or
    decode(T.PAYMENT_TERMS,S.PAYMENT_TERMS,1,0) = 0 or
    decode(T.PAYMENT_PRIORITY,S.PAYMENT_PRIORITY,1,0) = 0 or
    decode(T.CHARGE_PRIORITY,S.CHARGE_PRIORITY,1,0) = 0 or
    decode(T.ENCUMBRANCE_DAYS,S.ENCUMBRANCE_DAYS,1,0) = 0 or
    decode(T.ENCUMBRANCE_PCT,S.ENCUMBRANCE_PCT,1,0) = 0 or
    decode(T.SSF_STATE_FLAG,S.SSF_STATE_FLAG,1,0) = 0 or
    decode(T.TENDER_SPEC,S.TENDER_SPEC,1,0) = 0 or
    decode(T.TENDER_CATEGORY,S.TENDER_CATEGORY,1,0) = 0 or
    decode(T.ALL_OR_NONE,S.ALL_OR_NONE,1,0) = 0 or
    decode(T.ENROL_FLAG,S.ENROL_FLAG,1,0) = 0 or
    decode(T.KEYWORD1,S.KEYWORD1,1,0) = 0 or
    decode(T.KEYWORD2,S.KEYWORD2,1,0) = 0 or
    decode(T.KEYWORD3,S.KEYWORD3,1,0) = 0 or
    decode(T.DAYS_SINCE_EFFDT,S.DAYS_SINCE_EFFDT,1,0) = 0 or
    decode(T.DAYS_TO_EFFDT,S.DAYS_TO_EFFDT,1,0) = 0 or
    decode(T.DUEDAYS_PAST_EFFDT,S.DUEDAYS_PAST_EFFDT,1,0) = 0 or
    decode(T.DUEDAYS_PRIOR_EFFD,S.DUEDAYS_PRIOR_EFFD,1,0) = 0 or
    decode(T.TERM_ENROLL_REQ,S.TERM_ENROLL_REQ,1,0) = 0 or
    decode(T.REFUNDABLE_IND,S.REFUNDABLE_IND,1,0) = 0 or
    decode(T.ERNCD,S.ERNCD,1,0) = 0 or
    decode(T.TAXABLE_Y_N,S.TAXABLE_Y_N,1,0) = 0 or
    decode(T.TUITION_DEPOSIT,S.TUITION_DEPOSIT,1,0) = 0 or
    decode(T.GL_INTERFACE_REQ,S.GL_INTERFACE_REQ,1,0) = 0 or
    decode(T.DEFAULT_AMT,S.DEFAULT_AMT,1,0) = 0 or
    decode(T.ERNCD_NOTAX,S.ERNCD_NOTAX,1,0) = 0 or
    decode(T.RECVABLE_FROM_CHRG,S.RECVABLE_FROM_CHRG,1,0) = 0 or
    decode(T.PRIORITY,S.PRIORITY,1,0) = 0 or
    decode(T.PRIORITY_PMT_FLG,S.PRIORITY_PMT_FLG,1,0) = 0 or
    decode(T.NRA_CREDIT_TAX_FLG,S.NRA_CREDIT_TAX_FLG,1,0) = 0 or
    decode(T.NRA_DEBIT_TAX_FLG,S.NRA_DEBIT_TAX_FLG,1,0) = 0 or
    decode(T.MATCH_WRITEOFF,S.MATCH_WRITEOFF,1,0) = 0 or
    decode(T.LOCAL_TAX_OFFSET,S.LOCAL_TAX_OFFSET,1,0) = 0 or
    decode(T.LOCAL_TAX_PMT,S.LOCAL_TAX_PMT,1,0) = 0 or
    decode(T.STATE_TAX_OFFSET,S.STATE_TAX_OFFSET,1,0) = 0 or
    decode(T.STATE_TAX_PMT,S.STATE_TAX_PMT,1,0) = 0 or
    decode(T.GL_CRSE_CLASS_SPC,S.GL_CRSE_CLASS_SPC,1,0) = 0 or
    decode(T.TAX_CD,S.TAX_CD,1,0) = 0 or
    decode(T.T4_INCOME,S.T4_INCOME,1,0) = 0 or
    decode(T.WAGE_LOSS_PLAN,S.WAGE_LOSS_PLAN,1,0) = 0 or
    decode(T.T2202A_FLG,S.T2202A_FLG,1,0) = 0 or
    decode(T.T2202A_PCT,S.T2202A_PCT,1,0) = 0 or
    decode(T.T2202A_OFFSET_FLG,S.T2202A_OFFSET_FLG,1,0) = 0 or
    decode(T.PAY_PRIORITY_ID,S.PAY_PRIORITY_ID,1,0) = 0 or
    decode(T.ACCTG_DT_CNTL,S.ACCTG_DT_CNTL,1,0) = 0 or
    decode(T.SF_1098_FLG,S.SF_1098_FLG,1,0) = 0 or
    decode(T.SSF_DEST_TUT_TYPE,S.SSF_DEST_TUT_TYPE,1,0) = 0 or
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
       SYSDATE,
       SYSDATE,
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

strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting'
 where TABLE_NAME = 'PS_ITEM_TYPE_TBL';

strSqlCommand := 'commit';
commit;

strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_ITEM_TYPE_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_ITEM_TYPE_TBL';
update CSSTG_OWNER.PS_ITEM_TYPE_TBL T
        set T.DATA_ORIGIN = 'D',
            T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and T.SETID in ('UMAMH')
   and exists 
(select 1 from
(select SETID, ITEM_TYPE, EFFDT
   from CSSTG_OWNER.PS_ITEM_TYPE_TBL T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ITEM_TYPE_TBL') = 'Y'
    and SETID = 'UMAMH'
  minus
 select COLUMN_001 SETID, to_char(replace(COLUMN_002,CHR(9),'')) ITEM_TYPE, to_date(COLUMN_003,'DD-MON-YYYY') EFFDT
   from COMMON_OWNER.UPLOAD_S1_VW S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ITEM_TYPE_TBL') = 'Y'
--    and UPLOAD_ID = i_UploadId
    and UPLOAD_ID = 'ITEM_TYPE_TBL_AMH'
    and COLUMN_001 = 'UMAMH') S
 where T.SETID = S.SETID
   and T.ITEM_TYPE = S.ITEM_TYPE
   and T.EFFDT = S.EFFDT
   and T.SRC_SYS_ID = 'CS90') 
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

strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
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

END PS_ITEM_TYPE_TBL_AMH_P;
/
