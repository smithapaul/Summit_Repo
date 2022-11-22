DROP PROCEDURE CSMRT_OWNER.AM_PS_SF_ACCTG_LN_P
/

--
-- AM_PS_SF_ACCTG_LN_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER.AM_PS_SF_ACCTG_LN_P IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_SF_ACCTG_LN from PeopleSoft table PS_SF_ACCTG_LN.
--
-- V01  SMT-xxxx 04/04/2017,    Jim Doucette
--                              Converted from PS_SF_ACCTG_LN.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'AM_PS_SF_ACCTG_LN';
        intProcessSid                   Integer;
        dtProcessStart                  Date            := SYSDATE;
        strMessage01                    Varchar2(4000);
        strMessage02                    Varchar2(512);
        strMessage03                    Varchar2(512)   :='';
        strNewLine                      Varchar2(2)     := chr(13) || chr(10);
        strSqlCommand                   Varchar2(32767) :='';
        strSqlDynamic                   Varchar2(32767) :='';
        strClientInfo                   Varchar2(100);
        strDELETE_FLG                   Varchar2(1);
        intRowCount                     Integer;
        intTotalRowCount                Integer         := 0;
        intOLD_MAX_SCN                  Integer         := 0;
        intNEW_MAX_SCN                  Integer         := 0;
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
 where TABLE_NAME = 'PS_SF_ACCTG_LN'
;

strSqlCommand := 'commit';
commit;

strSqlCommand   := 'update NEW_MAX_SCN on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_SF_ACCTG_LN@AMSOURCE S)
 where TABLE_NAME = 'PS_SF_ACCTG_LN'
;

strSqlCommand := 'commit';
commit;

strMessage01    := 'Selecting variables from AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

select DELETE_FLG, 
       OLD_MAX_SCN, 
       NEW_MAX_SCN
  into strDELETE_FLG,
       intOLD_MAX_SCN,
       intNEW_MAX_SCN
  from AMSTG_OWNER.UM_STAGE_JOBS
 where TABLE_NAME = 'PS_SF_ACCTG_LN'
;

strMessage01    := 'Merging data into AMSTG_OWNER.PS_SF_ACCTG_LN';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into AMSTG_OWNER.PS_SF_ACCTG_LN';
merge /*+ use_hash(S,T) parallel(8) enable_parallel_dml */ into AMSTG_OWNER.PS_SF_ACCTG_LN T
using (select /*+ full(S) */
    to_date(to_char(case when RUN_DT < '01-JAN-1800' then NULL 
                    else RUN_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') RUN_DT,
    nvl(SEQNUM,0) SEQNUM, 
    nvl(SF_LINE_NBR,0) SF_LINE_NBR, 
    nvl(trim(IN_PROCESS_FLG),'-') IN_PROCESS_FLG, 
    nvl(trim(BUSINESS_UNIT_GL),'-') BUSINESS_UNIT_GL, 
    nvl(trim(JOURNAL_ID),'-') JOURNAL_ID, 
    to_date(to_char(case when JOURNAL_DATE < '01-JAN-1800' then NULL 
                    else JOURNAL_DATE end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') JOURNAL_DATE,
    JOURNAL_LINE JOURNAL_LINE,
    ACCOUNT ACCOUNT,
    FUND_CODE FUND_CODE,
    PROGRAM_CODE PROGRAM_CODE,
    DEPTID DEPTID,
    PROJECT_ID PROJECT_ID,
    STATISTICS_CODE STATISTICS_CODE,
    MONETARY_AMOUNT MONETARY_AMOUNT,
    STATISTIC_AMOUNT STATISTIC_AMOUNT,
    JRNL_LN_REF JRNL_LN_REF,
    OPEN_ITEM_STATUS OPEN_ITEM_STATUS,
    LINE_DESCR LINE_DESCR,
    JRNL_LINE_STATUS JRNL_LINE_STATUS,
    to_date(to_char(case when JOURNAL_LINE_DATE < '01-JAN-1800' then NULL 
                    else JOURNAL_LINE_DATE end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') JOURNAL_LINE_DATE, 
    BUSINESS_UNIT BUSINESS_UNIT,
    APPL_JRNL_ID APPL_JRNL_ID,
    to_date(to_char(case when ACCOUNTING_DT < '01-JAN-1800' then NULL 
                    else ACCOUNTING_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') ACCOUNTING_DT, 
    GL_DISTRIB_STATUS GL_DISTRIB_STATUS,
    PROCESS_INSTANCE PROCESS_INSTANCE,
    CURRENCY_CD CURRENCY_CD,
    ACCOUNTING_PERIOD ACCOUNTING_PERIOD,
    FISCAL_YEAR FISCAL_YEAR,
    FOREIGN_AMOUNT FOREIGN_AMOUNT,
    FOREIGN_CURRENCY FOREIGN_CURRENCY,
    LEDGER LEDGER,
    LEDGER_GROUP LEDGER_GROUP,
    EXT_GL_CHARTFLD EXT_GL_CHARTFLD,
    EMPLID EMPLID,
    SF_EXT_ORG_ID SF_EXT_ORG_ID,
    ITEM_NBR ITEM_NBR,
    BUDGET_PERIOD BUDGET_PERIOD,
    CLASS_FLD CLASS_FLD,
    AFFILIATE AFFILIATE,
    BUDGET_REF BUDGET_REF,
    CHARTFIELD1 CHARTFIELD1,
    CHARTFIELD2 CHARTFIELD2,
    CHARTFIELD3 CHARTFIELD3,
    ALTACCT ALTACCT,
    OPERATING_UNIT OPERATING_UNIT,
    PRODUCT PRODUCT,
    AFFILIATE_INTRA1 AFFILIATE_INTRA1,
    AFFILIATE_INTRA2 AFFILIATE_INTRA2,
    SF_DEPOSIT_ID SF_DEPOSIT_ID,
    RT_TYPE RT_TYPE,
    RATE_DIV RATE_DIV,
    RATE_MULT RATE_MULT,
    SF_GL_RUN_INSTANCE SF_GL_RUN_INSTANCE,
    AUDIT_ACTN AUDIT_ACTN,
    COMMON_ID COMMON_ID,
    SA_ID_TYPE SA_ID_TYPE,
    SSF_GL_TRANS_ID SSF_GL_TRANS_ID,
    SSF_GL_TRANS_SEQNO SSF_GL_TRANS_SEQNO,
    SCC_ROW_ADD_OPRID SCC_ROW_ADD_OPRID,
    to_date(to_char(case when SCC_ROW_ADD_DTTM < '01-JAN-1800' then NULL 
                    else SCC_ROW_ADD_DTTM end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') SCC_ROW_ADD_DTTM,
    SCC_ROW_UPD_OPRID SCC_ROW_UPD_OPRID,
    to_date(to_char(case when SCC_ROW_UPD_DTTM < '01-JAN-1800' then NULL 
                    else SCC_ROW_UPD_DTTM end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') SCC_ROW_UPD_DTTM,
    ITEM_TERM ITEM_TERM,
    ITEM_TYPE ITEM_TYPE,
    REF1_DESCR REF1_DESCR,
    RECEIPT_NBR RECEIPT_NBR
from SYSADM.PS_SF_ACCTG_LN@AMSOURCE S 
--where ORA_ROWSCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_SF_ACCTG_LN') ) S
where ORA_ROWSCN > intOLD_MAX_SCN ) S
 on ( 
    T.RUN_DT = S.RUN_DT and 
    T.SEQNUM = S.SEQNUM and 
    T.SF_LINE_NBR = S.SF_LINE_NBR and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.IN_PROCESS_FLG = S.IN_PROCESS_FLG,
    T.BUSINESS_UNIT_GL = S.BUSINESS_UNIT_GL,
    T.JOURNAL_ID = S.JOURNAL_ID,
    T.JOURNAL_DATE = S.JOURNAL_DATE,
    T.JOURNAL_LINE = S.JOURNAL_LINE,
    T.ACCOUNT = S.ACCOUNT,
    T.FUND_CODE = S.FUND_CODE,
    T.PROGRAM_CODE = S.PROGRAM_CODE,
    T.DEPTID = S.DEPTID,
    T.PROJECT_ID = S.PROJECT_ID,
    T.STATISTICS_CODE = S.STATISTICS_CODE,
    T.MONETARY_AMOUNT = S.MONETARY_AMOUNT,
    T.STATISTIC_AMOUNT = S.STATISTIC_AMOUNT,
    T.JRNL_LN_REF = S.JRNL_LN_REF,
    T.OPEN_ITEM_STATUS = S.OPEN_ITEM_STATUS,
    T.LINE_DESCR = S.LINE_DESCR,
    T.JRNL_LINE_STATUS = S.JRNL_LINE_STATUS,
    T.JOURNAL_LINE_DATE = S.JOURNAL_LINE_DATE,
    T.BUSINESS_UNIT = S.BUSINESS_UNIT,
    T.APPL_JRNL_ID = S.APPL_JRNL_ID,
    T.ACCOUNTING_DT = S.ACCOUNTING_DT,
    T.GL_DISTRIB_STATUS = S.GL_DISTRIB_STATUS,
    T.PROCESS_INSTANCE = S.PROCESS_INSTANCE,
    T.CURRENCY_CD = S.CURRENCY_CD,
    T.ACCOUNTING_PERIOD = S.ACCOUNTING_PERIOD,
    T.FISCAL_YEAR = S.FISCAL_YEAR,
    T.FOREIGN_AMOUNT = S.FOREIGN_AMOUNT,
    T.FOREIGN_CURRENCY = S.FOREIGN_CURRENCY,
    T.LEDGER = S.LEDGER,
    T.LEDGER_GROUP = S.LEDGER_GROUP,
    T.EXT_GL_CHARTFLD = S.EXT_GL_CHARTFLD,
    T.EMPLID = S.EMPLID,
    T.SF_EXT_ORG_ID = S.SF_EXT_ORG_ID,
    T.ITEM_NBR = S.ITEM_NBR,
    T.BUDGET_PERIOD = S.BUDGET_PERIOD,
    T.CLASS_FLD = S.CLASS_FLD,
    T.AFFILIATE = S.AFFILIATE,
    T.BUDGET_REF = S.BUDGET_REF,
    T.CHARTFIELD1 = S.CHARTFIELD1,
    T.CHARTFIELD2 = S.CHARTFIELD2,
    T.CHARTFIELD3 = S.CHARTFIELD3,
    T.ALTACCT = S.ALTACCT,
    T.OPERATING_UNIT = S.OPERATING_UNIT,
    T.PRODUCT = S.PRODUCT,
    T.AFFILIATE_INTRA1 = S.AFFILIATE_INTRA1,
    T.AFFILIATE_INTRA2 = S.AFFILIATE_INTRA2,
    T.SF_DEPOSIT_ID = S.SF_DEPOSIT_ID,
    T.RT_TYPE = S.RT_TYPE,
    T.RATE_DIV = S.RATE_DIV,
    T.RATE_MULT = S.RATE_MULT,
    T.SF_GL_RUN_INSTANCE = S.SF_GL_RUN_INSTANCE,
    T.AUDIT_ACTN = S.AUDIT_ACTN,
    T.COMMON_ID = S.COMMON_ID,
    T.SA_ID_TYPE = S.SA_ID_TYPE,
    T.SSF_GL_TRANS_ID = S.SSF_GL_TRANS_ID,
    T.SSF_GL_TRANS_SEQNO = S.SSF_GL_TRANS_SEQNO,
    T.SCC_ROW_ADD_OPRID = S.SCC_ROW_ADD_OPRID,
    T.SCC_ROW_ADD_DTTM = S.SCC_ROW_ADD_DTTM,
    T.SCC_ROW_UPD_OPRID = S.SCC_ROW_UPD_OPRID,
    T.SCC_ROW_UPD_DTTM = S.SCC_ROW_UPD_DTTM,
    T.ITEM_TERM = S.ITEM_TERM,
    T.ITEM_TYPE = S.ITEM_TYPE,
    T.REF1_DESCR = S.REF1_DESCR,
    T.RECEIPT_NBR = S.RECEIPT_NBR,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.IN_PROCESS_FLG <> S.IN_PROCESS_FLG or 
    T.BUSINESS_UNIT_GL <> S.BUSINESS_UNIT_GL or 
    T.JOURNAL_ID <> S.JOURNAL_ID or 
    nvl(trim(T.JOURNAL_DATE),0) <> nvl(trim(S.JOURNAL_DATE),0) or 
    nvl(trim(T.JOURNAL_LINE),0) <> nvl(trim(S.JOURNAL_LINE),0) or 
    nvl(trim(T.ACCOUNT),0) <> nvl(trim(S.ACCOUNT),0) or 
    nvl(trim(T.FUND_CODE),0) <> nvl(trim(S.FUND_CODE),0) or 
    nvl(trim(T.PROGRAM_CODE),0) <> nvl(trim(S.PROGRAM_CODE),0) or 
    nvl(trim(T.DEPTID),0) <> nvl(trim(S.DEPTID),0) or 
    nvl(trim(T.PROJECT_ID),0) <> nvl(trim(S.PROJECT_ID),0) or 
    nvl(trim(T.STATISTICS_CODE),0) <> nvl(trim(S.STATISTICS_CODE),0) or 
    nvl(trim(T.MONETARY_AMOUNT),0) <> nvl(trim(S.MONETARY_AMOUNT),0) or 
    nvl(trim(T.STATISTIC_AMOUNT),0) <> nvl(trim(S.STATISTIC_AMOUNT),0) or 
    nvl(trim(T.JRNL_LN_REF),0) <> nvl(trim(S.JRNL_LN_REF),0) or 
    nvl(trim(T.OPEN_ITEM_STATUS),0) <> nvl(trim(S.OPEN_ITEM_STATUS),0) or 
    nvl(trim(T.LINE_DESCR),0) <> nvl(trim(S.LINE_DESCR),0) or 
    nvl(trim(T.JRNL_LINE_STATUS),0) <> nvl(trim(S.JRNL_LINE_STATUS),0) or 
    nvl(trim(T.JOURNAL_LINE_DATE),0) <> nvl(trim(S.JOURNAL_LINE_DATE),0) or 
    nvl(trim(T.BUSINESS_UNIT),0) <> nvl(trim(S.BUSINESS_UNIT),0) or 
    nvl(trim(T.APPL_JRNL_ID),0) <> nvl(trim(S.APPL_JRNL_ID),0) or 
    nvl(trim(T.ACCOUNTING_DT),0) <> nvl(trim(S.ACCOUNTING_DT),0) or 
    nvl(trim(T.GL_DISTRIB_STATUS),0) <> nvl(trim(S.GL_DISTRIB_STATUS),0) or 
    nvl(trim(T.PROCESS_INSTANCE),0) <> nvl(trim(S.PROCESS_INSTANCE),0) or 
    nvl(trim(T.CURRENCY_CD),0) <> nvl(trim(S.CURRENCY_CD),0) or 
    nvl(trim(T.ACCOUNTING_PERIOD),0) <> nvl(trim(S.ACCOUNTING_PERIOD),0) or 
    nvl(trim(T.FISCAL_YEAR),0) <> nvl(trim(S.FISCAL_YEAR),0) or 
    nvl(trim(T.FOREIGN_AMOUNT),0) <> nvl(trim(S.FOREIGN_AMOUNT),0) or 
    nvl(trim(T.FOREIGN_CURRENCY),0) <> nvl(trim(S.FOREIGN_CURRENCY),0) or 
    nvl(trim(T.LEDGER),0) <> nvl(trim(S.LEDGER),0) or 
    nvl(trim(T.LEDGER_GROUP),0) <> nvl(trim(S.LEDGER_GROUP),0) or 
    nvl(trim(T.EXT_GL_CHARTFLD),0) <> nvl(trim(S.EXT_GL_CHARTFLD),0) or 
    nvl(trim(T.EMPLID),0) <> nvl(trim(S.EMPLID),0) or 
    nvl(trim(T.SF_EXT_ORG_ID),0) <> nvl(trim(S.SF_EXT_ORG_ID),0) or 
    nvl(trim(T.ITEM_NBR),0) <> nvl(trim(S.ITEM_NBR),0) or 
    nvl(trim(T.BUDGET_PERIOD),0) <> nvl(trim(S.BUDGET_PERIOD),0) or 
    nvl(trim(T.CLASS_FLD),0) <> nvl(trim(S.CLASS_FLD),0) or 
    nvl(trim(T.AFFILIATE),0) <> nvl(trim(S.AFFILIATE),0) or 
    nvl(trim(T.BUDGET_REF),0) <> nvl(trim(S.BUDGET_REF),0) or 
    nvl(trim(T.CHARTFIELD1),0) <> nvl(trim(S.CHARTFIELD1),0) or 
    nvl(trim(T.CHARTFIELD2),0) <> nvl(trim(S.CHARTFIELD2),0) or 
    nvl(trim(T.CHARTFIELD3),0) <> nvl(trim(S.CHARTFIELD3),0) or 
    nvl(trim(T.ALTACCT),0) <> nvl(trim(S.ALTACCT),0) or 
    nvl(trim(T.OPERATING_UNIT),0) <> nvl(trim(S.OPERATING_UNIT),0) or 
    nvl(trim(T.PRODUCT),0) <> nvl(trim(S.PRODUCT),0) or 
    nvl(trim(T.AFFILIATE_INTRA1),0) <> nvl(trim(S.AFFILIATE_INTRA1),0) or 
    nvl(trim(T.AFFILIATE_INTRA2),0) <> nvl(trim(S.AFFILIATE_INTRA2),0) or 
    nvl(trim(T.SF_DEPOSIT_ID),0) <> nvl(trim(S.SF_DEPOSIT_ID),0) or 
    nvl(trim(T.RT_TYPE),0) <> nvl(trim(S.RT_TYPE),0) or 
    nvl(trim(T.RATE_DIV),0) <> nvl(trim(S.RATE_DIV),0) or 
    nvl(trim(T.RATE_MULT),0) <> nvl(trim(S.RATE_MULT),0) or 
    nvl(trim(T.SF_GL_RUN_INSTANCE),0) <> nvl(trim(S.SF_GL_RUN_INSTANCE),0) or 
    nvl(trim(T.AUDIT_ACTN),0) <> nvl(trim(S.AUDIT_ACTN),0) or 
    nvl(trim(T.COMMON_ID),0) <> nvl(trim(S.COMMON_ID),0) or 
    nvl(trim(T.SA_ID_TYPE),0) <> nvl(trim(S.SA_ID_TYPE),0) or 
    nvl(trim(T.SSF_GL_TRANS_ID),0) <> nvl(trim(S.SSF_GL_TRANS_ID),0) or 
    nvl(trim(T.SSF_GL_TRANS_SEQNO),0) <> nvl(trim(S.SSF_GL_TRANS_SEQNO),0) or 
    nvl(trim(T.SCC_ROW_ADD_OPRID),0) <> nvl(trim(S.SCC_ROW_ADD_OPRID),0) or 
    nvl(trim(T.SCC_ROW_ADD_DTTM),0) <> nvl(trim(S.SCC_ROW_ADD_DTTM),0) or 
    nvl(trim(T.SCC_ROW_UPD_OPRID),0) <> nvl(trim(S.SCC_ROW_UPD_OPRID),0) or 
    nvl(trim(T.SCC_ROW_UPD_DTTM),0) <> nvl(trim(S.SCC_ROW_UPD_DTTM),0) or 
    nvl(trim(T.ITEM_TERM),0) <> nvl(trim(S.ITEM_TERM),0) or 
    nvl(trim(T.ITEM_TYPE),0) <> nvl(trim(S.ITEM_TYPE),0) or 
    nvl(trim(T.REF1_DESCR),0) <> nvl(trim(S.REF1_DESCR),0) or 
    nvl(trim(T.RECEIPT_NBR),0) <> nvl(trim(S.RECEIPT_NBR),0) or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.RUN_DT, 
    T.SEQNUM,
    T.SF_LINE_NBR,
    T.SRC_SYS_ID, 
    T.IN_PROCESS_FLG, 
    T.BUSINESS_UNIT_GL, 
    T.JOURNAL_ID, 
    T.JOURNAL_DATE, 
    T.JOURNAL_LINE, 
    T.ACCOUNT,
    T.FUND_CODE,
    T.PROGRAM_CODE, 
    T.DEPTID, 
    T.PROJECT_ID, 
    T.STATISTICS_CODE,
    T.MONETARY_AMOUNT,
    T.STATISTIC_AMOUNT, 
    T.JRNL_LN_REF,
    T.OPEN_ITEM_STATUS, 
    T.LINE_DESCR, 
    T.JRNL_LINE_STATUS, 
    T.JOURNAL_LINE_DATE,
    T.BUSINESS_UNIT,
    T.APPL_JRNL_ID, 
    T.ACCOUNTING_DT,
    T.GL_DISTRIB_STATUS,
    T.PROCESS_INSTANCE, 
    T.CURRENCY_CD,
    T.ACCOUNTING_PERIOD,
    T.FISCAL_YEAR,
    T.FOREIGN_AMOUNT, 
    T.FOREIGN_CURRENCY, 
    T.LEDGER, 
    T.LEDGER_GROUP, 
    T.EXT_GL_CHARTFLD,
    T.EMPLID, 
    T.SF_EXT_ORG_ID,
    T.ITEM_NBR, 
    T.BUDGET_PERIOD,
    T.CLASS_FLD,
    T.AFFILIATE,
    T.BUDGET_REF, 
    T.CHARTFIELD1,
    T.CHARTFIELD2,
    T.CHARTFIELD3,
    T.ALTACCT,
    T.OPERATING_UNIT, 
    T.PRODUCT,
    T.AFFILIATE_INTRA1, 
    T.AFFILIATE_INTRA2, 
    T.SF_DEPOSIT_ID,
    T.RT_TYPE,
    T.RATE_DIV, 
    T.RATE_MULT,
    T.SF_GL_RUN_INSTANCE, 
    T.AUDIT_ACTN, 
    T.COMMON_ID,
    T.SA_ID_TYPE, 
    T.SSF_GL_TRANS_ID,
    T.SSF_GL_TRANS_SEQNO, 
    T.SCC_ROW_ADD_OPRID,
    T.SCC_ROW_ADD_DTTM, 
    T.SCC_ROW_UPD_OPRID,
    T.SCC_ROW_UPD_DTTM, 
    T.ITEM_TERM,
    T.ITEM_TYPE,
    T.REF1_DESCR, 
    T.RECEIPT_NBR,
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
) 
values (
    S.RUN_DT, 
    S.SEQNUM, 
    S.SF_LINE_NBR,
    'CS90', 
    S.IN_PROCESS_FLG, 
    S.BUSINESS_UNIT_GL, 
    S.JOURNAL_ID, 
    S.JOURNAL_DATE, 
    S.JOURNAL_LINE, 
    S.ACCOUNT,
    S.FUND_CODE,
    S.PROGRAM_CODE, 
    S.DEPTID, 
    S.PROJECT_ID, 
    S.STATISTICS_CODE,
    S.MONETARY_AMOUNT,
    S.STATISTIC_AMOUNT, 
    S.JRNL_LN_REF,
    S.OPEN_ITEM_STATUS, 
    S.LINE_DESCR, 
    S.JRNL_LINE_STATUS, 
    S.JOURNAL_LINE_DATE,
    S.BUSINESS_UNIT,
    S.APPL_JRNL_ID, 
    S.ACCOUNTING_DT,
    S.GL_DISTRIB_STATUS,
    S.PROCESS_INSTANCE, 
    S.CURRENCY_CD,
    S.ACCOUNTING_PERIOD,
    S.FISCAL_YEAR,
    S.FOREIGN_AMOUNT, 
    S.FOREIGN_CURRENCY, 
    S.LEDGER, 
    S.LEDGER_GROUP, 
    S.EXT_GL_CHARTFLD,
    S.EMPLID, 
    S.SF_EXT_ORG_ID,
    S.ITEM_NBR, 
    S.BUDGET_PERIOD,
    S.CLASS_FLD,
    S.AFFILIATE,
    S.BUDGET_REF, 
    S.CHARTFIELD1,
    S.CHARTFIELD2,
    S.CHARTFIELD3,
    S.ALTACCT,
    S.OPERATING_UNIT, 
    S.PRODUCT,
    S.AFFILIATE_INTRA1, 
    S.AFFILIATE_INTRA2, 
    S.SF_DEPOSIT_ID,
    S.RT_TYPE,
    S.RATE_DIV, 
    S.RATE_MULT,
    S.SF_GL_RUN_INSTANCE, 
    S.AUDIT_ACTN, 
    S.COMMON_ID,
    S.SA_ID_TYPE, 
    S.SSF_GL_TRANS_ID,
    S.SSF_GL_TRANS_SEQNO, 
    S.SCC_ROW_ADD_OPRID,
    S.SCC_ROW_ADD_DTTM, 
    S.SCC_ROW_UPD_OPRID,
    S.SCC_ROW_UPD_DTTM, 
    S.ITEM_TERM,
    S.ITEM_TYPE,
    S.REF1_DESCR, 
    S.RECEIPT_NBR,
    'N',
    'S',
    sysdate,
    sysdate,
    1234);

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_SF_ACCTG_LN rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_SF_ACCTG_LN',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

If strDELETE_FLG = 'Y' then

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_SF_ACCTG_LN';

strSqlCommand := 'commit';
commit;

strMessage01    := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_SF_ACCTG_LN';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on AMSTG_OWNER.PS_SF_ACCTG_LN';
update /*+ parallel(8) enable_parallel_dml */ AMSTG_OWNER.PS_SF_ACCTG_LN T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select RUN_DT, SEQNUM, SF_LINE_NBR
   from AMSTG_OWNER.PS_SF_ACCTG_LN T2
--  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_SF_ACCTG_LN') = 'Y'
  minus
 select RUN_DT, SEQNUM, SF_LINE_NBR
   from SYSADM.PS_SF_ACCTG_LN@AMSOURCE S2
--  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_SF_ACCTG_LN') = 'Y'
   ) S
 where T.RUN_DT = S.RUN_DT
   and T.SEQNUM = S.SEQNUM
   and T.SF_LINE_NBR = S.SF_LINE_NBR
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_SF_ACCTG_LN rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_SF_ACCTG_LN',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

End if;

strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_SF_ACCTG_LN'
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

END AM_PS_SF_ACCTG_LN_P;
/
