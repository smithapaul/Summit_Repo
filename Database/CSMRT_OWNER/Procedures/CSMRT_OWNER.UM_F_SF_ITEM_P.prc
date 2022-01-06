CREATE OR REPLACE PROCEDURE             "UM_F_SF_ITEM_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads table UM_F_SF_ITEM.
--
-- V01   SMT-xxxx 06/26/2019,    James Doucette
--                               Converted from Script
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_SF_ITEM';
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

strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_SF_ITEM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_SF_ITEM';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_SF_ITEM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_SF_ITEM');

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_SF_ITEM disable constraint PK_UM_F_SF_ITEM';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_SF_ITEM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_SF_ITEM';
insert /*+ append parallel(8) enable_parallel_dml */ into CSMRT_OWNER.UM_F_SF_ITEM
with X as (
select /*+ inline parallel(8) */
       FIELDNAME, FIELDVALUE, EFFDT, SRC_SYS_ID,
       XLATLONGNAME, XLATSHORTNAME, DATA_ORIGIN,
       row_number() over (partition by FIELDNAME, FIELDVALUE, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1900') else EFFDT end) desc) X_ORDER
  from CSSTG_OWNER.PSXLATITEM
 where DATA_ORIGIN <> 'D'),
T0_SF AS
(
SELECT /*+ inline parallel(8) */ --T0.*,
       BUSINESS_UNIT, COMMON_ID, SA_ID_TYPE, ITEM_NBR, SRC_SYS_ID, ACAD_CAREER,
       COUNT(DISTINCT ACAD_CAREER) OVER (PARTITION BY BUSINESS_UNIT, COMMON_ID, ITEM_TERM) CAREER_COUNT,
       MAX(ACAD_CAREER) OVER (PARTITION BY BUSINESS_UNIT, COMMON_ID, ITEM_TERM) MAX_ACAD_CAREER
  from CSSTG_OWNER.PS_ITEM_SF T0
 where DATA_ORIGIN <> 'D'),
/*For students with one valid career in a term, all transaction rows with bliack career can be filled with that valid career*/
T1_SF AS
(
SELECT /*+ inline parallel(8) */ --T1.*,
       BUSINESS_UNIT, COMMON_ID, SA_ID_TYPE, ITEM_NBR, SRC_SYS_ID, ACAD_CAREER,
       CASE
            WHEN(T1.ACAD_CAREER = '-' AND CAREER_COUNT = 2)
            THEN MAX_ACAD_CAREER
            ELSE T1.ACAD_CAREER
        END AS SMT_ACAD_CAREER
  FROM T0_SF T1)
select /*+ parallel(8) */
		I.BUSINESS_UNIT INSTITUTION_CD,
		I.COMMON_ID PERSON_ID,
		I.SA_ID_TYPE,
		I.ITEM_NBR,
		I.SRC_SYS_ID,
		I.ACCOUNT_NBR,
		I.ACCOUNT_TERM,
		T1.DESCR ACCOUNT_TERM_LD,
		ITEM_TERM,
		T2.DESCR ITEM_TERM_LD,
		I.ACAD_CAREER ACAD_CAR_CD,
		nvl(B.INSTITUTION_SID,2147483646) INSTITUTION_SID,
		nvl(C.ACAD_CAR_SID,2147483646) ACAD_CAR_SID,
		nvl(T0_ITEM_TERM.TERM_SID,2147483646) ITEM_TERM_SID,          -- Default SID
		nvl(T1_ACCOUNT_TERM.TERM_SID,2147483646) ACCOUNT_TERM_SID,       -- Default SID
		nvl(P.PERSON_SID,2147483646) PERSON_SID,
		nvl(Y.ITEM_TYPE_SID,2147483646) ITEM_TYPE_SID,
		I.ACAD_YEAR,
		I.ITEM_TYPE,
		I.ITEM_AMT,
		SUM(I.ITEM_AMT) OVER (PARTITION BY I.BUSINESS_UNIT, I.COMMON_ID) CUM_ITEM_AMT,
		SUM(I.ITEM_AMT) OVER (PARTITION BY I.BUSINESS_UNIT, I.COMMON_ID, I.ITEM_TERM) TERM_ITEM_AMT,
		I.APPLIED_AMT,
		I.ENCUMBERED_AMT,
		I.REF1_DESCR,
		I.ITEM_BALANCE,
		I.ACAD_CAREER,
		I.STDNT_CAR_NBR,
		SESSION_CODE,
		CLASS_NBR,
		FEE_CD,
		SEL_GROUP,
		ADM_APPL_NBR,
		GL_SATISFIED_AMT,
		GL_ASSESSED_AMT,
		PAYMENT_ID_NBR,
		ORIGINAL_ACCT_TERM,
		BILLING_CAREER,
		DEPOSIT_NBR,
		REFUND_NBR,
		EXT_ORG_ID,
		REFUND_EMPLID,
		I.CONTRACT_NUM,
		COLLECTION_ID,
		RECEIPT_NBR,
		ITEM_TYPE_CD,
	    nvl(X1.XLATLONGNAME,'-') ITEM_TYPE_LD,
		CONTRACT_EMPLID,
		CHARGE_PRIORITY,
		COURSE_LIST,
		CRSE_ID,
		WAIVER_CODE,
		PRIORITY,
		PRIORITY_PMT_FLG,
		NRA_TAXATION_SWTCH,
		GL_BALANCED_AMT,
		STATE_TAX_RT,
		LOCAL_TAX_RT,
		FEDERAL_TAX_RT,
		BILLING_DT,
		DUE_DT,
		ACTUAL_BILLING_DT,
		CALENDAR_YEAR,
		SRVC_IND_DTTM,
		TAX_ADJ_WHOLDINGS,
		TRANS_FEE_CD,
		LATE_FEE_CODE,
		CUR_RT_TYPE,
		RATE_MULT,
		RATE_DIV,
		CURRENCY_CD,
		ORIGNL_CURRENCY_CD,
		ORIGNL_ITEM_AMT,
		ITEM_STATUS,
		TRANSFER_DT,
		TRANSFER_STATUS,
		TRANSFER_PAYMNT_ID,
		TRANSFER_AMT,
		ITEM_NBR_SOURCE,
		TAX_AUTHORITY_CD,
		CONTRACT_AMT,
		INTEREST_DT,
		T4_SENT_AMT,
		GL_FROM_SUBFEE,
		ITEM_EFFECTIVE_DT,
		TRACER_NBR,
		I.AID_YEAR,
		DISBURSEMENT_DATE,
		ORIG_EFF_DT,
		SF_DEPOSIT_ID,
		LAST_ACTIVITY_DATE,
		REFUND_EXT_ORG_ID,
		REFUND_ORG_CONTACT,
		TAX_CD,
		SF_PMT_REF_NBR,
		SF_ADM_APPL_DEL,
		CLASS_CRSE_FEE_IND,
		SSF_BILLED_AMOUNT,
		SSF_INSTMNT_ID,
		SCC_ROW_ADD_OPRID,
		SCC_ROW_ADD_DTTM,
		SCC_ROW_UPD_OPRID,
		SCC_ROW_UPD_DTTM,
		nvl(A.ACCOUNT_TYPE_SF,'-') ACCOUNT_TYPE_SF,
		nvl(ACCT_STATUS,'-') ACCT_STATUS,
		nvl(OPEN_DT,to_date('01-JAN-1900')) OPEN_DT,
		CLOSE_DT,
		nvl(ACCOUNT_BALANCE,0) ACCOUNT_BALANCE,
		LAST_AGING_DT,
		LAST_ACCT_DT_AGED,
		nvl(BILL_REQ_ID,'-') BILL_REQ_ID,
		nvl(OVERR_BILL_REQ_ID,'-') OVERR_BILL_REQ_ID,
		nvl(INCLUDE_IN_BALANCE,'-') INCLUDE_IN_BALANCE,
		nvl(INCLUDE_BILLING,'-') INCLUDE_BILLING,
		nvl(INCLUDE_TRANSFER,'-') INCLUDE_TRANSFER,
		nvl(INCLUDE_PREPAY,'-') INCLUDE_PREPAY,
		'N' LOAD_ERROR,
		'S' DATA_ORIGIN,
		sysdate CREATED_EW_DTTM,
		sysdate LASTUPD_EW_DTTM,
		1234 BATCH_SID
    from CSSTG_OWNER.PS_ITEM_SF I
    join T1_SF T1
		  on I.BUSINESS_UNIT = T1.BUSINESS_UNIT
		 and I.COMMON_ID = T1.COMMON_ID
		 and I.SA_ID_TYPE = T1.SA_ID_TYPE
		 and I.ITEM_NBR = T1.ITEM_NBR
		 and I.SRC_SYS_ID = T1.SRC_SYS_ID
	left outer join CSSTG_OWNER.PS_ACCOUNT_SF A
		  on I.BUSINESS_UNIT = A.BUSINESS_UNIT
		 and I.EMPLID = A.EMPLID
		 and I.ACCOUNT_NBR = A.ACCOUNT_NBR
		 and I.ACCOUNT_TERM = A.ACCOUNT_TERM
		 and I.SRC_SYS_ID = A.SRC_SYS_ID
		 and A.DATA_ORIGIN <> 'D'
	left outer join CSMRT_OWNER.PS_D_INSTITUTION B
		  on I.BUSINESS_UNIT = B.INSTITUTION_CD
		 and I.SRC_SYS_ID = B.SRC_SYS_ID
		 and B.DATA_ORIGIN <> 'D'
	left outer join CSMRT_OWNER.PS_D_ACAD_CAR C
		  on I.BUSINESS_UNIT = C.INSTITUTION_CD
		 and I.ACAD_CAREER = C.ACAD_CAR_CD
		 and I.SRC_SYS_ID = C.SRC_SYS_ID
		 and C.DATA_ORIGIN <> 'D'
	left outer join CSSTG_OWNER.PS_TERM_VAL_TBL T1
		  on I.ACCOUNT_TERM = T1.STRM
		 and I.SRC_SYS_ID = T1.SRC_SYS_ID
		 and T1.DATA_ORIGIN <> 'D'
	left outer join CSSTG_OWNER.PS_TERM_VAL_TBL T2
		  on I.ITEM_TERM = T2.STRM
		 and I.SRC_SYS_ID = T2.SRC_SYS_ID
		 and T2.DATA_ORIGIN <> 'D'
	left outer join CSMRT_OWNER.PS_D_PERSON P
		  on I.COMMON_ID = P.PERSON_ID
		 and I.SRC_SYS_ID = P.SRC_SYS_ID
		 and P.DATA_ORIGIN <> 'D'
	left outer join CSMRT_OWNER.PS_D_ITEM_TYPE Y
		  on I.BUSINESS_UNIT = Y.SETID
		 and I.ITEM_TYPE = Y.ITEM_TYPE_ID
		 and I.SRC_SYS_ID = Y.SRC_SYS_ID
		 and Y.DATA_ORIGIN <> 'D'
	left outer join CSMRT_OWNER.PS_D_TERM T0_ITEM_TERM
         on I.BUSINESS_UNIT = T0_ITEM_TERM.INSTITUTION_CD
         /*Since term_cd and descr do not change with career, the code adjusted to pick any one sid*/
		 and CASE WHEN(T1.SMT_ACAD_CAREER = '-') THEN('UGRD') ELSE(T1.SMT_ACAD_CAREER) END = T0_ITEM_TERM.ACAD_CAR_CD
         and I.ITEM_TERM = T0_ITEM_TERM.TERM_CD
         and I.SRC_SYS_ID = T0_ITEM_TERM.SRC_SYS_ID
    left outer join CSMRT_OWNER.PS_D_TERM T1_ACCOUNT_TERM
         on I.BUSINESS_UNIT = T1_ACCOUNT_TERM.INSTITUTION_CD
         /*Since term_cd and descr do not change with career, the code adjusted to pick any one sid*/
		 and CASE WHEN(T1.SMT_ACAD_CAREER = '-') THEN('UGRD') ELSE(T1.SMT_ACAD_CAREER) END = T1_ACCOUNT_TERM.ACAD_CAR_CD
         and I.ACCOUNT_TERM = T1_ACCOUNT_TERM.TERM_CD
         and I.SRC_SYS_ID = T1_ACCOUNT_TERM.SRC_SYS_ID
    left outer join X X1
      on X1.FIELDNAME = 'ITEM_TYPE_CD'
     and X1.FIELDVALUE = I.ITEM_TYPE_CD
     and X1.SRC_SYS_ID = I.SRC_SYS_ID
     and X1.X_ORDER = 1
   where I.DATA_ORIGIN <> 'D'
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_SF_ITEM rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_SF_ITEM',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_SF_ITEM',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_SF_ITEM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_SF_ITEM enable constraint PK_UM_F_SF_ITEM';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_SF_ITEM');

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

END UM_F_SF_ITEM_P;
/
