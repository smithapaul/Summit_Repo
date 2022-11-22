DROP PROCEDURE CSMRT_OWNER.UM_F_SF_ITEM_LINE_P
/

--
-- UM_F_SF_ITEM_LINE_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_F_SF_ITEM_LINE_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads table UM_F_SF_ITEM_LINE.
--
-- V01   SMT-xxxx 01/14/2019,    James Doucette
--                               Converted from Script
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_SF_ITEM_LINE';
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

strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_SF_ITEM_LINE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_SF_ITEM_LINE';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_SF_ITEM_LINE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_SF_ITEM_LINE');

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_SF_ITEM_LINE disable constraint PK_UM_F_SF_ITEM_LINE';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_SF_ITEM_LINE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_SF_ITEM_LINE';
insert /*+ append parallel(8) enable_parallel_dml */ into CSMRT_OWNER.UM_F_SF_ITEM_LINE
  with X as (
select /*+ inline parallel(8) no_merge */
       FIELDNAME, FIELDVALUE, EFFDT, SRC_SYS_ID,
       XLATLONGNAME, XLATSHORTNAME, DATA_ORIGIN,
       row_number() over (partition by FIELDNAME, FIELDVALUE, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1900') else EFFDT end) desc) X_ORDER
  from CSSTG_OWNER.PSXLATITEM
 where DATA_ORIGIN <> 'D'),
       RSN as (
select /*+ inline parallel(8) no_merge */
       SETID, LINE_REASON_CD, EFFDT, SRC_SYS_ID, EFF_STATUS, DESCR, DESCRSHORT,
       row_number() over (partition by SETID, LINE_REASON_CD, SRC_SYS_ID
                              order by EFFDT desc) RSN_ORDER
  from CSSTG_OWNER.PS_LINE_REASON_TBL
 where DATA_ORIGIN <> 'D'),
       I as (
select /*+ inline parallel(8) no_merge */
       BUSINESS_UNIT, COMMON_ID, SA_ID_TYPE, ITEM_NBR, SRC_SYS_ID,
       ITEM_TYPE
  from CSSTG_OWNER.PS_ITEM_SF
 where DATA_ORIGIN <> 'D'),
       L as (
select /*+ inline parallel(8) no_merge */
       L.BUSINESS_UNIT, L.COMMON_ID, L.SA_ID_TYPE, L.ITEM_NBR, L.LINE_SEQ_NBR, L.SRC_SYS_ID,
       L.EMPLID, L.ACCOUNT_NBR, L.ACCOUNT_TERM, L.POSTED_DATE, L.ITEM_EFFECTIVE_DT,
       L.GL_POSTING_DTTM, L.BILLING_DT, L.DUE_DT, L.REF1_DESCR, L.LINE_AMT, L.DUE_AMT,
       L.ACAD_CAREER, L.STDNT_CAR_NBR, L.ITEM_TERM, L.SESSION_CODE, L.CLASS_NBR,
       L.FINANCE_CHARGE, L.ENCUMBRANCE_DT, L.FEE_CD, L.SEL_GROUP, L.OPRID,
       L.LINE_STATUS, L.LINE_ACTION, L.LINE_REASON_CD, L.AGING_DT,
       L.PAYMENT_ID_NBR, L.DUE_DATE_BY, L.DESCR, L.REFUND_NBR, L.REFUND_EMPLID,
       L.ITEM_TYPE_CD, L.ACTUAL_BILLING_DT, L.BILLING_FLAG, L.CLASS_PRICE_DTTM,
       L.ORIG_DUE_DATE, L.SCC_ROW_ADD_OPRID, L.SCC_ROW_ADD_DTTM, L.SCC_ROW_UPD_OPRID, L.SCC_ROW_UPD_DTTM,
       I.ITEM_TYPE,
       CASE WHEN (L.ACAD_CAREER = '-' AND (COUNT(DISTINCT L.ACAD_CAREER) OVER (PARTITION BY L.BUSINESS_UNIT, L.COMMON_ID, L.ITEM_TERM, L.SRC_SYS_ID)) > 1)  -- Was = 2!!!
            THEN (MAX(L.ACAD_CAREER) OVER (PARTITION BY L.BUSINESS_UNIT, L.COMMON_ID, L.ITEM_TERM, L.SRC_SYS_ID))
            ELSE L.ACAD_CAREER
        END SMT_ACAD_CAREER
  from CSSTG_OWNER.PS_ITEM_LINE_SF L
  join I
    on L.BUSINESS_UNIT = I.BUSINESS_UNIT
   and L.COMMON_ID = I.COMMON_ID
   and L.SA_ID_TYPE = I.SA_ID_TYPE
   and L.ITEM_NBR = I.ITEM_NBR
   and L.SRC_SYS_ID = I.SRC_SYS_ID
 where L.DATA_ORIGIN <> 'D')
select /*+ inline parallel(8) no_merge */
	I.BUSINESS_UNIT INSTITUTION_CD,
	I.COMMON_ID PERSON_ID,
	I.SA_ID_TYPE,
	I.ITEM_NBR,
	I.LINE_SEQ_NBR,
	I.SRC_SYS_ID,
	nvl(B.INSTITUTION_SID,2147483646) INSTITUTION_SID,
	nvl(C.ACAD_CAR_SID,2147483646) ACAD_CAR_SID,
	nvl(P.PERSON_SID,2147483646) PERSON_SID,
	nvl(T0_ITEM_TERM.TERM_SID,2147483646) ITEM_TERM_SID,        -- Apr 2020
    nvl(T1_ACCOUNT_TERM.TERM_SID,2147483646) ACCOUNT_TERM_SID,  -- Apr 2020
    nvl(Y.ITEM_TYPE_SID,2147483646) ITEM_TYPE_SID,              -- Dec 2020
    I.ACAD_CAREER,                                              -- Apr 2020
	I.ACCOUNT_NBR,
	I.ACCOUNT_TERM,
	T1.DESCR ACCOUNT_TERM_LD,
	I.ITEM_TERM,
	T2.DESCR ITEM_TERM_LD,
	I.AGING_DT,
	I.ACTUAL_BILLING_DT,
	I.BILLING_DT,
	I.BILLING_FLAG,
	I.CLASS_NBR,
	I.CLASS_PRICE_DTTM,
	I.DESCR,
	I.DUE_AMT,
	I.DUE_DT,
	DUE_DATE_BY,
	nvl(X1.XLATSHORTNAME,'-') DUE_DATE_BY_SD,
	nvl(X1.XLATLONGNAME,'-') DUE_DATE_BY_LD,
	I.ENCUMBRANCE_DT,
	I.FEE_CD,
	I.FINANCE_CHARGE,
	I.GL_POSTING_DTTM,
	I.ITEM_EFFECTIVE_DT,
	I.ITEM_TYPE_CD,
	nvl(X2.XLATSHORTNAME,'-') ITEM_TYPE_SD,
	nvl(X2.XLATLONGNAME,'-') ITEM_TYPE_LD,
	I.LINE_AMT,
	I.LINE_STATUS,
	nvl(X3.XLATSHORTNAME,'-') LINE_STATUS_SD,
	nvl(X3.XLATLONGNAME,'-') LINE_STATUS_LD,
	I.LINE_ACTION,
	nvl(X4.XLATSHORTNAME,'-') LINE_ACTION_SD,
	nvl(X4.XLATLONGNAME,'-') LINE_ACTION_LD,
	I.LINE_REASON_CD,
	nvl(RSN.DESCRSHORT,'-') LINE_REASON_SD,
	nvl(RSN.DESCR,'-') LINE_REASON_LD,
	I.OPRID,
	I.ORIG_DUE_DATE,
	I.PAYMENT_ID_NBR,
	I.POSTED_DATE,
	I.REF1_DESCR,
	I.REFUND_NBR,
	I.REFUND_EMPLID,
	I.SEL_GROUP,
	I.SESSION_CODE,
	I.STDNT_CAR_NBR,
	I.SCC_ROW_ADD_OPRID,
	I.SCC_ROW_ADD_DTTM,
	I.SCC_ROW_UPD_OPRID,
	I.SCC_ROW_UPD_DTTM,
	nvl(A.ACCOUNT_TYPE_SF,'-') ACCOUNT_TYPE_SF,
	nvl(A.ACCT_STATUS,'-') ACCT_STATUS,
	nvl(A.OPEN_DT,to_date('01-JAN-1900')) OPEN_DT,
	A.CLOSE_DT,
	nvl(A.ACCOUNT_BALANCE,0) ACCOUNT_BALANCE,
	A.LAST_AGING_DT,
	A.LAST_ACCT_DT_AGED,
	nvl(A.BILL_REQ_ID,'-') BILL_REQ_ID,
	nvl(A.OVERR_BILL_REQ_ID,'-') OVERR_BILL_REQ_ID,
	nvl(A.INCLUDE_IN_BALANCE,'-') INCLUDE_IN_BALANCE,
	nvl(A.INCLUDE_BILLING,'-') INCLUDE_BILLING,
	nvl(A.INCLUDE_TRANSFER,'-') INCLUDE_TRANSFER,
	nvl(A.INCLUDE_PREPAY,'-') INCLUDE_PREPAY,
	'N' LOAD_ERROR,
	'S' DATA_ORIGIN,
	sysdate CREATED_EW_DTTM,
	sysdate LASTUPD_EW_DTTM,
	1234 BATCH_SID
from L I
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
left outer join CSMRT_OWNER.PS_D_PERSON P
  on I.COMMON_ID = P.PERSON_ID
 and I.SRC_SYS_ID = P.SRC_SYS_ID
 and P.DATA_ORIGIN <> 'D'
left outer join CSSTG_OWNER.PS_TERM_VAL_TBL T1
  on I.ACCOUNT_TERM = T1.STRM
 and I.SRC_SYS_ID = T1.SRC_SYS_ID
 and T1.DATA_ORIGIN <> 'D'
left outer join CSSTG_OWNER.PS_TERM_VAL_TBL T2
  on I.ITEM_TERM = T2.STRM
 and I.SRC_SYS_ID = T2.SRC_SYS_ID
 and T2.DATA_ORIGIN <> 'D'
left outer join CSMRT_OWNER.PS_D_TERM T0_ITEM_TERM
 on I.BUSINESS_UNIT = T0_ITEM_TERM.INSTITUTION_CD
 /*Since term_cd and descr do not change with career, the code adjusted to pick any one sid*/
 and CASE WHEN(I.SMT_ACAD_CAREER = '-') THEN('UGRD') ELSE(I.SMT_ACAD_CAREER) END = T0_ITEM_TERM.ACAD_CAR_CD
 and I.ITEM_TERM = T0_ITEM_TERM.TERM_CD
 and I.SRC_SYS_ID = T0_ITEM_TERM.SRC_SYS_ID
 and T0_ITEM_TERM.DATA_ORIGIN <> 'D'
left outer join CSMRT_OWNER.PS_D_TERM T1_ACCOUNT_TERM
 on I.BUSINESS_UNIT = T1_ACCOUNT_TERM.INSTITUTION_CD
 /*Since term_cd and descr do not change with career, the code adjusted to pick any one sid*/
 and CASE WHEN(I.SMT_ACAD_CAREER = '-') THEN('UGRD') ELSE(I.SMT_ACAD_CAREER) END = T1_ACCOUNT_TERM.ACAD_CAR_CD
 and I.ACCOUNT_TERM = T1_ACCOUNT_TERM.TERM_CD
 and I.SRC_SYS_ID = T1_ACCOUNT_TERM.SRC_SYS_ID
 and T1_ACCOUNT_TERM.DATA_ORIGIN <> 'D'
left outer join CSMRT_OWNER.PS_D_ITEM_TYPE Y
  on I.BUSINESS_UNIT = Y.SETID
 and I.ITEM_TYPE = Y.ITEM_TYPE_ID
 and I.SRC_SYS_ID = Y.SRC_SYS_ID
 and Y.DATA_ORIGIN <> 'D'
left outer join X X1
  on X1.FIELDNAME = 'DUE_DATE_BY'
 and X1.FIELDVALUE = I.DUE_DATE_BY
 and X1.SRC_SYS_ID = I.SRC_SYS_ID
 and X1.X_ORDER = 1
left outer join X X2
  on X2.FIELDNAME = 'ITEM_TYPE_CD'
 and X2.FIELDVALUE = I.ITEM_TYPE_CD
 and X2.SRC_SYS_ID = I.SRC_SYS_ID
 and X2.X_ORDER = 1
left outer join X X3
  on X3.FIELDNAME = 'LINE_STATUS'
 and X3.FIELDVALUE = I.LINE_STATUS
 and X3.SRC_SYS_ID = I.SRC_SYS_ID
 and X3.X_ORDER = 1
left outer join X X4
  on X4.FIELDNAME = 'LINE_ACTION'
 and X4.FIELDVALUE = I.LINE_ACTION
 and X4.SRC_SYS_ID = I.SRC_SYS_ID
 and X4.X_ORDER = 1
left outer join RSN
  on I.BUSINESS_UNIT = RSN.SETID
 and I.LINE_REASON_CD = RSN.LINE_REASON_CD
 and I.SRC_SYS_ID = RSN.SRC_SYS_ID
 and RSN.RSN_ORDER = 1
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_SF_ITEM_LINE rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_SF_ITEM_LINE',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_SF_ITEM_LINE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_SF_ITEM_LINE enable constraint PK_UM_F_SF_ITEM_LINE';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_SF_ITEM_LINE');

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

END UM_F_SF_ITEM_LINE_P;
/
