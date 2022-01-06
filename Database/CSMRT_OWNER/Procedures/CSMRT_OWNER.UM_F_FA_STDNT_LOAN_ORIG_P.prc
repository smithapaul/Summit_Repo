CREATE OR REPLACE PROCEDURE             "UM_F_FA_STDNT_LOAN_ORIG_P" AUTHID CURRENT_USER IS


------------------------------------------------------------------------
-- George Adams
--
-- Loads table UM_F_FA_STDNT_LOAN_ORIG.
--
 --V01  SMT-xxxx 07/06/2018,    James Doucette
--                              Converted from SQL Script
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_FA_STDNT_LOAN_ORIG';
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

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_FA_STDNT_LOAN_ORIG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_FA_STDNT_LOAN_ORIG');

--alter table UM_F_FA_STDNT_LOAN_ORIG disable constraint PK_UM_F_FA_STDNT_LOAN_ORIG;
strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_FA_STDNT_LOAN_ORIG disable constraint PK_UM_F_FA_STDNT_LOAN_ORIG';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
				
strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_FA_STDNT_LOAN_ORIG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_FA_STDNT_LOAN_ORIG';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );


strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_FA_STDNT_LOAN_ORIG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_FA_STDNT_LOAN_ORIG';				
insert /*+ append */ into UM_F_FA_STDNT_LOAN_ORIG
with XL as (select /*+ materialize */
                   FIELDNAME, FIELDVALUE, SRC_SYS_ID, XLATLONGNAME, XLATSHORTNAME
              from UM_D_XLATITEM
             where SRC_SYS_ID = 'CS90') 
select /*+ PARALLEL(8) INLINE */
       O.INSTITUTION INSTITUTION_CD, O.ACAD_CAREER ACAD_CAR_CD, O.AID_YEAR, O.EMPLID PERSON_ID, O.LOAN_TYPE, O.LN_APPL_SEQ, D.ITEM_TYPE, O.SRC_SYS_ID, 
       I.INSTITUTION_SID, 
       nvl(C.ACAD_CAR_SID,2147483646) ACAD_CAR_SID, 
       nvl(P.PERSON_SID,2147483646) PERSON_SID, 
       nvl(I2.ITEM_TYPE_SID,2147483646) ITEM_TYPE_SID, 
       nvl(L.DESCRSHORT,'-') LOAN_TYPE_SD, 
       nvl(L.DESCR,'-') LOAN_TYPE_LD, 
       D.LN_APPL_ID, D.DISBURSEMENT_PLAN, D.SPLIT_CODE, D.LOAN_INTEREST_ATTR, D.LN_BORRQSTD_DTL, D.LN_AMT_CERTIFIED, D.LN_ANTIC_FEE_TOT, D.LN_ANTIC_REBATE, D.LN_ANTIC_NET_TOT, D.LN_AMT_APPROVED, 
       D.ACCEPT_AMOUNT, D.OFFER_AMOUNT, D.LN_FEE_AMT, D.LN_REBATE_AMT, D.LN_TRNS_OFFER_AMT, D.LN_TRNS_ACCEPT_AMT, D.LN_TRNS_CERT_AMT, D.LN_TRNS_BQSTD_DTL, 
       D.LOAN_PROC_STAT, 
       nvl(X1.XLATSHORTNAME,'-') LOAN_PROC_STAT_SD, 
       nvl(X1.XLATLONGNAME,'-') LOAN_PROC_STAT_LD, 
       D.LN_ORIG_TRANS, 
       nvl(X2.XLATSHORTNAME,'-') LN_ORIG_TRANS_SD, 
       nvl(X2.XLATLONGNAME,'-') LN_ORIG_TRANS_LD, 
       O.BORR_EMPLID, 
       O.NSLDS_LOAN_YEAR, 
       nvl(X3.XLATSHORTNAME,'-') NSLDS_LOAN_YEAR_SD, 
       nvl(X3.XLATLONGNAME,'-') NSLDS_LOAN_YEAR_LD, 
       O.DIR_LND_YR, O.ACADEMIC_LEVEL, O.LN_DEST_NBR, O.LN_AMT_BORRQSTD, O.LN_TOTAL_CERT, O.LN_CERT_DT, 
       O.LOAN_CREDIT_CHK, 
       nvl(X4.XLATSHORTNAME,'-') LOAN_CREDIT_CHK_SD, 
       nvl(X4.XLATLONGNAME,'-') LOAN_CREDIT_CHK_LD, 
       O.LOAN_CRDT_OVRID, O.LOAN_CRDT_DT, O.ACADEMIC_LOAD, O.LN_PRGCMPLT_DT, O.EXP_GRAD_TERM, 
       O.LN_PERIOD_START, O.LN_PERIOD_END, O.LN_PERIOD_OVRD, O.LN_UPDT_DMGRPHC, O.LN_REFUND_INDC, O.LN_BORR_DFLT_RFND, O.LN_STDNT_DFLT_RFND, 
       O.SSN, O.BORR_SSN, O.DRIVERS_LIC_NBR, O.BORR_DRIVER_LIC_ST, O.BIRTHDATE, O.BORR_BIRTHDATE, O.LN_BORR_DOB_CHGDT, 
       O.VISA_WRKPMT_NBR, O.BORR_VISA_WKPT_NBR, O.CITIZENSHIP_STATUS, O.BORR_CTZNSHP_STAT, O.DL_DISC_PRT_IND, O.FED_DEPEND_STAT, O.LN_DEST_PROC_LVL, 
       O.LN_BOOK_STAT, 
       nvl(X5.XLATSHORTNAME,'-') LN_BOOK_STAT_SD, 
       nvl(X5.XLATLONGNAME,'-') LN_BOOK_STAT_LD, 
       O.LN_BOOK_DT, O.DL_ADD_USUB_SW, O.LN_MPN_SEQ, O.PHONE, O.LN_PHONE_OVRD, 
       O.LN_TRNS_PER_ST, O.LN_TRNS_PER_END, O.LN_TRNS_DEP_STAT, O.LN_TRNS_HEAL_SW, O.LN_TRNS_USUB_SW, O.LN_TRNS_DIR_YR, O.LN_TRNS_NSLDS_YEAR, O.LN_TRNS_PRGCMPLT, 
       O.LN_TRNS_BORR_CIT, O.LN_TRNS_BORR_DFLT, O.LN_TRNS_BORR_DOB, O.LN_TRNS_BORR_SSN, O.LN_TRNS_BORR_VISA, 
       O.LN_TRNS_DRIVER_LIC, O.LN_TRNS_DRIVER_ST, O.LN_TRNS_STU_CIT, O.LN_TRNS_STU_DFLT, O.LN_TRNS_STU_DOB, O.LN_TRNS_STU_SSN, 
       O.LN_TRNS_STU_VISA, O.LN_TRNS_PRNT_OPTN, O.LN_TRNS_DISC_PRT, O.LN_TRNS_DL_FEE, O.LN_TRNS_ACAD_ST, O.LN_TRNS_ACAD_END, O.LN_TRNS_PHONE, 
       O.DL_ORIG_FEE, O.ACAD_YEAR_START, O.ACAD_YEAR_END, O.LN_ORIG_PNOTE_STAT, O.DL_PNOTE_ID, O.LN_FED_DEP_ST_OVRD, O.LN_ACAD_END_OVRD, O.LN_ACAD_ST_OVRD, 
       O.LN_BORR_CIT_ST_OVR, O.LN_BORR_VISA_OVRD, O.LN_BORR_DOB_OVRD, O.LN_BORR_SSN_OVRD, O.LN_ENDORS_AMT, 
       O.SFA_COD_MPN_EXPIRE, O.SFA_PP_CRSEWRK_SW, O.SFA_TRNS_PP_CRS_SW, O.SFA_LN_STU_CIT_OVR, O.SFA_ATB_CD, 
       O.SFA_TRNS_ATB_CD, O.SFA_ATB_COMP_DT, O.SFA_PROG_LENGTH_MN, O.SFA_PROG_LENGTH_WK, O.SFA_PROG_LENGTH_YR, O.SFA_WK_PROG_ACADYR, O.SFA_SPEC_PROG_FLG, O.SFA_COD_CRED_LVL, 
       O.SFA_TRNS_PROGLN_MN, O.SFA_TRNS_PROGLN_WK, O.SFA_TRNS_PROGLN_YR, O.SFA_TRNS_WKPRGACYR, O.SFA_TRNS_SPPRG_FLG, O.SFA_TRNS_COD_CRDLV, 
       O.SFA_PROGLN_MN_OVRD, O.SFA_PROGLN_WK_OVRD, O.SFA_PROGLN_YR_OVRD, O.SFA_WKPRGACYR_OVRD, O.SFA_SPEC_PROG_OVRD, O.SFA_COD_CRDLV_OVRD, O.SFA_DL_CRD_DEC_ORG, O.SFA_DL_APPEAL_STAT, 
       O.SFA_DL_CRD_EXP_DT, O.SFA_DL_CRDT_ACTNST, O.SFA_DL_RECONS_ELG, O.SFA_DL_PLUS_CNSLCP, O.SFA_DL_PLUS_CNSLDT, O.SFA_DL_CREDIT_REQ, O.SFA_DL_ENDORS_APPR, O.SFA_DL_CRD_DEC_ST, 
       'N' LOAD_ERROR, 'S' DATA_ORIGIN, SYSDATE CREATED_EW_DTTM, SYSDATE LASTUPD_EW_DTTM, 1234 BATCH_SID
  from CSSTG_OWNER.PS_LOAN_ORIGNATN O
  join CSSTG_OWNER.PS_LOAN_ORIG_DTL D
    on O.EMPLID = D.EMPLID
   and O.INSTITUTION = D.INSTITUTION
   and O.AID_YEAR = D.AID_YEAR
   and O.ACAD_CAREER = D.ACAD_CAREER
   and O.LOAN_TYPE = D.LOAN_TYPE
   and O.LN_APPL_SEQ = D.LN_APPL_SEQ
   and O.SRC_SYS_ID = D.SRC_SYS_ID
   and D.DATA_ORIGIN <> 'D'
  join CSMRT_OWNER.PS_D_INSTITUTION I
    on O.INSTITUTION = I.INSTITUTION_CD
   and O.SRC_SYS_ID = I.SRC_SYS_ID
  left outer join CSMRT_OWNER.PS_D_ACAD_CAR C
    on O.INSTITUTION = C.INSTITUTION_CD
   and O.ACAD_CAREER = C.ACAD_CAR_CD
   and O.SRC_SYS_ID = C.SRC_SYS_ID
  left outer join CSMRT_OWNER.PS_D_PERSON P
    on O.EMPLID = P.PERSON_ID
   and O.SRC_SYS_ID = P.SRC_SYS_ID
  left outer join CSMRT_OWNER.PS_D_ITEM_TYPE I2
    on D.INSTITUTION = I2.SETID
   and D.ITEM_TYPE = I2.ITEM_TYPE_ID
   and D.SRC_SYS_ID = I2.SRC_SYS_ID
  left outer join CSSTG_OWNER.PS_LN_TYPE_TBL L
    on O.INSTITUTION = L.INSTITUTION
   and O.AID_YEAR = L.AID_YEAR
   and O.LOAN_TYPE = L.LOAN_TYPE
   and O.SRC_SYS_ID = L.SRC_SYS_ID
   and L.DATA_ORIGIN <> 'D'
  left outer join XL X1
    on X1.FIELDNAME = 'LOAN_PROC_STAT'
   and X1.FIELDVALUE = D.LOAN_PROC_STAT 
   and X1.SRC_SYS_ID = D.SRC_SYS_ID
  left outer join XL X2
    on X2.FIELDNAME = 'LN_ORIG_TRANS'
   and X2.FIELDVALUE = D.LN_ORIG_TRANS 
   and X2.SRC_SYS_ID = D.SRC_SYS_ID
  left outer join XL X3
    on X3.FIELDNAME = 'NSLDS_LOAN_YEAR'
   and X3.FIELDVALUE = O.NSLDS_LOAN_YEAR 
   and X3.SRC_SYS_ID = O.SRC_SYS_ID
  left outer join XL X4
    on X4.FIELDNAME = 'LOAN_CREDIT_CHK'
   and X4.FIELDVALUE = O.LOAN_CREDIT_CHK 
   and X4.SRC_SYS_ID = O.SRC_SYS_ID
  left outer join XL X5
    on X5.FIELDNAME = 'LN_BOOK_STAT'
   and X5.FIELDVALUE = O.LN_BOOK_STAT 
   and X5.SRC_SYS_ID = O.SRC_SYS_ID
 where O.DATA_ORIGIN <> 'D'
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_FA_STDNT_LOAN_ORIG rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_FA_STDNT_LOAN_ORIG',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );



strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_FA_STDNT_LOAN_ORIG',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_FA_STDNT_LOAN_ORIG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
--alter table UM_F_FA_STDNT_LOAN_ORIG enable constraint PK_UM_F_FA_STDNT_LOAN_ORIG;

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_FA_STDNT_LOAN_ORIG enable constraint PK_UM_F_FA_STDNT_LOAN_ORIG';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_FA_STDNT_LOAN_ORIG');

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

END UM_F_FA_STDNT_LOAN_ORIG_P;
/
