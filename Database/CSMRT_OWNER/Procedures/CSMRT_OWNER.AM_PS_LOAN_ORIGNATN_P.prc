DROP PROCEDURE CSMRT_OWNER.AM_PS_LOAN_ORIGNATN_P
/

--
-- AM_PS_LOAN_ORIGNATN_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."AM_PS_LOAN_ORIGNATN_P" IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_LOAN_ORIGNATN from PeopleSoft table PS_LOAN_ORIGNATN.
--
-- V01  SMT-xxxx 04/04/2017,    Jim Doucette
--                              Converted from PS_LOAN_ORIGNATN.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'AM_PS_LOAN_ORIGNATN';
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
 where TABLE_NAME = 'PS_LOAN_ORIGNATN'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_LOAN_ORIGNATN@AMSOURCE S)
 where TABLE_NAME = 'PS_LOAN_ORIGNATN'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into AMSTG_OWNER.PS_LOAN_ORIGNATN';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into AMSTG_OWNER.PS_LOAN_ORIGNATN';
merge /*+ use_hash(S,T) */ into AMSTG_OWNER.PS_LOAN_ORIGNATN T
using (select /*+ full(S) */
nvl(trim(EMPLID),'-') EMPLID,
nvl(trim(INSTITUTION),'-') INSTITUTION,
nvl(trim(AID_YEAR),'-') AID_YEAR,
nvl(trim(ACAD_CAREER),'-') ACAD_CAREER,
nvl(trim(LOAN_TYPE),'-') LOAN_TYPE,
nvl(LN_APPL_SEQ,0) LN_APPL_SEQ,
nvl(trim(BORR_EMPLID),'-') BORR_EMPLID,
nvl(trim(FED_OR_INST_COST),'-') FED_OR_INST_COST,
nvl(FED_EFC,0) FED_EFC,
nvl(INST_EFC,0) INST_EFC,
nvl(FED_YEAR_COA,0) FED_YEAR_COA,
nvl(INST_YEAR_COA,0) INST_YEAR_COA,
nvl(LN_FIN_AID,0) LN_FIN_AID,
nvl(LN_NET_COST,0) LN_NET_COST,
nvl(trim(NSLDS_LOAN_YEAR),'-') NSLDS_LOAN_YEAR,
nvl(trim(DIR_LND_YR),'-') DIR_LND_YR,
nvl(trim(ACADEMIC_LEVEL),'-') ACADEMIC_LEVEL,
nvl(LN_DEST_NBR,0) LN_DEST_NBR,
nvl(LEND_UNQ_ID,0) LEND_UNQ_ID,
nvl(GUAR_UNQ_ID,0) GUAR_UNQ_ID,
nvl(trim(GUAR_LN_ID),'-') GUAR_LN_ID,
nvl(trim(LN_EFT_AUTHRZTN),'-') LN_EFT_AUTHRZTN,
nvl(trim(LN_REQ_DEFRMNT),'-') LN_REQ_DEFRMNT,
nvl(trim(CAPTLZ_INTRST),'-') CAPTLZ_INTRST,
nvl(LN_AMT_BORRQSTD,0) LN_AMT_BORRQSTD,
nvl(LN_PNT_BORRQSTD,0) LN_PNT_BORRQSTD,
nvl(LN_TOTAL_CERT,0) LN_TOTAL_CERT,
to_date(to_char(case when LN_CERT_DT < '01-JAN-1800' then NULL else LN_CERT_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') LN_CERT_DT,
nvl(trim(LNDR_LAST_RSRT),'-') LNDR_LAST_RSRT,
nvl(trim(LOAN_CREDIT_CHK),'-') LOAN_CREDIT_CHK,
nvl(trim(LOAN_CRDT_OVRID),'-') LOAN_CRDT_OVRID,
to_date(to_char(case when LOAN_CRDT_DT < '01-JAN-1800' then NULL else LOAN_CRDT_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') LOAN_CRDT_DT,
nvl(trim(SCH_LNAPPL_ID),'-') SCH_LNAPPL_ID,
nvl(trim(ACADEMIC_LOAD),'-') ACADEMIC_LOAD,
to_date(to_char(case when LN_PRGCMPLT_DT < '01-JAN-1800' then NULL else LN_PRGCMPLT_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') LN_PRGCMPLT_DT,
nvl(trim(EXP_GRAD_TERM),'-') EXP_GRAD_TERM,
to_date(to_char(case when LN_PERIOD_START < '01-JAN-1800' then NULL else LN_PERIOD_START end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') LN_PERIOD_START,
to_date(to_char(case when LN_PERIOD_END < '01-JAN-1800' then NULL else LN_PERIOD_END end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') LN_PERIOD_END,
nvl(trim(LN_PERIOD_OVRD),'-') LN_PERIOD_OVRD,
nvl(trim(LN_UPDT_DMGRPHC),'-') LN_UPDT_DMGRPHC,
nvl(LOAN_AMT_COSIGN,0) LOAN_AMT_COSIGN,
nvl(trim(LN_ORIG_MANUAL),'-') LN_ORIG_MANUAL,
nvl(trim(LN_REFUND_INDC),'-') LN_REFUND_INDC,
nvl(trim(LN_BORR_DFLT_RFND),'-') LN_BORR_DFLT_RFND,
nvl(trim(LN_STDNT_DFLT_RFND),'-') LN_STDNT_DFLT_RFND,
nvl(LN_TOT_LOAN_DEBT,0) LN_TOT_LOAN_DEBT,
nvl(trim(SSN),'-') SSN,
nvl(trim(BORR_SSN),'-') BORR_SSN,
to_date(to_char(case when LN_BORR_SSN_CHGDT < '01-JAN-1800' then NULL else LN_BORR_SSN_CHGDT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') LN_BORR_SSN_CHGDT,
nvl(trim(DRIVERS_LIC_NBR),'-') DRIVERS_LIC_NBR,
nvl(trim(BORR_DRIVER_LIC_ST),'-') BORR_DRIVER_LIC_ST,
to_date(to_char(case when BIRTHDATE < '01-JAN-1800' then NULL else BIRTHDATE end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') BIRTHDATE,
to_date(to_char(case when BORR_BIRTHDATE < '01-JAN-1800' then NULL else BORR_BIRTHDATE end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') BORR_BIRTHDATE,
to_date(to_char(case when LN_BORR_DOB_CHGDT < '01-JAN-1800' then NULL else LN_BORR_DOB_CHGDT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') LN_BORR_DOB_CHGDT,
nvl(trim(VISA_WRKPMT_NBR),'-') VISA_WRKPMT_NBR,
nvl(trim(BORR_VISA_WKPT_NBR),'-') BORR_VISA_WKPT_NBR,
nvl(trim(CITIZENSHIP_STATUS),'-') CITIZENSHIP_STATUS,
nvl(trim(BORR_CTZNSHP_STAT),'-') BORR_CTZNSHP_STAT,
nvl(trim(LN_PRINT_OPTN),'-') LN_PRINT_OPTN,
nvl(trim(DL_DISC_PRT_IND),'-') DL_DISC_PRT_IND,
nvl(trim(INST_DEPEND_STAT),'-') INST_DEPEND_STAT,
nvl(trim(FED_DEPEND_STAT),'-') FED_DEPEND_STAT,
nvl(trim(CURRENCY_CD),'-') CURRENCY_CD,
nvl(trim(LN_DEST_PROC_LVL),'-') LN_DEST_PROC_LVL,
nvl(trim(LN_BOOK_STAT),'-') LN_BOOK_STAT,
to_date(to_char(case when LN_BOOK_DT < '01-JAN-1800' then NULL else LN_BOOK_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') LN_BOOK_DT,
nvl(trim(DL_ADD_USUB_SW),'-') DL_ADD_USUB_SW,
nvl(trim(DL_HEAL_LN_SW),'-') DL_HEAL_LN_SW,
nvl(trim(LN_VERSION),'-') LN_VERSION,
nvl(trim(LN_RPT_FORM_ID),'-') LN_RPT_FORM_ID,
nvl(PNOTE_PRINT_SEQ,0) PNOTE_PRINT_SEQ,
nvl(trim(LN_MPN_SEQ),'-') LN_MPN_SEQ,
nvl(trim(PHONE),'-') PHONE,
nvl(trim(LN_PHONE_OVRD),'-') LN_PHONE_OVRD,
to_date(to_char(case when LN_TRNS_PER_ST < '01-JAN-1800' then NULL else LN_TRNS_PER_ST end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') LN_TRNS_PER_ST,
to_date(to_char(case when LN_TRNS_PER_END < '01-JAN-1800' then NULL else LN_TRNS_PER_END end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') LN_TRNS_PER_END,
nvl(trim(LN_TRNS_DEP_STAT),'-') LN_TRNS_DEP_STAT,
nvl(trim(LN_TRNS_HEAL_SW),'-') LN_TRNS_HEAL_SW,
nvl(trim(LN_TRNS_USUB_SW),'-') LN_TRNS_USUB_SW,
nvl(trim(LN_TRNS_DIR_YR),'-') LN_TRNS_DIR_YR,
nvl(trim(LN_TRNS_NSLDS_YEAR),'-') LN_TRNS_NSLDS_YEAR,
to_date(to_char(case when LN_TRNS_PRGCMPLT < '01-JAN-1800' then NULL else LN_TRNS_PRGCMPLT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') LN_TRNS_PRGCMPLT,
nvl(trim(LN_TRNS_BORR_CIT),'-') LN_TRNS_BORR_CIT,
nvl(trim(LN_TRNS_BORR_DFLT),'-') LN_TRNS_BORR_DFLT,
to_date(to_char(case when LN_TRNS_BORR_DOB < '01-JAN-1800' then NULL else LN_TRNS_BORR_DOB end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') LN_TRNS_BORR_DOB,
nvl(trim(LN_TRNS_BORR_SSN),'-') LN_TRNS_BORR_SSN,
nvl(trim(LN_TRNS_BORR_VISA),'-') LN_TRNS_BORR_VISA,
nvl(trim(LN_TRNS_DRIVER_LIC),'-') LN_TRNS_DRIVER_LIC,
nvl(trim(LN_TRNS_DRIVER_ST),'-') LN_TRNS_DRIVER_ST,
nvl(trim(LN_TRNS_STU_CIT),'-') LN_TRNS_STU_CIT,
nvl(trim(LN_TRNS_STU_DFLT),'-') LN_TRNS_STU_DFLT,
to_date(to_char(case when LN_TRNS_STU_DOB < '01-JAN-1800' then NULL else LN_TRNS_STU_DOB end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') LN_TRNS_STU_DOB,
nvl(trim(LN_TRNS_STU_SSN),'-') LN_TRNS_STU_SSN,
nvl(trim(LN_TRNS_STU_VISA),'-') LN_TRNS_STU_VISA,
nvl(trim(LN_TRNS_PRNT_OPTN),'-') LN_TRNS_PRNT_OPTN,
nvl(trim(LN_TRNS_DISC_PRT),'-') LN_TRNS_DISC_PRT,
nvl(LN_TRNS_DL_FEE,0) LN_TRNS_DL_FEE,
to_date(to_char(case when LN_TRNS_ACAD_ST < '01-JAN-1800' then NULL else LN_TRNS_ACAD_ST end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') LN_TRNS_ACAD_ST,
to_date(to_char(case when LN_TRNS_ACAD_END < '01-JAN-1800' then NULL else LN_TRNS_ACAD_END end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') LN_TRNS_ACAD_END,
nvl(trim(LN_TRNS_PHONE),'-') LN_TRNS_PHONE,
to_date(to_char(case when LN_TRNS_DOB_CHGDT < '01-JAN-1800' then NULL else LN_TRNS_DOB_CHGDT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') LN_TRNS_DOB_CHGDT,
to_date(to_char(case when LN_TRNS_SSN_CHGDT < '01-JAN-1800' then NULL else LN_TRNS_SSN_CHGDT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') LN_TRNS_SSN_CHGDT,
nvl(trim(INTEREST_RATE_OPT),'-') INTEREST_RATE_OPT,
nvl(trim(REPAYMENT_OPT_CD),'-') REPAYMENT_OPT_CD,
nvl(trim(CL_ACT_INT_RATE),'-') CL_ACT_INT_RATE,
nvl(trim(CL_LNDR_USE),'-') CL_LNDR_USE,
nvl(trim(CL_SERIAL_LN_CD),'-') CL_SERIAL_LN_CD,
nvl(trim(CL_MPN_CONFIRM_CD),'-') CL_MPN_CONFIRM_CD,
nvl(trim(CL_BORR_CONFIRM_IN),'-') CL_BORR_CONFIRM_IN,
nvl(trim(CL_DIFF_NAME_CD),'-') CL_DIFF_NAME_CD,
nvl(trim(CL_FED_APP_FRM_CD),'-') CL_FED_APP_FRM_CD,
nvl(DL_ORIG_FEE,0) DL_ORIG_FEE,
to_date(to_char(case when ACAD_YEAR_START < '01-JAN-1800' then NULL else ACAD_YEAR_START end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') ACAD_YEAR_START,
to_date(to_char(case when ACAD_YEAR_END < '01-JAN-1800' then NULL else ACAD_YEAR_END end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') ACAD_YEAR_END,
nvl(trim(LN_ORIG_PNOTE_STAT),'-') LN_ORIG_PNOTE_STAT,
nvl(trim(DL_PNOTE_ID),'-') DL_PNOTE_ID,
nvl(trim(LN_FED_DEP_ST_OVRD),'-') LN_FED_DEP_ST_OVRD,
nvl(trim(LN_ACAD_END_OVRD),'-') LN_ACAD_END_OVRD,
nvl(trim(LN_ACAD_ST_OVRD),'-') LN_ACAD_ST_OVRD,
nvl(trim(LN_BORR_CIT_ST_OVR),'-') LN_BORR_CIT_ST_OVR,
nvl(trim(LN_BORR_VISA_OVRD),'-') LN_BORR_VISA_OVRD,
nvl(trim(LN_BORR_DOB_OVRD),'-') LN_BORR_DOB_OVRD,
nvl(trim(LN_BORR_SSN_OVRD),'-') LN_BORR_SSN_OVRD,
nvl(LN_ENDORS_AMT,0) LN_ENDORS_AMT,
to_date(to_char(case when SFA_COD_MPN_EXPIRE < '01-JAN-1800' then NULL else SFA_COD_MPN_EXPIRE end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') SFA_COD_MPN_EXPIRE,
nvl(trim(SFA_PP_CRSEWRK_SW),'-') SFA_PP_CRSEWRK_SW,
nvl(trim(SFA_TRNS_PP_CRS_SW),'-') SFA_TRNS_PP_CRS_SW,
nvl(trim(SFA_LN_STU_CIT_OVR),'-') SFA_LN_STU_CIT_OVR,
nvl(trim(SFA_ATB_CD),'-') SFA_ATB_CD,
nvl(trim(SFA_TRNS_ATB_CD),'-') SFA_TRNS_ATB_CD,
nvl(trim(SFA_ATB_TST_ADM_CD),'-') SFA_ATB_TST_ADM_CD,
nvl(trim(SFA_TRNS_ATB_TADMC),'-') SFA_TRNS_ATB_TADMC,
nvl(trim(SFA_ATB_TST_CD),'-') SFA_ATB_TST_CD,
nvl(trim(SFA_TRNS_ATB_TSTCD),'-') SFA_TRNS_ATB_TSTCD,
nvl(trim(SFA_ATB_STATE_CD),'-') SFA_ATB_STATE_CD,
nvl(trim(SFA_TRNS_ATB_STCD),'-') SFA_TRNS_ATB_STCD,
to_date(to_char(case when SFA_ATB_COMP_DT < '01-JAN-1800' then NULL else SFA_ATB_COMP_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') SFA_ATB_COMP_DT,
to_date(to_char(case when SFA_TRNS_ATB_CMPDT < '01-JAN-1800' then NULL else SFA_TRNS_ATB_CMPDT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') SFA_TRNS_ATB_CMPDT,
nvl(trim(SFA_STU_SSN_OVRD),'-') SFA_STU_SSN_OVRD,
nvl(trim(SFA_BORR_SSN_OVRD),'-') SFA_BORR_SSN_OVRD,
nvl(SFA_PROG_LENGTH_MN,0) SFA_PROG_LENGTH_MN,
nvl(SFA_PROG_LENGTH_WK,0) SFA_PROG_LENGTH_WK,
nvl(SFA_PROG_LENGTH_YR,0) SFA_PROG_LENGTH_YR,
nvl(SFA_WK_PROG_ACADYR,0) SFA_WK_PROG_ACADYR,
nvl(trim(SFA_SPEC_PROG_FLG),'-') SFA_SPEC_PROG_FLG,
nvl(trim(SFA_COD_CRED_LVL),'-') SFA_COD_CRED_LVL,
nvl(SFA_TRNS_PROGLN_MN,0) SFA_TRNS_PROGLN_MN,
nvl(SFA_TRNS_PROGLN_WK,0) SFA_TRNS_PROGLN_WK,
nvl(SFA_TRNS_PROGLN_YR,0) SFA_TRNS_PROGLN_YR,
nvl(SFA_TRNS_WKPRGACYR,0) SFA_TRNS_WKPRGACYR,
nvl(trim(SFA_TRNS_SPPRG_FLG),'-') SFA_TRNS_SPPRG_FLG,
nvl(trim(SFA_TRNS_COD_CRDLV),'-') SFA_TRNS_COD_CRDLV,
nvl(trim(SFA_PROGLN_MN_OVRD),'-') SFA_PROGLN_MN_OVRD,
nvl(trim(SFA_PROGLN_WK_OVRD),'-') SFA_PROGLN_WK_OVRD,
nvl(trim(SFA_PROGLN_YR_OVRD),'-') SFA_PROGLN_YR_OVRD,
nvl(trim(SFA_WKPRGACYR_OVRD),'-') SFA_WKPRGACYR_OVRD,
nvl(trim(SFA_SPEC_PROG_OVRD),'-') SFA_SPEC_PROG_OVRD,
nvl(trim(SFA_COD_CRDLV_OVRD),'-') SFA_COD_CRDLV_OVRD,
nvl(trim(SFA_DL_CRD_DEC_ORG),'-') SFA_DL_CRD_DEC_ORG,
nvl(trim(SFA_DL_CRD_ACTN_CH),'-') SFA_DL_CRD_ACTN_CH,
nvl(trim(SFA_DL_APPEAL_STAT),'-') SFA_DL_APPEAL_STAT,
nvl(trim(SFA_DL_CRD_BAL_OPT),'-') SFA_DL_CRD_BAL_OPT,
to_date(to_char(case when SFA_DL_CRD_EXP_DT < '01-JAN-1800' then NULL else SFA_DL_CRD_EXP_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') SFA_DL_CRD_EXP_DT,
nvl(trim(SFA_DL_CRDT_ACTNST),'-') SFA_DL_CRDT_ACTNST,
nvl(trim(SFA_DL_RECONS_ELG),'-') SFA_DL_RECONS_ELG,
nvl(trim(SFA_DL_PLUS_CNSLCP),'-') SFA_DL_PLUS_CNSLCP,
to_date(to_char(case when SFA_DL_PLUS_CNSLDT < '01-JAN-1800' then NULL else SFA_DL_PLUS_CNSLDT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') SFA_DL_PLUS_CNSLDT,
nvl(trim(SFA_DL_PLUS_CNSLEX),'-') SFA_DL_PLUS_CNSLEX,
nvl(trim(SFA_DL_CREDIT_REQ),'-') SFA_DL_CREDIT_REQ,
nvl(trim(SFA_DL_ENDORS_APPR),'-') SFA_DL_ENDORS_APPR,
to_date(to_char(case when SFA_DL_PLUS_EXPDT < '01-JAN-1800' then NULL else SFA_DL_PLUS_EXPDT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') SFA_DL_PLUS_EXPDT,
nvl(trim(SFA_DL_CRD_DEC_ST),'-') SFA_DL_CRD_DEC_ST
  from SYSADM.PS_LOAN_ORIGNATN@AMSOURCE S
 where ORA_ROWSCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_LOAN_ORIGNATN')
   and EMPLID between '00000000' and '99999999'
   and length(EMPLID) = 8 ) S
   on (
T.EMPLID = S.EMPLID and
T.INSTITUTION = S.INSTITUTION and
T.AID_YEAR = S.AID_YEAR and
T.ACAD_CAREER = S.ACAD_CAREER and
T.LOAN_TYPE = S.LOAN_TYPE and
T.LN_APPL_SEQ = S.LN_APPL_SEQ and
T.SRC_SYS_ID = 'CS90')
when matched then update set
T.BORR_EMPLID = S.BORR_EMPLID,
T.FED_OR_INST_COST = S.FED_OR_INST_COST,
T.FED_EFC = S.FED_EFC,
T.INST_EFC = S.INST_EFC,
T.FED_YEAR_COA = S.FED_YEAR_COA,
T.INST_YEAR_COA = S.INST_YEAR_COA,
T.LN_FIN_AID = S.LN_FIN_AID,
T.LN_NET_COST = S.LN_NET_COST,
T.NSLDS_LOAN_YEAR = S.NSLDS_LOAN_YEAR,
T.DIR_LND_YR = S.DIR_LND_YR,
T.ACADEMIC_LEVEL = S.ACADEMIC_LEVEL,
T.LN_DEST_NBR = S.LN_DEST_NBR,
T.LEND_UNQ_ID = S.LEND_UNQ_ID,
T.GUAR_UNQ_ID = S.GUAR_UNQ_ID,
T.GUAR_LN_ID = S.GUAR_LN_ID,
T.LN_EFT_AUTHRZTN = S.LN_EFT_AUTHRZTN,
T.LN_REQ_DEFRMNT = S.LN_REQ_DEFRMNT,
T.CAPTLZ_INTRST = S.CAPTLZ_INTRST,
T.LN_AMT_BORRQSTD = S.LN_AMT_BORRQSTD,
T.LN_PNT_BORRQSTD = S.LN_PNT_BORRQSTD,
T.LN_TOTAL_CERT = S.LN_TOTAL_CERT,
T.LN_CERT_DT = S.LN_CERT_DT,
T.LNDR_LAST_RSRT = S.LNDR_LAST_RSRT,
T.LOAN_CREDIT_CHK = S.LOAN_CREDIT_CHK,
T.LOAN_CRDT_OVRID = S.LOAN_CRDT_OVRID,
T.LOAN_CRDT_DT = S.LOAN_CRDT_DT,
T.SCH_LNAPPL_ID = S.SCH_LNAPPL_ID,
T.ACADEMIC_LOAD = S.ACADEMIC_LOAD,
T.LN_PRGCMPLT_DT = S.LN_PRGCMPLT_DT,
T.EXP_GRAD_TERM = S.EXP_GRAD_TERM,
T.LN_PERIOD_START = S.LN_PERIOD_START,
T.LN_PERIOD_END = S.LN_PERIOD_END,
T.LN_PERIOD_OVRD = S.LN_PERIOD_OVRD,
T.LN_UPDT_DMGRPHC = S.LN_UPDT_DMGRPHC,
T.LOAN_AMT_COSIGN = S.LOAN_AMT_COSIGN,
T.LN_ORIG_MANUAL = S.LN_ORIG_MANUAL,
T.LN_REFUND_INDC = S.LN_REFUND_INDC,
T.LN_BORR_DFLT_RFND = S.LN_BORR_DFLT_RFND,
T.LN_STDNT_DFLT_RFND = S.LN_STDNT_DFLT_RFND,
T.LN_TOT_LOAN_DEBT = S.LN_TOT_LOAN_DEBT,
T.SSN = S.SSN,
T.BORR_SSN = S.BORR_SSN,
T.LN_BORR_SSN_CHGDT = S.LN_BORR_SSN_CHGDT,
T.DRIVERS_LIC_NBR = S.DRIVERS_LIC_NBR,
T.BORR_DRIVER_LIC_ST = S.BORR_DRIVER_LIC_ST,
T.BIRTHDATE = S.BIRTHDATE,
T.BORR_BIRTHDATE = S.BORR_BIRTHDATE,
T.LN_BORR_DOB_CHGDT = S.LN_BORR_DOB_CHGDT,
T.VISA_WRKPMT_NBR = S.VISA_WRKPMT_NBR,
T.BORR_VISA_WKPT_NBR = S.BORR_VISA_WKPT_NBR,
T.CITIZENSHIP_STATUS = S.CITIZENSHIP_STATUS,
T.BORR_CTZNSHP_STAT = S.BORR_CTZNSHP_STAT,
T.LN_PRINT_OPTN = S.LN_PRINT_OPTN,
T.DL_DISC_PRT_IND = S.DL_DISC_PRT_IND,
T.INST_DEPEND_STAT = S.INST_DEPEND_STAT,
T.FED_DEPEND_STAT = S.FED_DEPEND_STAT,
T.CURRENCY_CD = S.CURRENCY_CD,
T.LN_DEST_PROC_LVL = S.LN_DEST_PROC_LVL,
T.LN_BOOK_STAT = S.LN_BOOK_STAT,
T.LN_BOOK_DT = S.LN_BOOK_DT,
T.DL_ADD_USUB_SW = S.DL_ADD_USUB_SW,
T.DL_HEAL_LN_SW = S.DL_HEAL_LN_SW,
T.LN_VERSION = S.LN_VERSION,
T.LN_RPT_FORM_ID = S.LN_RPT_FORM_ID,
T.PNOTE_PRINT_SEQ = S.PNOTE_PRINT_SEQ,
T.LN_MPN_SEQ = S.LN_MPN_SEQ,
T.PHONE = S.PHONE,
T.LN_PHONE_OVRD = S.LN_PHONE_OVRD,
T.LN_TRNS_PER_ST = S.LN_TRNS_PER_ST,
T.LN_TRNS_PER_END = S.LN_TRNS_PER_END,
T.LN_TRNS_DEP_STAT = S.LN_TRNS_DEP_STAT,
T.LN_TRNS_HEAL_SW = S.LN_TRNS_HEAL_SW,
T.LN_TRNS_USUB_SW = S.LN_TRNS_USUB_SW,
T.LN_TRNS_DIR_YR = S.LN_TRNS_DIR_YR,
T.LN_TRNS_NSLDS_YEAR = S.LN_TRNS_NSLDS_YEAR,
T.LN_TRNS_PRGCMPLT = S.LN_TRNS_PRGCMPLT,
T.LN_TRNS_BORR_CIT = S.LN_TRNS_BORR_CIT,
T.LN_TRNS_BORR_DFLT = S.LN_TRNS_BORR_DFLT,
T.LN_TRNS_BORR_DOB = S.LN_TRNS_BORR_DOB,
T.LN_TRNS_BORR_SSN = S.LN_TRNS_BORR_SSN,
T.LN_TRNS_BORR_VISA = S.LN_TRNS_BORR_VISA,
T.LN_TRNS_DRIVER_LIC = S.LN_TRNS_DRIVER_LIC,
T.LN_TRNS_DRIVER_ST = S.LN_TRNS_DRIVER_ST,
T.LN_TRNS_STU_CIT = S.LN_TRNS_STU_CIT,
T.LN_TRNS_STU_DFLT = S.LN_TRNS_STU_DFLT,
T.LN_TRNS_STU_DOB = S.LN_TRNS_STU_DOB,
T.LN_TRNS_STU_SSN = S.LN_TRNS_STU_SSN,
T.LN_TRNS_STU_VISA = S.LN_TRNS_STU_VISA,
T.LN_TRNS_PRNT_OPTN = S.LN_TRNS_PRNT_OPTN,
T.LN_TRNS_DISC_PRT = S.LN_TRNS_DISC_PRT,
T.LN_TRNS_DL_FEE = S.LN_TRNS_DL_FEE,
T.LN_TRNS_ACAD_ST = S.LN_TRNS_ACAD_ST,
T.LN_TRNS_ACAD_END = S.LN_TRNS_ACAD_END,
T.LN_TRNS_PHONE = S.LN_TRNS_PHONE,
T.LN_TRNS_DOB_CHGDT = S.LN_TRNS_DOB_CHGDT,
T.LN_TRNS_SSN_CHGDT = S.LN_TRNS_SSN_CHGDT,
T.INTEREST_RATE_OPT = S.INTEREST_RATE_OPT,
T.REPAYMENT_OPT_CD = S.REPAYMENT_OPT_CD,
T.CL_ACT_INT_RATE = S.CL_ACT_INT_RATE,
T.CL_LNDR_USE = S.CL_LNDR_USE,
T.CL_SERIAL_LN_CD = S.CL_SERIAL_LN_CD,
T.CL_MPN_CONFIRM_CD = S.CL_MPN_CONFIRM_CD,
T.CL_BORR_CONFIRM_IN = S.CL_BORR_CONFIRM_IN,
T.CL_DIFF_NAME_CD = S.CL_DIFF_NAME_CD,
T.CL_FED_APP_FRM_CD = S.CL_FED_APP_FRM_CD,
T.DL_ORIG_FEE = S.DL_ORIG_FEE,
T.ACAD_YEAR_START = S.ACAD_YEAR_START,
T.ACAD_YEAR_END = S.ACAD_YEAR_END,
T.LN_ORIG_PNOTE_STAT = S.LN_ORIG_PNOTE_STAT,
T.DL_PNOTE_ID = S.DL_PNOTE_ID,
T.LN_FED_DEP_ST_OVRD = S.LN_FED_DEP_ST_OVRD,
T.LN_ACAD_END_OVRD = S.LN_ACAD_END_OVRD,
T.LN_ACAD_ST_OVRD = S.LN_ACAD_ST_OVRD,
T.LN_BORR_CIT_ST_OVR = S.LN_BORR_CIT_ST_OVR,
T.LN_BORR_VISA_OVRD = S.LN_BORR_VISA_OVRD,
T.LN_BORR_DOB_OVRD = S.LN_BORR_DOB_OVRD,
T.LN_BORR_SSN_OVRD = S.LN_BORR_SSN_OVRD,
T.LN_ENDORS_AMT = S.LN_ENDORS_AMT,
T.SFA_COD_MPN_EXPIRE = S.SFA_COD_MPN_EXPIRE,
T.SFA_PP_CRSEWRK_SW = S.SFA_PP_CRSEWRK_SW,
T.SFA_TRNS_PP_CRS_SW = S.SFA_TRNS_PP_CRS_SW,
T.SFA_LN_STU_CIT_OVR = S.SFA_LN_STU_CIT_OVR,
T.SFA_ATB_CD = S.SFA_ATB_CD,
T.SFA_TRNS_ATB_CD = S.SFA_TRNS_ATB_CD,
T.SFA_ATB_TST_ADM_CD = S.SFA_ATB_TST_ADM_CD,
T.SFA_TRNS_ATB_TADMC = S.SFA_TRNS_ATB_TADMC,
T.SFA_ATB_TST_CD = S.SFA_ATB_TST_CD,
T.SFA_TRNS_ATB_TSTCD = S.SFA_TRNS_ATB_TSTCD,
T.SFA_ATB_STATE_CD = S.SFA_ATB_STATE_CD,
T.SFA_TRNS_ATB_STCD = S.SFA_TRNS_ATB_STCD,
T.SFA_ATB_COMP_DT = S.SFA_ATB_COMP_DT,
T.SFA_TRNS_ATB_CMPDT = S.SFA_TRNS_ATB_CMPDT,
T.SFA_STU_SSN_OVRD = S.SFA_STU_SSN_OVRD,
T.SFA_BORR_SSN_OVRD = S.SFA_BORR_SSN_OVRD,
T.SFA_PROG_LENGTH_MN = S.SFA_PROG_LENGTH_MN,
T.SFA_PROG_LENGTH_WK = S.SFA_PROG_LENGTH_WK,
T.SFA_PROG_LENGTH_YR = S.SFA_PROG_LENGTH_YR,
T.SFA_WK_PROG_ACADYR = S.SFA_WK_PROG_ACADYR,
T.SFA_SPEC_PROG_FLG = S.SFA_SPEC_PROG_FLG,
T.SFA_COD_CRED_LVL = S.SFA_COD_CRED_LVL,
T.SFA_TRNS_PROGLN_MN = S.SFA_TRNS_PROGLN_MN,
T.SFA_TRNS_PROGLN_WK = S.SFA_TRNS_PROGLN_WK,
T.SFA_TRNS_PROGLN_YR = S.SFA_TRNS_PROGLN_YR,
T.SFA_TRNS_WKPRGACYR = S.SFA_TRNS_WKPRGACYR,
T.SFA_TRNS_SPPRG_FLG = S.SFA_TRNS_SPPRG_FLG,
T.SFA_TRNS_COD_CRDLV = S.SFA_TRNS_COD_CRDLV,
T.SFA_PROGLN_MN_OVRD = S.SFA_PROGLN_MN_OVRD,
T.SFA_PROGLN_WK_OVRD = S.SFA_PROGLN_WK_OVRD,
T.SFA_PROGLN_YR_OVRD = S.SFA_PROGLN_YR_OVRD,
T.SFA_WKPRGACYR_OVRD = S.SFA_WKPRGACYR_OVRD,
T.SFA_SPEC_PROG_OVRD = S.SFA_SPEC_PROG_OVRD,
T.SFA_COD_CRDLV_OVRD = S.SFA_COD_CRDLV_OVRD,
T.SFA_DL_CRD_DEC_ORG = S.SFA_DL_CRD_DEC_ORG,
T.SFA_DL_CRD_ACTN_CH = S.SFA_DL_CRD_ACTN_CH,
T.SFA_DL_APPEAL_STAT = S.SFA_DL_APPEAL_STAT,
T.SFA_DL_CRD_BAL_OPT = S.SFA_DL_CRD_BAL_OPT,
T.SFA_DL_CRD_EXP_DT = S.SFA_DL_CRD_EXP_DT,
T.SFA_DL_CRDT_ACTNST = S.SFA_DL_CRDT_ACTNST,
T.SFA_DL_RECONS_ELG = S.SFA_DL_RECONS_ELG,
T.SFA_DL_PLUS_CNSLCP = S.SFA_DL_PLUS_CNSLCP,
T.SFA_DL_PLUS_CNSLDT = S.SFA_DL_PLUS_CNSLDT,
T.SFA_DL_PLUS_CNSLEX = S.SFA_DL_PLUS_CNSLEX,
T.SFA_DL_CREDIT_REQ = S.SFA_DL_CREDIT_REQ,
T.SFA_DL_ENDORS_APPR = S.SFA_DL_ENDORS_APPR,
T.SFA_DL_PLUS_EXPDT = S.SFA_DL_PLUS_EXPDT,
T.SFA_DL_CRD_DEC_ST = S.SFA_DL_CRD_DEC_ST,
T.DATA_ORIGIN = 'S',
T.LASTUPD_EW_DTTM = sysdate,
T.BATCH_SID   = 1234
where
T.BORR_EMPLID <> S.BORR_EMPLID or
T.FED_OR_INST_COST <> S.FED_OR_INST_COST or
T.FED_EFC <> S.FED_EFC or
T.INST_EFC <> S.INST_EFC or
T.FED_YEAR_COA <> S.FED_YEAR_COA or
T.INST_YEAR_COA <> S.INST_YEAR_COA or
T.LN_FIN_AID <> S.LN_FIN_AID or
T.LN_NET_COST <> S.LN_NET_COST or
T.NSLDS_LOAN_YEAR <> S.NSLDS_LOAN_YEAR or
T.DIR_LND_YR <> S.DIR_LND_YR or
T.ACADEMIC_LEVEL <> S.ACADEMIC_LEVEL or
T.LN_DEST_NBR <> S.LN_DEST_NBR or
T.LEND_UNQ_ID <> S.LEND_UNQ_ID or
T.GUAR_UNQ_ID <> S.GUAR_UNQ_ID or
T.GUAR_LN_ID <> S.GUAR_LN_ID or
T.LN_EFT_AUTHRZTN <> S.LN_EFT_AUTHRZTN or
T.LN_REQ_DEFRMNT <> S.LN_REQ_DEFRMNT or
T.CAPTLZ_INTRST <> S.CAPTLZ_INTRST or
T.LN_AMT_BORRQSTD <> S.LN_AMT_BORRQSTD or
T.LN_PNT_BORRQSTD <> S.LN_PNT_BORRQSTD or
T.LN_TOTAL_CERT <> S.LN_TOTAL_CERT or
nvl(trim(T.LN_CERT_DT),0) <> nvl(trim(S.LN_CERT_DT),0) or
T.LNDR_LAST_RSRT <> S.LNDR_LAST_RSRT or
T.LOAN_CREDIT_CHK <> S.LOAN_CREDIT_CHK or
T.LOAN_CRDT_OVRID <> S.LOAN_CRDT_OVRID or
nvl(trim(T.LOAN_CRDT_DT),0) <> nvl(trim(S.LOAN_CRDT_DT),0) or
T.SCH_LNAPPL_ID <> S.SCH_LNAPPL_ID or
T.ACADEMIC_LOAD <> S.ACADEMIC_LOAD or
nvl(trim(T.LN_PRGCMPLT_DT),0) <> nvl(trim(S.LN_PRGCMPLT_DT),0) or
T.EXP_GRAD_TERM <> S.EXP_GRAD_TERM or
nvl(trim(T.LN_PERIOD_START),0) <> nvl(trim(S.LN_PERIOD_START),0) or
nvl(trim(T.LN_PERIOD_END),0) <> nvl(trim(S.LN_PERIOD_END),0) or
T.LN_PERIOD_OVRD <> S.LN_PERIOD_OVRD or
T.LN_UPDT_DMGRPHC <> S.LN_UPDT_DMGRPHC or
T.LOAN_AMT_COSIGN <> S.LOAN_AMT_COSIGN or
T.LN_ORIG_MANUAL <> S.LN_ORIG_MANUAL or
T.LN_REFUND_INDC <> S.LN_REFUND_INDC or
T.LN_BORR_DFLT_RFND <> S.LN_BORR_DFLT_RFND or
T.LN_STDNT_DFLT_RFND <> S.LN_STDNT_DFLT_RFND or
T.LN_TOT_LOAN_DEBT <> S.LN_TOT_LOAN_DEBT or
T.SSN <> S.SSN or
T.BORR_SSN <> S.BORR_SSN or
nvl(trim(T.LN_BORR_SSN_CHGDT),0) <> nvl(trim(S.LN_BORR_SSN_CHGDT),0) or
T.DRIVERS_LIC_NBR <> S.DRIVERS_LIC_NBR or
T.BORR_DRIVER_LIC_ST <> S.BORR_DRIVER_LIC_ST or
nvl(trim(T.BIRTHDATE),0) <> nvl(trim(S.BIRTHDATE),0) or
nvl(trim(T.BORR_BIRTHDATE),0) <> nvl(trim(S.BORR_BIRTHDATE),0) or
nvl(trim(T.LN_BORR_DOB_CHGDT),0) <> nvl(trim(S.LN_BORR_DOB_CHGDT),0) or
T.VISA_WRKPMT_NBR <> S.VISA_WRKPMT_NBR or
T.BORR_VISA_WKPT_NBR <> S.BORR_VISA_WKPT_NBR or
T.CITIZENSHIP_STATUS <> S.CITIZENSHIP_STATUS or
T.BORR_CTZNSHP_STAT <> S.BORR_CTZNSHP_STAT or
T.LN_PRINT_OPTN <> S.LN_PRINT_OPTN or
T.DL_DISC_PRT_IND <> S.DL_DISC_PRT_IND or
T.INST_DEPEND_STAT <> S.INST_DEPEND_STAT or
T.FED_DEPEND_STAT <> S.FED_DEPEND_STAT or
T.CURRENCY_CD <> S.CURRENCY_CD or
T.LN_DEST_PROC_LVL <> S.LN_DEST_PROC_LVL or
T.LN_BOOK_STAT <> S.LN_BOOK_STAT or
nvl(trim(T.LN_BOOK_DT),0) <> nvl(trim(S.LN_BOOK_DT),0) or
T.DL_ADD_USUB_SW <> S.DL_ADD_USUB_SW or
T.DL_HEAL_LN_SW <> S.DL_HEAL_LN_SW or
T.LN_VERSION <> S.LN_VERSION or
T.LN_RPT_FORM_ID <> S.LN_RPT_FORM_ID or
T.PNOTE_PRINT_SEQ <> S.PNOTE_PRINT_SEQ or
T.LN_MPN_SEQ <> S.LN_MPN_SEQ or
T.PHONE <> S.PHONE or
T.LN_PHONE_OVRD <> S.LN_PHONE_OVRD or
nvl(trim(T.LN_TRNS_PER_ST),0) <> nvl(trim(S.LN_TRNS_PER_ST),0) or
nvl(trim(T.LN_TRNS_PER_END),0) <> nvl(trim(S.LN_TRNS_PER_END),0) or
T.LN_TRNS_DEP_STAT <> S.LN_TRNS_DEP_STAT or
T.LN_TRNS_HEAL_SW <> S.LN_TRNS_HEAL_SW or
T.LN_TRNS_USUB_SW <> S.LN_TRNS_USUB_SW or
T.LN_TRNS_DIR_YR <> S.LN_TRNS_DIR_YR or
T.LN_TRNS_NSLDS_YEAR <> S.LN_TRNS_NSLDS_YEAR or
nvl(trim(T.LN_TRNS_PRGCMPLT),0) <> nvl(trim(S.LN_TRNS_PRGCMPLT),0) or
T.LN_TRNS_BORR_CIT <> S.LN_TRNS_BORR_CIT or
T.LN_TRNS_BORR_DFLT <> S.LN_TRNS_BORR_DFLT or
nvl(trim(T.LN_TRNS_BORR_DOB),0) <> nvl(trim(S.LN_TRNS_BORR_DOB),0) or
T.LN_TRNS_BORR_SSN <> S.LN_TRNS_BORR_SSN or
T.LN_TRNS_BORR_VISA <> S.LN_TRNS_BORR_VISA or
T.LN_TRNS_DRIVER_LIC <> S.LN_TRNS_DRIVER_LIC or
T.LN_TRNS_DRIVER_ST <> S.LN_TRNS_DRIVER_ST or
T.LN_TRNS_STU_CIT <> S.LN_TRNS_STU_CIT or
T.LN_TRNS_STU_DFLT <> S.LN_TRNS_STU_DFLT or
nvl(trim(T.LN_TRNS_STU_DOB),0) <> nvl(trim(S.LN_TRNS_STU_DOB),0) or
T.LN_TRNS_STU_SSN <> S.LN_TRNS_STU_SSN or
T.LN_TRNS_STU_VISA <> S.LN_TRNS_STU_VISA or
T.LN_TRNS_PRNT_OPTN <> S.LN_TRNS_PRNT_OPTN or
T.LN_TRNS_DISC_PRT <> S.LN_TRNS_DISC_PRT or
T.LN_TRNS_DL_FEE <> S.LN_TRNS_DL_FEE or
nvl(trim(T.LN_TRNS_ACAD_ST),0) <> nvl(trim(S.LN_TRNS_ACAD_ST),0) or
nvl(trim(T.LN_TRNS_ACAD_END),0) <> nvl(trim(S.LN_TRNS_ACAD_END),0) or
T.LN_TRNS_PHONE <> S.LN_TRNS_PHONE or
nvl(trim(T.LN_TRNS_DOB_CHGDT),0) <> nvl(trim(S.LN_TRNS_DOB_CHGDT),0) or
nvl(trim(T.LN_TRNS_SSN_CHGDT),0) <> nvl(trim(S.LN_TRNS_SSN_CHGDT),0) or
T.INTEREST_RATE_OPT <> S.INTEREST_RATE_OPT or
T.REPAYMENT_OPT_CD <> S.REPAYMENT_OPT_CD or
T.CL_ACT_INT_RATE <> S.CL_ACT_INT_RATE or
T.CL_LNDR_USE <> S.CL_LNDR_USE or
T.CL_SERIAL_LN_CD <> S.CL_SERIAL_LN_CD or
T.CL_MPN_CONFIRM_CD <> S.CL_MPN_CONFIRM_CD or
T.CL_BORR_CONFIRM_IN <> S.CL_BORR_CONFIRM_IN or
T.CL_DIFF_NAME_CD <> S.CL_DIFF_NAME_CD or
T.CL_FED_APP_FRM_CD <> S.CL_FED_APP_FRM_CD or
T.DL_ORIG_FEE <> S.DL_ORIG_FEE or
nvl(trim(T.ACAD_YEAR_START),0) <> nvl(trim(S.ACAD_YEAR_START),0) or
nvl(trim(T.ACAD_YEAR_END),0) <> nvl(trim(S.ACAD_YEAR_END),0) or
T.LN_ORIG_PNOTE_STAT <> S.LN_ORIG_PNOTE_STAT or
T.DL_PNOTE_ID <> S.DL_PNOTE_ID or
T.LN_FED_DEP_ST_OVRD <> S.LN_FED_DEP_ST_OVRD or
T.LN_ACAD_END_OVRD <> S.LN_ACAD_END_OVRD or
T.LN_ACAD_ST_OVRD <> S.LN_ACAD_ST_OVRD or
T.LN_BORR_CIT_ST_OVR <> S.LN_BORR_CIT_ST_OVR or
T.LN_BORR_VISA_OVRD <> S.LN_BORR_VISA_OVRD or
T.LN_BORR_DOB_OVRD <> S.LN_BORR_DOB_OVRD or
T.LN_BORR_SSN_OVRD <> S.LN_BORR_SSN_OVRD or
T.LN_ENDORS_AMT <> S.LN_ENDORS_AMT or
nvl(trim(T.SFA_COD_MPN_EXPIRE),0) <> nvl(trim(S.SFA_COD_MPN_EXPIRE),0) or
T.SFA_PP_CRSEWRK_SW <> S.SFA_PP_CRSEWRK_SW or
T.SFA_TRNS_PP_CRS_SW <> S.SFA_TRNS_PP_CRS_SW or
T.SFA_LN_STU_CIT_OVR <> S.SFA_LN_STU_CIT_OVR or
T.SFA_ATB_CD <> S.SFA_ATB_CD or
T.SFA_TRNS_ATB_CD <> S.SFA_TRNS_ATB_CD or
T.SFA_ATB_TST_ADM_CD <> S.SFA_ATB_TST_ADM_CD or
T.SFA_TRNS_ATB_TADMC <> S.SFA_TRNS_ATB_TADMC or
T.SFA_ATB_TST_CD <> S.SFA_ATB_TST_CD or
T.SFA_TRNS_ATB_TSTCD <> S.SFA_TRNS_ATB_TSTCD or
T.SFA_ATB_STATE_CD <> S.SFA_ATB_STATE_CD or
T.SFA_TRNS_ATB_STCD <> S.SFA_TRNS_ATB_STCD or
nvl(trim(T.SFA_ATB_COMP_DT),0) <> nvl(trim(S.SFA_ATB_COMP_DT),0) or
nvl(trim(T.SFA_TRNS_ATB_CMPDT),0) <> nvl(trim(S.SFA_TRNS_ATB_CMPDT),0) or
T.SFA_STU_SSN_OVRD <> S.SFA_STU_SSN_OVRD or
T.SFA_BORR_SSN_OVRD <> S.SFA_BORR_SSN_OVRD or
T.SFA_PROG_LENGTH_MN <> S.SFA_PROG_LENGTH_MN or
T.SFA_PROG_LENGTH_WK <> S.SFA_PROG_LENGTH_WK or
T.SFA_PROG_LENGTH_YR <> S.SFA_PROG_LENGTH_YR or
T.SFA_WK_PROG_ACADYR <> S.SFA_WK_PROG_ACADYR or
T.SFA_SPEC_PROG_FLG <> S.SFA_SPEC_PROG_FLG or
T.SFA_COD_CRED_LVL <> S.SFA_COD_CRED_LVL or
T.SFA_TRNS_PROGLN_MN <> S.SFA_TRNS_PROGLN_MN or
T.SFA_TRNS_PROGLN_WK <> S.SFA_TRNS_PROGLN_WK or
T.SFA_TRNS_PROGLN_YR <> S.SFA_TRNS_PROGLN_YR or
T.SFA_TRNS_WKPRGACYR <> S.SFA_TRNS_WKPRGACYR or
T.SFA_TRNS_SPPRG_FLG <> S.SFA_TRNS_SPPRG_FLG or
T.SFA_TRNS_COD_CRDLV <> S.SFA_TRNS_COD_CRDLV or
T.SFA_PROGLN_MN_OVRD <> S.SFA_PROGLN_MN_OVRD or
T.SFA_PROGLN_WK_OVRD <> S.SFA_PROGLN_WK_OVRD or
T.SFA_PROGLN_YR_OVRD <> S.SFA_PROGLN_YR_OVRD or
T.SFA_WKPRGACYR_OVRD <> S.SFA_WKPRGACYR_OVRD or
T.SFA_SPEC_PROG_OVRD <> S.SFA_SPEC_PROG_OVRD or
T.SFA_COD_CRDLV_OVRD <> S.SFA_COD_CRDLV_OVRD or
T.SFA_DL_CRD_DEC_ORG <> S.SFA_DL_CRD_DEC_ORG or
T.SFA_DL_CRD_ACTN_CH <> S.SFA_DL_CRD_ACTN_CH or
T.SFA_DL_APPEAL_STAT <> S.SFA_DL_APPEAL_STAT or
T.SFA_DL_CRD_BAL_OPT <> S.SFA_DL_CRD_BAL_OPT or
nvl(trim(T.SFA_DL_CRD_EXP_DT),0) <> nvl(trim(S.SFA_DL_CRD_EXP_DT),0) or
T.SFA_DL_CRDT_ACTNST <> S.SFA_DL_CRDT_ACTNST or
T.SFA_DL_RECONS_ELG <> S.SFA_DL_RECONS_ELG or
T.SFA_DL_PLUS_CNSLCP <> S.SFA_DL_PLUS_CNSLCP or
nvl(trim(T.SFA_DL_PLUS_CNSLDT),0) <> nvl(trim(S.SFA_DL_PLUS_CNSLDT),0) or
T.SFA_DL_PLUS_CNSLEX <> S.SFA_DL_PLUS_CNSLEX or
T.SFA_DL_CREDIT_REQ <> S.SFA_DL_CREDIT_REQ or
T.SFA_DL_ENDORS_APPR <> S.SFA_DL_ENDORS_APPR or
nvl(trim(T.SFA_DL_PLUS_EXPDT),0) <> nvl(trim(S.SFA_DL_PLUS_EXPDT),0) or
T.SFA_DL_CRD_DEC_ST <> S.SFA_DL_CRD_DEC_ST or
T.DATA_ORIGIN = 'D'
when not matched then
insert (
T.EMPLID,
T.INSTITUTION,
T.AID_YEAR,
T.ACAD_CAREER,
T.LOAN_TYPE,
T.LN_APPL_SEQ,
T.SRC_SYS_ID,
T.BORR_EMPLID,
T.FED_OR_INST_COST,
T.FED_EFC,
T.INST_EFC,
T.FED_YEAR_COA,
T.INST_YEAR_COA,
T.LN_FIN_AID,
T.LN_NET_COST,
T.NSLDS_LOAN_YEAR,
T.DIR_LND_YR,
T.ACADEMIC_LEVEL,
T.LN_DEST_NBR,
T.LEND_UNQ_ID,
T.GUAR_UNQ_ID,
T.GUAR_LN_ID,
T.LN_EFT_AUTHRZTN,
T.LN_REQ_DEFRMNT,
T.CAPTLZ_INTRST,
T.LN_AMT_BORRQSTD,
T.LN_PNT_BORRQSTD,
T.LN_TOTAL_CERT,
T.LN_CERT_DT,
T.LNDR_LAST_RSRT,
T.LOAN_CREDIT_CHK,
T.LOAN_CRDT_OVRID,
T.LOAN_CRDT_DT,
T.SCH_LNAPPL_ID,
T.ACADEMIC_LOAD,
T.LN_PRGCMPLT_DT,
T.EXP_GRAD_TERM,
T.LN_PERIOD_START,
T.LN_PERIOD_END,
T.LN_PERIOD_OVRD,
T.LN_UPDT_DMGRPHC,
T.LOAN_AMT_COSIGN,
T.LN_ORIG_MANUAL,
T.LN_REFUND_INDC,
T.LN_BORR_DFLT_RFND,
T.LN_STDNT_DFLT_RFND,
T.LN_TOT_LOAN_DEBT,
T.SSN,
T.BORR_SSN,
T.LN_BORR_SSN_CHGDT,
T.DRIVERS_LIC_NBR,
T.BORR_DRIVER_LIC_ST,
T.BIRTHDATE,
T.BORR_BIRTHDATE,
T.LN_BORR_DOB_CHGDT,
T.VISA_WRKPMT_NBR,
T.BORR_VISA_WKPT_NBR,
T.CITIZENSHIP_STATUS,
T.BORR_CTZNSHP_STAT,
T.LN_PRINT_OPTN,
T.DL_DISC_PRT_IND,
T.INST_DEPEND_STAT,
T.FED_DEPEND_STAT,
T.CURRENCY_CD,
T.LN_DEST_PROC_LVL,
T.LN_BOOK_STAT,
T.LN_BOOK_DT,
T.DL_ADD_USUB_SW,
T.DL_HEAL_LN_SW,
T.LN_VERSION,
T.LN_RPT_FORM_ID,
T.PNOTE_PRINT_SEQ,
T.LN_MPN_SEQ,
T.PHONE,
T.LN_PHONE_OVRD,
T.LN_TRNS_PER_ST,
T.LN_TRNS_PER_END,
T.LN_TRNS_DEP_STAT,
T.LN_TRNS_HEAL_SW,
T.LN_TRNS_USUB_SW,
T.LN_TRNS_DIR_YR,
T.LN_TRNS_NSLDS_YEAR,
T.LN_TRNS_PRGCMPLT,
T.LN_TRNS_BORR_CIT,
T.LN_TRNS_BORR_DFLT,
T.LN_TRNS_BORR_DOB,
T.LN_TRNS_BORR_SSN,
T.LN_TRNS_BORR_VISA,
T.LN_TRNS_DRIVER_LIC,
T.LN_TRNS_DRIVER_ST,
T.LN_TRNS_STU_CIT,
T.LN_TRNS_STU_DFLT,
T.LN_TRNS_STU_DOB,
T.LN_TRNS_STU_SSN,
T.LN_TRNS_STU_VISA,
T.LN_TRNS_PRNT_OPTN,
T.LN_TRNS_DISC_PRT,
T.LN_TRNS_DL_FEE,
T.LN_TRNS_ACAD_ST,
T.LN_TRNS_ACAD_END,
T.LN_TRNS_PHONE,
T.LN_TRNS_DOB_CHGDT,
T.LN_TRNS_SSN_CHGDT,
T.INTEREST_RATE_OPT,
T.REPAYMENT_OPT_CD,
T.CL_ACT_INT_RATE,
T.CL_LNDR_USE,
T.CL_SERIAL_LN_CD,
T.CL_MPN_CONFIRM_CD,
T.CL_BORR_CONFIRM_IN,
T.CL_DIFF_NAME_CD,
T.CL_FED_APP_FRM_CD,
T.DL_ORIG_FEE,
T.ACAD_YEAR_START,
T.ACAD_YEAR_END,
T.LN_ORIG_PNOTE_STAT,
T.DL_PNOTE_ID,
T.LN_FED_DEP_ST_OVRD,
T.LN_ACAD_END_OVRD,
T.LN_ACAD_ST_OVRD,
T.LN_BORR_CIT_ST_OVR,
T.LN_BORR_VISA_OVRD,
T.LN_BORR_DOB_OVRD,
T.LN_BORR_SSN_OVRD,
T.LN_ENDORS_AMT,
T.SFA_COD_MPN_EXPIRE,
T.SFA_PP_CRSEWRK_SW,
T.SFA_TRNS_PP_CRS_SW,
T.SFA_LN_STU_CIT_OVR,
T.SFA_ATB_CD,
T.SFA_TRNS_ATB_CD,
T.SFA_ATB_TST_ADM_CD,
T.SFA_TRNS_ATB_TADMC,
T.SFA_ATB_TST_CD,
T.SFA_TRNS_ATB_TSTCD,
T.SFA_ATB_STATE_CD,
T.SFA_TRNS_ATB_STCD,
T.SFA_ATB_COMP_DT,
T.SFA_TRNS_ATB_CMPDT,
T.SFA_STU_SSN_OVRD,
T.SFA_BORR_SSN_OVRD,
T.SFA_PROG_LENGTH_MN,
T.SFA_PROG_LENGTH_WK,
T.SFA_PROG_LENGTH_YR,
T.SFA_WK_PROG_ACADYR,
T.SFA_SPEC_PROG_FLG,
T.SFA_COD_CRED_LVL,
T.SFA_TRNS_PROGLN_MN,
T.SFA_TRNS_PROGLN_WK,
T.SFA_TRNS_PROGLN_YR,
T.SFA_TRNS_WKPRGACYR,
T.SFA_TRNS_SPPRG_FLG,
T.SFA_TRNS_COD_CRDLV,
T.SFA_PROGLN_MN_OVRD,
T.SFA_PROGLN_WK_OVRD,
T.SFA_PROGLN_YR_OVRD,
T.SFA_WKPRGACYR_OVRD,
T.SFA_SPEC_PROG_OVRD,
T.SFA_COD_CRDLV_OVRD,
T.SFA_DL_CRD_DEC_ORG,
T.SFA_DL_CRD_ACTN_CH,
T.SFA_DL_APPEAL_STAT,
T.SFA_DL_CRD_BAL_OPT,
T.SFA_DL_CRD_EXP_DT,
T.SFA_DL_CRDT_ACTNST,
T.SFA_DL_RECONS_ELG,
T.SFA_DL_PLUS_CNSLCP,
T.SFA_DL_PLUS_CNSLDT,
T.SFA_DL_PLUS_CNSLEX,
T.SFA_DL_CREDIT_REQ,
T.SFA_DL_ENDORS_APPR,
T.SFA_DL_PLUS_EXPDT,
T.SFA_DL_CRD_DEC_ST,
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
S.ACAD_CAREER,
S.LOAN_TYPE,
S.LN_APPL_SEQ,
'CS90',
S.BORR_EMPLID,
S.FED_OR_INST_COST,
S.FED_EFC,
S.INST_EFC,
S.FED_YEAR_COA,
S.INST_YEAR_COA,
S.LN_FIN_AID,
S.LN_NET_COST,
S.NSLDS_LOAN_YEAR,
S.DIR_LND_YR,
S.ACADEMIC_LEVEL,
S.LN_DEST_NBR,
S.LEND_UNQ_ID,
S.GUAR_UNQ_ID,
S.GUAR_LN_ID,
S.LN_EFT_AUTHRZTN,
S.LN_REQ_DEFRMNT,
S.CAPTLZ_INTRST,
S.LN_AMT_BORRQSTD,
S.LN_PNT_BORRQSTD,
S.LN_TOTAL_CERT,
S.LN_CERT_DT,
S.LNDR_LAST_RSRT,
S.LOAN_CREDIT_CHK,
S.LOAN_CRDT_OVRID,
S.LOAN_CRDT_DT,
S.SCH_LNAPPL_ID,
S.ACADEMIC_LOAD,
S.LN_PRGCMPLT_DT,
S.EXP_GRAD_TERM,
S.LN_PERIOD_START,
S.LN_PERIOD_END,
S.LN_PERIOD_OVRD,
S.LN_UPDT_DMGRPHC,
S.LOAN_AMT_COSIGN,
S.LN_ORIG_MANUAL,
S.LN_REFUND_INDC,
S.LN_BORR_DFLT_RFND,
S.LN_STDNT_DFLT_RFND,
S.LN_TOT_LOAN_DEBT,
S.SSN,
S.BORR_SSN,
S.LN_BORR_SSN_CHGDT,
S.DRIVERS_LIC_NBR,
S.BORR_DRIVER_LIC_ST,
S.BIRTHDATE,
S.BORR_BIRTHDATE,
S.LN_BORR_DOB_CHGDT,
S.VISA_WRKPMT_NBR,
S.BORR_VISA_WKPT_NBR,
S.CITIZENSHIP_STATUS,
S.BORR_CTZNSHP_STAT,
S.LN_PRINT_OPTN,
S.DL_DISC_PRT_IND,
S.INST_DEPEND_STAT,
S.FED_DEPEND_STAT,
S.CURRENCY_CD,
S.LN_DEST_PROC_LVL,
S.LN_BOOK_STAT,
S.LN_BOOK_DT,
S.DL_ADD_USUB_SW,
S.DL_HEAL_LN_SW,
S.LN_VERSION,
S.LN_RPT_FORM_ID,
S.PNOTE_PRINT_SEQ,
S.LN_MPN_SEQ,
S.PHONE,
S.LN_PHONE_OVRD,
S.LN_TRNS_PER_ST,
S.LN_TRNS_PER_END,
S.LN_TRNS_DEP_STAT,
S.LN_TRNS_HEAL_SW,
S.LN_TRNS_USUB_SW,
S.LN_TRNS_DIR_YR,
S.LN_TRNS_NSLDS_YEAR,
S.LN_TRNS_PRGCMPLT,
S.LN_TRNS_BORR_CIT,
S.LN_TRNS_BORR_DFLT,
S.LN_TRNS_BORR_DOB,
S.LN_TRNS_BORR_SSN,
S.LN_TRNS_BORR_VISA,
S.LN_TRNS_DRIVER_LIC,
S.LN_TRNS_DRIVER_ST,
S.LN_TRNS_STU_CIT,
S.LN_TRNS_STU_DFLT,
S.LN_TRNS_STU_DOB,
S.LN_TRNS_STU_SSN,
S.LN_TRNS_STU_VISA,
S.LN_TRNS_PRNT_OPTN,
S.LN_TRNS_DISC_PRT,
S.LN_TRNS_DL_FEE,
S.LN_TRNS_ACAD_ST,
S.LN_TRNS_ACAD_END,
S.LN_TRNS_PHONE,
S.LN_TRNS_DOB_CHGDT,
S.LN_TRNS_SSN_CHGDT,
S.INTEREST_RATE_OPT,
S.REPAYMENT_OPT_CD,
S.CL_ACT_INT_RATE,
S.CL_LNDR_USE,
S.CL_SERIAL_LN_CD,
S.CL_MPN_CONFIRM_CD,
S.CL_BORR_CONFIRM_IN,
S.CL_DIFF_NAME_CD,
S.CL_FED_APP_FRM_CD,
S.DL_ORIG_FEE,
S.ACAD_YEAR_START,
S.ACAD_YEAR_END,
S.LN_ORIG_PNOTE_STAT,
S.DL_PNOTE_ID,
S.LN_FED_DEP_ST_OVRD,
S.LN_ACAD_END_OVRD,
S.LN_ACAD_ST_OVRD,
S.LN_BORR_CIT_ST_OVR,
S.LN_BORR_VISA_OVRD,
S.LN_BORR_DOB_OVRD,
S.LN_BORR_SSN_OVRD,
S.LN_ENDORS_AMT,
S.SFA_COD_MPN_EXPIRE,
S.SFA_PP_CRSEWRK_SW,
S.SFA_TRNS_PP_CRS_SW,
S.SFA_LN_STU_CIT_OVR,
S.SFA_ATB_CD,
S.SFA_TRNS_ATB_CD,
S.SFA_ATB_TST_ADM_CD,
S.SFA_TRNS_ATB_TADMC,
S.SFA_ATB_TST_CD,
S.SFA_TRNS_ATB_TSTCD,
S.SFA_ATB_STATE_CD,
S.SFA_TRNS_ATB_STCD,
S.SFA_ATB_COMP_DT,
S.SFA_TRNS_ATB_CMPDT,
S.SFA_STU_SSN_OVRD,
S.SFA_BORR_SSN_OVRD,
S.SFA_PROG_LENGTH_MN,
S.SFA_PROG_LENGTH_WK,
S.SFA_PROG_LENGTH_YR,
S.SFA_WK_PROG_ACADYR,
S.SFA_SPEC_PROG_FLG,
S.SFA_COD_CRED_LVL,
S.SFA_TRNS_PROGLN_MN,
S.SFA_TRNS_PROGLN_WK,
S.SFA_TRNS_PROGLN_YR,
S.SFA_TRNS_WKPRGACYR,
S.SFA_TRNS_SPPRG_FLG,
S.SFA_TRNS_COD_CRDLV,
S.SFA_PROGLN_MN_OVRD,
S.SFA_PROGLN_WK_OVRD,
S.SFA_PROGLN_YR_OVRD,
S.SFA_WKPRGACYR_OVRD,
S.SFA_SPEC_PROG_OVRD,
S.SFA_COD_CRDLV_OVRD,
S.SFA_DL_CRD_DEC_ORG,
S.SFA_DL_CRD_ACTN_CH,
S.SFA_DL_APPEAL_STAT,
S.SFA_DL_CRD_BAL_OPT,
S.SFA_DL_CRD_EXP_DT,
S.SFA_DL_CRDT_ACTNST,
S.SFA_DL_RECONS_ELG,
S.SFA_DL_PLUS_CNSLCP,
S.SFA_DL_PLUS_CNSLDT,
S.SFA_DL_PLUS_CNSLEX,
S.SFA_DL_CREDIT_REQ,
S.SFA_DL_ENDORS_APPR,
S.SFA_DL_PLUS_EXPDT,
S.SFA_DL_CRD_DEC_ST,
'N',
'S',
sysdate,
sysdate,
1234);


strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_LOAN_ORIGNATN rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_LOAN_ORIGNATN',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_LOAN_ORIGNATN';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_LOAN_ORIGNATN';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on AMSTG_OWNER.PS_LOAN_ORIGNATN';
update AMSTG_OWNER.PS_LOAN_ORIGNATN T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, INSTITUTION, AID_YEAR, ACAD_CAREER, LOAN_TYPE, LN_APPL_SEQ
   from AMSTG_OWNER.PS_LOAN_ORIGNATN T2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_LOAN_ORIGNATN') = 'Y'
  minus
 select EMPLID, INSTITUTION, AID_YEAR, ACAD_CAREER, LOAN_TYPE, LN_APPL_SEQ
   from SYSADM.PS_LOAN_ORIGNATN@AMSOURCE S2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_LOAN_ORIGNATN') = 'Y'
   ) S
 where T.EMPLID = S.EMPLID
   and T.INSTITUTION = S.INSTITUTION
   and T.AID_YEAR = S.AID_YEAR
   and T.ACAD_CAREER = S.ACAD_CAREER
   and T.LOAN_TYPE = S.LOAN_TYPE
   and T.LN_APPL_SEQ = S.LN_APPL_SEQ
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_LOAN_ORIGNATN rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_LOAN_ORIGNATN',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_LOAN_ORIGNATN'
;

strSqlCommand := 'commit';
commit;


strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := strProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);


EXCEPTION
    WHEN OTHERS THEN
        numSqlCode := SQLCODE;
        strSqlErrm := SQLERRM;

        ROLLBACK;
  
        strMessage01 := 'Error code: ' || TO_CHAR(SQLCODE) || ' Error Message: ' || SQLERRM;
        strMessage02 := TO_CHAR(SQLCODE);
  
        COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_FAILURE
                       (i_SqlCommand    => strSqlCommand,
                        i_ErrorText     => strMessage01,
                        i_ErrorCode     => strMessage02,
                        i_ErrorMessage  => strSqlErrm
                       );
               
        strMessage01 := 'Error...'
                        || strNewLine   || 'SQL Command:   ' || strSqlCommand
                        || strNewLine   || 'Error code:    ' || numSqlCode
                        || strNewLine   || 'Error Message: ' || strSqlErrm;

        COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
        RAISE_APPLICATION_ERROR( -20001, strMessage01);

END AM_PS_LOAN_ORIGNATN_P;
/
