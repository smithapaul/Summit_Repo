DROP PROCEDURE CSMRT_OWNER.AM_PS_LN_TYPE_TBL_P
/

--
-- AM_PS_LN_TYPE_TBL_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."AM_PS_LN_TYPE_TBL_P" IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_LN_TYPE_TBL from PeopleSoft table PS_LN_TYPE_TBL.
--
-- V01  SMT-xxxx 04/18/2017,    Jim Doucette
--                              Converted from PS_LN_TYPE_TBL.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'AM_PS_LN_TYPE_TBL';
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
 where TABLE_NAME = 'PS_LN_TYPE_TBL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_LN_TYPE_TBL@AMSOURCE S)
 where TABLE_NAME = 'PS_LN_TYPE_TBL'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into AMSTG_OWNER.PS_LN_TYPE_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into AMSTG_OWNER.PS_LN_TYPE_TBL';
merge /*+ use_hash(S,T) */ into AMSTG_OWNER.PS_LN_TYPE_TBL T
using (select /*+ full(S) */
    nvl(trim(INSTITUTION),'-') INSTITUTION, 
    nvl(trim(AID_YEAR),'-') AID_YEAR, 
    nvl(trim(LOAN_TYPE),'-') LOAN_TYPE, 
    nvl(trim(DESCR),'-') DESCR, 
    nvl(trim(DESCRSHORT),'-') DESCRSHORT, 
    nvl(trim(LOAN_PROGRAM),'-') LOAN_PROGRAM, 
    nvl(trim(LOAN_CATEGORY),'-') LOAN_CATEGORY, 
    nvl(LOAN_FEE_RT,0) LOAN_FEE_RT, 
    nvl(LOAN_INS_RT,0) LOAN_INS_RT, 
    nvl(trim(LOAN_REF_RQRD),'-') LOAN_REF_RQRD, 
    nvl(LOAN_NBR_REF_RQRD,0) LOAN_NBR_REF_RQRD, 
    nvl(trim(LOAN_COSIGN_RQRD),'-') LOAN_COSIGN_RQRD, 
    nvl(LOAN_AMT_COSIGN,0) LOAN_AMT_COSIGN, 
    nvl(LOAN_NBR_CSGN_RQRD,0) LOAN_NBR_CSGN_RQRD, 
    nvl(trim(LOAN_CREDIT_RQRD),'-') LOAN_CREDIT_RQRD, 
    nvl(trim(LOAN_PCKG_ORDR),'-') LOAN_PCKG_ORDR, 
    nvl(trim(FED_OR_INST_COST),'-') FED_OR_INST_COST, 
    nvl(trim(SVCR_PRINT_PNOTE),'-') SVCR_PRINT_PNOTE, 
    nvl(trim(LN_REFUND_INDC),'-') LN_REFUND_INDC, 
    nvl(trim(CL_TYPE_CODE),'-') CL_TYPE_CODE, 
    nvl(trim(CL_ALT_LN_TYPE_CD),'-') CL_ALT_LN_TYPE_CD, 
    nvl(trim(LN_RPT_PKG_ID),'-') LN_RPT_PKG_ID, 
    nvl(trim(CHECKLIST_CD),'-') CHECKLIST_CD, 
    nvl(trim(CURRENCY_CD),'-') CURRENCY_CD, 
    nvl(trim(LN_DL_DISB_SW),'-') LN_DL_DISB_SW, 
    nvl(trim(LN_DL_TRNS_DISB_SW),'-') LN_DL_TRNS_DISB_SW, 
    nvl(trim(DL_VER_TYPE_USE),'-') DL_VER_TYPE_USE, 
    nvl(LN_MAX_NBR_DISBS,0) LN_MAX_NBR_DISBS, 
    nvl(LN_MIN_LOAN_AMT,0) LN_MIN_LOAN_AMT, 
    nvl(LN_DL_ADD_DAYS,0) LN_DL_ADD_DAYS, 
    nvl(trim(SFA_LN_GRAD_PLUS),'-') SFA_LN_GRAD_PLUS, 
    nvl(trim(SFA_ADDL_USUB_IND),'-') SFA_ADDL_USUB_IND
from SYSADM.PS_LN_TYPE_TBL@AMSOURCE S 
where ORA_ROWSCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_LN_TYPE_TBL') ) S
 on (T.INSTITUTION = S.INSTITUTION and 
    T.AID_YEAR = S.AID_YEAR and 
    T.LOAN_TYPE = S.LOAN_TYPE and 
    T.SRC_SYS_ID = 'CS90') 
when matched then update set
    T.DESCR = S.DESCR,
    T.DESCRSHORT = S.DESCRSHORT,
    T.LOAN_PROGRAM = S.LOAN_PROGRAM,
    T.LOAN_CATEGORY = S.LOAN_CATEGORY,
    T.LOAN_FEE_RT = S.LOAN_FEE_RT,
    T.LOAN_INS_RT = S.LOAN_INS_RT,
    T.LOAN_REF_RQRD = S.LOAN_REF_RQRD,
    T.LOAN_NBR_REF_RQRD = S.LOAN_NBR_REF_RQRD,
    T.LOAN_COSIGN_RQRD = S.LOAN_COSIGN_RQRD,
    T.LOAN_AMT_COSIGN = S.LOAN_AMT_COSIGN,
    T.LOAN_NBR_CSGN_RQRD = S.LOAN_NBR_CSGN_RQRD,
    T.LOAN_CREDIT_RQRD = S.LOAN_CREDIT_RQRD,
    T.LOAN_PCKG_ORDR = S.LOAN_PCKG_ORDR,
    T.FED_OR_INST_COST = S.FED_OR_INST_COST,
    T.SVCR_PRINT_PNOTE = S.SVCR_PRINT_PNOTE,
    T.LN_REFUND_INDC = S.LN_REFUND_INDC,
    T.CL_TYPE_CODE = S.CL_TYPE_CODE,
    T.CL_ALT_LN_TYPE_CD = S.CL_ALT_LN_TYPE_CD,
    T.LN_RPT_PKG_ID = S.LN_RPT_PKG_ID,
    T.CHECKLIST_CD = S.CHECKLIST_CD,
    T.CURRENCY_CD = S.CURRENCY_CD,
    T.LN_DL_DISB_SW = S.LN_DL_DISB_SW,
    T.LN_DL_TRNS_DISB_SW = S.LN_DL_TRNS_DISB_SW,
    T.DL_VER_TYPE_USE = S.DL_VER_TYPE_USE,
    T.LN_MAX_NBR_DISBS = S.LN_MAX_NBR_DISBS,
    T.LN_MIN_LOAN_AMT = S.LN_MIN_LOAN_AMT,
    T.LN_DL_ADD_DAYS = S.LN_DL_ADD_DAYS,
    T.SFA_LN_GRAD_PLUS = S.SFA_LN_GRAD_PLUS,
    T.SFA_ADDL_USUB_IND = S.SFA_ADDL_USUB_IND,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.INSTITUTION <> S.INSTITUTION or 
    T.AID_YEAR <> S.AID_YEAR or 
    T.LOAN_TYPE <> S.LOAN_TYPE or 
--    T.SRC_SYS_ID <> S.SRC_SYS_ID or 
    T.DESCR <> S.DESCR or 
    T.DESCRSHORT <> S.DESCRSHORT or 
    T.LOAN_PROGRAM <> S.LOAN_PROGRAM or 
    T.LOAN_CATEGORY <> S.LOAN_CATEGORY or 
    T.LOAN_FEE_RT <> S.LOAN_FEE_RT or 
    T.LOAN_INS_RT <> S.LOAN_INS_RT or 
    T.LOAN_REF_RQRD <> S.LOAN_REF_RQRD or 
    T.LOAN_NBR_REF_RQRD <> S.LOAN_NBR_REF_RQRD or 
    T.LOAN_COSIGN_RQRD <> S.LOAN_COSIGN_RQRD or 
    T.LOAN_AMT_COSIGN <> S.LOAN_AMT_COSIGN or 
    T.LOAN_NBR_CSGN_RQRD <> S.LOAN_NBR_CSGN_RQRD or 
    T.LOAN_CREDIT_RQRD <> S.LOAN_CREDIT_RQRD or 
    T.LOAN_PCKG_ORDR <> S.LOAN_PCKG_ORDR or 
    T.FED_OR_INST_COST <> S.FED_OR_INST_COST or 
    T.SVCR_PRINT_PNOTE <> S.SVCR_PRINT_PNOTE or 
    T.LN_REFUND_INDC <> S.LN_REFUND_INDC or 
    T.CL_TYPE_CODE <> S.CL_TYPE_CODE or 
    T.CL_ALT_LN_TYPE_CD <> S.CL_ALT_LN_TYPE_CD or 
    T.LN_RPT_PKG_ID <> S.LN_RPT_PKG_ID or 
    T.CHECKLIST_CD <> S.CHECKLIST_CD or 
    T.CURRENCY_CD <> S.CURRENCY_CD or 
    T.LN_DL_DISB_SW <> S.LN_DL_DISB_SW or 
    T.LN_DL_TRNS_DISB_SW <> S.LN_DL_TRNS_DISB_SW or 
    T.DL_VER_TYPE_USE <> S.DL_VER_TYPE_USE or 
    T.LN_MAX_NBR_DISBS <> S.LN_MAX_NBR_DISBS or 
    T.LN_MIN_LOAN_AMT <> S.LN_MIN_LOAN_AMT or 
    T.LN_DL_ADD_DAYS <> S.LN_DL_ADD_DAYS or 
    T.SFA_LN_GRAD_PLUS <> S.SFA_LN_GRAD_PLUS or 
    T.SFA_ADDL_USUB_IND <> S.SFA_ADDL_USUB_IND or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.INSTITUTION,
    T.AID_YEAR, 
    T.LOAN_TYPE,
    T.SRC_SYS_ID, 
    T.DESCR,
    T.DESCRSHORT, 
    T.LOAN_PROGRAM, 
    T.LOAN_CATEGORY,
    T.LOAN_FEE_RT,
    T.LOAN_INS_RT,
    T.LOAN_REF_RQRD,
    T.LOAN_NBR_REF_RQRD,
    T.LOAN_COSIGN_RQRD, 
    T.LOAN_AMT_COSIGN,
    T.LOAN_NBR_CSGN_RQRD, 
    T.LOAN_CREDIT_RQRD, 
    T.LOAN_PCKG_ORDR, 
    T.FED_OR_INST_COST, 
    T.SVCR_PRINT_PNOTE, 
    T.LN_REFUND_INDC, 
    T.CL_TYPE_CODE, 
    T.CL_ALT_LN_TYPE_CD,
    T.LN_RPT_PKG_ID,
    T.CHECKLIST_CD, 
    T.CURRENCY_CD,
    T.LN_DL_DISB_SW,
    T.LN_DL_TRNS_DISB_SW, 
    T.DL_VER_TYPE_USE,
    T.LN_MAX_NBR_DISBS, 
    T.LN_MIN_LOAN_AMT,
    T.LN_DL_ADD_DAYS, 
    T.SFA_LN_GRAD_PLUS, 
    T.SFA_ADDL_USUB_IND,
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
    ) 
values (
    S.INSTITUTION,
    S.AID_YEAR, 
    S.LOAN_TYPE,
    'CS90', 
    S.DESCR,
    S.DESCRSHORT, 
    S.LOAN_PROGRAM, 
    S.LOAN_CATEGORY,
    S.LOAN_FEE_RT,
    S.LOAN_INS_RT,
    S.LOAN_REF_RQRD,
    S.LOAN_NBR_REF_RQRD,
    S.LOAN_COSIGN_RQRD, 
    S.LOAN_AMT_COSIGN,
    S.LOAN_NBR_CSGN_RQRD, 
    S.LOAN_CREDIT_RQRD, 
    S.LOAN_PCKG_ORDR, 
    S.FED_OR_INST_COST, 
    S.SVCR_PRINT_PNOTE, 
    S.LN_REFUND_INDC, 
    S.CL_TYPE_CODE, 
    S.CL_ALT_LN_TYPE_CD,
    S.LN_RPT_PKG_ID,
    S.CHECKLIST_CD, 
    S.CURRENCY_CD,
    S.LN_DL_DISB_SW,
    S.LN_DL_TRNS_DISB_SW, 
    S.DL_VER_TYPE_USE,
    S.LN_MAX_NBR_DISBS, 
    S.LN_MIN_LOAN_AMT,
    S.LN_DL_ADD_DAYS, 
    S.SFA_LN_GRAD_PLUS, 
    S.SFA_ADDL_USUB_IND,
    'N',
    'S',
    sysdate,
    sysdate,
    1234);

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_LN_TYPE_TBL rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_LN_TYPE_TBL',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_LN_TYPE_TBL';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_LN_TYPE_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on AMSTG_OWNER.PS_LN_TYPE_TBL';
update AMSTG_OWNER.PS_LN_TYPE_TBL T
        set T.DATA_ORIGIN = 'D',
               T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select INSTITUTION, AID_YEAR, LOAN_TYPE
   from AMSTG_OWNER.PS_LN_TYPE_TBL T2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_LN_TYPE_TBL') = 'Y'
  minus
 select INSTITUTION, AID_YEAR, LOAN_TYPE
   from SYSADM.PS_LN_TYPE_TBL@AMSOURCE S2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_LN_TYPE_TBL') = 'Y'
   ) S
 where T.INSTITUTION = S.INSTITUTION
   and T.AID_YEAR = S.AID_YEAR
   and T.LOAN_TYPE = S.LOAN_TYPE
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_LN_TYPE_TBL rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_LN_TYPE_TBL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_LN_TYPE_TBL'
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

END AM_PS_LN_TYPE_TBL_P;
/
