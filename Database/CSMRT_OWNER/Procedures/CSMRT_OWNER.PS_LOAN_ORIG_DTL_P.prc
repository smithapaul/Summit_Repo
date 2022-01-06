CREATE OR REPLACE PROCEDURE             "PS_LOAN_ORIG_DTL_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_LOAN_ORIG_DTL from PeopleSoft table PS_LOAN_ORIG_DTL.
--
-- V01  SMT-xxxx 04/04/2017,    Jim Doucette
--                              Converted from PS_LOAN_ORIG_DTL.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_LOAN_ORIG_DTL';
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
 where TABLE_NAME = 'PS_LOAN_ORIG_DTL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_LOAN_ORIG_DTL@SASOURCE S)
 where TABLE_NAME = 'PS_LOAN_ORIG_DTL'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_LOAN_ORIG_DTL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_LOAN_ORIG_DTL';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_LOAN_ORIG_DTL T
using (select /*+ full(S) */
nvl(trim(EMPLID),'-') EMPLID,
nvl(trim(INSTITUTION),'-') INSTITUTION,
nvl(trim(AID_YEAR),'-') AID_YEAR,
nvl(trim(ACAD_CAREER),'-') ACAD_CAREER,
nvl(trim(LOAN_TYPE),'-') LOAN_TYPE,
nvl(LN_APPL_SEQ,0) LN_APPL_SEQ,
nvl(trim(ITEM_TYPE),'-') ITEM_TYPE,
nvl(trim(LN_APPL_ID),'-') LN_APPL_ID,
nvl(trim(DISBURSEMENT_PLAN),'-') DISBURSEMENT_PLAN,
nvl(trim(SPLIT_CODE),'-') SPLIT_CODE,
nvl(trim(LOAN_INTEREST_ATTR),'-') LOAN_INTEREST_ATTR,
nvl(LN_BORRQSTD_DTL,0) LN_BORRQSTD_DTL,
nvl(LN_AMT_CERTIFIED,0) LN_AMT_CERTIFIED,
nvl(LN_ANTIC_FEE_TOT,0) LN_ANTIC_FEE_TOT,
nvl(LN_ANTIC_REBATE,0) LN_ANTIC_REBATE,
nvl(LN_ANTIC_NET_TOT,0) LN_ANTIC_NET_TOT,
nvl(LN_REMAIN_ELGBLTY,0) LN_REMAIN_ELGBLTY,
nvl(LN_AMT_APPROVED,0) LN_AMT_APPROVED,
nvl(ACCEPT_AMOUNT,0) ACCEPT_AMOUNT,
nvl(OFFER_AMOUNT,0) OFFER_AMOUNT,
nvl(LN_FEE_AMT,0) LN_FEE_AMT,
nvl(LN_REBATE_AMT,0) LN_REBATE_AMT,
nvl(trim(CL_TYPE_CODE),'-') CL_TYPE_CODE,
nvl(trim(CL_SEQ_NUM),'-') CL_SEQ_NUM,
nvl(trim(CURRENCY_CD),'-') CURRENCY_CD,
nvl(LN_TRNS_OFFER_AMT,0) LN_TRNS_OFFER_AMT,
nvl(LN_TRNS_ACCEPT_AMT,0) LN_TRNS_ACCEPT_AMT,
nvl(LN_TRNS_CERT_AMT,0) LN_TRNS_CERT_AMT,
nvl(LN_TRNS_BQSTD_DTL,0) LN_TRNS_BQSTD_DTL,
nvl(trim(LOAN_PROC_STAT),'-') LOAN_PROC_STAT,
nvl(trim(LN_ORIG_TRANS),'-') LN_ORIG_TRANS,
to_date(to_char(case when LN_GUAR_DT < '01-JAN-1800' then NULL else LN_GUAR_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') LN_GUAR_DT,
nvl(trim(CL_SERV_TYPE_CD),'-') CL_SERV_TYPE_CD,
nvl(trim(CL_REV_NOG_CD),'-') CL_REV_NOG_CD,
nvl(trim(CL_GUAR_AMT_RED_CD),'-') CL_GUAR_AMT_RED_CD,
nvl(trim(LN_FORCE_CHG_SW),'-') LN_FORCE_CHG_SW,
nvl(trim(SFA_CL_ESIGN_TYPE),'-') SFA_CL_ESIGN_TYPE
  from SYSADM.PS_LOAN_ORIG_DTL@SASOURCE S
 where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_LOAN_ORIG_DTL')
   and EMPLID between '00000000' and '99999999'
   and length(EMPLID) = 8 ) S
   on (
T.EMPLID = S.EMPLID and
T.INSTITUTION = S.INSTITUTION and
T.AID_YEAR = S.AID_YEAR and
T.ACAD_CAREER = S.ACAD_CAREER and
T.LOAN_TYPE = S.LOAN_TYPE and
T.LN_APPL_SEQ = S.LN_APPL_SEQ and
T.ITEM_TYPE = S.ITEM_TYPE and
T.SRC_SYS_ID = 'CS90')
when matched then update set
T.LN_APPL_ID = S.LN_APPL_ID,
T.DISBURSEMENT_PLAN = S.DISBURSEMENT_PLAN,
T.SPLIT_CODE = S.SPLIT_CODE,
T.LOAN_INTEREST_ATTR = S.LOAN_INTEREST_ATTR,
T.LN_BORRQSTD_DTL = S.LN_BORRQSTD_DTL,
T.LN_AMT_CERTIFIED = S.LN_AMT_CERTIFIED,
T.LN_ANTIC_FEE_TOT = S.LN_ANTIC_FEE_TOT,
T.LN_ANTIC_REBATE = S.LN_ANTIC_REBATE,
T.LN_ANTIC_NET_TOT = S.LN_ANTIC_NET_TOT,
T.LN_REMAIN_ELGBLTY = S.LN_REMAIN_ELGBLTY,
T.LN_AMT_APPROVED = S.LN_AMT_APPROVED,
T.ACCEPT_AMOUNT = S.ACCEPT_AMOUNT,
T.OFFER_AMOUNT = S.OFFER_AMOUNT,
T.LN_FEE_AMT = S.LN_FEE_AMT,
T.LN_REBATE_AMT = S.LN_REBATE_AMT,
T.CL_TYPE_CODE = S.CL_TYPE_CODE,
T.CL_SEQ_NUM = S.CL_SEQ_NUM,
T.CURRENCY_CD = S.CURRENCY_CD,
T.LN_TRNS_OFFER_AMT = S.LN_TRNS_OFFER_AMT,
T.LN_TRNS_ACCEPT_AMT = S.LN_TRNS_ACCEPT_AMT,
T.LN_TRNS_CERT_AMT = S.LN_TRNS_CERT_AMT,
T.LN_TRNS_BQSTD_DTL = S.LN_TRNS_BQSTD_DTL,
T.LOAN_PROC_STAT = S.LOAN_PROC_STAT,
T.LN_ORIG_TRANS = S.LN_ORIG_TRANS,
T.LN_GUAR_DT = S.LN_GUAR_DT,
T.CL_SERV_TYPE_CD = S.CL_SERV_TYPE_CD,
T.CL_REV_NOG_CD = S.CL_REV_NOG_CD,
T.CL_GUAR_AMT_RED_CD = S.CL_GUAR_AMT_RED_CD,
T.LN_FORCE_CHG_SW = S.LN_FORCE_CHG_SW,
T.SFA_CL_ESIGN_TYPE = S.SFA_CL_ESIGN_TYPE,
T.DATA_ORIGIN = 'S',
T.LASTUPD_EW_DTTM = sysdate,
T.BATCH_SID   = 1234
where
T.LN_APPL_ID <> S.LN_APPL_ID or
T.DISBURSEMENT_PLAN <> S.DISBURSEMENT_PLAN or
T.SPLIT_CODE <> S.SPLIT_CODE or
T.LOAN_INTEREST_ATTR <> S.LOAN_INTEREST_ATTR or
T.LN_BORRQSTD_DTL <> S.LN_BORRQSTD_DTL or
T.LN_AMT_CERTIFIED <> S.LN_AMT_CERTIFIED or
T.LN_ANTIC_FEE_TOT <> S.LN_ANTIC_FEE_TOT or
T.LN_ANTIC_REBATE <> S.LN_ANTIC_REBATE or
T.LN_ANTIC_NET_TOT <> S.LN_ANTIC_NET_TOT or
T.LN_REMAIN_ELGBLTY <> S.LN_REMAIN_ELGBLTY or
T.LN_AMT_APPROVED <> S.LN_AMT_APPROVED or
T.ACCEPT_AMOUNT <> S.ACCEPT_AMOUNT or
T.OFFER_AMOUNT <> S.OFFER_AMOUNT or
T.LN_FEE_AMT <> S.LN_FEE_AMT or
T.LN_REBATE_AMT <> S.LN_REBATE_AMT or
T.CL_TYPE_CODE <> S.CL_TYPE_CODE or
T.CL_SEQ_NUM <> S.CL_SEQ_NUM or
T.CURRENCY_CD <> S.CURRENCY_CD or
T.LN_TRNS_OFFER_AMT <> S.LN_TRNS_OFFER_AMT or
T.LN_TRNS_ACCEPT_AMT <> S.LN_TRNS_ACCEPT_AMT or
T.LN_TRNS_CERT_AMT <> S.LN_TRNS_CERT_AMT or
T.LN_TRNS_BQSTD_DTL <> S.LN_TRNS_BQSTD_DTL or
T.LOAN_PROC_STAT <> S.LOAN_PROC_STAT or
T.LN_ORIG_TRANS <> S.LN_ORIG_TRANS or
nvl(trim(T.LN_GUAR_DT),0) <> nvl(trim(S.LN_GUAR_DT),0) or
T.CL_SERV_TYPE_CD <> S.CL_SERV_TYPE_CD or
T.CL_REV_NOG_CD <> S.CL_REV_NOG_CD or
T.CL_GUAR_AMT_RED_CD <> S.CL_GUAR_AMT_RED_CD or
T.LN_FORCE_CHG_SW <> S.LN_FORCE_CHG_SW or
T.SFA_CL_ESIGN_TYPE <> S.SFA_CL_ESIGN_TYPE or
T.DATA_ORIGIN = 'D'
when not matched then
insert (
T.EMPLID,
T.INSTITUTION,
T.AID_YEAR,
T.ACAD_CAREER,
T.LOAN_TYPE,
T.LN_APPL_SEQ,
T.ITEM_TYPE,
T.SRC_SYS_ID,
T.LN_APPL_ID,
T.DISBURSEMENT_PLAN,
T.SPLIT_CODE,
T.LOAN_INTEREST_ATTR,
T.LN_BORRQSTD_DTL,
T.LN_AMT_CERTIFIED,
T.LN_ANTIC_FEE_TOT,
T.LN_ANTIC_REBATE,
T.LN_ANTIC_NET_TOT,
T.LN_REMAIN_ELGBLTY,
T.LN_AMT_APPROVED,
T.ACCEPT_AMOUNT,
T.OFFER_AMOUNT,
T.LN_FEE_AMT,
T.LN_REBATE_AMT,
T.CL_TYPE_CODE,
T.CL_SEQ_NUM,
T.CURRENCY_CD,
T.LN_TRNS_OFFER_AMT,
T.LN_TRNS_ACCEPT_AMT,
T.LN_TRNS_CERT_AMT,
T.LN_TRNS_BQSTD_DTL,
T.LOAN_PROC_STAT,
T.LN_ORIG_TRANS,
T.LN_GUAR_DT,
T.CL_SERV_TYPE_CD,
T.CL_REV_NOG_CD,
T.CL_GUAR_AMT_RED_CD,
T.LN_FORCE_CHG_SW,
T.SFA_CL_ESIGN_TYPE,
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
S.ITEM_TYPE,
'CS90',
S.LN_APPL_ID,
S.DISBURSEMENT_PLAN,
S.SPLIT_CODE,
S.LOAN_INTEREST_ATTR,
S.LN_BORRQSTD_DTL,
S.LN_AMT_CERTIFIED,
S.LN_ANTIC_FEE_TOT,
S.LN_ANTIC_REBATE,
S.LN_ANTIC_NET_TOT,
S.LN_REMAIN_ELGBLTY,
S.LN_AMT_APPROVED,
S.ACCEPT_AMOUNT,
S.OFFER_AMOUNT,
S.LN_FEE_AMT,
S.LN_REBATE_AMT,
S.CL_TYPE_CODE,
S.CL_SEQ_NUM,
S.CURRENCY_CD,
S.LN_TRNS_OFFER_AMT,
S.LN_TRNS_ACCEPT_AMT,
S.LN_TRNS_CERT_AMT,
S.LN_TRNS_BQSTD_DTL,
S.LOAN_PROC_STAT,
S.LN_ORIG_TRANS,
S.LN_GUAR_DT,
S.CL_SERV_TYPE_CD,
S.CL_REV_NOG_CD,
S.CL_GUAR_AMT_RED_CD,
S.LN_FORCE_CHG_SW,
S.SFA_CL_ESIGN_TYPE,
'N',
'S',
sysdate,
sysdate,
1234);


strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_LOAN_ORIG_DTL rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_LOAN_ORIG_DTL',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_LOAN_ORIG_DTL';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_LOAN_ORIG_DTL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_LOAN_ORIG_DTL';
update CSSTG_OWNER.PS_LOAN_ORIG_DTL T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, INSTITUTION, AID_YEAR, ACAD_CAREER, LOAN_TYPE, LN_APPL_SEQ, ITEM_TYPE
   from CSSTG_OWNER.PS_LOAN_ORIG_DTL T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_LOAN_ORIG_DTL') = 'Y'
  minus
 select EMPLID, INSTITUTION, AID_YEAR, ACAD_CAREER, LOAN_TYPE, LN_APPL_SEQ, ITEM_TYPE
   from SYSADM.PS_LOAN_ORIG_DTL@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_LOAN_ORIG_DTL') = 'Y'
   ) S
 where T.EMPLID = S.EMPLID
   and T.INSTITUTION = S.INSTITUTION
   and T.AID_YEAR = S.AID_YEAR
   and T.ACAD_CAREER = S.ACAD_CAREER
   and T.LOAN_TYPE = S.LOAN_TYPE
   and T.LN_APPL_SEQ = S.LN_APPL_SEQ
   and T.ITEM_TYPE = S.ITEM_TYPE
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_LOAN_ORIG_DTL rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_LOAN_ORIG_DTL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_LOAN_ORIG_DTL'
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

END PS_LOAN_ORIG_DTL_P;
/
