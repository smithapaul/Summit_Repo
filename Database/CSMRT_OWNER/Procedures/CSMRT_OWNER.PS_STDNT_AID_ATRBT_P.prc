DROP PROCEDURE CSMRT_OWNER.PS_STDNT_AID_ATRBT_P
/

--
-- PS_STDNT_AID_ATRBT_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_STDNT_AID_ATRBT_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_STDNT_AID_ATRBT from PeopleSoft table PS_STDNT_AID_ATRBT.
--
-- V01  SMT-xxxx 04/18/2017,    Jim Doucette
--                              Converted from PS_STDNT_AID_ATRBT.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_STDNT_AID_ATRBT';
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
 where TABLE_NAME = 'PS_STDNT_AID_ATRBT'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_STDNT_AID_ATRBT@SASOURCE S)
 where TABLE_NAME = 'PS_STDNT_AID_ATRBT'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_STDNT_AID_ATRBT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_STDNT_AID_ATRBT';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_STDNT_AID_ATRBT T
using (select /*+ full(S) */
nvl(trim(EMPLID),'-') EMPLID,
nvl(trim(INSTITUTION),'-') INSTITUTION,
nvl(trim(AID_YEAR),'-') AID_YEAR,
nvl(TABLE_ID,0) TABLE_ID,
nvl(trim(PROCESSING_STATUS),'-') PROCESSING_STATUS,
nvl(trim(VERIFICATON_STATUS),'-') VERIFICATON_STATUS,
nvl(trim(SFA_REVIEW_STATUS),'-') SFA_REVIEW_STATUS,
nvl(trim(SCHOLARSHIP_STATUS),'-') SCHOLARSHIP_STATUS,
nvl(trim(PACKAGING_METHOD),'-') PACKAGING_METHOD,
nvl(trim(PKG_PLAN_ID),'-') PKG_PLAN_ID,
nvl(trim(SAT_ACADEMIC_PRG),'-') SAT_ACADEMIC_PRG,
nvl(trim(DISBURSEMENT_HOLD),'-') DISBURSEMENT_HOLD,
nvl(trim(COUNSELOR),'-') COUNSELOR,
nvl(trim(COMMUNITY_SERVICE),'-') COMMUNITY_SERVICE,
nvl(trim(QA_VERF_SELECT),'-') QA_VERF_SELECT,
nvl(trim(QA_SELECTED),'-') QA_SELECTED,
nvl(trim(AID_APP_STATUS),'-') AID_APP_STATUS,
nvl(trim(VERIF_STATUS_CODE),'-') VERIF_STATUS_CODE,
nvl(trim(PELL_PROCESS_FIELD),'-') PELL_PROCESS_FIELD,
nvl(trim(ED_RECORD_STATUS),'-') ED_RECORD_STATUS,
nvl(trim(ED_LEVEL_SAT),'-') ED_LEVEL_SAT,
to_date(to_char(case when PELL_EFFDT < '01-JAN-1800' then NULL else PELL_EFFDT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') PELL_EFFDT,
nvl(PELL_EFFSEQ,0) PELL_EFFSEQ,
nvl(PELL_TRANS_NBR,0) PELL_TRANS_NBR,
nvl(trim(USE_SECONDARY_EFC),'-') USE_SECONDARY_EFC,
nvl(trim(ACAD_CAREER),'-') ACAD_CAREER,
nvl(trim(PAR_CREDIT_WORTHY),'-') PAR_CREDIT_WORTHY,
nvl(trim(TITLEIV_ELIG),'-') TITLEIV_ELIG,
nvl(trim(SS_MATCH),'-') SS_MATCH,
nvl(trim(SS_REGISTRATION),'-') SS_REGISTRATION,
nvl(trim(INS_MATCH),'-') INS_MATCH,
nvl(trim(SSN_MATCH),'-') SSN_MATCH,
nvl(trim(VA_MATCH),'-') VA_MATCH,
nvl(trim(NSLDS_LOAN_DEFAULT),'-') NSLDS_LOAN_DEFAULT,
nvl(trim(SSA_CITIZENSHP_IND),'-') SSA_CITIZENSHP_IND,
nvl(trim(NSLDS_MATCH),'-') NSLDS_MATCH,
nvl(trim(DL_HEAL_LN_SW),'-') DL_HEAL_LN_SW,
nvl(trim(DRUG_OFFENSE_CONV),'-') DRUG_OFFENSE_CONV,
nvl(trim(PRISONER_MATCH),'-') PRISONER_MATCH,
nvl(trim(LN_INTERVW_STATUS),'-') LN_INTERVW_STATUS,
nvl(trim(LN_EXIT_INTER_STAT),'-') LN_EXIT_INTER_STAT,
nvl(trim(SSN_MATCH_OVRD),'-') SSN_MATCH_OVRD,
nvl(trim(SSA_CITIZEN_OVRD),'-') SSA_CITIZEN_OVRD,
nvl(trim(INS_MATCH_OVRD),'-') INS_MATCH_OVRD,
nvl(trim(VA_MATCH_OVRD),'-') VA_MATCH_OVRD,
nvl(trim(SS_MATCH_OVRD),'-') SS_MATCH_OVRD,
nvl(trim(SS_REGISTER_OVRD),'-') SS_REGISTER_OVRD,
nvl(trim(NSLDS_OVRD),'-') NSLDS_OVRD,
nvl(trim(PRISONER_OVRD),'-') PRISONER_OVRD,
nvl(trim(DRUG_OFFENSE_OVRD),'-') DRUG_OFFENSE_OVRD,
nvl(trim(ISIR_SEC_INS_MATCH),'-') ISIR_SEC_INS_MATCH,
to_date(to_char(case when PKG_LASTUPDDTTM < '01-JAN-1800' then NULL else PKG_LASTUPDDTTM end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') PKG_LASTUPDDTTM,
nvl(trim(FA_SS_AWD_SECURITY),'-') FA_SS_AWD_SECURITY,
nvl(trim(FA_SS_INQ_SECURITY),'-') FA_SS_INQ_SECURITY,
nvl(trim(FA_SS_FAN_SECURITY),'-') FA_SS_FAN_SECURITY,
nvl(trim(FATHER_SSN_MATCH),'-') FATHER_SSN_MATCH,
nvl(trim(MOTHER_SSN_MATCH),'-') MOTHER_SSN_MATCH,
nvl(trim(PAR_SSN_MATCH_OVRD),'-') PAR_SSN_MATCH_OVRD,
nvl(trim(SFA_AGGR_SOURCE),'-') SFA_AGGR_SOURCE,
nvl(trim(SFA_AGGR_SRC_USED),'-') SFA_AGGR_SRC_USED,
nvl(trim(SFA_PKG_DEP_STAT),'-') SFA_PKG_DEP_STAT,
nvl(trim(SFA_RPKG_PLAN_ID),'-') SFA_RPKG_PLAN_ID,
nvl(trim(SFA_EASS_ACCESS),'-') SFA_EASS_ACCESS,
nvl(trim(SFA_PP_CRSEWRK_SW),'-') SFA_PP_CRSEWRK_SW,
nvl(trim(SFA_DOD_MATCH),'-') SFA_DOD_MATCH,
nvl(trim(SFA_DOD_MATCH_OVRD),'-') SFA_DOD_MATCH_OVRD,
nvl(trim(SFA_SPL_CIRCUM_FLG),'-') SFA_SPL_CIRCUM_FLG,
nvl(trim(SFA_SS_GROUP),'-') SFA_SS_GROUP
from SYSADM.PS_STDNT_AID_ATRBT@SASOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_STDNT_AID_ATRBT') 
AND LENGTH(EMPLID) = 8 AND EMPLID BETWEEN '00000000' AND '99999999') S
   on (
T.EMPLID = S.EMPLID and
T.INSTITUTION = S.INSTITUTION and
T.AID_YEAR = S.AID_YEAR and
T.TABLE_ID = S.TABLE_ID and
T.SRC_SYS_ID = 'CS90')
when matched then update set
T.PROCESSING_STATUS = S.PROCESSING_STATUS,
T.VERIFICATON_STATUS = S.VERIFICATON_STATUS,
T.SFA_REVIEW_STATUS = S.SFA_REVIEW_STATUS,
T.SCHOLARSHIP_STATUS = S.SCHOLARSHIP_STATUS,
T.PACKAGING_METHOD = S.PACKAGING_METHOD,
T.PKG_PLAN_ID = S.PKG_PLAN_ID,
T.SAT_ACADEMIC_PRG = S.SAT_ACADEMIC_PRG,
T.DISBURSEMENT_HOLD = S.DISBURSEMENT_HOLD,
T.COUNSELOR = S.COUNSELOR,
T.COMMUNITY_SERVICE = S.COMMUNITY_SERVICE,
T.QA_VERF_SELECT = S.QA_VERF_SELECT,
T.QA_SELECTED = S.QA_SELECTED,
T.AID_APP_STATUS = S.AID_APP_STATUS,
T.VERIF_STATUS_CODE = S.VERIF_STATUS_CODE,
T.PELL_PROCESS_FIELD = S.PELL_PROCESS_FIELD,
T.ED_RECORD_STATUS = S.ED_RECORD_STATUS,
T.ED_LEVEL_SAT = S.ED_LEVEL_SAT,
T.PELL_EFFDT = S.PELL_EFFDT,
T.PELL_EFFSEQ = S.PELL_EFFSEQ,
T.PELL_TRANS_NBR = S.PELL_TRANS_NBR,
T.USE_SECONDARY_EFC = S.USE_SECONDARY_EFC,
T.ACAD_CAREER = S.ACAD_CAREER,
T.PAR_CREDIT_WORTHY = S.PAR_CREDIT_WORTHY,
T.TITLEIV_ELIG = S.TITLEIV_ELIG,
T.SS_MATCH = S.SS_MATCH,
T.SS_REGISTRATION = S.SS_REGISTRATION,
T.INS_MATCH = S.INS_MATCH,
T.SSN_MATCH = S.SSN_MATCH,
T.VA_MATCH = S.VA_MATCH,
T.NSLDS_LOAN_DEFAULT = S.NSLDS_LOAN_DEFAULT,
T.SSA_CITIZENSHP_IND = S.SSA_CITIZENSHP_IND,
T.NSLDS_MATCH = S.NSLDS_MATCH,
T.DL_HEAL_LN_SW = S.DL_HEAL_LN_SW,
T.DRUG_OFFENSE_CONV = S.DRUG_OFFENSE_CONV,
T.PRISONER_MATCH = S.PRISONER_MATCH,
T.LN_INTERVW_STATUS = S.LN_INTERVW_STATUS,
T.LN_EXIT_INTER_STAT = S.LN_EXIT_INTER_STAT,
T.SSN_MATCH_OVRD = S.SSN_MATCH_OVRD,
T.SSA_CITIZEN_OVRD = S.SSA_CITIZEN_OVRD,
T.INS_MATCH_OVRD = S.INS_MATCH_OVRD,
T.VA_MATCH_OVRD = S.VA_MATCH_OVRD,
T.SS_MATCH_OVRD = S.SS_MATCH_OVRD,
T.SS_REGISTER_OVRD = S.SS_REGISTER_OVRD,
T.NSLDS_OVRD = S.NSLDS_OVRD,
T.PRISONER_OVRD = S.PRISONER_OVRD,
T.DRUG_OFFENSE_OVRD = S.DRUG_OFFENSE_OVRD,
T.ISIR_SEC_INS_MATCH = S.ISIR_SEC_INS_MATCH,
T.PKG_LASTUPDDTTM = S.PKG_LASTUPDDTTM,
T.FA_SS_AWD_SECURITY = S.FA_SS_AWD_SECURITY,
T.FA_SS_INQ_SECURITY = S.FA_SS_INQ_SECURITY,
T.FA_SS_FAN_SECURITY = S.FA_SS_FAN_SECURITY,
T.FATHER_SSN_MATCH = S.FATHER_SSN_MATCH,
T.MOTHER_SSN_MATCH = S.MOTHER_SSN_MATCH,
T.PAR_SSN_MATCH_OVRD = S.PAR_SSN_MATCH_OVRD,
T.SFA_AGGR_SOURCE = S.SFA_AGGR_SOURCE,
T.SFA_AGGR_SRC_USED = S.SFA_AGGR_SRC_USED,
T.SFA_PKG_DEP_STAT = S.SFA_PKG_DEP_STAT,
T.SFA_RPKG_PLAN_ID = S.SFA_RPKG_PLAN_ID,
T.SFA_EASS_ACCESS = S.SFA_EASS_ACCESS,
T.SFA_PP_CRSEWRK_SW = S.SFA_PP_CRSEWRK_SW,
T.SFA_DOD_MATCH = S.SFA_DOD_MATCH,
T.SFA_DOD_MATCH_OVRD = S.SFA_DOD_MATCH_OVRD,
T.SFA_SPL_CIRCUM_FLG = S.SFA_SPL_CIRCUM_FLG,
T.SFA_SS_GROUP = S.SFA_SS_GROUP,
T.DATA_ORIGIN = 'S',
T.LASTUPD_EW_DTTM = sysdate,
T.BATCH_SID   = 1234
where
T.PROCESSING_STATUS <> S.PROCESSING_STATUS or
T.VERIFICATON_STATUS <> S.VERIFICATON_STATUS or
T.SFA_REVIEW_STATUS <> S.SFA_REVIEW_STATUS or
T.SCHOLARSHIP_STATUS <> S.SCHOLARSHIP_STATUS or
T.PACKAGING_METHOD <> S.PACKAGING_METHOD or
T.PKG_PLAN_ID <> S.PKG_PLAN_ID or
T.SAT_ACADEMIC_PRG <> S.SAT_ACADEMIC_PRG or
T.DISBURSEMENT_HOLD <> S.DISBURSEMENT_HOLD or
T.COUNSELOR <> S.COUNSELOR or
T.COMMUNITY_SERVICE <> S.COMMUNITY_SERVICE or
T.QA_VERF_SELECT <> S.QA_VERF_SELECT or
T.QA_SELECTED <> S.QA_SELECTED or
T.AID_APP_STATUS <> S.AID_APP_STATUS or
T.VERIF_STATUS_CODE <> S.VERIF_STATUS_CODE or
T.PELL_PROCESS_FIELD <> S.PELL_PROCESS_FIELD or
T.ED_RECORD_STATUS <> S.ED_RECORD_STATUS or
T.ED_LEVEL_SAT <> S.ED_LEVEL_SAT or
nvl(trim(T.PELL_EFFDT),0) <> nvl(trim(S.PELL_EFFDT),0) or
T.PELL_EFFSEQ <> S.PELL_EFFSEQ or
T.PELL_TRANS_NBR <> S.PELL_TRANS_NBR or
T.USE_SECONDARY_EFC <> S.USE_SECONDARY_EFC or
T.ACAD_CAREER <> S.ACAD_CAREER or
T.PAR_CREDIT_WORTHY <> S.PAR_CREDIT_WORTHY or
T.TITLEIV_ELIG <> S.TITLEIV_ELIG or
T.SS_MATCH <> S.SS_MATCH or
T.SS_REGISTRATION <> S.SS_REGISTRATION or
T.INS_MATCH <> S.INS_MATCH or
T.SSN_MATCH <> S.SSN_MATCH or
T.VA_MATCH <> S.VA_MATCH or
T.NSLDS_LOAN_DEFAULT <> S.NSLDS_LOAN_DEFAULT or
T.SSA_CITIZENSHP_IND <> S.SSA_CITIZENSHP_IND or
T.NSLDS_MATCH <> S.NSLDS_MATCH or
T.DL_HEAL_LN_SW <> S.DL_HEAL_LN_SW or
T.DRUG_OFFENSE_CONV <> S.DRUG_OFFENSE_CONV or
T.PRISONER_MATCH <> S.PRISONER_MATCH or
T.LN_INTERVW_STATUS <> S.LN_INTERVW_STATUS or
T.LN_EXIT_INTER_STAT <> S.LN_EXIT_INTER_STAT or
T.SSN_MATCH_OVRD <> S.SSN_MATCH_OVRD or
T.SSA_CITIZEN_OVRD <> S.SSA_CITIZEN_OVRD or
T.INS_MATCH_OVRD <> S.INS_MATCH_OVRD or
T.VA_MATCH_OVRD <> S.VA_MATCH_OVRD or
T.SS_MATCH_OVRD <> S.SS_MATCH_OVRD or
T.SS_REGISTER_OVRD <> S.SS_REGISTER_OVRD or
T.NSLDS_OVRD <> S.NSLDS_OVRD or
T.PRISONER_OVRD <> S.PRISONER_OVRD or
T.DRUG_OFFENSE_OVRD <> S.DRUG_OFFENSE_OVRD or
T.ISIR_SEC_INS_MATCH <> S.ISIR_SEC_INS_MATCH or
nvl(trim(T.PKG_LASTUPDDTTM),0) <> nvl(trim(S.PKG_LASTUPDDTTM),0) or
T.FA_SS_AWD_SECURITY <> S.FA_SS_AWD_SECURITY or
T.FA_SS_INQ_SECURITY <> S.FA_SS_INQ_SECURITY or
T.FA_SS_FAN_SECURITY <> S.FA_SS_FAN_SECURITY or
T.FATHER_SSN_MATCH <> S.FATHER_SSN_MATCH or
T.MOTHER_SSN_MATCH <> S.MOTHER_SSN_MATCH or
T.PAR_SSN_MATCH_OVRD <> S.PAR_SSN_MATCH_OVRD or
T.SFA_AGGR_SOURCE <> S.SFA_AGGR_SOURCE or
T.SFA_AGGR_SRC_USED <> S.SFA_AGGR_SRC_USED or
T.SFA_PKG_DEP_STAT <> S.SFA_PKG_DEP_STAT or
T.SFA_RPKG_PLAN_ID <> S.SFA_RPKG_PLAN_ID or
T.SFA_EASS_ACCESS <> S.SFA_EASS_ACCESS or
T.SFA_PP_CRSEWRK_SW <> S.SFA_PP_CRSEWRK_SW or
T.SFA_DOD_MATCH <> S.SFA_DOD_MATCH or
T.SFA_DOD_MATCH_OVRD <> S.SFA_DOD_MATCH_OVRD or
T.SFA_SPL_CIRCUM_FLG <> S.SFA_SPL_CIRCUM_FLG or
T.SFA_SS_GROUP <> S.SFA_SS_GROUP or
T.DATA_ORIGIN = 'D'
when not matched then
insert (
T.EMPLID,
T.INSTITUTION,
T.AID_YEAR,
T.TABLE_ID,
T.SRC_SYS_ID,
T.PROCESSING_STATUS,
T.VERIFICATON_STATUS,
T.SFA_REVIEW_STATUS,
T.SCHOLARSHIP_STATUS,
T.PACKAGING_METHOD,
T.PKG_PLAN_ID,
T.SAT_ACADEMIC_PRG,
T.DISBURSEMENT_HOLD,
T.COUNSELOR,
T.COMMUNITY_SERVICE,
T.QA_VERF_SELECT,
T.QA_SELECTED,
T.AID_APP_STATUS,
T.VERIF_STATUS_CODE,
T.PELL_PROCESS_FIELD,
T.ED_RECORD_STATUS,
T.ED_LEVEL_SAT,
T.PELL_EFFDT,
T.PELL_EFFSEQ,
T.PELL_TRANS_NBR,
T.USE_SECONDARY_EFC,
T.ACAD_CAREER,
T.PAR_CREDIT_WORTHY,
T.TITLEIV_ELIG,
T.SS_MATCH,
T.SS_REGISTRATION,
T.INS_MATCH,
T.SSN_MATCH,
T.VA_MATCH,
T.NSLDS_LOAN_DEFAULT,
T.SSA_CITIZENSHP_IND,
T.NSLDS_MATCH,
T.DL_HEAL_LN_SW,
T.DRUG_OFFENSE_CONV,
T.PRISONER_MATCH,
T.LN_INTERVW_STATUS,
T.LN_EXIT_INTER_STAT,
T.SSN_MATCH_OVRD,
T.SSA_CITIZEN_OVRD,
T.INS_MATCH_OVRD,
T.VA_MATCH_OVRD,
T.SS_MATCH_OVRD,
T.SS_REGISTER_OVRD,
T.NSLDS_OVRD,
T.PRISONER_OVRD,
T.DRUG_OFFENSE_OVRD,
T.ISIR_SEC_INS_MATCH,
T.PKG_LASTUPDDTTM,
T.FA_SS_AWD_SECURITY,
T.FA_SS_INQ_SECURITY,
T.FA_SS_FAN_SECURITY,
T.FATHER_SSN_MATCH,
T.MOTHER_SSN_MATCH,
T.PAR_SSN_MATCH_OVRD,
T.SFA_AGGR_SOURCE,
T.SFA_AGGR_SRC_USED,
T.SFA_PKG_DEP_STAT,
T.SFA_RPKG_PLAN_ID,
T.SFA_EASS_ACCESS,
T.SFA_PP_CRSEWRK_SW,
T.SFA_DOD_MATCH,
T.SFA_DOD_MATCH_OVRD,
T.SFA_SPL_CIRCUM_FLG,
T.SFA_SS_GROUP,
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
S.TABLE_ID,
'CS90',
S.PROCESSING_STATUS,
S.VERIFICATON_STATUS,
S.SFA_REVIEW_STATUS,
S.SCHOLARSHIP_STATUS,
S.PACKAGING_METHOD,
S.PKG_PLAN_ID,
S.SAT_ACADEMIC_PRG,
S.DISBURSEMENT_HOLD,
S.COUNSELOR,
S.COMMUNITY_SERVICE,
S.QA_VERF_SELECT,
S.QA_SELECTED,
S.AID_APP_STATUS,
S.VERIF_STATUS_CODE,
S.PELL_PROCESS_FIELD,
S.ED_RECORD_STATUS,
S.ED_LEVEL_SAT,
S.PELL_EFFDT,
S.PELL_EFFSEQ,
S.PELL_TRANS_NBR,
S.USE_SECONDARY_EFC,
S.ACAD_CAREER,
S.PAR_CREDIT_WORTHY,
S.TITLEIV_ELIG,
S.SS_MATCH,
S.SS_REGISTRATION,
S.INS_MATCH,
S.SSN_MATCH,
S.VA_MATCH,
S.NSLDS_LOAN_DEFAULT,
S.SSA_CITIZENSHP_IND,
S.NSLDS_MATCH,
S.DL_HEAL_LN_SW,
S.DRUG_OFFENSE_CONV,
S.PRISONER_MATCH,
S.LN_INTERVW_STATUS,
S.LN_EXIT_INTER_STAT,
S.SSN_MATCH_OVRD,
S.SSA_CITIZEN_OVRD,
S.INS_MATCH_OVRD,
S.VA_MATCH_OVRD,
S.SS_MATCH_OVRD,
S.SS_REGISTER_OVRD,
S.NSLDS_OVRD,
S.PRISONER_OVRD,
S.DRUG_OFFENSE_OVRD,
S.ISIR_SEC_INS_MATCH,
S.PKG_LASTUPDDTTM,
S.FA_SS_AWD_SECURITY,
S.FA_SS_INQ_SECURITY,
S.FA_SS_FAN_SECURITY,
S.FATHER_SSN_MATCH,
S.MOTHER_SSN_MATCH,
S.PAR_SSN_MATCH_OVRD,
S.SFA_AGGR_SOURCE,
S.SFA_AGGR_SRC_USED,
S.SFA_PKG_DEP_STAT,
S.SFA_RPKG_PLAN_ID,
S.SFA_EASS_ACCESS,
S.SFA_PP_CRSEWRK_SW,
S.SFA_DOD_MATCH,
S.SFA_DOD_MATCH_OVRD,
S.SFA_SPL_CIRCUM_FLG,
S.SFA_SS_GROUP,
'N',
'S',
sysdate,
sysdate,
1234);

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_STDNT_AID_ATRBT rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_STDNT_AID_ATRBT',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_STDNT_AID_ATRBT';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_STDNT_AID_ATRBT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_STDNT_AID_ATRBT';
update CSSTG_OWNER.PS_STDNT_AID_ATRBT T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, INSTITUTION, AID_YEAR, TABLE_ID
   from CSSTG_OWNER.PS_STDNT_AID_ATRBT T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_STDNT_AID_ATRBT') = 'Y'
  minus
 select nvl(trim(EMPLID),'-'), nvl(trim(INSTITUTION),'-'), nvl(trim(AID_YEAR),'-'), nvl(trim(TABLE_ID),'-')
   from SYSADM.PS_STDNT_AID_ATRBT@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_STDNT_AID_ATRBT') = 'Y'
   ) S
 where T.EMPLID = S.EMPLID
   and T.INSTITUTION = S.INSTITUTION
   and T.AID_YEAR = S.AID_YEAR
   and T.TABLE_ID = S.TABLE_ID
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_STDNT_AID_ATRBT rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_STDNT_AID_ATRBT',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_STDNT_AID_ATRBT'
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

END PS_STDNT_AID_ATRBT_P;
/
