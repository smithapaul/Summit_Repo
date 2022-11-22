DROP PROCEDURE CSMRT_OWNER.UM_F_FA_STDNT_AID_ISIR_P
/

--
-- UM_F_FA_STDNT_AID_ISIR_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_F_FA_STDNT_AID_ISIR_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads mart table UM_F_FA_STDNT_AID_ISIR.
--
 --V01  SMT-xxxx 07/11/2018,    James Doucette
--                              Converted from SQL Script
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_FA_STDNT_AID_ISIR';
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

strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_FA_STDNT_AID_ISIR';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_FA_STDNT_AID_ISIR';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_FA_STDNT_AID_ISIR';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_FA_STDNT_AID_ISIR');

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_FA_STDNT_AID_ISIR disable constraint PK_UM_F_FA_STDNT_AID_ISIR';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_FA_STDNT_AID_ISIR';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_FA_STDNT_AID_ISIR';
insert /*+ append enable_parallel_dml parallel(8) */ into UM_F_FA_STDNT_AID_ISIR
    WITH
        STDNT
        AS
            (  SELECT /*+ INLINE */
                      INSTITUTION_CD,
                      PERSON_ID,
                      SRC_SYS_ID,
                      SUM (ADM_CNT)        ADM_CNT,
                      SUM (SR_CNT)         SR_CNT,
                      SUM (PRSPCT_CNT)     PRSPCT_CNT
                 FROM CSMRT_OWNER.UM_F_STDNT
             GROUP BY INSTITUTION_CD, PERSON_ID, SRC_SYS_ID),
        STAID
        AS
            (SELECT /*+ INLINE */
                    STDNT.INSTITUTION_CD,
                    STDNT.PERSON_ID,
                    STDNT.ADM_CNT,
                    STDNT.SR_CNT,
                    STDNT.PRSPCT_CNT,
                    STDNT.SRC_SYS_ID,
                    CASE WHEN (STAID.EMPLID IS NULL) THEN (0) ELSE (1) END
                        STAID_ACTV_CNT,
                    NVL (STAID.AID_YEAR, '-')
                        AID_YEAR,
                    NVL (STAID.FED_DEPEND_STAT, '-')
                        FED_DEPEND_STAT
               FROM STDNT
                    LEFT OUTER JOIN CSSTG_OWNER.PS_STUDENT_AID STAID
                        ON     STDNT.PERSON_ID = STAID.EMPLID
                           AND STDNT.INSTITUTION_CD = STAID.INSTITUTION
                           AND STAID.DATA_ORIGIN <> 'D'),
        PKG_PLAN
        AS
            (SELECT /*+ INLINE */
                    INSTITUTION,
                    AID_YEAR,
                    ACAD_CAREER,
                    PKG_PLAN_ID,
                    EFFDT,
                    SRC_SYS_ID,
                    EQUITY_LIMIT,
                    ROW_NUMBER ()
                        OVER (PARTITION BY INSTITUTION,
                                           AID_YEAR,
                                           ACAD_CAREER,
                                           PKG_PLAN_ID,
                                           SRC_SYS_ID
                              ORDER BY EFFDT DESC)
                        PKG_PLAN_ORDER
               FROM CSSTG_OWNER.PS_PKG_PLAN_TBL
              WHERE DATA_ORIGIN <> 'D'),
        ISIR
        AS
            (SELECT /*+ INLINE */
                    ISIRC.EMPLID,
                    ISIRC.INSTITUTION,
                    ISIRC.AID_YEAR,
                    ISIRC.EFFDT,
                    ISIRC.EFFSEQ,
                    ISIRC.SRC_SYS_ID,
                    ISIRC.ISIR_TXN_NBR,
                    ISIRC.ORIG_SSN,
                    ISIRC.NAME_CD,
                    ISIRC.DT_APP_RECEIVED,
                    ISIRC.PELL_ELIGIBILITY,
                    ISIRC.SAR_C_FLAG,
                    ISIRC.VERF_SELECTION_IND,
                    ISIRC.TRANS_PROCESS_DT,
                    ISIRC.CPS_REPROCESS_CD,
                    ISIRC.BATCH_NUMBER,
                    ISIRC.FAA_INSTITUT_NUM,
                    ISIRC.INS_VERF_NUM,
                    ISIRC.DEPNDNCY_OVERRIDE,
                    ISIRC.APP_STAT_DT,
                    ISIRC.EFC_STATUS,
                    ISIRC.ADJ_EFC_CALC_REQ,
                    ISIRC.CORRECTION_STATUS,
                    ISIRC.CORR_STAT_DT,
                    ISIRC.INST_PROCESS_DT,
                    ISIRC.OWNING_SCHOOL_CD,
                    ISIRC.NSLDS_TXN_NBR,
                    ISIRC.APP_SOURCE,
                    ISIRC.TRANS_SOURCE,
                    ISIRC.ISIR_CORR_SRC,
                    ISIRC.ISIR_EFC_CHANGE,
                    ISIRC.ISIR_DUP_SSN_CD,
                    ISIRC.VERIF_TRK_FLG,
                    ISIRC.ISIR_CPS_PUSHED,
                    ISIRC.ISIR_SAR_C_CHNG,
                    ISIRC.SFA_VF_SEL_CHG_FLG,
                    ISIRC.SFA_REJ_ST_CHG_FLG,
                    ISIRC.SFA_STU_IRS_REQUST,
                    ISIRC.SFA_PAR_IRS_REQUST,
                    ISIRC.SFA_HIGH_SCHL_FLAG,
                    ISIRC.TITLEIV_ELIG,
                    ISIRC.SFA_SPL_CIRCUM_FLG
                        AS SFA_SPL_CIRCUM_FLG_ISIR,
                    ISIRS.NUM_FAMILY_MEMBERS
                        AS S_NUM_FAMILY_MEMBERS,
                    ISIRS.NUMBER_IN_COLLEGE
                        AS S_NUMBER_IN_COLLEGE,
                    ISIRS.VETERAN
                        AS S_VETERAN,
                    ISIRS.GRADUATE_STUDENT
                        AS S_GRADUATE_STUDENT,
                    ISIRS.MARRIED
                        AS S_MARRIED,
                    ISIRS.MARITAL_STAT
                        AS S_MARITAL_STAT,
                    ISIRS.MAR_STATUS_DT
                        AS S_MAR_STATUS_DT,
                    ISIRS.ORPHAN
                        AS S_ORPHAN,
                    ISIRS.DEPENDENTS
                        AS S_DEPENDENTS,
                    ISIRS.TAX_FORM_FILED
                        AS S_TAX_FORM_FILED,
                    ISIRS.NUMBER_EXEMPTIONS
                        AS S_NUMBER_EXEMPTIONS,
                    ISIRS.AGI
                        AS S_AGI,
                    ISIRS.TAXES_PAID
                        AS S_TAXES_PAID,
                    ISIRS.STD_EARNED_INCOME
                        AS S_STD_EARNED_INCOME,
                    ISIRS.SPS_EARNED_INCOME
                        AS S_SPS_EARNED_INCOME,
                    ISIRS.CASH_SAVINGS
                        AS S_CASH_SAVINGS,
                    ISIRS.SCHOOL_CHOICE_1
                        AS S_SCHOOL_CHOICE_1,
                    ISIRS.HOUSING_CODE_1
                        AS S_HOUSING_CODE_1,
                    ISIRS.SCHOOL_CHOICE_2
                        AS S_SCHOOL_CHOICE_2,
                    ISIRS.HOUSING_CODE_2
                        AS S_HOUSING_CODE_2,
                    ISIRS.SCHOOL_CHOICE_3
                        AS S_SCHOOL_CHOICE_3,
                    ISIRS.HOUSING_CODE_3
                        AS S_HOUSING_CODE_3,
                    ISIRS.SCHOOL_CHOICE_4
                        AS S_SCHOOL_CHOICE_4,
                    ISIRS.HOUSING_CODE_4
                        AS S_HOUSING_CODE_4,
                    ISIRS.SCHOOL_CHOICE_5
                        AS S_SCHOOL_CHOICE_5,
                    ISIRS.HOUSING_CODE_5
                        AS S_HOUSING_CODE_5,
                    ISIRS.SCHOOL_CHOICE_6
                        AS S_SCHOOL_CHOICE_6,
                    ISIRS.HOUSING_CODE_6
                        AS S_HOUSING_CODE_6,
                    ISIRS.FIRST_BACH_DEGREE
                        AS S_FIRST_BACH_DEGREE,
                    ISIRS.INTERESTED_IN_WS
                        AS S_INTERESTED_IN_WS,
                    ISIRS.DT_APP_COMPLETED
                        AS S_DT_APP_COMPLETED,
                    ISIRS.DEGREE_CERTIF
                        AS S_DEGREE_CERTIF,
                    ISIRS.CURRENT_GRADE_LVL
                        AS S_CURRENT_GRADE_LVL,
                    ISIRS.TOTAL_FROM_WS3
                        AS S_TOTAL_FROM_WS3,
                    ISIRS.STATE_RESIDENCE
                        AS S_STATE_RESIDENCE,
                    ISIRS.RESIDENCY_DT
                        AS S_RESIDENCY_DT,
                    ISIRS.DRIV_LIC_STATE
                        AS S_DRIV_LIC_STATE,
                    ISIRS.FISAP_TOT_INC
                        AS S_FISAP_TOT_INC,
                    ISIRS.CITIZENSHIP_STATUS
                        AS S_CITIZENSHIP_STATUS,
                    ISIRS.TAX_RETURN_FILED
                        AS S_TAX_RETURN_FILED,
                    ISIRS.ELIG_FOR_1040A_EZ
                        AS S_ELIG_FOR_1040A_EZ,
                    ISIRS.INV_NET_WORTH
                        AS S_INV_NET_WORTH,
                    ISIRS.BUS_NET_WORTH
                        AS S_BUS_NET_WORTH,
                    ISIRS.LEGAL_RES_PRIOR
                        AS S_LEGAL_RES_PRIOR,
                    ISIRS.IWD_PERM_ADDR02
                        AS S_IWD_PERM_ADDR02,
                    ISIRS.IWD_CITY
                        AS S_IWD_CITY,
                    ISIRS.IWD_STATE
                        AS S_IWD_STATE,
                    ISIRS.IWD_ZIP
                        AS S_IWD_ZIP,
                    ISIRS.IWD_PERM_PHONE
                        AS S_IWD_PERM_PHONE,
                    ISIRS.IWD_STD_LAST_NAME
                        AS S_IWD_STD_LAST_NAME,
                    ISIRS.IWD_STD_FIRST_NM02
                        AS S_IWD_STD_FIRST_NM02,
                    ISIRS.IWD_STU_MI
                        AS S_IWD_STU_MI,
                    ISIRS.SSN
                        AS S_SSN,
                    ISIRS.BIRTHDATE
                        AS S_BIRTHDATE,
                    ISIRS.DRIVERS_LICENSE_NO
                        AS S_DRIVERS_LICENSE_NO,
                    ISIRS.IWD_STU_ALIEN_REG
                        AS S_IWD_STU_ALIEN_REG,
                    ISIRS.CHILDREN
                        AS S_CHILDREN,
                    ISIRS.TOTAL_FROM_WKC
                        AS S_TOTAL_FROM_WKC,
                    ISIRS.SFA_ACTIVE_DUTY
                        AS S_SFA_ACTIVE_DUTY,
                    ISIRS.SFA_SSI_INCOME
                        AS S_SFA_SSI_INCOME,
                    ISIRS.SFA_FOOD_STAMPS
                        AS S_SFA_FOOD_STAMPS,
                    ISIRS.SFA_SCHL_LUNCH_PRG
                        AS S_SFA_SCHL_LUNCH_PRG,
                    ISIRS.SFA_TANF_BENEFITS
                        AS S_SFA_TANF_BENEFITS,
                    ISIRS.SFA_WIC_BENEFITS
                        AS S_SFA_WIC_BENEFITS,
                    ISIRS.SFA_STDNT_GENDER
                        AS S_SFA_STDNT_GENDER,
                    ISIRS.SFA_SCHL_CHOICE_7
                        AS S_SFA_SCHL_CHOICE_7,
                    ISIRS.SFA_HOUSING_CODE7
                        AS S_SFA_HOUSING_CODE7,
                    ISIRS.SFA_SCHL_CHOICE_8
                        AS S_SFA_SCHL_CHOICE_8,
                    ISIRS.SFA_HOUSING_CODE8
                        AS S_SFA_HOUSING_CODE8,
                    ISIRS.SFA_SCHL_CHOICE_9
                        AS S_SFA_SCHL_CHOICE_9,
                    ISIRS.SFA_HOUSING_CODE9
                        AS S_SFA_HOUSING_CODE9,
                    ISIRS.SFA_SCHL_CHOICE_10
                        AS S_SFA_SCHL_CHOICE_10,
                    ISIRS.SFA_HOUSING_CODE10
                        AS S_SFA_HOUSING_CODE10,
                    ISIRS.SFA_EMANCIPT_MINOR
                        AS S_SFA_EMANCIPT_MINOR,
                    ISIRS.SFA_LEGAL_GUARDIAN
                        AS S_SFA_LEGAL_GUARDIAN,
                    ISIRS.SFA_YOUTH_LIASON
                        AS S_SFA_YOUTH_LIASON,
                    ISIRS.SFA_YOUTH_HUD
                        AS S_SFA_YOUTH_HUD,
                    ISIRS.SFA_RISK_HOMELESS
                        AS S_SFA_RISK_HOMELESS,
                    ISIRS.SFA_DISLOCATE_WRK
                        AS S_SFA_DISLOCATE_WRK,
                    ISIRS.SFA_EDU_CREDITS
                        AS S_SFA_EDU_CREDITS,
                    ISIRS.SFA_CHILD_SUP_PAID
                        AS S_SFA_CHILD_SUP_PAID,
                    ISIRS.SFA_NEED_EMPLOYMNT
                        AS S_SFA_NEED_EMPLOYMNT,
                    ISIRS.SFA_GRANT_AID
                        AS S_SFA_GRANT_AID,
                    ISIRS.SFA_COMBATPAY
                        AS S_SFA_COMBATPAY,
                    ISIRS.SFA_PENSION_PAY
                        AS S_SFA_PENSION_PAY,
                    ISIRS.SFA_IRA_PAY
                        AS S_SFA_IRA_PAY,
                    ISIRS.SFA_CHILD_SUP_RECV
                        AS S_SFA_CHILD_SUP_RECV,
                    ISIRS.SFA_INTERST_INCOME
                        AS S_SFA_INTERST_INCOME,
                    ISIRS.SFA_IRA_DIST
                        AS S_SFA_IRA_DIST,
                    ISIRS.SFA_UNTAX_PENSION
                        AS S_SFA_UNTAX_PENSION,
                    ISIRS.SFA_MILITARY_ALLOW
                        AS S_SFA_MILITARY_ALLOW,
                    ISIRS.SFA_VET_NONEDU_BEN
                        AS S_SFA_VET_NONEDU_BEN,
                    ISIRS.SFA_UNTAX_INCOME
                        AS S_SFA_UNTAX_INCOME,
                    ISIRS.SFA_NON_REP_MONEY
                        AS S_SFA_NON_REP_MONEY,
                    ISIRS.SFA_COOP_EARN
                        AS S_SFA_COOP_EARN,
                    ISIRS.SFA_HIGH_SCHL_NAME
                        AS S_SFA_HIGH_SCHL_NAME,
                    ISIRS.SFA_HIGH_SCHL_CITY
                        AS S_SFA_HIGH_SCHL_CITY,
                    ISIRS.SFA_HIGH_SCHL_STAT
                        AS S_SFA_HIGH_SCHL_STAT,
                    ISIRS.SFA_HIGH_SCHL_CODE
                        AS S_SFA_HIGH_SCHL_CODE,
                    ISIRS.SFA_STU_ASSET_THRS
                        AS S_SFA_STU_ASSET_THRS,
                    ISIRS.SFA_STU_TAX_RET
                        AS S_SFA_STU_TAX_RET,
                    ISIRS.DOB_PRIOR
                        AS S_DOB_PRIOR,
                    ISIRS.IWD_STD_EMAIL
                        AS S_IWD_STD_EMAIL,
                    ISIRS.DEPNDNCY_STAT
                        AS S_DEPNDNCY_STAT,
                    ISIRP.MARITAL_STAT
                        AS P_MARITAL_STAT,
                    ISIRP.LEGAL_RESIDENCE
                        AS P_LEGAL_RESIDENCE,
                    ISIRP.DT_LEGAL_RES
                        AS P_DT_LEGAL_RES,
                    ISIRP.NUMBER_IN_FAMILY
                        AS P_NUMBER_IN_FAMILY,
                    ISIRP.NUM_IN_COLLEGE
                        AS P_NUM_IN_COLLEGE,
                    ISIRP.TAX_FORM_FILED
                        AS P_TAX_FORM_FILED,
                    ISIRP.NUMBER_EXEMPTIONS
                        AS P_NUMBER_EXEMPTIONS,
                    ISIRP.AGI
                        AS P_AGI,
                    ISIRP.TAXES_PAID
                        AS P_TAXES_PAID,
                    ISIRP.FATHER_INCOME
                        AS P_FATHER_INCOME,
                    ISIRP.MOTHER_INCOME
                        AS P_MOTHER_INCOME,
                    ISIRP.CASH_SAVINGS
                        AS P_CASH_SAVINGS,
                    ISIRP.FATHER_GRADE_LVL
                        AS P_FATHER_GRADE_LVL,
                    ISIRP.MOTHER_GRADE_LVL
                        AS P_MOTHER_GRADE_LVL,
                    ISIRP.TOTAL_FROM_WS3
                        AS P_TOTAL_FROM_WS3,
                    ISIRP.TAX_RETURN_FILED
                        AS P_TAX_RETURN_FILED,
                    ISIRP.ELIG_FOR_1040A_EZ
                        AS P_ELIG_FOR_1040A_EZ,
                    ISIRP.TOTAL_FROM_WKA
                        AS P_TOTAL_FROM_WKA,
                    ISIRP.INV_NET_WORTH
                        AS P_INV_NET_WORTH,
                    ISIRP.BUS_NET_WORTH
                        AS P_BUS_NET_WORTH,
                    ISIRP.FATHER_SSN
                        AS P_FATHER_SSN,
                    ISIRP.FATHER_LAST_NAME
                        AS P_FATHER_LAST_NAME,
                    ISIRP.MOTHER_SSN
                        AS P_MOTHER_SSN,
                    ISIRP.MOTHER_LAST_NAME
                        AS P_MOTHER_LAST_NAME,
                    ISIRP.LEGAL_RES_PRIOR
                        AS P_LEGAL_RES_PRIOR,
                    ISIRP.TOTAL_FROM_WKC
                        AS P_TOTAL_FROM_WKC,
                    ISIRP.MAR_STATUS_DT
                        AS P_MAR_STATUS_DT,
                    ISIRP.FATHER_1ST_NM_INIT
                        AS P_FATHER_1ST_NM_INIT,
                    ISIRP.FATHER_DOB
                        AS P_FATHER_DOB,
                    ISIRP.MOTHER_1ST_NM_INIT
                        AS P_MOTHER_1ST_NM_INIT,
                    ISIRP.MOTHER_DOB
                        AS P_MOTHER_DOB,
                    ISIRP.IWD_PAR_EMAIL
                        AS P_IWD_PAR_EMAIL,
                    ISIRP.SFA_SSI_INCOME
                        AS P_SFA_SSI_INCOME,
                    ISIRP.SFA_FOOD_STAMPS
                        AS P_SFA_FOOD_STAMPS,
                    ISIRP.SFA_SCHL_LUNCH_PRG
                        AS P_SFA_SCHL_LUNCH_PRG,
                    ISIRP.SFA_TANF_BENEFITS
                        AS P_SFA_TANF_BENEFITS,
                    ISIRP.SFA_WIC_BENEFITS
                        AS P_SFA_WIC_BENEFITS,
                    ISIRP.SFA_DISLOCATE_WRK
                        AS P_SFA_DISLOCATE_WRK,
                    ISIRP.SFA_EDU_CREDITS
                        AS P_SFA_EDU_CREDITS,
                    ISIRP.SFA_CHILD_SUP_PAID
                        AS P_SFA_CHILD_SUP_PAID,
                    ISIRP.SFA_NEED_EMPLOYMNT
                        AS P_SFA_NEED_EMPLOYMNT,
                    ISIRP.SFA_GRANT_AID
                        AS P_SFA_GRANT_AID,
                    ISIRP.SFA_COMBATPAY
                        AS P_SFA_COMBATPAY,
                    ISIRP.SFA_PENSION_PAY
                        AS P_SFA_PENSION_PAY,
                    ISIRP.SFA_IRA_PAY
                        AS P_SFA_IRA_PAY,
                    ISIRP.SFA_CHILD_SUP_RECV
                        AS P_SFA_CHILD_SUP_RECV,
                    ISIRP.SFA_INTERST_INCOME
                        AS P_SFA_INTERST_INCOME,
                    ISIRP.SFA_IRA_DIST
                        AS P_SFA_IRA_DIST,
                    ISIRP.SFA_UNTAX_PENSION
                        AS P_SFA_UNTAX_PENSION,
                    ISIRP.SFA_MILITARY_ALLOW
                        AS P_SFA_MILITARY_ALLOW,
                    ISIRP.SFA_VET_NONEDU_BEN
                        AS P_SFA_VET_NONEDU_BEN,
                    ISIRP.SFA_UNTAX_INCOME
                        AS P_SFA_UNTAX_INCOME,
                    ISIRP.SFA_COOP_EARN
                        AS P_SFA_COOP_EARN,
                    ISIRP.SFA_PAR_ASSET_THRS
                        AS P_SFA_PAR_ASSET_THRS,
                    ISIRP.SFA_PAR_TAX_RET
                        AS P_SFA_PAR_TAX_RET,
                    ISIRCOMP.PRIMARY_EFC,
                    ISIRCOMP.SECONDARY_EFC,
                    ISIRCOMP.AUTO_ZERO_EFC,
                    ISIRCOMP.FORMULA_TYPE,
                    ISIRCOMP.TOTAL_INCOME,
                    ISIRCOMP.ALWNC_AGAINST_TI,
                    ISIRCOMP.STATE_TAX_ALWNC,
                    ISIRCOMP.EMPLOYMENT_ALWNC,
                    ISIRCOMP.INC_PROTECTN_ALWNC,
                    ISIRCOMP.AVAILABLE_INCOME,
                    ISIRCOMP.DESCRTN_NET_WORTH,
                    ISIRCOMP.AST_PROTECTN_ALWNC,
                    ISIRCOMP.CONTRIB_FROM_ASSET,
                    ISIRCOMP.ADJ_AVAILABLE_INC,
                    ISIRCOMP.TOTAL_PAR_CONTRIB,
                    ISIRCOMP.TOTAL_STU_CONTRIB,
                    ISIRCOMP.ADJ_PAR_CONTRIB,
                    ISIRCOMP.DEP_STU_I_CONTRIB,
                    ISIRCOMP.DEP_STU_A_CONTRIB,
                    ISIRCOMP.STU_TOTAL_INC,
                    ISIRCOMP.CONTRIB_AVAIL_INC,
                    ISIRCOMP.PRORATED_EFC,
                    ISIRCOMP.SECONDARY_EFC_TP,
                    ISIRCOMP.EFC_NET_WORTH,
                    ISIRCOMP.STU_ALLOW_VS_TI,
                    ISIRCOMP.STU_DISC_NET_WORTH,
                    ISIRCOMP.ISIR_CALC_SC,
                    ISIRCOMP.ISIR_CALC_PC,
                    ISIRCOMP.ISIR_CALC_EFC,
                    ISIRCOMP.SFA_SIG_REJ_EFC,
                    ISIRI.REJ_OVR_IND_STDNT,
                    ISIRI.REJ_OVR_STDNT_NAME,
                    ISIRI.REJ_OVR_BIG_FAMILY,
                    ISIRI.ASMPTN_OVR_FAM_MEM,
                    ISIRI.ASMPTN_OVR_COL_P,
                    ISIRI.ASMPTN_OVR_AGI_P,
                    ISIRI.ASMPTN_OVR_COL_S,
                    ISIRI.ASMPTN_OVR_AGI_S,
                    ISIRI.ASMPTN_OVR_WS3_P,
                    ISIRI.ASMPTN_OVR_WS3_S,
                    ISIRI.REJ_OVR_BIRTH_YEAR,
                    ISIRI.REJ_OVR_TAX_RANGE,
                    ISIRI.REJ_OVR_TAXRNG_DEP,
                    ISIRI.SFA_REJ_OVR_TX_PAR,
                    ISIRI.SFA_REJ_OVR_DADSSN,
                    ISIRI.SFA_REJ_OVR_MOMSSN,
                    ISIRI.SFA_REJ_OVR_TX_STU,
                    ISIRI.SFA_REJ_OVR_NO_TAX,
                    ISIRI.SFA_REJ_OVR_MAR_ST,
                    ISIRS.SFA_HS_DIP_EQUIV
                        S_SFA_HS_DIP_EQUIV,
                    ISIRCOMP.BUDGET_DURATION,
                    ROW_NUMBER ()
                        OVER (
                            PARTITION BY ISIRC.EMPLID,
                                         ISIRC.INSTITUTION,
                                         ISIRC.AID_YEAR
                            ORDER BY ISIRC.EFFDT DESC, ISIRC.EFFSEQ DESC)
                        ISIR_ORDER
               FROM CSSTG_OWNER.PS_ISIR_CONTROL    ISIRC,
                    CSSTG_OWNER.PS_ISIR_STUDENT    ISIRS,
                    CSSTG_OWNER.PS_ISIR_PARENT     ISIRP,
                    CSSTG_OWNER.PS_ISIR_COMPUTED   ISIRCOMP,
                    CSSTG_OWNER.PS_ISIR_INTERPRET  ISIRI
              WHERE     ISIRC.EMPLID = ISIRP.EMPLID
                    AND ISIRC.INSTITUTION = ISIRP.INSTITUTION
                    AND ISIRC.AID_YEAR = ISIRP.AID_YEAR
                    AND ISIRC.EFFDT = ISIRP.EFFDT
                    AND ISIRC.EFFSEQ = ISIRP.EFFSEQ
                    AND ISIRC.SRC_SYS_ID = ISIRP.SRC_SYS_ID
                    AND ISIRC.EMPLID = ISIRS.EMPLID
                    AND ISIRC.INSTITUTION = ISIRS.INSTITUTION
                    AND ISIRC.AID_YEAR = ISIRS.AID_YEAR
                    AND ISIRC.EFFDT = ISIRS.EFFDT
                    AND ISIRC.EFFSEQ = ISIRS.EFFSEQ
                    AND ISIRC.SRC_SYS_ID = ISIRS.SRC_SYS_ID
                    AND ISIRC.EMPLID = ISIRCOMP.EMPLID
                    AND ISIRC.INSTITUTION = ISIRCOMP.INSTITUTION
                    AND ISIRC.AID_YEAR = ISIRCOMP.AID_YEAR
                    AND ISIRC.EFFDT = ISIRCOMP.EFFDT
                    AND ISIRC.EFFSEQ = ISIRCOMP.EFFSEQ
                    AND ISIRC.SRC_SYS_ID = ISIRCOMP.SRC_SYS_ID
                    AND ISIRC.EMPLID = ISIRI.EMPLID
                    AND ISIRC.INSTITUTION = ISIRI.INSTITUTION
                    AND ISIRC.AID_YEAR = ISIRI.AID_YEAR
                    AND ISIRC.EFFDT = ISIRI.EFFDT
                    AND ISIRC.EFFSEQ = ISIRI.EFFSEQ
                    AND ISIRC.SRC_SYS_ID = ISIRI.SRC_SYS_ID
                    AND ISIRC.EFFDT <= SYSDATE
                    AND ISIRC.DATA_ORIGIN <> 'D'
                    AND ISIRS.DATA_ORIGIN <> 'D'
                    AND ISIRP.DATA_ORIGIN <> 'D'
                    AND ISIRCOMP.DATA_ORIGIN <> 'D'
                    AND ISIRI.DATA_ORIGIN <> 'D'),
        XL
        AS
            (SELECT /*+ INLINE */
                    FIELDNAME,
                    FIELDVALUE,
                    SRC_SYS_ID,
                    XLATLONGNAME,
                    XLATSHORTNAME
               FROM UM_D_XLATITEM
              WHERE SRC_SYS_ID = 'CS90'),
SRVC as (select /*+ INLINE */
                INSTITUTION_SID, PERSON_SID, min(PERSON_SID) MIN_PERSON_SID    -- Mar 2019
           from UM_D_PERSON_SRVC_IND
          where DATA_ORIGIN <> 'D'
          group by INSTITUTION_SID, PERSON_SID)
    SELECT STAID.INSTITUTION_CD,
           STAID.PERSON_ID,
           STAID.AID_YEAR,
           STAID.SRC_SYS_ID,
           STAID.ADM_CNT,
           STAID.SR_CNT,
           STAID.PRSPCT_CNT,
           STAID.STAID_ACTV_CNT,
           CASE WHEN ISIR.EMPLID IS NULL THEN 0 ELSE 1 END
               ISIR_CNT,
           I.INSTITUTION_SID,
           NVL (P.PERSON_SID, 2147483646)
               PERSON_SID,
--           NVL (
--               (SELECT MIN (S.PERSON_SID)
--                  FROM UM_D_PERSON_SRVC_IND S
--                 WHERE     S.PERSON_SID = P.PERSON_SID
--                       AND S.INSTITUTION_CD = I.INSTITUTION_CD
--                       AND S.SRC_SYS_ID = I.SRC_SYS_ID
--                       AND S.DATA_ORIGIN <> 'D'),
--               2147483646)
--               PERSON_SRVC_IND_SID,
           nvl(SRVC.MIN_PERSON_SID, 2147483646) PERSON_SRVC_IND_SID,    -- Mar 2019
           STAID.FED_DEPEND_STAT,
           NVL (X1.XLATLONGNAME, '')
               FED_DEPEND_STAT_LD,
           STAID_ATR.PROCESSING_STATUS,
           NVL (X2.XLATLONGNAME, '')
               PROCESSING_STATUS_LD,
           STAID_ATR.VERIFICATON_STATUS,
           NVL (X3.XLATLONGNAME, '')
               VERIFICATON_STATUS_LD,
           STAID_ATR.SFA_REVIEW_STATUS,
           NVL (X4.XLATLONGNAME, '')
               SFA_REVIEW_STATUS_LD,
           STAID_ATR.SCHOLARSHIP_STATUS,
           NVL (X5.XLATLONGNAME, '')
               SCHOLARSHIP_STATUS_LD,
           STAID_ATR.PACKAGING_METHOD,
           NVL (X6.XLATLONGNAME, '')
               PACKAGING_METHOD_LD,
           STAID_ATR.PKG_PLAN_ID,
           NVL (PKG_PLAN.EQUITY_LIMIT, 0)
               EQUITY_LIMIT,
           STAID_ATR.SAT_ACADEMIC_PRG,
           NVL (X7.XLATLONGNAME, '')
               SAT_ACADEMIC_PRG_LD,
           STAID_ATR.QA_VERF_SELECT,
           NVL (X8.XLATLONGNAME, '')
               QA_VERF_SELECT_LD,
           STAID_ATR.AID_APP_STATUS,
           NVL (X9.XLATLONGNAME, '')
               AID_APP_STATUS_LD,
           STAID_ATR.VERIF_STATUS_CODE,
           NVL (X10.XLATLONGNAME, '')
               VERIF_STATUS_CODE_LD,
           STAID_ATR.PELL_PROCESS_FIELD,
           NVL (X11.XLATLONGNAME, '')
               PELL_PROCESS_FIELD_LD,
           STAID_ATR.PELL_EFFDT,
           STAID_ATR.PELL_EFFSEQ,
           STAID_ATR.PELL_TRANS_NBR,
           STAID_ATR.ACAD_CAREER,
           STAID_ATR.PAR_CREDIT_WORTHY,
           ISIR.TITLEIV_ELIG,
           ISIR.SFA_SPL_CIRCUM_FLG_ISIR,
           NVL (X78.XLATLONGNAME, '')
               SFA_SPL_CIRCUM_FLG_ISIR_LD,
           STAID_ATR.SS_MATCH,
           STAID_ATR.SS_REGISTRATION,
           STAID_ATR.INS_MATCH,
           STAID_ATR.SSN_MATCH,
           STAID_ATR.VA_MATCH,
           STAID_ATR.SSA_CITIZENSHP_IND,
           NVL (X12.XLATLONGNAME, '')
               SSA_CITIZENSHP_IND_LD,
           STAID_ATR.NSLDS_MATCH,
           NVL (X13.XLATLONGNAME, '')
               NSLDS_MATCH_LD,
           STAID_ATR.DRUG_OFFENSE_CONV,
           STAID_ATR.PRISONER_MATCH,
           STAID_ATR.SSN_MATCH_OVRD,
           STAID_ATR.SSA_CITIZEN_OVRD,
           STAID_ATR.INS_MATCH_OVRD,
           STAID_ATR.VA_MATCH_OVRD,
           STAID_ATR.SS_MATCH_OVRD,
           STAID_ATR.SS_REGISTER_OVRD,
           STAID_ATR.NSLDS_OVRD,
           STAID_ATR.PRISONER_OVRD,
           STAID_ATR.DRUG_OFFENSE_OVRD,
           STAID_ATR.ISIR_SEC_INS_MATCH,
           STAID_ATR.FATHER_SSN_MATCH,
           STAID_ATR.MOTHER_SSN_MATCH,
           STAID_ATR.PAR_SSN_MATCH_OVRD,
           STAID_ATR.SFA_RPKG_PLAN_ID,
           STAID_ATR.SFA_EASS_ACCESS,
           STAID_ATR.SFA_PP_CRSEWRK_SW,
           STAID_ATR.SFA_DOD_MATCH,
           STAID_ATR.SFA_DOD_MATCH_OVRD,
           STAID_ATR.SFA_SPL_CIRCUM_FLG,
           STAID_ATR.SFA_SS_GROUP,
           ISIR.EFFDT,
           ISIR.EFFSEQ
               AS ISIR_EFFSEQ,
           ISIR.ISIR_TXN_NBR,
           ISIR.ORIG_SSN,
           ISIR.NAME_CD,
           ISIR.DT_APP_RECEIVED,
           ISIR.PELL_ELIGIBILITY,
           ISIR.SAR_C_FLAG,
           ISIR.VERF_SELECTION_IND,
           ISIR.TRANS_PROCESS_DT,
           ISIR.CPS_REPROCESS_CD,
           ISIR.BATCH_NUMBER,
           ISIR.FAA_INSTITUT_NUM,
           ISIR.INS_VERF_NUM,
           ISIR.DEPNDNCY_OVERRIDE,
           ISIR.APP_STAT_DT,
           ISIR.EFC_STATUS,
           ISIR.ADJ_EFC_CALC_REQ,
           ISIR.CORRECTION_STATUS,
           NVL (X14.XLATSHORTNAME, '')
               CORRECTION_STATUS_SD,
           ISIR.CORR_STAT_DT,
           ISIR.INST_PROCESS_DT,
           ISIR.OWNING_SCHOOL_CD,
           ISIR.NSLDS_TXN_NBR,
           ISIR.APP_SOURCE,
           NVL (X15.XLATLONGNAME, '')
               APP_SOURCE_LD,
           ISIR.TRANS_SOURCE,
           NVL (X16.XLATLONGNAME, '')
               TRANS_SOURCE_LD,
           ISIR.ISIR_CORR_SRC,
           ISIR.ISIR_EFC_CHANGE,
           ISIR.ISIR_DUP_SSN_CD,
           ISIR.VERIF_TRK_FLG,
           NVL (X17.XLATLONGNAME, '')
               VERIF_TRK_FLG_LD,
           ISIR.ISIR_CPS_PUSHED,
           ISIR.ISIR_SAR_C_CHNG,
           ISIR.SFA_VF_SEL_CHG_FLG,
           ISIR.SFA_REJ_ST_CHG_FLG,
           ISIR.SFA_STU_IRS_REQUST,
           NVL (X18.XLATLONGNAME, '')
               SFA_STU_IRS_REQUST_LD,
           ISIR.SFA_PAR_IRS_REQUST,
           NVL (X19.XLATLONGNAME, '')
               SFA_PAR_IRS_REQUST_LD,
           ISIR.SFA_HIGH_SCHL_FLAG,
           ISIR.S_NUM_FAMILY_MEMBERS,
           ISIR.S_NUMBER_IN_COLLEGE,
           ISIR.S_VETERAN,
           NVL (X20.XLATLONGNAME, '')
               S_VETERAN_LD,
           ISIR.S_GRADUATE_STUDENT,
           NVL (X21.XLATLONGNAME, '')
               S_GRADUATE_STUDENT_LD,
           ISIR.S_MARRIED,
           NVL (X22.XLATLONGNAME, '')
               S_MARRIED_LD,
           ISIR.S_MARITAL_STAT,
           NVL (X23.XLATLONGNAME, '')
               S_MARITAL_STAT_LD,
           ISIR.S_MAR_STATUS_DT,
           ISIR.S_ORPHAN,
           NVL (X24.XLATLONGNAME, '')
               S_ORPHAN_LD,
           ISIR.S_DEPENDENTS,
           NVL (X25.XLATLONGNAME, '')
               S_DEPENDENTS_LD,
           ISIR.S_TAX_FORM_FILED,
           NVL (X26.XLATLONGNAME, '')
               S_TAX_FORM_FILED_LD,
           ISIR.S_NUMBER_EXEMPTIONS,
           ISIR.S_AGI,
           ISIR.S_TAXES_PAID,
           ISIR.S_STD_EARNED_INCOME,
           ISIR.S_SPS_EARNED_INCOME,
           ISIR.S_CASH_SAVINGS,
           ISIR.S_SCHOOL_CHOICE_1,
           ISIR.S_HOUSING_CODE_1,
           NVL (X27.XLATLONGNAME, '')
               S_HOUSING_CODE_1_LD,
           ISIR.S_SCHOOL_CHOICE_2,
           ISIR.S_HOUSING_CODE_2,
           NVL (X28.XLATLONGNAME, '')
               S_HOUSING_CODE_2_LD,
           ISIR.S_SCHOOL_CHOICE_3,
           ISIR.S_HOUSING_CODE_3,
           NVL (X29.XLATLONGNAME, '')
               S_HOUSING_CODE_3_LD,
           ISIR.S_SCHOOL_CHOICE_4,
           ISIR.S_HOUSING_CODE_4,
           NVL (X30.XLATLONGNAME, '')
               S_HOUSING_CODE_4_LD,
           ISIR.S_SCHOOL_CHOICE_5,
           ISIR.S_HOUSING_CODE_5,
           NVL (X31.XLATLONGNAME, '')
               S_HOUSING_CODE_5_LD,
           ISIR.S_SCHOOL_CHOICE_6,
           ISIR.S_HOUSING_CODE_6,
           NVL (X32.XLATLONGNAME, '')
               S_HOUSING_CODE_6_LD,
           ISIR.S_FIRST_BACH_DEGREE,
           NVL (X33.XLATLONGNAME, '')
               S_FIRST_BACH_DEGREE_LD,
           ISIR.S_INTERESTED_IN_WS,
           NVL (X34.XLATLONGNAME, '')
               S_INTERESTED_IN_WS_LD,
           ISIR.S_DT_APP_COMPLETED,
           NVL (X34A.XLATLONGNAME, '')
               S_DEGREE_CERTIF,
           ISIR.S_CURRENT_GRADE_LVL,
           NVL (X35.XLATLONGNAME, '')
               S_CURRENT_GRADE_LVL_LD,
           ISIR.S_TOTAL_FROM_WS3,
           ISIR.S_STATE_RESIDENCE,
           ISIR.S_RESIDENCY_DT,
           ISIR.S_DRIV_LIC_STATE,
           ISIR.S_FISAP_TOT_INC,
           ISIR.S_CITIZENSHIP_STATUS,
           NVL (X36.XLATLONGNAME, '')
               S_CITIZENSHIP_STATUS_LD,
           ISIR.S_TAX_RETURN_FILED,
           NVL (X37.XLATLONGNAME, '')
               S_TAX_RETURN_FILED_LD,
           ISIR.S_ELIG_FOR_1040A_EZ,
           NVL (X38.XLATLONGNAME, '')
               S_ELIG_FOR_1040A_EZ_LD,
           ISIR.S_INV_NET_WORTH,
           ISIR.S_BUS_NET_WORTH,
           ISIR.S_LEGAL_RES_PRIOR,
           NVL (X39.XLATLONGNAME, '')
               S_LEGAL_RES_PRIOR_LD,
           ISIR.S_IWD_PERM_ADDR02,
           ISIR.S_IWD_CITY,
           ISIR.S_IWD_STATE,
           ISIR.S_IWD_ZIP,
           ISIR.S_IWD_PERM_PHONE,
           ISIR.S_IWD_STD_LAST_NAME,
           ISIR.S_IWD_STD_FIRST_NM02,
           ISIR.S_IWD_STU_MI,
           ISIR.S_IWD_STD_EMAIL,
           ISIR.S_SSN,
           ISIR.S_BIRTHDATE,
           ISIR.S_DRIVERS_LICENSE_NO,
           ISIR.S_IWD_STU_ALIEN_REG,
           ISIR.S_CHILDREN,
           NVL (X40.XLATLONGNAME, '')
               S_CHILDREN_LD,
           ISIR.S_TOTAL_FROM_WKC,
           ISIR.S_SFA_ACTIVE_DUTY,
           NVL (X41.XLATLONGNAME, '')
               S_SFA_ACTIVE_DUTY_LD,
           ISIR.S_SFA_SSI_INCOME,
           NVL (X42.XLATLONGNAME, '')
               S_SFA_SSI_INCOME_LD,
           ISIR.S_SFA_FOOD_STAMPS,
           NVL (X43.XLATLONGNAME, '')
               S_SFA_FOOD_STAMPS_LD,
           ISIR.S_SFA_SCHL_LUNCH_PRG,
           NVL (X44.XLATLONGNAME, '')
               S_SFA_SCHL_LUNCH_PRG_LD,
           ISIR.S_SFA_TANF_BENEFITS,
           NVL (X45.XLATLONGNAME, '')
               S_SFA_TANF_BENEFITS_LD,
           ISIR.S_SFA_WIC_BENEFITS,
           NVL (X46.XLATLONGNAME, '')
               S_SFA_WIC_BENEFITS_LD,
           ISIR.S_SFA_STDNT_GENDER,
           NVL (X47.XLATLONGNAME, '')
               S_SFA_STDNT_GENDER_LD,
           ISIR.S_SFA_SCHL_CHOICE_7,
           ISIR.S_SFA_HOUSING_CODE7,
           NVL (X48.XLATLONGNAME, '')
               S_SFA_HOUSING_CODE7_LD,
           ISIR.S_SFA_SCHL_CHOICE_8,
           ISIR.S_SFA_HOUSING_CODE8,
           NVL (X49.XLATLONGNAME, '')
               S_SFA_HOUSING_CODE8_LD,
           ISIR.S_SFA_SCHL_CHOICE_9,
           ISIR.S_SFA_HOUSING_CODE9,
           NVL (X50.XLATLONGNAME, '')
               S_SFA_HOUSING_CODE9_LD,
           ISIR.S_SFA_SCHL_CHOICE_10,
           ISIR.S_SFA_HOUSING_CODE10,
           NVL (X51.XLATLONGNAME, '')
               S_SFA_HOUSING_CODE10_LD,
           ISIR.S_SFA_EMANCIPT_MINOR,
           NVL (X52.XLATLONGNAME, '')
               S_SFA_EMANCIPT_MINOR_LD,
           ISIR.S_SFA_LEGAL_GUARDIAN,
           NVL (X53.XLATLONGNAME, '')
               S_SFA_LEGAL_GUARDIAN_LD,
           ISIR.S_SFA_YOUTH_LIASON,
           NVL (X54.XLATLONGNAME, '')
               S_SFA_YOUTH_LIASON_LD,
           ISIR.S_SFA_YOUTH_HUD,
           NVL (X55.XLATLONGNAME, '')
               S_SFA_YOUTH_HUD_LD,
           ISIR.S_SFA_RISK_HOMELESS,
           NVL (X56.XLATLONGNAME, '')
               S_SFA_RISK_HOMELESS_LD,
           ISIR.S_SFA_DISLOCATE_WRK,
           NVL (X57.XLATLONGNAME, '')
               S_SFA_DISLOCATE_WRK_LD,
           ISIR.S_SFA_EDU_CREDITS,
           ISIR.S_SFA_CHILD_SUP_PAID,
           ISIR.S_SFA_NEED_EMPLOYMNT,
           ISIR.S_SFA_GRANT_AID,
           ISIR.S_SFA_COMBATPAY,
           ISIR.S_SFA_PENSION_PAY,
           ISIR.S_SFA_IRA_PAY,
           ISIR.S_SFA_CHILD_SUP_RECV,
           ISIR.S_SFA_INTERST_INCOME,
           ISIR.S_SFA_IRA_DIST,
           ISIR.S_SFA_UNTAX_PENSION,
           ISIR.S_SFA_MILITARY_ALLOW,
           ISIR.S_SFA_VET_NONEDU_BEN,
           ISIR.S_SFA_UNTAX_INCOME,
           ISIR.S_SFA_NON_REP_MONEY,
           ISIR.S_SFA_COOP_EARN,
           ISIR.S_SFA_HIGH_SCHL_NAME,
           ISIR.S_SFA_HIGH_SCHL_CITY,
           ISIR.S_SFA_HIGH_SCHL_STAT,
           ISIR.S_SFA_HIGH_SCHL_CODE,
           ISIR.S_SFA_STU_ASSET_THRS,
           NVL (X58.XLATLONGNAME, '')
               S_SFA_STU_ASSET_THRS_LD,
           ISIR.S_SFA_STU_TAX_RET,
           NVL (X59.XLATLONGNAME, '')
               S_SFA_STU_TAX_RET_LD,
           CASE
               WHEN ISIR.S_DOB_PRIOR = '1' THEN 'Yes'
               WHEN ISIR.S_DOB_PRIOR = '2' THEN 'No'
               ELSE '-'
           END
               S_DOB_PRIOR,
           ISIR.S_SFA_HS_DIP_EQUIV,
           NVL (X60.XLATLONGNAME, '')
               S_SFA_HS_DIP_EQUIV_LD,
           ISIR.S_DEPNDNCY_STAT,
           NVL (X61.XLATLONGNAME, '')
               S_DEPNDNCY_STAT_LD,
           ISIR.P_MARITAL_STAT,
           NVL (X62.XLATLONGNAME, '')
               P_MARITAL_STAT_LD,
           ISIR.P_LEGAL_RESIDENCE,
           ISIR.P_DT_LEGAL_RES,
           ISIR.P_NUMBER_IN_FAMILY,
           ISIR.P_NUM_IN_COLLEGE,
           ISIR.P_TAX_FORM_FILED,
           NVL (X63.XLATLONGNAME, '')
               P_TAX_FORM_FILED_LD,
           ISIR.P_NUMBER_EXEMPTIONS,
           ISIR.P_AGI,
           ISIR.P_TAXES_PAID,
           ISIR.P_FATHER_INCOME,
           ISIR.P_MOTHER_INCOME,
           ISIR.P_CASH_SAVINGS,
           ISIR.P_FATHER_GRADE_LVL,
           NVL (X64.XLATLONGNAME, '')
               P_FATHER_GRADE_LVL_LD,
           ISIR.P_MOTHER_GRADE_LVL,
           NVL (X65.XLATLONGNAME, '')
               P_MOTHER_GRADE_LVL_LD,
           ISIR.P_TOTAL_FROM_WS3,
           ISIR.P_TAX_RETURN_FILED,
           NVL (X66.XLATLONGNAME, '')
               P_TAX_RETURN_FILED_LD,
           ISIR.P_ELIG_FOR_1040A_EZ,
           NVL (X67.XLATLONGNAME, '')
               P_ELIG_FOR_1040A_EZ_LD,
           ISIR.P_TOTAL_FROM_WKA,
           ISIR.P_INV_NET_WORTH,
           ISIR.P_BUS_NET_WORTH,
           ISIR.P_FATHER_SSN,
           ISIR.P_FATHER_LAST_NAME,
           ISIR.P_MOTHER_SSN,
           ISIR.P_MOTHER_LAST_NAME,
           ISIR.P_LEGAL_RES_PRIOR,
           NVL (X68.XLATLONGNAME, '')
               P_LEGAL_RES_PRIOR_LD,
           ISIR.P_TOTAL_FROM_WKC,
           ISIR.P_MAR_STATUS_DT,
           ISIR.P_FATHER_1ST_NM_INIT,
           ISIR.P_FATHER_DOB,
           ISIR.P_MOTHER_1ST_NM_INIT,
           ISIR.P_MOTHER_DOB,
           ISIR.P_IWD_PAR_EMAIL,
           ISIR.P_SFA_SSI_INCOME,
           NVL (X69.XLATLONGNAME, '')
               P_SFA_SSI_INCOME_LD,
           ISIR.P_SFA_FOOD_STAMPS,
           NVL (X70.XLATLONGNAME, '')
               P_SFA_FOOD_STAMPS_LD,
           ISIR.P_SFA_SCHL_LUNCH_PRG,
           NVL (X71.XLATLONGNAME, '')
               P_SFA_SCHL_LUNCH_PRG_LD,
           ISIR.P_SFA_TANF_BENEFITS,
           NVL (X72.XLATLONGNAME, '')
               P_SFA_TANF_BENEFITS_LD,
           ISIR.P_SFA_WIC_BENEFITS,
           NVL (X73.XLATLONGNAME, '')
               P_SFA_WIC_BENEFITS_LD,
           ISIR.P_SFA_DISLOCATE_WRK,
           NVL (X74.XLATLONGNAME, '')
               P_SFA_DISLOCATE_WRK_LD,
           ISIR.P_SFA_EDU_CREDITS,
           ISIR.P_SFA_CHILD_SUP_PAID,
           ISIR.P_SFA_NEED_EMPLOYMNT,
           ISIR.P_SFA_GRANT_AID,
           ISIR.P_SFA_COMBATPAY,
           ISIR.P_SFA_PENSION_PAY,
           ISIR.P_SFA_IRA_PAY,
           ISIR.P_SFA_CHILD_SUP_RECV,
           ISIR.P_SFA_INTERST_INCOME,
           ISIR.P_SFA_IRA_DIST,
           ISIR.P_SFA_UNTAX_PENSION,
           ISIR.P_SFA_MILITARY_ALLOW,
           ISIR.P_SFA_VET_NONEDU_BEN,
           ISIR.P_SFA_UNTAX_INCOME,
           ISIR.P_SFA_COOP_EARN,
           ISIR.P_SFA_PAR_ASSET_THRS,
           NVL (X75.XLATLONGNAME, '')
               P_SFA_PAR_ASSET_THRS_LD,
           ISIR.P_SFA_PAR_TAX_RET,
           NVL (X76.XLATLONGNAME, '')
               P_SFA_PAR_TAX_RET_LD,
           ISIR.BUDGET_DURATION,
           ISIR.PRIMARY_EFC,
           ISIR.SECONDARY_EFC,
           ISIR.AUTO_ZERO_EFC,
           ISIR.FORMULA_TYPE,
           NVL (X77.XLATLONGNAME, '')
               FORMULA_TYPE_LD,
           ISIR.TOTAL_INCOME,
           ISIR.ALWNC_AGAINST_TI,
           ISIR.STATE_TAX_ALWNC,
           ISIR.EMPLOYMENT_ALWNC,
           ISIR.INC_PROTECTN_ALWNC,
           ISIR.AVAILABLE_INCOME,
           ISIR.DESCRTN_NET_WORTH,
           ISIR.AST_PROTECTN_ALWNC,
           ISIR.CONTRIB_FROM_ASSET,
           ISIR.ADJ_AVAILABLE_INC,
           ISIR.TOTAL_PAR_CONTRIB,
           ISIR.TOTAL_STU_CONTRIB,
           ISIR.ADJ_PAR_CONTRIB,
           ISIR.DEP_STU_I_CONTRIB,
           ISIR.DEP_STU_A_CONTRIB,
           ISIR.STU_TOTAL_INC,
           ISIR.CONTRIB_AVAIL_INC,
           ISIR.PRORATED_EFC,
           ISIR.SECONDARY_EFC_TP,
           ISIR.EFC_NET_WORTH,
           ISIR.STU_ALLOW_VS_TI,
           ISIR.STU_DISC_NET_WORTH,
           ISIR.ISIR_CALC_SC,
           ISIR.ISIR_CALC_PC,
           ISIR.ISIR_CALC_EFC,
           ISIR.SFA_SIG_REJ_EFC,
           ISIR.REJ_OVR_IND_STDNT,
           ISIR.REJ_OVR_STDNT_NAME,
           ISIR.REJ_OVR_BIG_FAMILY,
           ISIR.ASMPTN_OVR_FAM_MEM,
           ISIR.ASMPTN_OVR_COL_P,
           ISIR.ASMPTN_OVR_AGI_P,
           ISIR.ASMPTN_OVR_COL_S,
           ISIR.ASMPTN_OVR_AGI_S,
           ISIR.ASMPTN_OVR_WS3_P,
           ISIR.ASMPTN_OVR_WS3_S,
           ISIR.REJ_OVR_BIRTH_YEAR,
           ISIR.REJ_OVR_TAX_RANGE,
           ISIR.REJ_OVR_TAXRNG_DEP,
           ISIR.SFA_REJ_OVR_TX_PAR,
           ISIR.SFA_REJ_OVR_DADSSN,
           ISIR.SFA_REJ_OVR_MOMSSN,
           ISIR.SFA_REJ_OVR_TX_STU,
           ISIR.SFA_REJ_OVR_NO_TAX,
           ISIR.SFA_REJ_OVR_MAR_ST,
           'N'
               LOAD_ERROR,
           'S'
               DATA_ORIGIN,
           SYSDATE
               CREATED_EW_DTTM,
           SYSDATE
               LASTUPD_EW_DTTM,
           1234
               BATCH_SID
      FROM STAID
           JOIN PS_D_INSTITUTION I
               ON     STAID.INSTITUTION_CD = I.INSTITUTION_CD
                  AND STAID.SRC_SYS_ID = I.SRC_SYS_ID
           LEFT OUTER JOIN PS_D_PERSON P
               ON     STAID.PERSON_ID = P.PERSON_ID
                  AND STAID.SRC_SYS_ID = P.SRC_SYS_ID
           LEFT OUTER JOIN CSSTG_OWNER.PS_STDNT_AID_ATRBT STAID_ATR
               ON     STAID.PERSON_ID = STAID_ATR.EMPLID
                  AND STAID.INSTITUTION_CD = STAID_ATR.INSTITUTION
                  AND STAID.AID_YEAR = STAID_ATR.AID_YEAR
                  AND STAID_ATR.DATA_ORIGIN <> 'D'
           LEFT OUTER JOIN ISIR
               ON     STAID.PERSON_ID = ISIR.EMPLID
                  AND STAID.INSTITUTION_CD = ISIR.INSTITUTION
                  AND STAID.AID_YEAR = ISIR.AID_YEAR
                  AND ISIR.ISIR_ORDER = 1
           LEFT OUTER JOIN PKG_PLAN
               ON     STAID.INSTITUTION_CD = PKG_PLAN.INSTITUTION
                  AND STAID.AID_YEAR = PKG_PLAN.AID_YEAR
                  AND STAID_ATR.ACAD_CAREER = PKG_PLAN.ACAD_CAREER
                  AND STAID_ATR.PKG_PLAN_ID = PKG_PLAN.PKG_PLAN_ID
                  AND STAID.SRC_SYS_ID = PKG_PLAN.SRC_SYS_ID
                  AND STAID.AID_YEAR = ISIR.AID_YEAR
                  AND PKG_PLAN.PKG_PLAN_ORDER = 1
           left outer join SRVC
             on I.INSTITUTION_SID = SRVC.INSTITUTION_SID      -- Mar 2019
            and P.PERSON_SID = SRVC.PERSON_SID
           LEFT OUTER JOIN XL X1
               ON     X1.FIELDNAME = 'FED_DEPEND_STAT'
                  AND X1.FIELDVALUE = STAID.FED_DEPEND_STAT
           LEFT OUTER JOIN XL X2
               ON     X2.FIELDNAME = 'PROCESSING_STATUS'
                  AND X2.FIELDVALUE = STAID_ATR.PROCESSING_STATUS
           LEFT OUTER JOIN XL X3
               ON     X3.FIELDNAME = 'VERIFICATON_STATUS'
                  AND X3.FIELDVALUE = STAID_ATR.VERIFICATON_STATUS
           LEFT OUTER JOIN XL X4
               ON     X4.FIELDNAME = 'SFA_REVIEW_STATUS'
                  AND X4.FIELDVALUE = STAID_ATR.SFA_REVIEW_STATUS
           LEFT OUTER JOIN XL X5
               ON     X5.FIELDNAME = 'SCHOLARSHIP_STATUS'
                  AND X5.FIELDVALUE = STAID_ATR.SCHOLARSHIP_STATUS
           LEFT OUTER JOIN XL X6
               ON     X6.FIELDNAME = 'PACKAGING_METHOD'
                  AND X6.FIELDVALUE = STAID_ATR.PACKAGING_METHOD
           LEFT OUTER JOIN XL X7
               ON     X7.FIELDNAME = 'SAT_ACADEMIC_PRG'
                  AND X7.FIELDVALUE = STAID_ATR.SAT_ACADEMIC_PRG
           LEFT OUTER JOIN XL X8
               ON     X8.FIELDNAME = 'QA_VERF_SELECT'
                  AND X8.FIELDVALUE = STAID_ATR.QA_VERF_SELECT
           LEFT OUTER JOIN XL X9
               ON     X9.FIELDNAME = 'AID_APP_STATUS'
                  AND X9.FIELDVALUE = STAID_ATR.AID_APP_STATUS
           LEFT OUTER JOIN XL X10
               ON     X10.FIELDNAME = 'VERIF_STATUS_CODE'
                  AND X10.FIELDVALUE = STAID_ATR.VERIF_STATUS_CODE
           LEFT OUTER JOIN XL X11
               ON     X11.FIELDNAME = 'PELL_PROCESS_FIELD'
                  AND X11.FIELDVALUE = STAID_ATR.PELL_PROCESS_FIELD
           LEFT OUTER JOIN XL X12
               ON     X12.FIELDNAME = 'SSA_CITIZENSHP_IND'
                  AND X12.FIELDVALUE = STAID_ATR.SSA_CITIZENSHP_IND
           LEFT OUTER JOIN XL X13
               ON     X13.FIELDNAME = 'NSLDS_MATCH'
                  AND X13.FIELDVALUE = STAID_ATR.NSLDS_MATCH
           LEFT OUTER JOIN XL X14
               ON     X14.FIELDNAME = 'CORRECTION_STATUS'
                  AND X14.FIELDVALUE = ISIR.CORRECTION_STATUS
           LEFT OUTER JOIN XL X15
               ON     X15.FIELDNAME = 'APP_SOURCE'
                  AND X15.FIELDVALUE = ISIR.APP_SOURCE
           LEFT OUTER JOIN XL X16
               ON     X16.FIELDNAME = 'TRANS_SOURCE'
                  AND X16.FIELDVALUE = ISIR.TRANS_SOURCE
           LEFT OUTER JOIN XL X17
               ON     X17.FIELDNAME = 'VERIF_TRK_FLG'
                  AND X17.FIELDVALUE = ISIR.VERIF_TRK_FLG
           LEFT OUTER JOIN XL X18
               ON     X18.FIELDNAME = 'SFA_STU_IRS_REQUST'
                  AND X18.FIELDVALUE = ISIR.SFA_STU_IRS_REQUST
           LEFT OUTER JOIN XL X19
               ON     X19.FIELDNAME = 'SFA_PAR_IRS_REQUST'
                  AND X19.FIELDVALUE = ISIR.SFA_PAR_IRS_REQUST
           LEFT OUTER JOIN XL X20
               ON     X20.FIELDNAME = 'VETERAN'
                  AND X20.FIELDVALUE = ISIR.S_VETERAN
           LEFT OUTER JOIN XL X21
               ON     X21.FIELDNAME = 'GRADUATE_STUDENT'
                  AND X21.FIELDVALUE = ISIR.S_GRADUATE_STUDENT
           LEFT OUTER JOIN XL X22
               ON     X22.FIELDNAME = 'MARRIED'
                  AND X22.FIELDVALUE = ISIR.S_MARRIED
           LEFT OUTER JOIN XL X23
               ON     X23.FIELDNAME = 'MARITAL_STAT_STU11'
                  AND X23.FIELDVALUE = ISIR.S_MARITAL_STAT
           LEFT OUTER JOIN XL X24
               ON X24.FIELDNAME = 'ORPHAN' AND X24.FIELDVALUE = ISIR.S_ORPHAN
           LEFT OUTER JOIN XL X25
               ON     X25.FIELDNAME = 'DEPENDENTS'
                  AND X25.FIELDVALUE = ISIR.S_DEPENDENTS
           LEFT OUTER JOIN XL X26
               ON     X26.FIELDNAME = 'TAX_FORM_FILED07'
                  AND X26.FIELDVALUE = ISIR.S_TAX_FORM_FILED
           LEFT OUTER JOIN XL X27
               ON     X27.FIELDNAME = 'SFA_HOUSING_CD_01'
                  AND X27.FIELDVALUE = ISIR.S_HOUSING_CODE_1
           LEFT OUTER JOIN XL X28
               ON     X28.FIELDNAME = 'SFA_HOUSING_CD_02'
                  AND X28.FIELDVALUE = ISIR.S_HOUSING_CODE_2
           LEFT OUTER JOIN XL X29
               ON     X29.FIELDNAME = 'SFA_HOUSING_CD_03'
                  AND X29.FIELDVALUE = ISIR.S_HOUSING_CODE_3
           LEFT OUTER JOIN XL X30
               ON     X30.FIELDNAME = 'SFA_HOUSING_CD_04'
                  AND X30.FIELDVALUE = ISIR.S_HOUSING_CODE_4
           LEFT OUTER JOIN XL X31
               ON     X31.FIELDNAME = 'SFA_HOUSING_CD_05'
                  AND X31.FIELDVALUE = ISIR.S_HOUSING_CODE_5
           LEFT OUTER JOIN XL X32
               ON     X32.FIELDNAME = 'SFA_HOUSING_CD_06'
                  AND X32.FIELDVALUE = ISIR.S_HOUSING_CODE_6
           LEFT OUTER JOIN XL X33
               ON     X33.FIELDNAME = 'FIRST_BACH_DEGREE'
                  AND X33.FIELDVALUE = ISIR.S_FIRST_BACH_DEGREE
           LEFT OUTER JOIN XL X34
               ON     X34.FIELDNAME = 'INTERESTED_IN_WS'
                  AND X34.FIELDVALUE = ISIR.S_INTERESTED_IN_WS
           LEFT OUTER JOIN XL X34A
               ON     X34A.FIELDNAME = 'DEGREE_CERTIF'
                  AND X34A.FIELDVALUE = ISIR.S_DEGREE_CERTIF
           LEFT OUTER JOIN XL X35
               ON     X35.FIELDNAME = 'CURRENT_GRADE_LVL'
                  AND X35.FIELDVALUE = ISIR.S_CURRENT_GRADE_LVL
           LEFT OUTER JOIN XL X36
               ON     X36.FIELDNAME = 'ISIR_CIT_STAT02'
                  AND X36.FIELDVALUE = ISIR.S_CITIZENSHIP_STATUS
           LEFT OUTER JOIN XL X37
               ON     X37.FIELDNAME = 'TAX_RETURN_FILED'
                  AND X37.FIELDVALUE = ISIR.S_TAX_RETURN_FILED
           LEFT OUTER JOIN XL X38
               ON     X38.FIELDNAME = 'ELIG_FOR_1040A_EZ'
                  AND X38.FIELDVALUE = ISIR.S_ELIG_FOR_1040A_EZ
           LEFT OUTER JOIN XL X39
               ON     X39.FIELDNAME = 'LEGAL_RES_PRIOR'
                  AND X39.FIELDVALUE = ISIR.S_LEGAL_RES_PRIOR
           LEFT OUTER JOIN XL X40
               ON     X40.FIELDNAME = 'CHILDREN'
                  AND X40.FIELDVALUE = ISIR.S_CHILDREN
           LEFT OUTER JOIN XL X41
               ON     X41.FIELDNAME = 'SFA_ACTIVE_DUTY'
                  AND X41.FIELDVALUE = ISIR.S_SFA_ACTIVE_DUTY
           LEFT OUTER JOIN XL X42
               ON     X42.FIELDNAME = 'SFA_SSI_INCOME'
                  AND X42.FIELDVALUE = ISIR.S_SFA_SSI_INCOME
           LEFT OUTER JOIN XL X43
               ON     X43.FIELDNAME = 'SFA_FOOD_STAMPS'
                  AND X43.FIELDVALUE = ISIR.S_SFA_FOOD_STAMPS
           LEFT OUTER JOIN XL X44
               ON     X44.FIELDNAME = 'SFA_SCHL_LUNCH_PRG'
                  AND X44.FIELDVALUE = ISIR.S_SFA_SCHL_LUNCH_PRG
           LEFT OUTER JOIN XL X45
               ON     X45.FIELDNAME = 'SFA_TANF_BENEFITS'
                  AND X45.FIELDVALUE = ISIR.S_SFA_TANF_BENEFITS
           LEFT OUTER JOIN XL X46
               ON     X46.FIELDNAME = 'SFA_WIC_BENEFITS'
                  AND X46.FIELDVALUE = ISIR.S_SFA_WIC_BENEFITS
           LEFT OUTER JOIN XL X47
               ON     X47.FIELDNAME = 'SFA_STDNT_GENDER'
                  AND X47.FIELDVALUE = ISIR.S_SFA_STDNT_GENDER
           LEFT OUTER JOIN XL X48
               ON     X48.FIELDNAME = 'SFA_HOUSING_CD_07'
                  AND X48.FIELDVALUE = ISIR.S_SFA_HOUSING_CODE7
           LEFT OUTER JOIN XL X49
               ON     X49.FIELDNAME = 'SFA_HOUSING_CD_08'
                  AND X49.FIELDVALUE = ISIR.S_SFA_HOUSING_CODE8
           LEFT OUTER JOIN XL X50
               ON     X50.FIELDNAME = 'SFA_HOUSING_CD_09'
                  AND X50.FIELDVALUE = ISIR.S_SFA_HOUSING_CODE9
           LEFT OUTER JOIN XL X51
               ON     X51.FIELDNAME = 'SFA_HOUSING_CD_10'
                  AND X51.FIELDVALUE = ISIR.S_SFA_HOUSING_CODE10
           LEFT OUTER JOIN XL X52
               ON     X52.FIELDNAME = 'SFA_EMANCIPT_MINOR'
                  AND X52.FIELDVALUE = ISIR.S_SFA_EMANCIPT_MINOR
           LEFT OUTER JOIN XL X53
               ON     X53.FIELDNAME = 'SFA_LEGAL_GUARDIAN'
                  AND X53.FIELDVALUE = ISIR.S_SFA_LEGAL_GUARDIAN
           LEFT OUTER JOIN XL X54
               ON     X54.FIELDNAME = 'SFA_YOUTH_LIASON'
                  AND X54.FIELDVALUE = ISIR.S_SFA_YOUTH_LIASON
           LEFT OUTER JOIN XL X55
               ON     X55.FIELDNAME = 'SFA_YOUTH_HUD'
                  AND X55.FIELDVALUE = ISIR.S_SFA_YOUTH_HUD
           LEFT OUTER JOIN XL X56
               ON     X56.FIELDNAME = 'SFA_RISK_HOMELESS'
                  AND X56.FIELDVALUE = ISIR.S_SFA_RISK_HOMELESS
           LEFT OUTER JOIN XL X57
               ON     X57.FIELDNAME = 'SFA_DISLOCATE_WRK'
                  AND X57.FIELDVALUE = ISIR.S_SFA_DISLOCATE_WRK
           LEFT OUTER JOIN XL X58
               ON     X58.FIELDNAME = 'SFA_STU_ASSET_THRS'
                  AND X58.FIELDVALUE = ISIR.S_SFA_STU_ASSET_THRS
           LEFT OUTER JOIN XL X59
               ON     X59.FIELDNAME = 'SFA_STU_TAX_RET'
                  AND X59.FIELDVALUE = ISIR.S_SFA_STU_TAX_RET
           LEFT OUTER JOIN XL X60
               ON     X60.FIELDNAME = 'SFA_HS_DIP_EQUIV'
                  AND X60.FIELDVALUE = ISIR.S_SFA_HS_DIP_EQUIV
           LEFT OUTER JOIN XL X61
               ON     X61.FIELDNAME = 'DEPNDNCY_STAT'
                  AND X61.FIELDVALUE = ISIR.S_DEPNDNCY_STAT
           LEFT OUTER JOIN XL X62
               ON     X62.FIELDNAME =
                      (CASE
                           WHEN STAID.AID_YEAR < '2000'
                           THEN
                               'MARITAL_STAT_PAR'
                           WHEN     STAID.AID_YEAR >= '2000'
                                AND STAID.AID_YEAR < '2015'
                           THEN
                               'MARITAL_STAT_PAR00'
                           ELSE
                               'MARITAL_STAT_PAR15'
                       END)
                  AND X62.FIELDVALUE = ISIR.P_MARITAL_STAT
           LEFT OUTER JOIN XL X63
               ON     X63.FIELDNAME = 'TAX_FORM_FILED07'
                  AND X63.FIELDVALUE = ISIR.P_TAX_FORM_FILED
           LEFT OUTER JOIN XL X64
               ON     X64.FIELDNAME = 'FATHER_GRADE_LVL'
                  AND X64.FIELDVALUE = ISIR.P_FATHER_GRADE_LVL
           LEFT OUTER JOIN XL X65
               ON     X65.FIELDNAME = 'MOTHER_GRADE_LVL'
                  AND X65.FIELDVALUE = ISIR.P_MOTHER_GRADE_LVL
           LEFT OUTER JOIN XL X66
               ON     X66.FIELDNAME = 'TAX_RETURN_FILED'
                  AND X66.FIELDVALUE = ISIR.P_TAX_RETURN_FILED
           LEFT OUTER JOIN XL X67
               ON     X67.FIELDNAME = 'ELIG_FOR_1040A_EZ'
                  AND X67.FIELDVALUE = ISIR.P_ELIG_FOR_1040A_EZ
           LEFT OUTER JOIN XL X68
               ON     X68.FIELDNAME = 'LEGAL_RES_PRIOR'
                  AND X68.FIELDVALUE = ISIR.P_LEGAL_RES_PRIOR
           LEFT OUTER JOIN XL X69
               ON     X69.FIELDNAME = 'SFA_SSI_INCOME'
                  AND X69.FIELDVALUE = ISIR.P_SFA_SSI_INCOME
           LEFT OUTER JOIN XL X70
               ON     X70.FIELDNAME = 'SFA_FOOD_STAMPS'
                  AND X70.FIELDVALUE = ISIR.P_SFA_FOOD_STAMPS
           LEFT OUTER JOIN XL X71
               ON     X71.FIELDNAME = 'SFA_SCHL_LUNCH_PRG'
                  AND X71.FIELDVALUE = ISIR.P_SFA_SCHL_LUNCH_PRG
           LEFT OUTER JOIN XL X72
               ON     X72.FIELDNAME = 'SFA_TANF_BENEFITS'
                  AND X72.FIELDVALUE = ISIR.P_SFA_TANF_BENEFITS
           LEFT OUTER JOIN XL X73
               ON     X73.FIELDNAME = 'SFA_WIC_BENEFITS'
                  AND X73.FIELDVALUE = ISIR.P_SFA_WIC_BENEFITS
           LEFT OUTER JOIN XL X74
               ON     X74.FIELDNAME = 'SFA_DISLOCATE_WRK'
                  AND X74.FIELDVALUE = ISIR.P_SFA_DISLOCATE_WRK
           LEFT OUTER JOIN XL X75
               ON     X75.FIELDNAME = 'SFA_PAR_ASSET_THRS'
                  AND X75.FIELDVALUE = ISIR.P_SFA_PAR_ASSET_THRS
           LEFT OUTER JOIN XL X76
               ON     X76.FIELDNAME = 'SFA_PAR_TAX_RET'
                  AND X76.FIELDVALUE = ISIR.P_SFA_PAR_TAX_RET
           LEFT OUTER JOIN XL X77
               ON     X77.FIELDNAME = 'FORMULA_TYPE'
                  AND X77.FIELDVALUE = ISIR.FORMULA_TYPE
           LEFT OUTER JOIN XL X78
               ON     X78.FIELDNAME = 'SFA_SPL_CIRCUM_FLG'
                  AND X78.FIELDVALUE = ISIR.SFA_SPL_CIRCUM_FLG_ISIR
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_FA_STDNT_AID_ISIR rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_FA_STDNT_AID_ISIR',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_FA_STDNT_AID_ISIR',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_FA_STDNT_AID_ISIR';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_FA_STDNT_AID_ISIR enable constraint PK_UM_F_FA_STDNT_AID_ISIR';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_FA_STDNT_AID_ISIR');

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

END UM_F_FA_STDNT_AID_ISIR_P;
/
