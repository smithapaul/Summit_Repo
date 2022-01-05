CREATE TABLE UM_F_FA_STDNT_AID_ISIR
(
  INSTITUTION_CD              VARCHAR2(5 BYTE),
  PERSON_ID                   VARCHAR2(11 BYTE),
  AID_YEAR                    VARCHAR2(4 BYTE),
  SRC_SYS_ID                  VARCHAR2(5 BYTE),
  ADM_CNT                     INTEGER,
  SR_CNT                      INTEGER,
  PRSPCT_CNT                  INTEGER,
  STAID_ACTV_CNT              INTEGER,
  ISIR_CNT                    INTEGER,
  INSTITUTION_SID             INTEGER,
  PERSON_SID                  INTEGER,
  PERSON_SRVC_IND_SID         INTEGER,
  FED_DEPEND_STAT             VARCHAR2(1 BYTE),
  FED_DEPEND_STAT_LD          VARCHAR2(30 BYTE),
  PROCESSING_STATUS           VARCHAR2(1 BYTE),
  PROCESSING_STATUS_LD        VARCHAR2(30 BYTE),
  VERIFICATON_STATUS          VARCHAR2(1 BYTE),
  VERIFICATON_STATUS_LD       VARCHAR2(30 BYTE),
  SFA_REVIEW_STATUS           VARCHAR2(1 BYTE),
  SFA_REVIEW_STATUS_LD        VARCHAR2(30 BYTE),
  SCHOLARSHIP_STATUS          VARCHAR2(1 BYTE),
  SCHOLARSHIP_STATUS_LD       VARCHAR2(30 BYTE),
  PACKAGING_METHOD            VARCHAR2(1 BYTE),
  PACKAGING_METHOD_LD         VARCHAR2(30 BYTE),
  PKG_PLAN_ID                 VARCHAR2(10 BYTE),
  EQUITY_LIMIT                NUMBER(11,2),
  SAT_ACADEMIC_PRG            VARCHAR2(1 BYTE),
  SAT_ACADEMIC_PRG_LD         VARCHAR2(30 BYTE),
  QA_VERF_SELECT              VARCHAR2(1 BYTE),
  QA_VERF_SELECT_LD           VARCHAR2(30 BYTE),
  AID_APP_STATUS              VARCHAR2(1 BYTE),
  AID_APP_STATUS_LD           VARCHAR2(30 BYTE),
  VERIF_STATUS_CODE           VARCHAR2(1 BYTE),
  VERIF_STATUS_CODE_LD        VARCHAR2(30 BYTE),
  PELL_PROCESS_FIELD          VARCHAR2(1 BYTE),
  PELL_PROCESS_FIELD_LD       VARCHAR2(30 BYTE),
  PELL_EFFDT                  DATE,
  PELL_EFFSEQ                 INTEGER,
  PELL_TRANS_NBR              INTEGER,
  ACAD_CAREER                 VARCHAR2(4 BYTE),
  PAR_CREDIT_WORTHY           VARCHAR2(1 BYTE),
  TITLEIV_ELIG                VARCHAR2(1 BYTE),
  SFA_SPL_CIRCUM_FLG_ISIR     VARCHAR2(1 BYTE),
  SFA_SPL_CIRCUM_FLG_ISIR_LD  VARCHAR2(30 BYTE),
  SS_MATCH                    VARCHAR2(1 BYTE),
  SS_REGISTRATION             VARCHAR2(1 BYTE),
  INS_MATCH                   VARCHAR2(1 BYTE),
  SSN_MATCH                   VARCHAR2(1 BYTE),
  VA_MATCH                    VARCHAR2(1 BYTE),
  SSA_CITIZENSHP_IND          VARCHAR2(1 BYTE),
  SSA_CITIZENSHP_IND_LD       VARCHAR2(30 BYTE),
  NSLDS_MATCH                 VARCHAR2(1 BYTE),
  NSLDS_MATCH_LD              VARCHAR2(30 BYTE),
  DRUG_OFFENSE_CONV           VARCHAR2(1 BYTE),
  PRISONER_MATCH              VARCHAR2(1 BYTE),
  SSN_MATCH_OVRD              VARCHAR2(1 BYTE),
  SSA_CITIZEN_OVRD            VARCHAR2(1 BYTE),
  INS_MATCH_OVRD              VARCHAR2(1 BYTE),
  VA_MATCH_OVRD               VARCHAR2(1 BYTE),
  SS_MATCH_OVRD               VARCHAR2(1 BYTE),
  SS_REGISTER_OVRD            VARCHAR2(1 BYTE),
  NSLDS_OVRD                  VARCHAR2(1 BYTE),
  PRISONER_OVRD               VARCHAR2(1 BYTE),
  DRUG_OFFENSE_OVRD           VARCHAR2(1 BYTE),
  ISIR_SEC_INS_MATCH          VARCHAR2(1 BYTE),
  FATHER_SSN_MATCH            VARCHAR2(1 BYTE),
  MOTHER_SSN_MATCH            VARCHAR2(1 BYTE),
  PAR_SSN_MATCH_OVRD          VARCHAR2(1 BYTE),
  SFA_RPKG_PLAN_ID            VARCHAR2(10 BYTE),
  SFA_EASS_ACCESS             VARCHAR2(1 BYTE),
  SFA_PP_CRSEWRK_SW           VARCHAR2(1 BYTE),
  SFA_DOD_MATCH               VARCHAR2(1 BYTE),
  SFA_DOD_MATCH_OVRD          VARCHAR2(1 BYTE),
  SFA_SPL_CIRCUM_FLG          VARCHAR2(1 BYTE),
  SFA_SS_GROUP                VARCHAR2(10 BYTE),
  ISIR_EFFDT                  DATE,
  ISIR_EFFSEQ                 INTEGER,
  ISIR_TXN_NBR                INTEGER,
  ORIG_SSN                    VARCHAR2(9 BYTE),
  NAME_CD                     VARCHAR2(2 BYTE),
  DT_APP_RECEIVED             DATE,
  PELL_ELIGIBILITY            VARCHAR2(1 BYTE),
  SAR_C_FLAG                  VARCHAR2(1 BYTE),
  VERF_SELECTION_IND          VARCHAR2(2 BYTE),
  TRANS_PROCESS_DT            DATE,
  CPS_REPROCESS_CD            VARCHAR2(2 BYTE),
  BATCH_NUMBER                VARCHAR2(23 BYTE),
  FAA_INSTITUT_NUM            VARCHAR2(6 BYTE),
  INS_VERF_NUM                VARCHAR2(15 BYTE),
  DEPNDNCY_OVERRIDE           VARCHAR2(1 BYTE),
  APP_STAT_DT                 DATE,
  EFC_STATUS                  VARCHAR2(1 BYTE),
  ADJ_EFC_CALC_REQ            VARCHAR2(1 BYTE),
  CORRECTION_STATUS           VARCHAR2(1 BYTE),
  CORRECTION_STATUS_SD        VARCHAR2(10 BYTE),
  CORR_STAT_DT                DATE,
  INST_PROCESS_DT             DATE,
  OWNING_SCHOOL_CD            VARCHAR2(6 BYTE),
  NSLDS_TXN_NBR               VARCHAR2(2 BYTE),
  APP_SOURCE                  VARCHAR2(2 BYTE),
  APP_SOURCE_LD               VARCHAR2(30 BYTE),
  TRANS_SOURCE                VARCHAR2(2 BYTE),
  TRANS_SOURCE_LD             VARCHAR2(30 BYTE),
  ISIR_CORR_SRC               VARCHAR2(1 BYTE),
  ISIR_EFC_CHANGE             VARCHAR2(1 BYTE),
  ISIR_DUP_SSN_CD             VARCHAR2(1 BYTE),
  VERIF_TRK_FLG               VARCHAR2(4 BYTE),
  VERIF_TRK_FLG_LD            VARCHAR2(30 BYTE),
  ISIR_CPS_PUSHED             VARCHAR2(1 BYTE),
  ISIR_SAR_C_CHNG             VARCHAR2(1 BYTE),
  SFA_VF_SEL_CHG_FLG          VARCHAR2(1 BYTE),
  SFA_REJ_ST_CHG_FLG          VARCHAR2(1 BYTE),
  SFA_STU_IRS_REQUST          VARCHAR2(2 BYTE),
  SFA_STU_IRS_REQUST_LD       VARCHAR2(30 BYTE),
  SFA_PAR_IRS_REQUST          VARCHAR2(2 BYTE),
  SFA_PAR_IRS_REQUST_LD       VARCHAR2(30 BYTE),
  SFA_HIGH_SCHL_FLAG          VARCHAR2(1 BYTE),
  S_NUM_FAMILY_MEMBERS        VARCHAR2(2 BYTE),
  S_NUMBER_IN_COLLEGE         VARCHAR2(1 BYTE),
  S_VETERAN                   VARCHAR2(1 BYTE),
  S_VETERAN_LD                VARCHAR2(30 BYTE),
  S_GRADUATE_STUDENT          VARCHAR2(1 BYTE),
  S_GRADUATE_STUDENT_LD       VARCHAR2(30 BYTE),
  S_MARRIED                   VARCHAR2(1 BYTE),
  S_MARRIED_LD                VARCHAR2(30 BYTE),
  S_MARITAL_STAT              VARCHAR2(1 BYTE),
  S_MARITAL_STAT_LD           VARCHAR2(30 BYTE),
  S_MAR_STATUS_DT             DATE,
  S_ORPHAN                    VARCHAR2(1 BYTE),
  S_ORPHAN_LD                 VARCHAR2(30 BYTE),
  S_DEPENDENTS                VARCHAR2(1 BYTE),
  S_DEPENDENTS_LD             VARCHAR2(30 BYTE),
  S_TAX_FORM_FILED            VARCHAR2(1 BYTE),
  S_TAX_FORM_FILED_LD         VARCHAR2(30 BYTE),
  S_NUMBER_EXEMPTIONS         VARCHAR2(2 BYTE),
  S_AGI                       INTEGER,
  S_TAXES_PAID                INTEGER,
  S_STD_EARNED_INCOME         INTEGER,
  S_SPS_EARNED_INCOME         INTEGER,
  S_CASH_SAVINGS              INTEGER,
  S_SCHOOL_CHOICE_1           VARCHAR2(6 BYTE),
  S_HOUSING_CODE_1            VARCHAR2(1 BYTE),
  S_HOUSING_CODE_1_LD         VARCHAR2(30 BYTE),
  S_SCHOOL_CHOICE_2           VARCHAR2(6 BYTE),
  S_HOUSING_CODE_2            VARCHAR2(1 BYTE),
  S_HOUSING_CODE_2_LD         VARCHAR2(30 BYTE),
  S_SCHOOL_CHOICE_3           VARCHAR2(6 BYTE),
  S_HOUSING_CODE_3            VARCHAR2(1 BYTE),
  S_HOUSING_CODE_3_LD         VARCHAR2(30 BYTE),
  S_SCHOOL_CHOICE_4           VARCHAR2(6 BYTE),
  S_HOUSING_CODE_4            VARCHAR2(1 BYTE),
  S_HOUSING_CODE_4_LD         VARCHAR2(30 BYTE),
  S_SCHOOL_CHOICE_5           VARCHAR2(6 BYTE),
  S_HOUSING_CODE_5            VARCHAR2(1 BYTE),
  S_HOUSING_CODE_5_LD         VARCHAR2(30 BYTE),
  S_SCHOOL_CHOICE_6           VARCHAR2(6 BYTE),
  S_HOUSING_CODE_6            VARCHAR2(1 BYTE),
  S_HOUSING_CODE_6_LD         VARCHAR2(30 BYTE),
  S_FIRST_BACH_DEGREE         VARCHAR2(1 BYTE),
  S_FIRST_BACH_DEGREE_LD      VARCHAR2(30 BYTE),
  S_INTERESTED_IN_WS          VARCHAR2(1 BYTE),
  S_INTERESTED_IN_WS_LD       VARCHAR2(30 BYTE),
  S_DT_APP_COMPLETED          DATE,
  S_DEGREE_CERTIF             VARCHAR2(30 BYTE),
  S_CURRENT_GRADE_LVL         VARCHAR2(2 BYTE),
  S_CURRENT_GRADE_LVL_LD      VARCHAR2(30 BYTE),
  S_TOTAL_FROM_WS3            INTEGER,
  S_STATE_RESIDENCE           VARCHAR2(6 BYTE),
  S_RESIDENCY_DT              DATE,
  S_DRIV_LIC_STATE            VARCHAR2(6 BYTE),
  S_FISAP_TOT_INC             INTEGER,
  S_CITIZENSHIP_STATUS        VARCHAR2(1 BYTE),
  S_CITIZENSHIP_STATUS_LD     VARCHAR2(30 BYTE),
  S_TAX_RETURN_FILED          VARCHAR2(1 BYTE),
  S_TAX_RETURN_FILED_LD       VARCHAR2(30 BYTE),
  S_ELIG_FOR_1040A_EZ         VARCHAR2(1 BYTE),
  S_ELIG_FOR_1040A_EZ_LD      VARCHAR2(30 BYTE),
  S_INV_NET_WORTH             INTEGER,
  S_BUS_NET_WORTH             INTEGER,
  S_LEGAL_RES_PRIOR           VARCHAR2(1 BYTE),
  S_LEGAL_RES_PRIOR_LD        VARCHAR2(30 BYTE),
  S_IWD_PERM_ADDR02           VARCHAR2(35 BYTE),
  S_IWD_CITY                  VARCHAR2(16 BYTE),
  S_IWD_STATE                 VARCHAR2(2 BYTE),
  S_IWD_ZIP                   VARCHAR2(5 BYTE),
  S_IWD_PERM_PHONE            VARCHAR2(10 BYTE),
  S_IWD_STD_LAST_NAME         VARCHAR2(16 BYTE),
  S_IWD_STD_FIRST_NM02        VARCHAR2(12 BYTE),
  S_IWD_STU_MI                VARCHAR2(1 BYTE),
  S_IWD_STD_EMAIL             VARCHAR2(50 BYTE),
  S_SSN                       VARCHAR2(9 BYTE),
  S_BIRTHDATE                 DATE,
  S_DRIVERS_LICENSE_NO        VARCHAR2(20 BYTE),
  S_IWD_STU_ALIEN_REG         VARCHAR2(9 BYTE),
  S_CHILDREN                  VARCHAR2(1 BYTE),
  S_CHILDREN_LD               VARCHAR2(30 BYTE),
  S_TOTAL_FROM_WKC            INTEGER,
  S_SFA_ACTIVE_DUTY           VARCHAR2(1 BYTE),
  S_SFA_ACTIVE_DUTY_LD        VARCHAR2(30 BYTE),
  S_SFA_SSI_INCOME            VARCHAR2(1 BYTE),
  S_SFA_SSI_INCOME_LD         VARCHAR2(30 BYTE),
  S_SFA_FOOD_STAMPS           VARCHAR2(1 BYTE),
  S_SFA_FOOD_STAMPS_LD        VARCHAR2(30 BYTE),
  S_SFA_SCHL_LUNCH_PRG        VARCHAR2(1 BYTE),
  S_SFA_SCHL_LUNCH_PRG_LD     VARCHAR2(30 BYTE),
  S_SFA_TANF_BENEFITS         VARCHAR2(1 BYTE),
  S_SFA_TANF_BENEFITS_LD      VARCHAR2(30 BYTE),
  S_SFA_WIC_BENEFITS          VARCHAR2(1 BYTE),
  S_SFA_WIC_BENEFITS_LD       VARCHAR2(30 BYTE),
  S_SFA_STDNT_GENDER          VARCHAR2(1 BYTE),
  S_SFA_STDNT_GENDER_LD       VARCHAR2(30 BYTE),
  S_SFA_SCHL_CHOICE_7         VARCHAR2(6 BYTE),
  S_SFA_HOUSING_CODE7         VARCHAR2(1 BYTE),
  S_SFA_HOUSING_CODE7_LD      VARCHAR2(30 BYTE),
  S_SFA_SCHL_CHOICE_8         VARCHAR2(6 BYTE),
  S_SFA_HOUSING_CODE8         VARCHAR2(1 BYTE),
  S_SFA_HOUSING_CODE8_LD      VARCHAR2(30 BYTE),
  S_SFA_SCHL_CHOICE_9         VARCHAR2(6 BYTE),
  S_SFA_HOUSING_CODE9         VARCHAR2(1 BYTE),
  S_SFA_HOUSING_CODE9_LD      VARCHAR2(30 BYTE),
  S_SFA_SCHL_CHOICE_10        VARCHAR2(6 BYTE),
  S_SFA_HOUSING_CODE10        VARCHAR2(1 BYTE),
  S_SFA_HOUSING_CODE10_LD     VARCHAR2(30 BYTE),
  S_SFA_EMANCIPT_MINOR        VARCHAR2(1 BYTE),
  S_SFA_EMANCIPT_MINOR_LD     VARCHAR2(30 BYTE),
  S_SFA_LEGAL_GUARDIAN        VARCHAR2(1 BYTE),
  S_SFA_LEGAL_GUARDIAN_LD     VARCHAR2(30 BYTE),
  S_SFA_YOUTH_LIASON          VARCHAR2(1 BYTE),
  S_SFA_YOUTH_LIASON_LD       VARCHAR2(30 BYTE),
  S_SFA_YOUTH_HUD             VARCHAR2(1 BYTE),
  S_SFA_YOUTH_HUD_LD          VARCHAR2(30 BYTE),
  S_SFA_RISK_HOMELESS         VARCHAR2(1 BYTE),
  S_SFA_RISK_HOMELESS_LD      VARCHAR2(30 BYTE),
  S_SFA_DISLOCATE_WRK         VARCHAR2(1 BYTE),
  S_SFA_DISLOCATE_WRK_LD      VARCHAR2(30 BYTE),
  S_SFA_EDU_CREDITS           INTEGER,
  S_SFA_CHILD_SUP_PAID        INTEGER,
  S_SFA_NEED_EMPLOYMNT        INTEGER,
  S_SFA_GRANT_AID             INTEGER,
  S_SFA_COMBATPAY             INTEGER,
  S_SFA_PENSION_PAY           INTEGER,
  S_SFA_IRA_PAY               INTEGER,
  S_SFA_CHILD_SUP_RECV        INTEGER,
  S_SFA_INTERST_INCOME        INTEGER,
  S_SFA_IRA_DIST              INTEGER,
  S_SFA_UNTAX_PENSION         INTEGER,
  S_SFA_MILITARY_ALLOW        INTEGER,
  S_SFA_VET_NONEDU_BEN        INTEGER,
  S_SFA_UNTAX_INCOME          INTEGER,
  S_SFA_NON_REP_MONEY         INTEGER,
  S_SFA_COOP_EARN             INTEGER,
  S_SFA_HIGH_SCHL_NAME        VARCHAR2(50 BYTE),
  S_SFA_HIGH_SCHL_CITY        VARCHAR2(28 BYTE),
  S_SFA_HIGH_SCHL_STAT        VARCHAR2(2 BYTE),
  S_SFA_HIGH_SCHL_CODE        VARCHAR2(12 BYTE),
  S_SFA_STU_ASSET_THRS        VARCHAR2(1 BYTE),
  S_SFA_STU_ASSET_THRS_LD     VARCHAR2(30 BYTE),
  S_SFA_STU_TAX_RET           VARCHAR2(1 BYTE),
  S_SFA_STU_TAX_RET_LD        VARCHAR2(30 BYTE),
  S_DOB_PRIOR                 VARCHAR2(5 BYTE),
  S_SFA_HS_DIP_EQUIV          VARCHAR2(1 BYTE),
  S_SFA_HS_DIP_EQUIV_LD       VARCHAR2(30 BYTE),
  S_DEPNDNCY_STAT             VARCHAR2(1 BYTE),
  S_DEPNDNCY_STAT_LD          VARCHAR2(30 BYTE),
  P_MARITAL_STAT              VARCHAR2(1 BYTE),
  P_MARITAL_STAT_LD           VARCHAR2(30 BYTE),
  P_LEGAL_RESIDENCE           VARCHAR2(2 BYTE),
  P_DT_LEGAL_RES              DATE,
  P_NUMBER_IN_FAMILY          VARCHAR2(2 BYTE),
  P_NUM_IN_COLLEGE            VARCHAR2(1 BYTE),
  P_TAX_FORM_FILED            VARCHAR2(1 BYTE),
  P_TAX_FORM_FILED_LD         VARCHAR2(30 BYTE),
  P_NUMBER_EXEMPTIONS         VARCHAR2(2 BYTE),
  P_AGI                       INTEGER,
  P_TAXES_PAID                INTEGER,
  P_FATHER_INCOME             INTEGER,
  P_MOTHER_INCOME             INTEGER,
  P_CASH_SAVINGS              INTEGER,
  P_FATHER_GRADE_LVL          VARCHAR2(1 BYTE),
  P_FATHER_GRADE_LVL_LD       VARCHAR2(30 BYTE),
  P_MOTHER_GRADE_LVL          VARCHAR2(1 BYTE),
  P_MOTHER_GRADE_LVL_LD       VARCHAR2(30 BYTE),
  P_TOTAL_FROM_WS3            INTEGER,
  P_TAX_RETURN_FILED          VARCHAR2(1 BYTE),
  P_TAX_RETURN_FILED_LD       VARCHAR2(30 BYTE),
  P_ELIG_FOR_1040A_EZ         VARCHAR2(1 BYTE),
  P_ELIG_FOR_1040A_EZ_LD      VARCHAR2(30 BYTE),
  P_TOTAL_FROM_WKA            INTEGER,
  P_INV_NET_WORTH             INTEGER,
  P_BUS_NET_WORTH             INTEGER,
  P_FATHER_SSN                VARCHAR2(9 BYTE),
  P_FATHER_LAST_NAME          VARCHAR2(16 BYTE),
  P_MOTHER_SSN                VARCHAR2(9 BYTE),
  P_MOTHER_LAST_NAME          VARCHAR2(16 BYTE),
  P_LEGAL_RES_PRIOR           VARCHAR2(1 BYTE),
  P_LEGAL_RES_PRIOR_LD        VARCHAR2(30 BYTE),
  P_TOTAL_FROM_WKC            INTEGER,
  P_MAR_STATUS_DT             DATE,
  P_FATHER_1ST_NM_INIT        VARCHAR2(1 BYTE),
  P_FATHER_DOB                DATE,
  P_MOTHER_1ST_NM_INIT        VARCHAR2(1 BYTE),
  P_MOTHER_DOB                DATE,
  P_IWD_PAR_EMAIL             VARCHAR2(50 BYTE),
  P_SFA_SSI_INCOME            VARCHAR2(1 BYTE),
  P_SFA_SSI_INCOME_LD         VARCHAR2(30 BYTE),
  P_SFA_FOOD_STAMPS           VARCHAR2(1 BYTE),
  P_SFA_FOOD_STAMPS_LD        VARCHAR2(30 BYTE),
  P_SFA_SCHL_LUNCH_PRG        VARCHAR2(1 BYTE),
  P_SFA_SCHL_LUNCH_PRG_LD     VARCHAR2(30 BYTE),
  P_SFA_TANF_BENEFITS         VARCHAR2(1 BYTE),
  P_SFA_TANF_BENEFITS_LD      VARCHAR2(30 BYTE),
  P_SFA_WIC_BENEFITS          VARCHAR2(1 BYTE),
  P_SFA_WIC_BENEFITS_LD       VARCHAR2(30 BYTE),
  P_SFA_DISLOCATE_WRK         VARCHAR2(1 BYTE),
  P_SFA_DISLOCATE_WRK_LD      VARCHAR2(30 BYTE),
  P_SFA_EDU_CREDITS           INTEGER,
  P_SFA_CHILD_SUP_PAID        INTEGER,
  P_SFA_NEED_EMPLOYMNT        INTEGER,
  P_SFA_GRANT_AID             INTEGER,
  P_SFA_COMBATPAY             INTEGER,
  P_SFA_PENSION_PAY           INTEGER,
  P_SFA_IRA_PAY               INTEGER,
  P_SFA_CHILD_SUP_RECV        INTEGER,
  P_SFA_INTERST_INCOME        INTEGER,
  P_SFA_IRA_DIST              INTEGER,
  P_SFA_UNTAX_PENSION         INTEGER,
  P_SFA_MILITARY_ALLOW        INTEGER,
  P_SFA_VET_NONEDU_BEN        INTEGER,
  P_SFA_UNTAX_INCOME          INTEGER,
  P_SFA_COOP_EARN             INTEGER,
  P_SFA_PAR_ASSET_THRS        VARCHAR2(1 BYTE),
  P_SFA_PAR_ASSET_THRS_LD     VARCHAR2(30 BYTE),
  P_SFA_PAR_TAX_RET           VARCHAR2(1 BYTE),
  P_SFA_PAR_TAX_RET_LD        VARCHAR2(30 BYTE),
  BUDGET_DURATION             INTEGER,
  PRIMARY_EFC                 INTEGER,
  SECONDARY_EFC               INTEGER,
  AUTO_ZERO_EFC               VARCHAR2(1 BYTE),
  FORMULA_TYPE                VARCHAR2(1 BYTE),
  FORMULA_TYPE_LD             VARCHAR2(30 BYTE),
  TOTAL_INCOME                INTEGER,
  ALWNC_AGAINST_TI            INTEGER,
  STATE_TAX_ALWNC             INTEGER,
  EMPLOYMENT_ALWNC            INTEGER,
  INC_PROTECTN_ALWNC          INTEGER,
  AVAILABLE_INCOME            INTEGER,
  DESCRTN_NET_WORTH           INTEGER,
  AST_PROTECTN_ALWNC          INTEGER,
  CONTRIB_FROM_ASSET          INTEGER,
  ADJ_AVAILABLE_INC           INTEGER,
  TOTAL_PAR_CONTRIB           INTEGER,
  TOTAL_STU_CONTRIB           INTEGER,
  ADJ_PAR_CONTRIB             INTEGER,
  DEP_STU_I_CONTRIB           INTEGER,
  DEP_STU_A_CONTRIB           INTEGER,
  STU_TOTAL_INC               INTEGER,
  CONTRIB_AVAIL_INC           INTEGER,
  PRORATED_EFC                INTEGER,
  SECONDARY_EFC_TP            VARCHAR2(1 BYTE),
  EFC_NET_WORTH               INTEGER,
  STU_ALLOW_VS_TI             INTEGER,
  STU_DISC_NET_WORTH          INTEGER,
  ISIR_CALC_SC                INTEGER,
  ISIR_CALC_PC                INTEGER,
  ISIR_CALC_EFC               INTEGER,
  SFA_SIG_REJ_EFC             INTEGER,
  REJ_OVR_IND_STDNT           VARCHAR2(1 BYTE),
  REJ_OVR_STDNT_NAME          VARCHAR2(1 BYTE),
  REJ_OVR_BIG_FAMILY          VARCHAR2(1 BYTE),
  ASMPTN_OVR_FAM_MEM          VARCHAR2(1 BYTE),
  ASMPTN_OVR_COL_P            VARCHAR2(1 BYTE),
  ASMPTN_OVR_AGI_P            VARCHAR2(1 BYTE),
  ASMPTN_OVR_COL_S            VARCHAR2(1 BYTE),
  ASMPTN_OVR_AGI_S            VARCHAR2(1 BYTE),
  ASMPTN_OVR_WS3_P            VARCHAR2(1 BYTE),
  ASMPTN_OVR_WS3_S            VARCHAR2(1 BYTE),
  REJ_OVR_BIRTH_YEAR          VARCHAR2(1 BYTE),
  REJ_OVR_TAX_RANGE           VARCHAR2(1 BYTE),
  REJ_OVR_TAXRNG_DEP          VARCHAR2(1 BYTE),
  SFA_REJ_OVR_TX_PAR          VARCHAR2(1 BYTE),
  SFA_REJ_OVR_DADSSN          VARCHAR2(1 BYTE),
  SFA_REJ_OVR_MOMSSN          VARCHAR2(1 BYTE),
  SFA_REJ_OVR_TX_STU          VARCHAR2(1 BYTE),
  SFA_REJ_OVR_NO_TAX          VARCHAR2(1 BYTE),
  SFA_REJ_OVR_MAR_ST          VARCHAR2(1 BYTE),
  LOAD_ERROR                  VARCHAR2(1 BYTE),
  DATA_ORIGIN                 VARCHAR2(1 BYTE),
  CREATED_EW_DTTM             DATE,
  LASTUPD_EW_DTTM             DATE,
  BATCH_SID                   INTEGER
)
NOLOGGING 
NOCOMPRESS 
NO INMEMORY
NOCACHE
RESULT_CACHE (MODE DEFAULT)
NOPARALLEL;
