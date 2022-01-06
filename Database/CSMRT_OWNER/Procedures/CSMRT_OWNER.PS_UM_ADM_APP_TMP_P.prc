CREATE OR REPLACE PROCEDURE             PS_UM_ADM_APP_TMP_P AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_UM_ADM_APP_TMP from PeopleSoft table PS_UM_ADM_APP_TMP.
--
-- V01  SMT-xxxx 05/11/2017,    Jim Doucette
--                              Converted from PS_UM_ADM_APP_TMP.SQL
-- V01  SMT-7627 02/07/2018,    Jim Doucette
--                              Added new field UM_ADM_HEAR_US_CD.
-- V02  Case 72761 11/13/2020   Jim Doucette
--                              Added UM_ENG_AS_SEC_LNG from source.
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_UM_ADM_APP_TMP';
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
       START_DT = SYSDATE,
       END_DT = NULL
 where TABLE_NAME = 'PS_UM_ADM_APP_TMP'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Truncating',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_UM_ADM_APP_TMP@SASOURCE S)
 where TABLE_NAME = 'PS_UM_ADM_APP_TMP'
;

strSqlCommand := 'commit';
commit;


strSqlDynamic   := 'truncate table CSSTG_OWNER.PS_T_UM_ADM_APP_TMP';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );


strSqlCommand   := 'Loading temp table for CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Loading'
 where TABLE_NAME = 'PS_UM_ADM_APP_TMP'
;

strSqlCommand := 'commit';
commit;


strSqlCommand := 'insert';
insert /*+ append */  into CSSTG_OWNER.PS_T_UM_ADM_APP_TMP
select /*+ full(S) */
    nvl(trim(UM_ADM_USERID),'-') UM_ADM_USERID, 
    nvl(trim(UM_ADM_APP_SEQ),'-') UM_ADM_APP_SEQ, 
    nvl(trim(UM_ADM_REC_NBR),'-') UM_ADM_REC_NBR, 
    nvl(trim(INSTITUTION),'-') INSTITUTION, 
    EMPLID, 
    ADM_APPL_NBR, 
    UM_ADM_EMAIL, 
    EMAILID, 
    ACAD_CAREER, 
    ADM_APPL_CTR, 
    UM_ADM_SUB_DT, 
    FIRST_NAME, 
    MIDDLE_NAME, 
    LAST_NAME, 
    UM_SSN, 
    BIRTHDATE, 
    SEX, 
    UM_US_CITIZEN, 
    UM_CITIZNSHIP_STAT, 
    UM_BIRTH_CNTRY, 
    UM_ADM_NOT_MA_RES, 
    VISA_PERMIT_TYPE, 
    VISA_WRKPMT_STATUS, 
    UM_VISA_WRKPMT_NBR, 
    UM_ADM_VISA_START, 
    UM_ADM_VISA_END, 
    UM_SUFFIX, 
    UM_PREFIX, 
    UM_FORMER_FNAME1, 
    UM_FORMER_MNAME1, 
    UM_FORMER_LNAME1, 
    UM_PREF_FNAME1, 
    UM_PREF_MNAME1, 
    UM_PREF_LNAME1, 
    UM_PERM_ADDRESS1, 
    UM_PERM_ADDRESS2, 
    UM_PERM_ADDRESS3, 
    UM_PERM_CITY, 
    UM_PERM_STATE, 
    UM_PERM_POSTAL, 
    COUNTRY_CODE_PERM, 
    UM_PERM_COUNTRY, 
    UM_VALID_UNTIL,
    UM_MAIL_ADDRESS1, 
    UM_MAIL_ADDRESS2, 
    UM_MAIL_ADDRESS3, 
    UM_MAIL_CITY, 
    UM_MAIL_STATE, 
    UM_MAIL_POSTAL, 
    UM_MAIL_COUNTRY, 
    COUNTRY_CD, 
    COUNTRY_CODE, 
    UM_PERM_PHONE, 
    UM_PERM_PHONE1, 
    UM_PERM_PHONE2, 
    UM_PERM_PHONE3, 
    UM_CELL1, 
    UM_CELL2, 
    UM_CELL3, 
    UM_BUSN_CNTRY_CD, 
    UM_BUSN_PHONE, 
    UM_BUSN_PHONE1, 
    UM_BUSN_PHONE3, 
    UM_BUSN_EXTENSION, 
    UM_ADM_SESSION, 
    ADMIT_TYPE, 
    UM_ADM_SUBFIELD, 
    UM_PRG_PLN_SBPLN, 
    UM_PRG_PLN_SBPLN2, 
    UM_ACAD_PROG, 
    UM_ACAD_PLAN, 
    UM_ACAD_SUB_PLAN, 
    UM_ACAD_PROG1, 
    UM_ACAD_PLAN1, 
    UM_ACAD_SUB_PLAN1, 
    UM_ADM_APP_FIN_AID, 
    UM_GRE_MO, 
    UM_GRE_YR, 
    UM_GRE_VERB, 
    UM_GRE_QUAN, 
    UM_GRE_ANAL, 
    UM_GRE_SUBJ_SC, 
    UM_GRE_SUBJ_MO, 
    UM_GRE_SUBJ_YR, 
    UM_GRE_SUBJECT, 
    UM_GMAT_MO, 
    UM_GMAT_YR, 
    UM_GMAT_SC, 
    UM_TOEFL_MO, 
    UM_TOEFL_YR, 
    UM_TOEFL_SC, 
    UM_TEST1_MO, 
    UM_TEST1_NAME, 
    UM_TEST1_SC, 
    UM_TEST1_YR, 
    UM_ADM_APP_GPA_ALL, 
    UM_ADM_APP_GPA_GRD, 
    UM_ADM_APP_GPA_MAJ, 
    UM_ADM_APP_GPA_TWO, 
    UM_ACT1_MO, 
    UM_ACT1_SC, 
    UM_ACT1_YR, 
    UM_ACT2_MO, 
    UM_ACT2_SC, 
    UM_ACT2_YR, 
    UM_ACT3_MO, 
    UM_ACT3_SC, 
    UM_ACT3_YR, 
    UM_SAT1_MO, 
    UM_SAT1_SC, 
    UM_SAT1_YR, 
    UM_SAT2_MO, 
    UM_SAT2_SC, 
    UM_SAT2_YR, 
    UM_SAT3_MO, 
    UM_SAT3_SC, 
    UM_SAT3_YR, 
    UM_TOEFL1_MO, 
    UM_TOEFL1_SC, 
    UM_TOEFL1_YR, 
    UM_TOEFL2_MO, 
    UM_TOEFL2_SC, 
    UM_TOEFL2_YR, 
    UM_TOEFL3_MO, 
    UM_TOEFL3_SC, 
    UM_TOEFL3_YR, 
    UM_IELTS1_MO, 
    UM_IELTS1_SC, 
    UM_IELTS1_YR, 
    UM_IELTS2_MO, 
    UM_IELTS2_SC, 
    UM_IELTS2_YR, 
    UM_IELTS3_MO, 
    UM_IELTS3_SC, 
    UM_IELTS3_YR, 
    UM_GMAT1_MO, 
    UM_GMAT1_SC, 
    UM_GMAT1_YR, 
    UM_GMAT2_MO, 
    UM_GMAT2_SC, 
    UM_GMAT2_YR, 
    UM_GMAT3_MO, 
    UM_GMAT3_SC, 
    UM_GMAT3_YR, 
    UM_GRE_AW_MO, 
    UM_GRE_AW_SC, 
    UM_GRE_AW_YR, 
    UM_GRE_Q_MO, 
    UM_GRE_Q_SC, 
    UM_GRE_Q_YR, 
    UM_GRE_V_MO, 
    UM_GRE_V_SC, 
    UM_GRE_V_YR, 
    UM_LSAT1_MO, 
    UM_LSAT1_SC, 
    UM_LSAT1_YR, 
    UM_LSAT2_MO, 
    UM_LSAT2_SC, 
    UM_LSAT2_YR, 
    UM_LSAT3_MO, 
    UM_LSAT3_SC, 
    UM_LSAT3_YR, 
    UM_COURSE_1_CREDIT, 
    UM_COURSE_1_ID, 
    UM_COURSE_1_TITLE, 
    UM_COURSE_2_CREDIT, 
    UM_COURSE_2_ID, 
    UM_COURSE_2_TITLE, 
    UM_COURSE_3_CREDIT, 
    UM_COURSE_3_ID, 
    UM_COURSE_3_TITLE, 
    UM_COURSE_4_CREDIT, 
    UM_COURSE_4_ID, 
    UM_COURSE_4_TITLE, 
    UM_COURSE_5_CREDIT, 
    UM_COURSE_5_ID, 
    UM_COURSE_5_TITLE, 
    MILITARY_STATUS, 
    UM_ETHNIC, 
    UM_EMPLOY_STATE, 
    UM_LAW4, 
    UM_ADM_FAC_FNAME, 
    UM_ADM_FAC_LNAME, 
    UM_LAW5 UM_LAW5, 
    UM_ADM_APP_MO_FR2, 
    UM_ADM_APP_MO_FR3, 
    UM_ADM_APP_MO_FR4, 
    UM_ADM_APP_MO_TO2, 
    UM_ADM_APP_MO_TO3, 
    UM_ADM_APP_MO_TO4, 
    UM_HS_ID, 
    UM_ACAD_LOAD_APPR, 
    UM_ADM_ONLINE_DEGR, 
    UM_ADM_MUSIC_INST1, 
    UM_PRV1_ID, 
    UM_PRV2_ID, 
    UM_PRV3_ID, 
    UM_PRV4_ID, 
    UM_PHONE_TYPE, 
    UM_INTL_ZIP, 
    UM_ADM_APP_MO_FR1, 
    UM_ADM_APP_MO_TO1, 
    UM_ADM_APP_TITLE1, 
    UM_ADM_APP_YR_FR1, 
    UM_ADM_APP_YR_TO1, 
    UM_ADM_APP_POS1, 
    UM_ADM_APP_POS2, 
    UM_ADM_APP_POS3, 
    UM_ADM_APP_ADDR1, 
    UM_ADM_APP_ADDR2, 
    UM_ADM_APP_ADDR3, 
    UM_ADM_APP_NAM1, 
    UM_ADM_APP_NAM2, 
    UM_ADM_APP_NAM3, 
    UM_ADM_APP_CNTRY1, 
    UM_ADM_APP_CNTRY2, 
    UM_ADM_APP_CNTRY3, 
    UM_ADM_APP_CNTRY4, 
    UM_ADM_APP_STATE1, 
    UM_ADM_APP_STATE2, 
    UM_ADM_APP_STATE3, 
    UM_ADM_APP_STATE4, 
    UM_ADM_APP_PR_TRM, 
    UM_ADM_APP_EN_TRM, 
    UM_ADM_APP_PRIOR, 
    UM_ADM_APP_ENRL, 
    UM_ADM_APP_CAR1, 
    UM_ADM_APP_CAR2, 
    UM_ADM_APP_CAR3, 
    UM_ADM_APP_CAR4, 
    UM_ADM_APP_DEG1, 
    UM_ADM_APP_DEG2, 
    UM_ADM_APP_DEG3, 
    UM_ADM_APP_DEG4, 
    UM_ADM_APP_DEGYR1, 
    UM_ADM_APP_DEGYR2, 
    UM_ADM_APP_DEGYR3, 
    UM_ADM_APP_DEGYR4, 
    UM_ADM_APP_FROM1, 
    UM_ADM_APP_FROM2, 
    UM_ADM_APP_FROM3, 
    UM_ADM_APP_FROM4, 
    UM_ADM_APP_MONTH1, 
    UM_ADM_APP_MONTH2, 
    UM_ADM_APP_MONTH3, 
    UM_ADM_APP_MONTH4, 
    UM_ADM_APP_PRV1, 
    UM_ADM_APP_PRV2, 
    UM_ADM_APP_PRV3, 
    UM_ADM_APP_PRV4, 
    UM_ADM_APP_TO1, 
    UM_ADM_APP_TO2, 
    UM_ADM_APP_TO3, 
    UM_ADM_APP_TO4, 
    UM_ADM_ASSIST, 
    UM_ADM_FELLOW, 
    MBA_ABITUS, 
    MBA_CURRENT_STUDNT, 
    MBA_DAY, 
    MBA_EXCLSVE_ONLINE, 
    MBA_WRK_PROFESNLS, 
    CONTACT_NAME, 
    UM_EMERG_COUNTRY, 
    UM_ADM_REL_TYPE, 
    UM_EMERG_CNTRY_CD, 
    UM_EMERG_PHONE, 
    CONTACT_PHONE, 
    CONTACT_PHONE_EXT, 
    UM_PARENT_NAME, 
    UM_PARENT_ADDR1, 
    UM_PARENT_ADDR2, 
    UM_PARENT_ADDR3, 
    UM_PARENT_CITY, 
    UM_PARENT_STATE, 
    UM_PARENT_COUNTRY, 
    UM_PARENT_PHONE, 
    UM_PARENT_CNTRY_CD, 
    UM_PARENT_PHONE1, 
    UM_PARENT_PHONE2, 
    UM_PARENT_PHONE3, 
    UM_PARENT_TYPE, 
    ALUMNI_EVER, 
    HIGHEST_EDUC_LVL, 
    UM_PARENT2_NAME, 
    UM_PARENT2_ADDR1, 
    UM_PARENT2_ADDR2, 
    UM_PARENT2_ADDR3, 
    UM_PARENT2_CITY, 
    UM_PARENT2_STATE, 
    UM_PARENT2_POSTAL, 
    UM_PARENT2_INT_ZIP, 
    UM_PARENT2_COUNTRY, 
    UM_PARENT_CNTRY_C2, 
    UM_PARENT2_PHONE, 
    UM_PARENT2_PHONE1, 
    UM_PARENT2_PHONE2, 
    UM_PARENT2_PHONE3, 
    UM_PARENT2_TYPE, 
    UM_ALUMNI_EVER_P2, 
    UM_HIGH_EDUCLVL_P2, 
    UM_GUARD_NAME, 
    UM_GUARD_ADDR1, 
    UM_GUARD_ADDR2, 
    UM_GUARD_ADDR3, 
    UM_GUARD_CITY, 
    UM_GUARD_STATE, 
    UM_GUARD_POSTAL, 
    UM_GUARD_INT_ZIP, 
    UM_GUARD_COUNTRY, 
    UM_GUARD_CNTRY_CD, 
    UM_GUARD_PHONE, 
    UM_GUARD_PHONE1, 
    UM_GUARD_PHONE2, 
    UM_GUARD_PHONE3, 
    UM_MASS_RESIDENT, 
    UM_ALUMNI_EVER_GUA, 
    UM_HIGH_EDUCLVL_GU, 
    UM_CNTRY_CITIZENSH, 
    UM_BIRTHPLACE, 
    UM_ADM_ENGINEERING, 
    UM_ADM_O_ENGINEER, 
    UM_ADM_E_DIS_NAME, 
    UM_ADM_SCIENCE_DIS, 
    UM_ADM_S_DIS_NAME, 
    UM_COUNSELOR_FNAME, 
    UM_COUNSELOR_LNAME, 
    UM_COUNSELOR_EMAIL, 
    UM_ADM_APP_5YR, 
    UM_ADM_EARLY_D, 
    UM_ADM_REF1_FNAME, 
    UM_ADM_REF1_LNAME, 
    UM_ADM_REF1_MNAME, 
    UM_ADM_REF2_FNAME, 
    UM_ADM_REF2_LNAME, 
    UM_ADM_REF2_MNAME, 
    UM_ADM_REF3_FNAME, 
    UM_ADM_REF3_LNAME, 
    UM_ADM_REF3_MNAME, 
    UM_REF_PRIVATE1, 
    UM_REF_PRIVATE2, 
    UM_REF_PRIVATE3, 
    UM_ADM_BA_MASTER, 
    UM_ADM_UMB_TEACH, 
    UM_ADM_CAR_SWITCH, 
    UM_ADM_UMB_MTEL, 
    UM_ADM_UMB_VISION, 
    UM_ADM_NATL_CERTIF, 
    UM_ADM_CERTIFICATN, 
    UM_ADM_CERT_EXP_DT,
    UM_ADM_CNOW_LOW_IN, 
    UM_ADM_CNOW_FRST_G, 
    UM_ADM_CNOW_NOT_AP, 
    UM_ADM_ARCHELOGY, 
    UM_ADM_SCHL_NAME, 
    UM_ADM_SCHL_LOC, 
    UM_ADM_PREV_BCKGRD, 
    UM_ADM_NBR_MTHS, 
    UM_ADM_INIT_LICNSE, 
    UM_ADM_LICNSE_DESR, 
    UM_ADM_TEACH_SUBJ, 
    UM_ADM_NE_REGIONAL, 
    UM_ADM_NO_VISA, 
    UM_PARENT_EMP_COLL, 
    UM_PARENT_LIVING, 
    UM_PARENT_POSTAL, 
    UM_PARENT_INT_ZIP, 
    UM_PARENT_JOBTITLE, 
    UM_PARENT_GRADSCHL, 
    UM_ADM_SUCCESS_DEG, 
    UM_ADM_CRS_STR,
    UM_ADM_CRS_END,
    UM_ADM_PREV_APPLD, 
    UM_PARENT_EMPLOYER, 
    UM_PARENT_OCCUPTN, 
    UM_PARENT_EMAIL, 
    UM_GUARD_EMAIL, 
    UM_GUARD_EMPLOYER, 
    UM_GUARD_EMP_COLL, 
    UM_GUARD_GRADSCHL, 
    UM_GUARD_OCCUPTN, 
    UM_GUARD_JOBTITLE, 
    UM_GUARD_DEGREE, 
    UM_GUARD_DEGREE_G, 
    UM_PARENT2_DEGREE, 
    UM_PARENT_DEGREE, 
    UM_PARENT_DEGREE_G, 
    UM_ADM_RELIANT_FA, 
    UM_PARENT_CEEB_G, 
    UM_GUARD_CEEB, 
    UM_PARENT2_EMAIL, 
    UM_PARENT2_EMPCOLL, 
    UM_PARENT2_EMPLOYR, 
    UM_PARENT2_GRADSCH, 
    UM_PARENT2_JOBTITL, 
    UM_PARENT2_OCCUPTN, 
    UM_PARENT2_LIVING, 
    UM_ADM_CSCE_TUITN, 
    UM_ADM_CURR_EMP, 
    UM_ADM_CURR_JOB, 
    UM_ADM_LAW_SCHL1, 
    UM_ADM_LAW_SCHL2, 
    UM_ADM_LAW_SCHL3, 
    UM_ADM_LAW_SCHL4, 
    UM_ADM_LAW_3_3_PRG, 
    UM_ADM_LAW_ATTD_B4, 
    UM_ADM_LAW_JT_MBA, 
    UM_ADM_LAW_PRV_APP, 
    UM_ADM_LAW_SRV_ACC, 
    UM_GUARD_CEEB_G, 
    UM_PARENT2_CEEB, 
    UM_PARENT2_CEEB_G, 
    UM_PARENT_CEEB, 
    UM_PARENT_COLLEGE, 
    UM_GUARD_COLLEGE, 
    UM_PARENT2_COLLEGE, 
    UM_PARENT2_DEGRE_G, 
    UM_ADM_RESID_HALL, 
    UM_ADM_HS_DUAL_ENR, 
    UM_ADM_SPORT, 
    UM_ADM_HONORS_PRG, 
    UM_ADM_PREV_CRSE, 
    UM_ADM_MUSIC_INSTR, 
    UM_ADM_EXCER_PRGM, 
    UM_ADM_MUSIC_ENSEM, 
    UM_ADM_BACH_PATHWY, 
--    '-' UM_ADM_UML_DISABLE, 
    UM_ADM_DISABLED UM_ADM_UML_DISABLE,   -- Sep 2017  
    UM_ADM_SCHL_NAME2, 
    UM_ADM_SCHL_LOC2, 
    UM_HIGH_S_OR_GED, 
    UM_COUNSELOR_PHONE, 
    UM_HS_CNTRY1, 
    UM_HS_DEGREE, 
    UM_HS_DEGYR1, 
    UM_HS_FROM_DT1, 
    UM_HS_MONTH1, 
    UM_HS_NAME, 
    UM_HS_STATE1, 
    UM_HS_TO_DT1, 
    UM_HS_TXT, 
    UM_ADM_PREP_CAREER, 
    YEAR, 
    UM_LAW_JT_MBA_OPTN, 
    UM_LAW1, 
    UM_LAW2, 
    UM_LAW3, 
    UM_INTL_MAIL_ZIP, 
    UM_BUSN_PHONE2, 
    UM_COURSE_6_CREDIT, 
    UM_COURSE_6_ID, 
    UM_COURSE_6_TITLE, 
    UM_CELL, 
    UM_DISCIPLINE, 
    UM_FELONY, 
    UM_ADM_RA_TA, 
    UM_ADM_MAJ1_DESCR, 
    UM_ADM_MAJ2_DESCR, 
    UM_CSCE_NURSE_LIC, 
    UM_REF_POSTED_SEQ, 
    UM_ADM_APPL_WAIVER, 
    UM_PAY_WITH_CC, 
    UM_ADM_WAIVER_OPTN, 
    UM_ADM_PAY_BY_CHK, 
    ADM_APPL_COMPLETE, 
    UM_CS_REQUEST_ID, 
    UM_REQUEST_ID, 
    UM_ADM_PAY_STS, 
    UM_CYBERSRC_ERR_CD, 
    UM_CYBERSRC_ERR_D, 
    ADM_APPL_METHOD, 
    AMOUNT, 
    UM_ADM_APP_SIG, 
    UM_ADM_APP_SIG_DT, 
    UM_ADM_APP_NAME, 
    UM_ADM_LOWG_CLS, 
    UM_ADM_DG_P_TEACH, 
    UM_ADM_DG_MAT_COM, 
    UM_ADM_DG_MAT_CONT, 
    UM_ADM_DG_PORT_SM, 
    UM_ADM_DG_PORT_SR, 
    UM_ADM_DG_PH_STDNT, 
    UM_ADM_DG_PH_STDN1, 
    UM_ADM_DG_ACKNOWLG, 
    UM_ADM_DG_ANALYTIC, 
    UM_ADM_DG_BIOCHEM, 
    UM_ADM_DG_COMPU, 
    UM_ADM_DG_ECOM_MTH, 
    UM_ADM_DG_ECOM_YR, 
    UM_ADM_DG_INORGANI, 
    UM_ADM_DG_ORGANIC, 
    UM_ADM_DG_MARINE, 
    UM_ADM_DG_POLYMER, 
    UM_ADM_DG_PHYSICAL, 
    UM_ADM_DG_UNDECID, 
    UM_ADM_G_CHEM_YR, 
    UM_ADM_G_CHEM_CR, 
    UM_ADM_G_CHEM_GR, 
    UM_ADM_A_CHEM_YR, 
    UM_ADM_A_CHEM_CR, 
    UM_ADM_A_CHEM_GR, 
    UM_ADM_AI_CHEM_YR, 
    UM_ADM_AI_CHEM_CR, 
    UM_ADM_AI_CHEM_GR, 
    UM_ADM_OR_CHEM1_YR, 
    UM_ADM_OR_CHEM1_CR, 
    UM_ADM_OR_CHEM1_GR, 
    UM_ADM_OR_CHEM2_YR, 
    UM_ADM_OR_CHEM2_CR, 
    UM_ADM_OR_CHEM2_GR, 
    UM_ADM_PHYSICS_YR, 
    UM_ADM_PHYSICS_CR, 
    UM_ADM_PHYSICS_GR, 
    UM_ADM_PHY_CHM1_YR, 
    UM_ADM_PHY_CHM1_CR, 
    UM_ADM_PHY_CHM1_GR, 
    UM_ADM_PHY_CHM2_YR, 
    UM_ADM_PHY_CHM2_CR, 
    UM_ADM_PHY_CHM2_GR, 
    UM_ADM_CALCULUS_YR, 
    UM_ADM_CALCULUS_CR, 
    UM_ADM_CALCULUS_GR, 
    UM_ADM_CHEM_E1_CRS, 
    UM_ADM_CHEM_E1_YR, 
    UM_ADM_CHEM_E1_CR, 
    UM_ADM_CHEM_E1_GR, 
    UM_ADM_CHEM_E2_CRS, 
    UM_ADM_CHEM_E2_YR, 
    UM_ADM_CHEM_E2_CR, 
    UM_ADM_CHEM_E2_GR, 
    UM_ADM_CHEM_E3_CRS, 
    UM_ADM_CHEM_E3_YR, 
    UM_ADM_CHEM_E3_CR, 
    UM_ADM_CHEM_E3_GR, 
    UM_ADM_CHEM_E4_CRS, 
    UM_ADM_CHEM_E4_YR, 
    UM_ADM_CHEM_E4_CR, 
    UM_ADM_CHEM_E4_GR, 
    UM_ADM_DG_CONCENTR, 
    UM_ADM_DG_ELEM_SCH, 
    UM_ADM_DG_COMM_MTH, 
    UM_ADM_DG_COMM_YR, 
    UM_ADM_DG_CONT_YR, 
    UM_ADM_DG_CONT_MTH, 
    UM_ADM_DG_FOUN_MTH, 
    UM_ADM_DG_FOUN_YR, 
    UM_ADM_DG_MIDL_SCH, 
    UM_ADM_DG_SUBJ_MTH, 
    UM_ADM_DG_SUBJ_YR, 
    UM_ADM_DG_N_APPLIC, 
    UM_ADM_DG_MATP_ACK, 
    UM_ADM_BG_EDC_LIC1, 
    UM_ADM_BG_EDC_LIC2, 
    UM_ADM_BG_EDC_LIC3, 
    UM_ADM_BG_ADMIN_L, 
    UM_ADM_BG_OTH_LIC, 
    UM_ADM_BG_GRD_DGR, 
    UM_ADM_BG_CERT_NP, 
    UM_ADM_BG_ADULT_NP, 
    UM_ADM_BG_PEDI_NP, 
    UM_ADM_BG_FACULTY1, 
    UM_ADM_BG_FACULTY2, 
    UM_ADM_BG_FACULTY3, 
    UM_ADM_BG_CAR_GOAL, 
    UM_ADM_BG_CAR_OTH, 
    UM_ADM_BG_DEGR_IN, 
    UM_ADM_BG_GRAD_SER, 
    UM_ADM_BG_SERV_HRS, 
    UM_ADM_BG_ON_CAMP, 
    UM_ADM_BG_ONLINE, 
    UM_ADM_DU_ONLINE, 
    UM_ADM_DU_CERT, 
    UM_ADM_BG_RESEARCH, 
    UM_LOWU_CLINICAL_1, 
    UM_ADM_BG_ADVISOR1, 
    UM_ADM_BG_ADVISOR2, 
    UM_ADM_BG_ADVISOR3, 
    UM_ADM_BG_SPEC_ED, 
    UM_ADM_UMB_Z_AD_ON, 
    UM_ADM_UMB_Z_F_EMP, 
    UM_ADM_UMB_Z_FAM, 
    UM_ADM_UMB_Z_OTHER, 
    UM_ADM_UMB_Z_PRINT, 
    UM_ADM_UMB_Z_RADIO, 
    UM_ADM_UMB_Z_TEXT, 
    UM_ADM_UMB_Z_TV, 
    UM_ADM_UMB_Z_WEB, 
    UM_ADM_DU_UNIV_EXT, 
    UM_ADM_LG_AREA_INT, 
    UM_ADM_DU_DAY, 
    UM_ADM_DU_NIGHT, 
    UM_ADM_BG_SOC_PHD, 
    UM_ADM_BG_BPE_QUES, 
    UM_ADM_PAY_REDO_DT,
    UM_ADM_PAY_CMPL_DT,
    UM_ADM_BG_DISB_LIC, 
    UM_ADM_BG_MASTERS, 
    UM_ADM_BG_LICENS, 
    UM_ADM_PARTNERSHIP, 
    UM_BOSG_FLEX_MBA, 
    UM_BOSG_PRO_MBA, 
    UM_BOSG_ACCEL_MAST, 
    UM_ADM_LU_DIAGN_DT,
    UM_ADM_LU_MUS_AUD, 
	UM_ADM_HEAR_US_CD, 
	UM_ADM_HEAR_TEXT,
    UM_ENG_AS_SEC_LNG,	
    substr(to_char(trim(UM_ADM_DG_PARTTIME)),1,4000)  UM_ADM_DG_PARTTIME,
    to_number(ORA_ROWSCN) SRC_SCN
from SYSADM.PS_UM_ADM_APP_TMP@SASOURCE S;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging'
 where TABLE_NAME = 'PS_UM_ADM_APP_TMP'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_UM_ADM_APP_TMP';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_UM_ADM_APP_TMP';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_UM_ADM_APP_TMP T 
using (select /*+ full(S) */
    nvl(trim(UM_ADM_USERID),'-') UM_ADM_USERID, 
    nvl(trim(UM_ADM_APP_SEQ),'-') UM_ADM_APP_SEQ, 
    nvl(trim(UM_ADM_REC_NBR),'-') UM_ADM_REC_NBR, 
    nvl(trim(INSTITUTION),'-') INSTITUTION, 
    nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(ADM_APPL_NBR),'-') ADM_APPL_NBR, 
    nvl(trim(UM_ADM_EMAIL),'-') UM_ADM_EMAIL, 
    nvl(trim(EMAILID),'-') EMAILID, 
    nvl(trim(ACAD_CAREER),'-') ACAD_CAREER, 
    nvl(trim(ADM_APPL_CTR),'-') ADM_APPL_CTR, 
    nvl(trim(UM_ADM_SUB_DT),'-') UM_ADM_SUB_DT, 
    nvl(trim(FIRST_NAME),'-') FIRST_NAME, 
    nvl(trim(MIDDLE_NAME),'-') MIDDLE_NAME, 
    nvl(trim(LAST_NAME),'-') LAST_NAME, 
    nvl(trim(UM_SSN),'-') UM_SSN, 
    to_date(to_char(case when BIRTHDATE < '01-JAN-1800' then NULL 
                    else BIRTHDATE end ,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') BIRTHDATE, 
    nvl(trim(SEX),'-') SEX, 
    nvl(trim(UM_US_CITIZEN),'-') UM_US_CITIZEN, 
    nvl(trim(UM_CITIZNSHIP_STAT),'-') UM_CITIZNSHIP_STAT, 
    nvl(trim(UM_BIRTH_CNTRY),'-') UM_BIRTH_CNTRY, 
    nvl(trim(UM_ADM_NOT_MA_RES),'-') UM_ADM_NOT_MA_RES, 
    nvl(trim(VISA_PERMIT_TYPE),'-') VISA_PERMIT_TYPE, 
    nvl(trim(VISA_WRKPMT_STATUS),'-') VISA_WRKPMT_STATUS, 
    nvl(trim(UM_VISA_WRKPMT_NBR),'-') UM_VISA_WRKPMT_NBR, 
    to_date(to_char(case when UM_ADM_VISA_START < '01-JAN-1800' then NULL 
                    else UM_ADM_VISA_START end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') UM_ADM_VISA_START, 
    to_date(to_char(case when UM_ADM_VISA_END < '01-JAN-1800' then NULL 
                    else UM_ADM_VISA_END end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') UM_ADM_VISA_END, 
    nvl(trim(UM_SUFFIX),'-') UM_SUFFIX, 
    nvl(trim(UM_PREFIX),'-') UM_PREFIX, 
    nvl(trim(UM_FORMER_FNAME1),'-') UM_FORMER_FNAME1, 
    nvl(trim(UM_FORMER_MNAME1),'-') UM_FORMER_MNAME1, 
    nvl(trim(UM_FORMER_LNAME1),'-') UM_FORMER_LNAME1, 
    nvl(trim(UM_PREF_FNAME1),'-') UM_PREF_FNAME1, 
    nvl(trim(UM_PREF_MNAME1),'-') UM_PREF_MNAME1, 
    nvl(trim(UM_PREF_LNAME1),'-') UM_PREF_LNAME1, 
    nvl(trim(UM_PERM_ADDRESS1),'-') UM_PERM_ADDRESS1, 
    nvl(trim(UM_PERM_ADDRESS2),'-') UM_PERM_ADDRESS2, 
    nvl(trim(UM_PERM_ADDRESS3),'-') UM_PERM_ADDRESS3, 
    nvl(trim(UM_PERM_CITY),'-') UM_PERM_CITY, 
    nvl(trim(UM_PERM_STATE),'-') UM_PERM_STATE, 
    nvl(trim(UM_PERM_POSTAL),'-') UM_PERM_POSTAL, 
    nvl(trim(COUNTRY_CODE_PERM),'-') COUNTRY_CODE_PERM, 
    nvl(trim(UM_PERM_COUNTRY),'-') UM_PERM_COUNTRY, 
    to_date(to_char(case when UM_VALID_UNTIL < '01-JAN-1800' then NULL 
                    else UM_VALID_UNTIL end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') UM_VALID_UNTIL,
    nvl(trim(UM_MAIL_ADDRESS1),'-') UM_MAIL_ADDRESS1, 
    nvl(trim(UM_MAIL_ADDRESS2),'-') UM_MAIL_ADDRESS2, 
    nvl(trim(UM_MAIL_ADDRESS3),'-') UM_MAIL_ADDRESS3, 
    nvl(trim(UM_MAIL_CITY),'-') UM_MAIL_CITY, 
    nvl(trim(UM_MAIL_STATE),'-') UM_MAIL_STATE, 
    nvl(trim(UM_MAIL_POSTAL),'-') UM_MAIL_POSTAL, 
    nvl(trim(UM_MAIL_COUNTRY),'-') UM_MAIL_COUNTRY, 
    nvl(trim(COUNTRY_CD),'-') COUNTRY_CD, 
    nvl(trim(COUNTRY_CODE),'-') COUNTRY_CODE, 
    nvl(trim(UM_PERM_PHONE),'-') UM_PERM_PHONE, 
    nvl(trim(UM_PERM_PHONE1),'-') UM_PERM_PHONE1, 
    nvl(trim(UM_PERM_PHONE2),'-') UM_PERM_PHONE2, 
    nvl(trim(UM_PERM_PHONE3),'-') UM_PERM_PHONE3, 
    nvl(trim(UM_CELL1),'-') UM_CELL1, 
    nvl(trim(UM_CELL2),'-') UM_CELL2, 
    nvl(trim(UM_CELL3),'-') UM_CELL3, 
    nvl(trim(UM_BUSN_CNTRY_CD),'-') UM_BUSN_CNTRY_CD, 
    nvl(trim(UM_BUSN_PHONE),'-') UM_BUSN_PHONE, 
    nvl(trim(UM_BUSN_PHONE1),'-') UM_BUSN_PHONE1, 
    nvl(trim(UM_BUSN_PHONE3),'-') UM_BUSN_PHONE3, 
    nvl(trim(UM_BUSN_EXTENSION),'-') UM_BUSN_EXTENSION, 
    nvl(trim(UM_ADM_SESSION),'-') UM_ADM_SESSION, 
    nvl(trim(ADMIT_TYPE),'-') ADMIT_TYPE, 
    nvl(trim(UM_ADM_SUBFIELD),'-') UM_ADM_SUBFIELD, 
    nvl(trim(UM_PRG_PLN_SBPLN),'-') UM_PRG_PLN_SBPLN, 
    nvl(trim(UM_PRG_PLN_SBPLN2),'-') UM_PRG_PLN_SBPLN2, 
    nvl(trim(UM_ACAD_PROG),'-') UM_ACAD_PROG, 
    nvl(trim(UM_ACAD_PLAN),'-') UM_ACAD_PLAN, 
    nvl(trim(UM_ACAD_SUB_PLAN),'-') UM_ACAD_SUB_PLAN, 
    nvl(trim(UM_ACAD_PROG1),'-') UM_ACAD_PROG1, 
    nvl(trim(UM_ACAD_PLAN1),'-') UM_ACAD_PLAN1, 
    nvl(trim(UM_ACAD_SUB_PLAN1),'-') UM_ACAD_SUB_PLAN1, 
    nvl(trim(UM_ADM_APP_FIN_AID),'-') UM_ADM_APP_FIN_AID, 
    nvl(trim(UM_GRE_MO),'-') UM_GRE_MO, 
    nvl(trim(UM_GRE_YR),'-') UM_GRE_YR, 
    nvl(trim(UM_GRE_VERB),'-') UM_GRE_VERB, 
    nvl(trim(UM_GRE_QUAN),'-') UM_GRE_QUAN, 
    nvl(trim(UM_GRE_ANAL),'-') UM_GRE_ANAL, 
    nvl(trim(UM_GRE_SUBJ_SC),'-') UM_GRE_SUBJ_SC, 
    nvl(trim(UM_GRE_SUBJ_MO),'-') UM_GRE_SUBJ_MO, 
    nvl(trim(UM_GRE_SUBJ_YR),'-') UM_GRE_SUBJ_YR, 
    nvl(trim(UM_GRE_SUBJECT),'-') UM_GRE_SUBJECT, 
    nvl(trim(UM_GMAT_MO),'-') UM_GMAT_MO, 
    nvl(trim(UM_GMAT_YR),'-') UM_GMAT_YR, 
    nvl(trim(UM_GMAT_SC),'-') UM_GMAT_SC, 
    nvl(trim(UM_TOEFL_MO),'-') UM_TOEFL_MO, 
    nvl(trim(UM_TOEFL_YR),'-') UM_TOEFL_YR, 
    nvl(trim(UM_TOEFL_SC),'-') UM_TOEFL_SC, 
    nvl(UM_TEST1_MO,0) UM_TEST1_MO, 
    nvl(trim(UM_TEST1_NAME),'-') UM_TEST1_NAME, 
    nvl(trim(UM_TEST1_SC),'-') UM_TEST1_SC, 
    nvl(UM_TEST1_YR,0) UM_TEST1_YR, 
    nvl(UM_ADM_APP_GPA_ALL,0) UM_ADM_APP_GPA_ALL, 
    nvl(UM_ADM_APP_GPA_GRD,0) UM_ADM_APP_GPA_GRD, 
    nvl(UM_ADM_APP_GPA_MAJ,0) UM_ADM_APP_GPA_MAJ, 
    nvl(UM_ADM_APP_GPA_TWO,0) UM_ADM_APP_GPA_TWO, 
    nvl(UM_ACT1_MO,0) UM_ACT1_MO, 
    nvl(trim(UM_ACT1_SC),'-') UM_ACT1_SC, 
    nvl(UM_ACT1_YR,0) UM_ACT1_YR, 
    nvl(UM_ACT2_MO,0) UM_ACT2_MO, 
    nvl(trim(UM_ACT2_SC),'-') UM_ACT2_SC, 
    nvl(UM_ACT2_YR,0) UM_ACT2_YR, 
    nvl(UM_ACT3_MO,0) UM_ACT3_MO, 
    nvl(trim(UM_ACT3_SC),'-') UM_ACT3_SC, 
    nvl(UM_ACT3_YR,0) UM_ACT3_YR, 
    nvl(UM_SAT1_MO,0) UM_SAT1_MO, 
    nvl(trim(UM_SAT1_SC),'-') UM_SAT1_SC, 
    nvl(UM_SAT1_YR,0) UM_SAT1_YR, 
    nvl(UM_SAT2_MO,0) UM_SAT2_MO, 
    nvl(trim(UM_SAT2_SC),'-') UM_SAT2_SC, 
    nvl(UM_SAT2_YR,0) UM_SAT2_YR, 
    nvl(UM_SAT3_MO,0) UM_SAT3_MO, 
    nvl(trim(UM_SAT3_SC),'-') UM_SAT3_SC, 
    nvl(UM_SAT3_YR,0) UM_SAT3_YR, 
    nvl(UM_TOEFL1_MO,0) UM_TOEFL1_MO, 
    nvl(trim(UM_TOEFL1_SC),'-') UM_TOEFL1_SC, 
    nvl(UM_TOEFL1_YR,0) UM_TOEFL1_YR, 
    nvl(UM_TOEFL2_MO,0) UM_TOEFL2_MO, 
    nvl(trim(UM_TOEFL2_SC),'-') UM_TOEFL2_SC, 
    nvl(UM_TOEFL2_YR,0) UM_TOEFL2_YR, 
    nvl(UM_TOEFL3_MO,0) UM_TOEFL3_MO, 
    nvl(trim(UM_TOEFL3_SC),'-') UM_TOEFL3_SC, 
    nvl(UM_TOEFL3_YR,0) UM_TOEFL3_YR, 
    nvl(UM_IELTS1_MO,0) UM_IELTS1_MO, 
    nvl(trim(UM_IELTS1_SC),'-') UM_IELTS1_SC, 
    nvl(UM_IELTS1_YR,0) UM_IELTS1_YR, 
    nvl(UM_IELTS2_MO,0) UM_IELTS2_MO, 
    nvl(trim(UM_IELTS2_SC),'-') UM_IELTS2_SC, 
    nvl(UM_IELTS2_YR,0) UM_IELTS2_YR, 
    nvl(UM_IELTS3_MO,0) UM_IELTS3_MO, 
    nvl(trim(UM_IELTS3_SC),'-') UM_IELTS3_SC, 
    nvl(UM_IELTS3_YR,0) UM_IELTS3_YR, 
    nvl(UM_GMAT1_MO,0) UM_GMAT1_MO, 
    nvl(trim(UM_GMAT1_SC),'-') UM_GMAT1_SC, 
    nvl(UM_GMAT1_YR,0) UM_GMAT1_YR, 
    nvl(UM_GMAT2_MO,0) UM_GMAT2_MO, 
    nvl(trim(UM_GMAT2_SC),'-') UM_GMAT2_SC, 
    nvl(UM_GMAT2_YR,0) UM_GMAT2_YR, 
    nvl(UM_GMAT3_MO,0) UM_GMAT3_MO, 
    nvl(trim(UM_GMAT3_SC),'-') UM_GMAT3_SC, 
    nvl(UM_GMAT3_YR,0) UM_GMAT3_YR, 
    nvl(UM_GRE_AW_MO,0) UM_GRE_AW_MO, 
    nvl(trim(UM_GRE_AW_SC),'-') UM_GRE_AW_SC, 
    nvl(UM_GRE_AW_YR,0) UM_GRE_AW_YR, 
    nvl(UM_GRE_Q_MO,0) UM_GRE_Q_MO, 
    nvl(trim(UM_GRE_Q_SC),'-') UM_GRE_Q_SC, 
    nvl(UM_GRE_Q_YR,0) UM_GRE_Q_YR, 
    nvl(UM_GRE_V_MO,0) UM_GRE_V_MO, 
    nvl(trim(UM_GRE_V_SC),'-') UM_GRE_V_SC, 
    nvl(UM_GRE_V_YR,0) UM_GRE_V_YR, 
    nvl(UM_LSAT1_MO,0) UM_LSAT1_MO, 
    nvl(trim(UM_LSAT1_SC),'-') UM_LSAT1_SC, 
    nvl(UM_LSAT1_YR,0) UM_LSAT1_YR, 
    nvl(UM_LSAT2_MO,0) UM_LSAT2_MO, 
    nvl(trim(UM_LSAT2_SC),'-') UM_LSAT2_SC, 
    nvl(UM_LSAT2_YR,0) UM_LSAT2_YR, 
    nvl(UM_LSAT3_MO,0) UM_LSAT3_MO, 
    nvl(trim(UM_LSAT3_SC),'-') UM_LSAT3_SC, 
    nvl(UM_LSAT3_YR,0) UM_LSAT3_YR, 
    nvl(trim(UM_COURSE_1_CREDIT),'-') UM_COURSE_1_CREDIT, 
    nvl(trim(UM_COURSE_1_ID),'-') UM_COURSE_1_ID, 
    nvl(trim(UM_COURSE_1_TITLE),'-') UM_COURSE_1_TITLE, 
    nvl(trim(UM_COURSE_2_CREDIT),'-') UM_COURSE_2_CREDIT, 
    nvl(trim(UM_COURSE_2_ID),'-') UM_COURSE_2_ID, 
    nvl(trim(UM_COURSE_2_TITLE),'-') UM_COURSE_2_TITLE, 
    nvl(trim(UM_COURSE_3_CREDIT),'-') UM_COURSE_3_CREDIT, 
    nvl(trim(UM_COURSE_3_ID),'-') UM_COURSE_3_ID, 
    nvl(trim(UM_COURSE_3_TITLE),'-') UM_COURSE_3_TITLE, 
    nvl(trim(UM_COURSE_4_CREDIT),'-') UM_COURSE_4_CREDIT, 
    nvl(trim(UM_COURSE_4_ID),'-') UM_COURSE_4_ID, 
    nvl(trim(UM_COURSE_4_TITLE),'-') UM_COURSE_4_TITLE, 
    nvl(trim(UM_COURSE_5_CREDIT),'-') UM_COURSE_5_CREDIT, 
    nvl(trim(UM_COURSE_5_ID),'-') UM_COURSE_5_ID, 
    nvl(trim(UM_COURSE_5_TITLE),'-') UM_COURSE_5_TITLE, 
    nvl(trim(MILITARY_STATUS),'-') MILITARY_STATUS, 
    nvl(trim(UM_ETHNIC),'-') UM_ETHNIC, 
    nvl(trim(UM_EMPLOY_STATE),'-') UM_EMPLOY_STATE, 
    nvl(trim(UM_LAW4),'-') UM_LAW4, 
    nvl(trim(UM_ADM_FAC_FNAME),'-') UM_ADM_FAC_FNAME, 
    nvl(trim(UM_ADM_FAC_LNAME),'-') UM_ADM_FAC_LNAME, 
    nvl(trim(UM_LAW5),'-') UM_LAW5, 
    nvl(UM_ADM_APP_MO_FR2,0) UM_ADM_APP_MO_FR2, 
    nvl(UM_ADM_APP_MO_FR3,0) UM_ADM_APP_MO_FR3, 
    nvl(UM_ADM_APP_MO_FR4,0) UM_ADM_APP_MO_FR4, 
    nvl(UM_ADM_APP_MO_TO2,0) UM_ADM_APP_MO_TO2, 
    nvl(UM_ADM_APP_MO_TO3,0) UM_ADM_APP_MO_TO3, 
    nvl(UM_ADM_APP_MO_TO4,0) UM_ADM_APP_MO_TO4, 
    nvl(trim(UM_HS_ID),'-') UM_HS_ID, 
    nvl(trim(UM_ACAD_LOAD_APPR),'-') UM_ACAD_LOAD_APPR, 
    nvl(trim(UM_ADM_ONLINE_DEGR),'-') UM_ADM_ONLINE_DEGR, 
    nvl(trim(UM_ADM_MUSIC_INST1),'-') UM_ADM_MUSIC_INST1, 
    nvl(trim(UM_PRV1_ID),'-') UM_PRV1_ID, 
    nvl(trim(UM_PRV2_ID),'-') UM_PRV2_ID, 
    nvl(trim(UM_PRV3_ID),'-') UM_PRV3_ID, 
    nvl(trim(UM_PRV4_ID),'-') UM_PRV4_ID, 
    nvl(trim(UM_PHONE_TYPE),'-') UM_PHONE_TYPE, 
    nvl(trim(UM_INTL_ZIP),'-') UM_INTL_ZIP, 
    nvl(UM_ADM_APP_MO_FR1,0) UM_ADM_APP_MO_FR1, 
    nvl(UM_ADM_APP_MO_TO1,0) UM_ADM_APP_MO_TO1, 
    nvl(trim(UM_ADM_APP_TITLE1),'-') UM_ADM_APP_TITLE1, 
    nvl(UM_ADM_APP_YR_FR1,0) UM_ADM_APP_YR_FR1, 
    nvl(UM_ADM_APP_YR_TO1,0) UM_ADM_APP_YR_TO1, 
    nvl(trim(UM_ADM_APP_POS1),'-') UM_ADM_APP_POS1, 
    nvl(trim(UM_ADM_APP_POS2),'-') UM_ADM_APP_POS2, 
    nvl(trim(UM_ADM_APP_POS3),'-') UM_ADM_APP_POS3, 
    nvl(trim(UM_ADM_APP_ADDR1),'-') UM_ADM_APP_ADDR1, 
    nvl(trim(UM_ADM_APP_ADDR2),'-') UM_ADM_APP_ADDR2, 
    nvl(trim(UM_ADM_APP_ADDR3),'-') UM_ADM_APP_ADDR3, 
    nvl(trim(UM_ADM_APP_NAM1),'-') UM_ADM_APP_NAM1, 
    nvl(trim(UM_ADM_APP_NAM2),'-') UM_ADM_APP_NAM2, 
    nvl(trim(UM_ADM_APP_NAM3),'-') UM_ADM_APP_NAM3, 
    nvl(trim(UM_ADM_APP_CNTRY1),'-') UM_ADM_APP_CNTRY1, 
    nvl(trim(UM_ADM_APP_CNTRY2),'-') UM_ADM_APP_CNTRY2, 
    nvl(trim(UM_ADM_APP_CNTRY3),'-') UM_ADM_APP_CNTRY3, 
    nvl(trim(UM_ADM_APP_CNTRY4),'-') UM_ADM_APP_CNTRY4, 
    nvl(trim(UM_ADM_APP_STATE1),'-') UM_ADM_APP_STATE1, 
    nvl(trim(UM_ADM_APP_STATE2),'-') UM_ADM_APP_STATE2, 
    nvl(trim(UM_ADM_APP_STATE3),'-') UM_ADM_APP_STATE3, 
    nvl(trim(UM_ADM_APP_STATE4),'-') UM_ADM_APP_STATE4, 
    nvl(trim(UM_ADM_APP_PR_TRM),'-') UM_ADM_APP_PR_TRM, 
    nvl(trim(UM_ADM_APP_EN_TRM),'-') UM_ADM_APP_EN_TRM, 
    nvl(trim(UM_ADM_APP_PRIOR),'-') UM_ADM_APP_PRIOR, 
    nvl(trim(UM_ADM_APP_ENRL),'-') UM_ADM_APP_ENRL, 
    nvl(trim(UM_ADM_APP_CAR1),'-') UM_ADM_APP_CAR1, 
    nvl(trim(UM_ADM_APP_CAR2),'-') UM_ADM_APP_CAR2, 
    nvl(trim(UM_ADM_APP_CAR3),'-') UM_ADM_APP_CAR3, 
    nvl(trim(UM_ADM_APP_CAR4),'-') UM_ADM_APP_CAR4, 
    nvl(trim(UM_ADM_APP_DEG1),'-') UM_ADM_APP_DEG1, 
    nvl(trim(UM_ADM_APP_DEG2),'-') UM_ADM_APP_DEG2, 
    nvl(trim(UM_ADM_APP_DEG3),'-') UM_ADM_APP_DEG3, 
    nvl(trim(UM_ADM_APP_DEG4),'-') UM_ADM_APP_DEG4, 
    nvl(UM_ADM_APP_DEGYR1,0) UM_ADM_APP_DEGYR1, 
    nvl(UM_ADM_APP_DEGYR2,0) UM_ADM_APP_DEGYR2, 
    nvl(UM_ADM_APP_DEGYR3,0) UM_ADM_APP_DEGYR3, 
    nvl(UM_ADM_APP_DEGYR4,0) UM_ADM_APP_DEGYR4, 
    nvl(UM_ADM_APP_FROM1,0) UM_ADM_APP_FROM1, 
    nvl(UM_ADM_APP_FROM2,0) UM_ADM_APP_FROM2, 
    nvl(UM_ADM_APP_FROM3,0) UM_ADM_APP_FROM3, 
    nvl(UM_ADM_APP_FROM4,0) UM_ADM_APP_FROM4, 
    nvl(UM_ADM_APP_MONTH1,0) UM_ADM_APP_MONTH1, 
    nvl(UM_ADM_APP_MONTH2,0) UM_ADM_APP_MONTH2, 
    nvl(UM_ADM_APP_MONTH3,0) UM_ADM_APP_MONTH3, 
    nvl(UM_ADM_APP_MONTH4,0) UM_ADM_APP_MONTH4, 
    nvl(trim(UM_ADM_APP_PRV1),'-') UM_ADM_APP_PRV1, 
    nvl(trim(UM_ADM_APP_PRV2),'-') UM_ADM_APP_PRV2, 
    nvl(trim(UM_ADM_APP_PRV3),'-') UM_ADM_APP_PRV3, 
    nvl(trim(UM_ADM_APP_PRV4),'-') UM_ADM_APP_PRV4, 
    nvl(UM_ADM_APP_TO1,0) UM_ADM_APP_TO1, 
    nvl(UM_ADM_APP_TO2,0) UM_ADM_APP_TO2, 
    nvl(UM_ADM_APP_TO3,0) UM_ADM_APP_TO3, 
    nvl(UM_ADM_APP_TO4,0) UM_ADM_APP_TO4, 
    nvl(trim(UM_ADM_ASSIST),'-') UM_ADM_ASSIST, 
    nvl(trim(UM_ADM_FELLOW),'-') UM_ADM_FELLOW, 
    nvl(trim(MBA_ABITUS),'-') MBA_ABITUS, 
    nvl(trim(MBA_CURRENT_STUDNT),'-') MBA_CURRENT_STUDNT, 
    nvl(trim(MBA_DAY),'-') MBA_DAY, 
    nvl(trim(MBA_EXCLSVE_ONLINE),'-') MBA_EXCLSVE_ONLINE, 
    nvl(trim(MBA_WRK_PROFESNLS),'-') MBA_WRK_PROFESNLS, 
    nvl(trim(CONTACT_NAME),'-') CONTACT_NAME, 
    nvl(trim(UM_EMERG_COUNTRY),'-') UM_EMERG_COUNTRY, 
    nvl(trim(UM_ADM_REL_TYPE),'-') UM_ADM_REL_TYPE, 
    nvl(trim(UM_EMERG_CNTRY_CD),'-') UM_EMERG_CNTRY_CD, 
    nvl(trim(UM_EMERG_PHONE),'-') UM_EMERG_PHONE, 
    nvl(trim(CONTACT_PHONE),'-') CONTACT_PHONE, 
    nvl(trim(CONTACT_PHONE_EXT),'-') CONTACT_PHONE_EXT, 
    nvl(trim(UM_PARENT_NAME),'-') UM_PARENT_NAME, 
    nvl(trim(UM_PARENT_ADDR1),'-') UM_PARENT_ADDR1, 
    nvl(trim(UM_PARENT_ADDR2),'-') UM_PARENT_ADDR2, 
    nvl(trim(UM_PARENT_ADDR3),'-') UM_PARENT_ADDR3, 
    nvl(trim(UM_PARENT_CITY),'-') UM_PARENT_CITY, 
    nvl(trim(UM_PARENT_STATE),'-') UM_PARENT_STATE, 
    nvl(trim(UM_PARENT_COUNTRY),'-') UM_PARENT_COUNTRY, 
    nvl(trim(UM_PARENT_PHONE),'-') UM_PARENT_PHONE, 
    nvl(trim(UM_PARENT_CNTRY_CD),'-') UM_PARENT_CNTRY_CD, 
    nvl(trim(UM_PARENT_PHONE1),'-') UM_PARENT_PHONE1, 
    nvl(trim(UM_PARENT_PHONE2),'-') UM_PARENT_PHONE2, 
    nvl(trim(UM_PARENT_PHONE3),'-') UM_PARENT_PHONE3, 
    nvl(trim(UM_PARENT_TYPE),'-') UM_PARENT_TYPE, 
    nvl(trim(ALUMNI_EVER),'-') ALUMNI_EVER, 
    nvl(trim(HIGHEST_EDUC_LVL),'-') HIGHEST_EDUC_LVL, 
    nvl(trim(UM_PARENT2_NAME),'-') UM_PARENT2_NAME, 
    nvl(trim(UM_PARENT2_ADDR1),'-') UM_PARENT2_ADDR1, 
    nvl(trim(UM_PARENT2_ADDR2),'-') UM_PARENT2_ADDR2, 
    nvl(trim(UM_PARENT2_ADDR3),'-') UM_PARENT2_ADDR3, 
    nvl(trim(UM_PARENT2_CITY),'-') UM_PARENT2_CITY, 
    nvl(trim(UM_PARENT2_STATE),'-') UM_PARENT2_STATE, 
    nvl(trim(UM_PARENT2_POSTAL),'-') UM_PARENT2_POSTAL, 
    nvl(trim(UM_PARENT2_INT_ZIP),'-') UM_PARENT2_INT_ZIP, 
    nvl(trim(UM_PARENT2_COUNTRY),'-') UM_PARENT2_COUNTRY, 
    nvl(trim(UM_PARENT_CNTRY_C2),'-') UM_PARENT_CNTRY_C2, 
    nvl(trim(UM_PARENT2_PHONE),'-') UM_PARENT2_PHONE, 
    nvl(trim(UM_PARENT2_PHONE1),'-') UM_PARENT2_PHONE1, 
    nvl(trim(UM_PARENT2_PHONE2),'-') UM_PARENT2_PHONE2, 
    nvl(trim(UM_PARENT2_PHONE3),'-') UM_PARENT2_PHONE3, 
    nvl(trim(UM_PARENT2_TYPE),'-') UM_PARENT2_TYPE, 
    nvl(trim(UM_ALUMNI_EVER_P2),'-') UM_ALUMNI_EVER_P2, 
    nvl(trim(UM_HIGH_EDUCLVL_P2),'-') UM_HIGH_EDUCLVL_P2, 
    nvl(trim(UM_GUARD_NAME),'-') UM_GUARD_NAME, 
    nvl(trim(UM_GUARD_ADDR1),'-') UM_GUARD_ADDR1, 
    nvl(trim(UM_GUARD_ADDR2),'-') UM_GUARD_ADDR2, 
    nvl(trim(UM_GUARD_ADDR3),'-') UM_GUARD_ADDR3, 
    nvl(trim(UM_GUARD_CITY),'-') UM_GUARD_CITY, 
    nvl(trim(UM_GUARD_STATE),'-') UM_GUARD_STATE, 
    nvl(trim(UM_GUARD_POSTAL),'-') UM_GUARD_POSTAL, 
    nvl(trim(UM_GUARD_INT_ZIP),'-') UM_GUARD_INT_ZIP, 
    nvl(trim(UM_GUARD_COUNTRY),'-') UM_GUARD_COUNTRY, 
    nvl(trim(UM_GUARD_CNTRY_CD),'-') UM_GUARD_CNTRY_CD, 
    nvl(trim(UM_GUARD_PHONE),'-') UM_GUARD_PHONE, 
    nvl(trim(UM_GUARD_PHONE1),'-') UM_GUARD_PHONE1, 
    nvl(trim(UM_GUARD_PHONE2),'-') UM_GUARD_PHONE2, 
    nvl(trim(UM_GUARD_PHONE3),'-') UM_GUARD_PHONE3, 
    nvl(trim(UM_MASS_RESIDENT),'-') UM_MASS_RESIDENT, 
    nvl(trim(UM_ALUMNI_EVER_GUA),'-') UM_ALUMNI_EVER_GUA, 
    nvl(trim(UM_HIGH_EDUCLVL_GU),'-') UM_HIGH_EDUCLVL_GU, 
    nvl(trim(UM_CNTRY_CITIZENSH),'-') UM_CNTRY_CITIZENSH, 
    nvl(trim(UM_BIRTHPLACE),'-') UM_BIRTHPLACE, 
    nvl(trim(UM_ADM_ENGINEERING),'-') UM_ADM_ENGINEERING, 
    nvl(trim(UM_ADM_O_ENGINEER),'-') UM_ADM_O_ENGINEER, 
    nvl(trim(UM_ADM_E_DIS_NAME),'-') UM_ADM_E_DIS_NAME, 
    nvl(trim(UM_ADM_SCIENCE_DIS),'-') UM_ADM_SCIENCE_DIS, 
    nvl(trim(UM_ADM_S_DIS_NAME),'-') UM_ADM_S_DIS_NAME, 
    nvl(trim(UM_COUNSELOR_FNAME),'-') UM_COUNSELOR_FNAME, 
    nvl(trim(UM_COUNSELOR_LNAME),'-') UM_COUNSELOR_LNAME, 
    nvl(trim(UM_COUNSELOR_EMAIL),'-') UM_COUNSELOR_EMAIL, 
    nvl(trim(UM_ADM_APP_5YR),'-') UM_ADM_APP_5YR, 
    nvl(trim(UM_ADM_EARLY_D),'-') UM_ADM_EARLY_D, 
    nvl(trim(UM_ADM_REF1_FNAME),'-') UM_ADM_REF1_FNAME, 
    nvl(trim(UM_ADM_REF1_LNAME),'-') UM_ADM_REF1_LNAME, 
    nvl(trim(UM_ADM_REF1_MNAME),'-') UM_ADM_REF1_MNAME, 
    nvl(trim(UM_ADM_REF2_FNAME),'-') UM_ADM_REF2_FNAME, 
    nvl(trim(UM_ADM_REF2_LNAME),'-') UM_ADM_REF2_LNAME, 
    nvl(trim(UM_ADM_REF2_MNAME),'-') UM_ADM_REF2_MNAME, 
    nvl(trim(UM_ADM_REF3_FNAME),'-') UM_ADM_REF3_FNAME, 
    nvl(trim(UM_ADM_REF3_LNAME),'-') UM_ADM_REF3_LNAME, 
    nvl(trim(UM_ADM_REF3_MNAME),'-') UM_ADM_REF3_MNAME, 
    nvl(trim(UM_REF_PRIVATE1),'-') UM_REF_PRIVATE1, 
    nvl(trim(UM_REF_PRIVATE2),'-') UM_REF_PRIVATE2, 
    nvl(trim(UM_REF_PRIVATE3),'-') UM_REF_PRIVATE3, 
    nvl(trim(UM_ADM_BA_MASTER),'-') UM_ADM_BA_MASTER, 
    nvl(trim(UM_ADM_UMB_TEACH),'-') UM_ADM_UMB_TEACH, 
    nvl(trim(UM_ADM_CAR_SWITCH),'-') UM_ADM_CAR_SWITCH, 
    nvl(trim(UM_ADM_UMB_MTEL),'-') UM_ADM_UMB_MTEL, 
    nvl(trim(UM_ADM_UMB_VISION),'-') UM_ADM_UMB_VISION, 
    nvl(trim(UM_ADM_NATL_CERTIF),'-') UM_ADM_NATL_CERTIF, 
    nvl(trim(UM_ADM_CERTIFICATN),'-') UM_ADM_CERTIFICATN, 
    to_date(to_char(case when UM_ADM_CERT_EXP_DT < '01-JAN-1800' then NULL 
                    else UM_ADM_CERT_EXP_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') UM_ADM_CERT_EXP_DT,
    nvl(trim(UM_ADM_CNOW_LOW_IN),'-') UM_ADM_CNOW_LOW_IN, 
    nvl(trim(UM_ADM_CNOW_FRST_G),'-') UM_ADM_CNOW_FRST_G, 
    nvl(trim(UM_ADM_CNOW_NOT_AP),'-') UM_ADM_CNOW_NOT_AP, 
    nvl(trim(UM_ADM_ARCHELOGY),'-') UM_ADM_ARCHELOGY, 
    nvl(trim(UM_ADM_SCHL_NAME),'-') UM_ADM_SCHL_NAME, 
    nvl(trim(UM_ADM_SCHL_LOC),'-') UM_ADM_SCHL_LOC, 
    nvl(trim(UM_ADM_PREV_BCKGRD),'-') UM_ADM_PREV_BCKGRD, 
    nvl(UM_ADM_NBR_MTHS,0) UM_ADM_NBR_MTHS, 
    nvl(trim(UM_ADM_INIT_LICNSE),'-') UM_ADM_INIT_LICNSE, 
    nvl(trim(UM_ADM_LICNSE_DESR),'-') UM_ADM_LICNSE_DESR, 
    nvl(trim(UM_ADM_TEACH_SUBJ),'-') UM_ADM_TEACH_SUBJ, 
    nvl(trim(UM_ADM_NE_REGIONAL),'-') UM_ADM_NE_REGIONAL, 
    nvl(trim(UM_ADM_NO_VISA),'-') UM_ADM_NO_VISA, 
    nvl(trim(UM_PARENT_EMP_COLL),'-') UM_PARENT_EMP_COLL, 
    nvl(trim(UM_PARENT_LIVING),'-') UM_PARENT_LIVING, 
    nvl(trim(UM_PARENT_POSTAL),'-') UM_PARENT_POSTAL, 
    nvl(trim(UM_PARENT_INT_ZIP),'-') UM_PARENT_INT_ZIP, 
    nvl(trim(UM_PARENT_JOBTITLE),'-') UM_PARENT_JOBTITLE, 
    nvl(trim(UM_PARENT_GRADSCHL),'-') UM_PARENT_GRADSCHL, 
    nvl(trim(UM_ADM_SUCCESS_DEG),'-') UM_ADM_SUCCESS_DEG, 
    to_date(to_char(case when UM_ADM_CRS_STR < '01-JAN-1800' then NULL 
                    else UM_ADM_CRS_STR end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') UM_ADM_CRS_STR,
    to_date(to_char(case when UM_ADM_CRS_END < '01-JAN-1800' then NULL 
                    else UM_ADM_CRS_END end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') UM_ADM_CRS_END,
    nvl(trim(UM_ADM_PREV_APPLD),'-') UM_ADM_PREV_APPLD, 
    nvl(trim(UM_PARENT_EMPLOYER),'-') UM_PARENT_EMPLOYER, 
    nvl(trim(UM_PARENT_OCCUPTN),'-') UM_PARENT_OCCUPTN, 
    nvl(trim(UM_PARENT_EMAIL),'-') UM_PARENT_EMAIL, 
    nvl(trim(UM_GUARD_EMAIL),'-') UM_GUARD_EMAIL, 
    nvl(trim(UM_GUARD_EMPLOYER),'-') UM_GUARD_EMPLOYER, 
    nvl(trim(UM_GUARD_EMP_COLL),'-') UM_GUARD_EMP_COLL, 
    nvl(trim(UM_GUARD_GRADSCHL),'-') UM_GUARD_GRADSCHL, 
    nvl(trim(UM_GUARD_OCCUPTN),'-') UM_GUARD_OCCUPTN, 
    nvl(trim(UM_GUARD_JOBTITLE),'-') UM_GUARD_JOBTITLE, 
    nvl(trim(UM_GUARD_DEGREE),'-') UM_GUARD_DEGREE, 
    nvl(trim(UM_GUARD_DEGREE_G),'-') UM_GUARD_DEGREE_G, 
    nvl(trim(UM_PARENT2_DEGREE),'-') UM_PARENT2_DEGREE, 
    nvl(trim(UM_PARENT_DEGREE),'-') UM_PARENT_DEGREE, 
    nvl(trim(UM_PARENT_DEGREE_G),'-') UM_PARENT_DEGREE_G, 
    nvl(trim(UM_ADM_RELIANT_FA),'-') UM_ADM_RELIANT_FA, 
    nvl(trim(UM_PARENT_CEEB_G),'-') UM_PARENT_CEEB_G, 
    nvl(trim(UM_GUARD_CEEB),'-') UM_GUARD_CEEB, 
    nvl(trim(UM_PARENT2_EMAIL),'-') UM_PARENT2_EMAIL, 
    nvl(trim(UM_PARENT2_EMPCOLL),'-') UM_PARENT2_EMPCOLL, 
    nvl(trim(UM_PARENT2_EMPLOYR),'-') UM_PARENT2_EMPLOYR, 
    nvl(trim(UM_PARENT2_GRADSCH),'-') UM_PARENT2_GRADSCH, 
    nvl(trim(UM_PARENT2_JOBTITL),'-') UM_PARENT2_JOBTITL, 
    nvl(trim(UM_PARENT2_OCCUPTN),'-') UM_PARENT2_OCCUPTN, 
    nvl(trim(UM_PARENT2_LIVING),'-') UM_PARENT2_LIVING, 
    nvl(trim(UM_ADM_CSCE_TUITN),'-') UM_ADM_CSCE_TUITN, 
    nvl(trim(UM_ADM_CURR_EMP),'-') UM_ADM_CURR_EMP, 
    nvl(trim(UM_ADM_CURR_JOB),'-') UM_ADM_CURR_JOB, 
    nvl(trim(UM_ADM_LAW_SCHL1),'-') UM_ADM_LAW_SCHL1, 
    nvl(trim(UM_ADM_LAW_SCHL2),'-') UM_ADM_LAW_SCHL2, 
    nvl(trim(UM_ADM_LAW_SCHL3),'-') UM_ADM_LAW_SCHL3, 
    nvl(trim(UM_ADM_LAW_SCHL4),'-') UM_ADM_LAW_SCHL4, 
    nvl(trim(UM_ADM_LAW_3_3_PRG),'-') UM_ADM_LAW_3_3_PRG, 
    nvl(trim(UM_ADM_LAW_ATTD_B4),'-') UM_ADM_LAW_ATTD_B4, 
    nvl(trim(UM_ADM_LAW_JT_MBA),'-') UM_ADM_LAW_JT_MBA, 
    nvl(trim(UM_ADM_LAW_PRV_APP),'-') UM_ADM_LAW_PRV_APP, 
    nvl(trim(UM_ADM_LAW_SRV_ACC),'-') UM_ADM_LAW_SRV_ACC, 
    nvl(trim(UM_GUARD_CEEB_G),'-') UM_GUARD_CEEB_G, 
    nvl(trim(UM_PARENT2_CEEB),'-') UM_PARENT2_CEEB, 
    nvl(trim(UM_PARENT2_CEEB_G),'-') UM_PARENT2_CEEB_G, 
    nvl(trim(UM_PARENT_CEEB),'-') UM_PARENT_CEEB, 
    nvl(trim(UM_PARENT_COLLEGE),'-') UM_PARENT_COLLEGE, 
    nvl(trim(UM_GUARD_COLLEGE),'-') UM_GUARD_COLLEGE, 
    nvl(trim(UM_PARENT2_COLLEGE),'-') UM_PARENT2_COLLEGE, 
    nvl(trim(UM_PARENT2_DEGRE_G),'-') UM_PARENT2_DEGRE_G, 
    nvl(trim(UM_ADM_RESID_HALL),'-') UM_ADM_RESID_HALL, 
    nvl(trim(UM_ADM_HS_DUAL_ENR),'-') UM_ADM_HS_DUAL_ENR, 
    nvl(trim(UM_ADM_SPORT),'-') UM_ADM_SPORT, 
    nvl(trim(UM_ADM_HONORS_PRG),'-') UM_ADM_HONORS_PRG, 
    nvl(trim(UM_ADM_PREV_CRSE),'-') UM_ADM_PREV_CRSE, 
    nvl(trim(UM_ADM_MUSIC_INSTR),'-') UM_ADM_MUSIC_INSTR, 
    nvl(trim(UM_ADM_EXCER_PRGM),'-') UM_ADM_EXCER_PRGM, 
    nvl(trim(UM_ADM_MUSIC_ENSEM),'-') UM_ADM_MUSIC_ENSEM, 
    nvl(trim(UM_ADM_BACH_PATHWY),'-') UM_ADM_BACH_PATHWY, 
    nvl(trim(UM_ADM_UML_DISABLE),'-') UM_ADM_UML_DISABLE, 
    nvl(trim(UM_ADM_SCHL_NAME2),'-') UM_ADM_SCHL_NAME2, 
    nvl(trim(UM_ADM_SCHL_LOC2),'-') UM_ADM_SCHL_LOC2, 
    nvl(trim(UM_HIGH_S_OR_GED),'-') UM_HIGH_S_OR_GED, 
    nvl(trim(UM_COUNSELOR_PHONE),'-') UM_COUNSELOR_PHONE, 
    nvl(trim(UM_HS_CNTRY1),'-') UM_HS_CNTRY1, 
    nvl(trim(UM_HS_DEGREE),'-') UM_HS_DEGREE, 
    nvl(UM_HS_DEGYR1,0) UM_HS_DEGYR1, 
    nvl(UM_HS_FROM_DT1,0) UM_HS_FROM_DT1, 
    nvl(UM_HS_MONTH1,0) UM_HS_MONTH1, 
    nvl(trim(UM_HS_NAME),'-') UM_HS_NAME, 
    nvl(trim(UM_HS_STATE1),'-') UM_HS_STATE1, 
    nvl(UM_HS_TO_DT1,0) UM_HS_TO_DT1, 
    nvl(trim(UM_HS_TXT),'-') UM_HS_TXT, 
    nvl(trim(UM_ADM_PREP_CAREER),'-') UM_ADM_PREP_CAREER, 
    nvl(trim(YEAR),'-') YEAR, 
    nvl(trim(UM_LAW_JT_MBA_OPTN),'-') UM_LAW_JT_MBA_OPTN, 
    nvl(trim(UM_LAW1),'-') UM_LAW1, 
    nvl(trim(UM_LAW2),'-') UM_LAW2, 
    nvl(trim(UM_LAW3),'-') UM_LAW3, 
    nvl(trim(UM_INTL_MAIL_ZIP),'-') UM_INTL_MAIL_ZIP, 
    nvl(trim(UM_BUSN_PHONE2),'-') UM_BUSN_PHONE2, 
    nvl(trim(UM_COURSE_6_CREDIT),'-') UM_COURSE_6_CREDIT, 
    nvl(trim(UM_COURSE_6_ID),'-') UM_COURSE_6_ID, 
    nvl(trim(UM_COURSE_6_TITLE),'-') UM_COURSE_6_TITLE, 
    nvl(trim(UM_CELL),'-') UM_CELL, 
    nvl(trim(UM_DISCIPLINE),'-') UM_DISCIPLINE, 
    nvl(trim(UM_FELONY),'-') UM_FELONY, 
    nvl(trim(UM_ADM_RA_TA),'-') UM_ADM_RA_TA, 
    nvl(trim(UM_ADM_MAJ1_DESCR),'-') UM_ADM_MAJ1_DESCR, 
    nvl(trim(UM_ADM_MAJ2_DESCR),'-') UM_ADM_MAJ2_DESCR, 
    nvl(trim(UM_CSCE_NURSE_LIC),'-') UM_CSCE_NURSE_LIC, 
    nvl(UM_REF_POSTED_SEQ,0) UM_REF_POSTED_SEQ, 
    nvl(trim(UM_ADM_APPL_WAIVER),'-') UM_ADM_APPL_WAIVER, 
    nvl(trim(UM_PAY_WITH_CC),'-') UM_PAY_WITH_CC, 
    nvl(trim(UM_ADM_WAIVER_OPTN),'-') UM_ADM_WAIVER_OPTN, 
    nvl(trim(UM_ADM_PAY_BY_CHK),'-') UM_ADM_PAY_BY_CHK, 
    nvl(trim(ADM_APPL_COMPLETE),'-') ADM_APPL_COMPLETE, 
    nvl(trim(UM_CS_REQUEST_ID),'-') UM_CS_REQUEST_ID, 
    nvl(trim(UM_REQUEST_ID),'-') UM_REQUEST_ID, 
    nvl(trim(UM_ADM_PAY_STS),'-') UM_ADM_PAY_STS, 
    nvl(trim(UM_CYBERSRC_ERR_CD),'-') UM_CYBERSRC_ERR_CD, 
    nvl(trim(UM_CYBERSRC_ERR_D),'-') UM_CYBERSRC_ERR_D, 
    nvl(trim(ADM_APPL_METHOD),'-') ADM_APPL_METHOD, 
    nvl(AMOUNT,0) AMOUNT, 
    nvl(trim(UM_ADM_APP_SIG),'-') UM_ADM_APP_SIG, 
    nvl(trim(UM_ADM_APP_SIG_DT),'-') UM_ADM_APP_SIG_DT, 
    nvl(trim(UM_ADM_APP_NAME),'-') UM_ADM_APP_NAME, 
    nvl(trim(UM_ADM_LOWG_CLS),'-') UM_ADM_LOWG_CLS, 
    nvl(trim(UM_ADM_DG_P_TEACH),'-') UM_ADM_DG_P_TEACH, 
    nvl(trim(UM_ADM_DG_MAT_COM),'-') UM_ADM_DG_MAT_COM, 
    nvl(trim(UM_ADM_DG_MAT_CONT),'-') UM_ADM_DG_MAT_CONT, 
    nvl(trim(UM_ADM_DG_PORT_SM),'-') UM_ADM_DG_PORT_SM, 
    nvl(trim(UM_ADM_DG_PORT_SR),'-') UM_ADM_DG_PORT_SR, 
    nvl(trim(UM_ADM_DG_PH_STDNT),'-') UM_ADM_DG_PH_STDNT, 
    nvl(trim(UM_ADM_DG_PH_STDN1),'-') UM_ADM_DG_PH_STDN1, 
    nvl(trim(UM_ADM_DG_ACKNOWLG),'-') UM_ADM_DG_ACKNOWLG, 
    nvl(trim(UM_ADM_DG_ANALYTIC),'-') UM_ADM_DG_ANALYTIC, 
    nvl(trim(UM_ADM_DG_BIOCHEM),'-') UM_ADM_DG_BIOCHEM, 
    nvl(trim(UM_ADM_DG_COMPU),'-') UM_ADM_DG_COMPU, 
    nvl(UM_ADM_DG_ECOM_MTH,0) UM_ADM_DG_ECOM_MTH, 
    nvl(UM_ADM_DG_ECOM_YR,0) UM_ADM_DG_ECOM_YR, 
    nvl(trim(UM_ADM_DG_INORGANI),'-') UM_ADM_DG_INORGANI, 
    nvl(trim(UM_ADM_DG_ORGANIC),'-') UM_ADM_DG_ORGANIC, 
    nvl(trim(UM_ADM_DG_MARINE),'-') UM_ADM_DG_MARINE, 
    nvl(trim(UM_ADM_DG_POLYMER),'-') UM_ADM_DG_POLYMER, 
    nvl(trim(UM_ADM_DG_PHYSICAL),'-') UM_ADM_DG_PHYSICAL, 
    nvl(trim(UM_ADM_DG_UNDECID),'-') UM_ADM_DG_UNDECID, 
    nvl(UM_ADM_G_CHEM_YR,0) UM_ADM_G_CHEM_YR, 
    nvl(UM_ADM_G_CHEM_CR,0) UM_ADM_G_CHEM_CR, 
    nvl(trim(UM_ADM_G_CHEM_GR),'-') UM_ADM_G_CHEM_GR, 
    nvl(UM_ADM_A_CHEM_YR,0) UM_ADM_A_CHEM_YR, 
    nvl(UM_ADM_A_CHEM_CR,0) UM_ADM_A_CHEM_CR, 
    nvl(trim(UM_ADM_A_CHEM_GR),'-') UM_ADM_A_CHEM_GR, 
    nvl(UM_ADM_AI_CHEM_YR,0) UM_ADM_AI_CHEM_YR, 
    nvl(UM_ADM_AI_CHEM_CR,0) UM_ADM_AI_CHEM_CR, 
    nvl(trim(UM_ADM_AI_CHEM_GR),'-') UM_ADM_AI_CHEM_GR, 
    nvl(UM_ADM_OR_CHEM1_YR,0) UM_ADM_OR_CHEM1_YR, 
    nvl(UM_ADM_OR_CHEM1_CR,0) UM_ADM_OR_CHEM1_CR, 
    nvl(trim(UM_ADM_OR_CHEM1_GR),'-') UM_ADM_OR_CHEM1_GR, 
    nvl(UM_ADM_OR_CHEM2_YR,0) UM_ADM_OR_CHEM2_YR, 
    nvl(UM_ADM_OR_CHEM2_CR,0) UM_ADM_OR_CHEM2_CR, 
    nvl(trim(UM_ADM_OR_CHEM2_GR),'-') UM_ADM_OR_CHEM2_GR, 
    nvl(UM_ADM_PHYSICS_YR,0) UM_ADM_PHYSICS_YR, 
    nvl(UM_ADM_PHYSICS_CR,0) UM_ADM_PHYSICS_CR, 
    nvl(trim(UM_ADM_PHYSICS_GR),'-') UM_ADM_PHYSICS_GR, 
    nvl(UM_ADM_PHY_CHM1_YR,0) UM_ADM_PHY_CHM1_YR, 
    nvl(UM_ADM_PHY_CHM1_CR,0) UM_ADM_PHY_CHM1_CR, 
    nvl(trim(UM_ADM_PHY_CHM1_GR),'-') UM_ADM_PHY_CHM1_GR, 
    nvl(UM_ADM_PHY_CHM2_YR,0) UM_ADM_PHY_CHM2_YR, 
    nvl(UM_ADM_PHY_CHM2_CR,0) UM_ADM_PHY_CHM2_CR, 
    nvl(trim(UM_ADM_PHY_CHM2_GR),'-') UM_ADM_PHY_CHM2_GR, 
    nvl(UM_ADM_CALCULUS_YR,0) UM_ADM_CALCULUS_YR, 
    nvl(UM_ADM_CALCULUS_CR,0) UM_ADM_CALCULUS_CR, 
    nvl(trim(UM_ADM_CALCULUS_GR),'-') UM_ADM_CALCULUS_GR, 
    nvl(trim(UM_ADM_CHEM_E1_CRS),'-') UM_ADM_CHEM_E1_CRS, 
    nvl(UM_ADM_CHEM_E1_YR,0) UM_ADM_CHEM_E1_YR, 
    nvl(UM_ADM_CHEM_E1_CR,0) UM_ADM_CHEM_E1_CR, 
    nvl(trim(UM_ADM_CHEM_E1_GR),'-') UM_ADM_CHEM_E1_GR, 
    nvl(trim(UM_ADM_CHEM_E2_CRS),'-') UM_ADM_CHEM_E2_CRS, 
    nvl(UM_ADM_CHEM_E2_YR,0) UM_ADM_CHEM_E2_YR, 
    nvl(UM_ADM_CHEM_E2_CR,0) UM_ADM_CHEM_E2_CR, 
    nvl(trim(UM_ADM_CHEM_E2_GR),'-') UM_ADM_CHEM_E2_GR, 
    nvl(trim(UM_ADM_CHEM_E3_CRS),'-') UM_ADM_CHEM_E3_CRS, 
    nvl(UM_ADM_CHEM_E3_YR,0) UM_ADM_CHEM_E3_YR, 
    nvl(UM_ADM_CHEM_E3_CR,0) UM_ADM_CHEM_E3_CR, 
    nvl(trim(UM_ADM_CHEM_E3_GR),'-') UM_ADM_CHEM_E3_GR, 
    nvl(trim(UM_ADM_CHEM_E4_CRS),'-') UM_ADM_CHEM_E4_CRS, 
    nvl(UM_ADM_CHEM_E4_YR,0) UM_ADM_CHEM_E4_YR, 
    nvl(UM_ADM_CHEM_E4_CR,0) UM_ADM_CHEM_E4_CR, 
    nvl(trim(UM_ADM_CHEM_E4_GR),'-') UM_ADM_CHEM_E4_GR, 
    nvl(trim(UM_ADM_DG_CONCENTR),'-') UM_ADM_DG_CONCENTR, 
    nvl(trim(UM_ADM_DG_ELEM_SCH),'-') UM_ADM_DG_ELEM_SCH, 
    nvl(UM_ADM_DG_COMM_MTH,0) UM_ADM_DG_COMM_MTH, 
    nvl(UM_ADM_DG_COMM_YR,0) UM_ADM_DG_COMM_YR, 
    nvl(UM_ADM_DG_CONT_YR,0) UM_ADM_DG_CONT_YR, 
    nvl(UM_ADM_DG_CONT_MTH,0) UM_ADM_DG_CONT_MTH, 
    nvl(UM_ADM_DG_FOUN_MTH,0) UM_ADM_DG_FOUN_MTH, 
    nvl(UM_ADM_DG_FOUN_YR,0) UM_ADM_DG_FOUN_YR, 
    nvl(trim(UM_ADM_DG_MIDL_SCH),'-') UM_ADM_DG_MIDL_SCH, 
    nvl(UM_ADM_DG_SUBJ_MTH,0) UM_ADM_DG_SUBJ_MTH, 
    nvl(UM_ADM_DG_SUBJ_YR,0) UM_ADM_DG_SUBJ_YR, 
    nvl(trim(UM_ADM_DG_N_APPLIC),'-') UM_ADM_DG_N_APPLIC, 
    nvl(trim(UM_ADM_DG_MATP_ACK),'-') UM_ADM_DG_MATP_ACK, 
    nvl(trim(UM_ADM_BG_EDC_LIC1),'-') UM_ADM_BG_EDC_LIC1, 
    nvl(trim(UM_ADM_BG_EDC_LIC2),'-') UM_ADM_BG_EDC_LIC2, 
    nvl(trim(UM_ADM_BG_EDC_LIC3),'-') UM_ADM_BG_EDC_LIC3, 
    nvl(trim(UM_ADM_BG_ADMIN_L),'-') UM_ADM_BG_ADMIN_L, 
    nvl(trim(UM_ADM_BG_OTH_LIC),'-') UM_ADM_BG_OTH_LIC, 
    nvl(trim(UM_ADM_BG_GRD_DGR),'-') UM_ADM_BG_GRD_DGR, 
    nvl(trim(UM_ADM_BG_CERT_NP),'-') UM_ADM_BG_CERT_NP, 
    nvl(trim(UM_ADM_BG_ADULT_NP),'-') UM_ADM_BG_ADULT_NP, 
    nvl(trim(UM_ADM_BG_PEDI_NP),'-') UM_ADM_BG_PEDI_NP, 
    nvl(trim(UM_ADM_BG_FACULTY1),'-') UM_ADM_BG_FACULTY1, 
    nvl(trim(UM_ADM_BG_FACULTY2),'-') UM_ADM_BG_FACULTY2, 
    nvl(trim(UM_ADM_BG_FACULTY3),'-') UM_ADM_BG_FACULTY3, 
    nvl(trim(UM_ADM_BG_CAR_GOAL),'-') UM_ADM_BG_CAR_GOAL, 
    nvl(trim(UM_ADM_BG_CAR_OTH),'-') UM_ADM_BG_CAR_OTH, 
    nvl(trim(UM_ADM_BG_DEGR_IN),'-') UM_ADM_BG_DEGR_IN, 
    nvl(trim(UM_ADM_BG_GRAD_SER),'-') UM_ADM_BG_GRAD_SER, 
    nvl(UM_ADM_BG_SERV_HRS,0) UM_ADM_BG_SERV_HRS, 
    nvl(trim(UM_ADM_BG_ON_CAMP),'-') UM_ADM_BG_ON_CAMP, 
    nvl(trim(UM_ADM_BG_ONLINE),'-') UM_ADM_BG_ONLINE, 
    nvl(trim(UM_ADM_DU_ONLINE),'-') UM_ADM_DU_ONLINE, 
    nvl(trim(UM_ADM_DU_CERT),'-') UM_ADM_DU_CERT, 
    nvl(trim(UM_ADM_BG_RESEARCH),'-') UM_ADM_BG_RESEARCH, 
    nvl(trim(UM_LOWU_CLINICAL_1),'-') UM_LOWU_CLINICAL_1, 
    nvl(trim(UM_ADM_BG_ADVISOR1),'-') UM_ADM_BG_ADVISOR1, 
    nvl(trim(UM_ADM_BG_ADVISOR2),'-') UM_ADM_BG_ADVISOR2, 
    nvl(trim(UM_ADM_BG_ADVISOR3),'-') UM_ADM_BG_ADVISOR3, 
    nvl(trim(UM_ADM_BG_SPEC_ED),'-') UM_ADM_BG_SPEC_ED, 
    nvl(trim(UM_ADM_UMB_Z_AD_ON),'-') UM_ADM_UMB_Z_AD_ON, 
    nvl(trim(UM_ADM_UMB_Z_F_EMP),'-') UM_ADM_UMB_Z_F_EMP, 
    nvl(trim(UM_ADM_UMB_Z_FAM),'-') UM_ADM_UMB_Z_FAM, 
    nvl(trim(UM_ADM_UMB_Z_OTHER),'-') UM_ADM_UMB_Z_OTHER, 
    nvl(trim(UM_ADM_UMB_Z_PRINT),'-') UM_ADM_UMB_Z_PRINT, 
    nvl(trim(UM_ADM_UMB_Z_RADIO),'-') UM_ADM_UMB_Z_RADIO, 
    nvl(trim(UM_ADM_UMB_Z_TEXT),'-') UM_ADM_UMB_Z_TEXT, 
    nvl(trim(UM_ADM_UMB_Z_TV),'-') UM_ADM_UMB_Z_TV, 
    nvl(trim(UM_ADM_UMB_Z_WEB),'-') UM_ADM_UMB_Z_WEB, 
    nvl(trim(UM_ADM_DU_UNIV_EXT),'-') UM_ADM_DU_UNIV_EXT, 
    nvl(trim(UM_ADM_LG_AREA_INT),'-') UM_ADM_LG_AREA_INT, 
    nvl(trim(UM_ADM_DU_DAY),'-') UM_ADM_DU_DAY, 
    nvl(trim(UM_ADM_DU_NIGHT),'-') UM_ADM_DU_NIGHT, 
    nvl(trim(UM_ADM_BG_SOC_PHD),'-') UM_ADM_BG_SOC_PHD, 
    nvl(trim(UM_ADM_BG_BPE_QUES),'-') UM_ADM_BG_BPE_QUES, 
    to_date(to_char(case when UM_ADM_PAY_REDO_DT < '01-JAN-1800' then NULL 
                    else UM_ADM_PAY_REDO_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') UM_ADM_PAY_REDO_DT,
    to_date(to_char(case when UM_ADM_PAY_CMPL_DT < '01-JAN-1800' then NULL 
                    else UM_ADM_PAY_CMPL_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') UM_ADM_PAY_CMPL_DT,
    nvl(trim(UM_ADM_BG_DISB_LIC),'-') UM_ADM_BG_DISB_LIC, 
    nvl(trim(UM_ADM_BG_MASTERS),'-') UM_ADM_BG_MASTERS, 
    nvl(trim(UM_ADM_BG_LICENS),'-') UM_ADM_BG_LICENS, 
    nvl(trim(UM_ADM_PARTNERSHIP),'-') UM_ADM_PARTNERSHIP, 
    nvl(trim(UM_BOSG_FLEX_MBA),'-') UM_BOSG_FLEX_MBA, 
    nvl(trim(UM_BOSG_PRO_MBA),'-') UM_BOSG_PRO_MBA, 
    nvl(trim(UM_BOSG_ACCEL_MAST),'-') UM_BOSG_ACCEL_MAST, 
    to_date(to_char(case when UM_ADM_LU_DIAGN_DT < '01-JAN-1800' then NULL 
                    else UM_ADM_LU_DIAGN_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') UM_ADM_LU_DIAGN_DT,
    nvl(trim(UM_ADM_LU_MUS_AUD),'-') UM_ADM_LU_MUS_AUD, 
	nvl(trim(UM_ADM_HEAR_US_CD),'-') UM_ADM_HEAR_US_CD,
	nvl(trim(UM_ADM_HEAR_TEXT),'-') UM_ADM_HEAR_TEXT,
	nvl(trim(UM_ENG_AS_SEC_LNG),'-') UM_ENG_AS_SEC_LNG,
    UM_ADM_DG_PARTTIME  UM_ADM_DG_PARTTIME
from CSSTG_OWNER.PS_T_UM_ADM_APP_TMP S
where SRC_SCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_UM_ADM_APP_TMP') ) S 
 on ( 
    T.UM_ADM_USERID = S.UM_ADM_USERID and 
    T.UM_ADM_APP_SEQ = S.UM_ADM_APP_SEQ and 
    T.UM_ADM_REC_NBR = S.UM_ADM_REC_NBR and 
    T.INSTITUTION = S.INSTITUTION and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.EMPLID = S.EMPLID,
    T.ADM_APPL_NBR = S.ADM_APPL_NBR,
    T.UM_ADM_EMAIL = S.UM_ADM_EMAIL,
    T.EMAILID = S.EMAILID,
    T.ACAD_CAREER = S.ACAD_CAREER,
    T.ADM_APPL_CTR = S.ADM_APPL_CTR,
    T.UM_ADM_SUB_DT = S.UM_ADM_SUB_DT,
    T.FIRST_NAME = S.FIRST_NAME,
    T.MIDDLE_NAME = S.MIDDLE_NAME,
    T.LAST_NAME = S.LAST_NAME,
    T.UM_SSN = S.UM_SSN,
    T.BIRTHDATE = S.BIRTHDATE,
    T.SEX = S.SEX,
    T.UM_US_CITIZEN = S.UM_US_CITIZEN,
    T.UM_CITIZNSHIP_STAT = S.UM_CITIZNSHIP_STAT,
    T.UM_BIRTH_CNTRY = S.UM_BIRTH_CNTRY,
    T.UM_ADM_NOT_MA_RES = S.UM_ADM_NOT_MA_RES,
    T.VISA_PERMIT_TYPE = S.VISA_PERMIT_TYPE,
    T.VISA_WRKPMT_STATUS = S.VISA_WRKPMT_STATUS,
    T.UM_VISA_WRKPMT_NBR = S.UM_VISA_WRKPMT_NBR,
    T.UM_ADM_VISA_START = S.UM_ADM_VISA_START,
    T.UM_ADM_VISA_END = S.UM_ADM_VISA_END,
    T.UM_SUFFIX = S.UM_SUFFIX,
    T.UM_PREFIX = S.UM_PREFIX,
    T.UM_FORMER_FNAME1 = S.UM_FORMER_FNAME1,
    T.UM_FORMER_MNAME1 = S.UM_FORMER_MNAME1,
    T.UM_FORMER_LNAME1 = S.UM_FORMER_LNAME1,
    T.UM_PREF_FNAME1 = S.UM_PREF_FNAME1,
    T.UM_PREF_MNAME1 = S.UM_PREF_MNAME1,
    T.UM_PREF_LNAME1 = S.UM_PREF_LNAME1,
    T.UM_PERM_ADDRESS1 = S.UM_PERM_ADDRESS1,
    T.UM_PERM_ADDRESS2 = S.UM_PERM_ADDRESS2,
    T.UM_PERM_ADDRESS3 = S.UM_PERM_ADDRESS3,
    T.UM_PERM_CITY = S.UM_PERM_CITY,
    T.UM_PERM_STATE = S.UM_PERM_STATE,
    T.UM_PERM_POSTAL = S.UM_PERM_POSTAL,
    T.COUNTRY_CODE_PERM = S.COUNTRY_CODE_PERM,
    T.UM_PERM_COUNTRY = S.UM_PERM_COUNTRY,
    T.UM_VALID_UNTIL = S.UM_VALID_UNTIL,
    T.UM_MAIL_ADDRESS1 = S.UM_MAIL_ADDRESS1,
    T.UM_MAIL_ADDRESS2 = S.UM_MAIL_ADDRESS2,
    T.UM_MAIL_ADDRESS3 = S.UM_MAIL_ADDRESS3,
    T.UM_MAIL_CITY = S.UM_MAIL_CITY,
    T.UM_MAIL_STATE = S.UM_MAIL_STATE,
    T.UM_MAIL_POSTAL = S.UM_MAIL_POSTAL,
    T.UM_MAIL_COUNTRY = S.UM_MAIL_COUNTRY,
    T.COUNTRY_CD = S.COUNTRY_CD,
    T.COUNTRY_CODE = S.COUNTRY_CODE,
    T.UM_PERM_PHONE = S.UM_PERM_PHONE,
    T.UM_PERM_PHONE1 = S.UM_PERM_PHONE1,
    T.UM_PERM_PHONE2 = S.UM_PERM_PHONE2,
    T.UM_PERM_PHONE3 = S.UM_PERM_PHONE3,
    T.UM_CELL1 = S.UM_CELL1,
    T.UM_CELL2 = S.UM_CELL2,
    T.UM_CELL3 = S.UM_CELL3,
    T.UM_BUSN_CNTRY_CD = S.UM_BUSN_CNTRY_CD,
    T.UM_BUSN_PHONE = S.UM_BUSN_PHONE,
    T.UM_BUSN_PHONE1 = S.UM_BUSN_PHONE1,
    T.UM_BUSN_PHONE3 = S.UM_BUSN_PHONE3,
    T.UM_BUSN_EXTENSION = S.UM_BUSN_EXTENSION,
    T.UM_ADM_SESSION = S.UM_ADM_SESSION,
    T.ADMIT_TYPE = S.ADMIT_TYPE,
    T.UM_ADM_SUBFIELD = S.UM_ADM_SUBFIELD,
    T.UM_PRG_PLN_SBPLN = S.UM_PRG_PLN_SBPLN,
    T.UM_PRG_PLN_SBPLN2 = S.UM_PRG_PLN_SBPLN2,
    T.UM_ACAD_PROG = S.UM_ACAD_PROG,
    T.UM_ACAD_PLAN = S.UM_ACAD_PLAN,
    T.UM_ACAD_SUB_PLAN = S.UM_ACAD_SUB_PLAN,
    T.UM_ACAD_PROG1 = S.UM_ACAD_PROG1,
    T.UM_ACAD_PLAN1 = S.UM_ACAD_PLAN1,
    T.UM_ACAD_SUB_PLAN1 = S.UM_ACAD_SUB_PLAN1,
    T.UM_ADM_APP_FIN_AID = S.UM_ADM_APP_FIN_AID,
    T.UM_GRE_MO = S.UM_GRE_MO,
    T.UM_GRE_YR = S.UM_GRE_YR,
    T.UM_GRE_VERB = S.UM_GRE_VERB,
    T.UM_GRE_QUAN = S.UM_GRE_QUAN,
    T.UM_GRE_ANAL = S.UM_GRE_ANAL,
    T.UM_GRE_SUBJ_SC = S.UM_GRE_SUBJ_SC,
    T.UM_GRE_SUBJ_MO = S.UM_GRE_SUBJ_MO,
    T.UM_GRE_SUBJ_YR = S.UM_GRE_SUBJ_YR,
    T.UM_GRE_SUBJECT = S.UM_GRE_SUBJECT,
    T.UM_GMAT_MO = S.UM_GMAT_MO,
    T.UM_GMAT_YR = S.UM_GMAT_YR,
    T.UM_GMAT_SC = S.UM_GMAT_SC,
    T.UM_TOEFL_MO = S.UM_TOEFL_MO,
    T.UM_TOEFL_YR = S.UM_TOEFL_YR,
    T.UM_TOEFL_SC = S.UM_TOEFL_SC,
    T.UM_TEST1_MO = S.UM_TEST1_MO,
    T.UM_TEST1_NAME = S.UM_TEST1_NAME,
    T.UM_TEST1_SC = S.UM_TEST1_SC,
    T.UM_TEST1_YR = S.UM_TEST1_YR,
    T.UM_ADM_APP_GPA_ALL = S.UM_ADM_APP_GPA_ALL,
    T.UM_ADM_APP_GPA_GRD = S.UM_ADM_APP_GPA_GRD,
    T.UM_ADM_APP_GPA_MAJ = S.UM_ADM_APP_GPA_MAJ,
    T.UM_ADM_APP_GPA_TWO = S.UM_ADM_APP_GPA_TWO,
    T.UM_ACT1_MO = S.UM_ACT1_MO,
    T.UM_ACT1_SC = S.UM_ACT1_SC,
    T.UM_ACT1_YR = S.UM_ACT1_YR,
    T.UM_ACT2_MO = S.UM_ACT2_MO,
    T.UM_ACT2_SC = S.UM_ACT2_SC,
    T.UM_ACT2_YR = S.UM_ACT2_YR,
    T.UM_ACT3_MO = S.UM_ACT3_MO,
    T.UM_ACT3_SC = S.UM_ACT3_SC,
    T.UM_ACT3_YR = S.UM_ACT3_YR,
    T.UM_SAT1_MO = S.UM_SAT1_MO,
    T.UM_SAT1_SC = S.UM_SAT1_SC,
    T.UM_SAT1_YR = S.UM_SAT1_YR,
    T.UM_SAT2_MO = S.UM_SAT2_MO,
    T.UM_SAT2_SC = S.UM_SAT2_SC,
    T.UM_SAT2_YR = S.UM_SAT2_YR,
    T.UM_SAT3_MO = S.UM_SAT3_MO,
    T.UM_SAT3_SC = S.UM_SAT3_SC,
    T.UM_SAT3_YR = S.UM_SAT3_YR,
    T.UM_TOEFL1_MO = S.UM_TOEFL1_MO,
    T.UM_TOEFL1_SC = S.UM_TOEFL1_SC,
    T.UM_TOEFL1_YR = S.UM_TOEFL1_YR,
    T.UM_TOEFL2_MO = S.UM_TOEFL2_MO,
    T.UM_TOEFL2_SC = S.UM_TOEFL2_SC,
    T.UM_TOEFL2_YR = S.UM_TOEFL2_YR,
    T.UM_TOEFL3_MO = S.UM_TOEFL3_MO,
    T.UM_TOEFL3_SC = S.UM_TOEFL3_SC,
    T.UM_TOEFL3_YR = S.UM_TOEFL3_YR,
    T.UM_IELTS1_MO = S.UM_IELTS1_MO,
    T.UM_IELTS1_SC = S.UM_IELTS1_SC,
    T.UM_IELTS1_YR = S.UM_IELTS1_YR,
    T.UM_IELTS2_MO = S.UM_IELTS2_MO,
    T.UM_IELTS2_SC = S.UM_IELTS2_SC,
    T.UM_IELTS2_YR = S.UM_IELTS2_YR,
    T.UM_IELTS3_MO = S.UM_IELTS3_MO,
    T.UM_IELTS3_SC = S.UM_IELTS3_SC,
    T.UM_IELTS3_YR = S.UM_IELTS3_YR,
    T.UM_GMAT1_MO = S.UM_GMAT1_MO,
    T.UM_GMAT1_SC = S.UM_GMAT1_SC,
    T.UM_GMAT1_YR = S.UM_GMAT1_YR,
    T.UM_GMAT2_MO = S.UM_GMAT2_MO,
    T.UM_GMAT2_SC = S.UM_GMAT2_SC,
    T.UM_GMAT2_YR = S.UM_GMAT2_YR,
    T.UM_GMAT3_MO = S.UM_GMAT3_MO,
    T.UM_GMAT3_SC = S.UM_GMAT3_SC,
    T.UM_GMAT3_YR = S.UM_GMAT3_YR,
    T.UM_GRE_AW_MO = S.UM_GRE_AW_MO,
    T.UM_GRE_AW_SC = S.UM_GRE_AW_SC,
    T.UM_GRE_AW_YR = S.UM_GRE_AW_YR,
    T.UM_GRE_Q_MO = S.UM_GRE_Q_MO,
    T.UM_GRE_Q_SC = S.UM_GRE_Q_SC,
    T.UM_GRE_Q_YR = S.UM_GRE_Q_YR,
    T.UM_GRE_V_MO = S.UM_GRE_V_MO,
    T.UM_GRE_V_SC = S.UM_GRE_V_SC,
    T.UM_GRE_V_YR = S.UM_GRE_V_YR,
    T.UM_LSAT1_MO = S.UM_LSAT1_MO,
    T.UM_LSAT1_SC = S.UM_LSAT1_SC,
    T.UM_LSAT1_YR = S.UM_LSAT1_YR,
    T.UM_LSAT2_MO = S.UM_LSAT2_MO,
    T.UM_LSAT2_SC = S.UM_LSAT2_SC,
    T.UM_LSAT2_YR = S.UM_LSAT2_YR,
    T.UM_LSAT3_MO = S.UM_LSAT3_MO,
    T.UM_LSAT3_SC = S.UM_LSAT3_SC,
    T.UM_LSAT3_YR = S.UM_LSAT3_YR,
    T.UM_COURSE_1_CREDIT = S.UM_COURSE_1_CREDIT,
    T.UM_COURSE_1_ID = S.UM_COURSE_1_ID,
    T.UM_COURSE_1_TITLE = S.UM_COURSE_1_TITLE,
    T.UM_COURSE_2_CREDIT = S.UM_COURSE_2_CREDIT,
    T.UM_COURSE_2_ID = S.UM_COURSE_2_ID,
    T.UM_COURSE_2_TITLE = S.UM_COURSE_2_TITLE,
    T.UM_COURSE_3_CREDIT = S.UM_COURSE_3_CREDIT,
    T.UM_COURSE_3_ID = S.UM_COURSE_3_ID,
    T.UM_COURSE_3_TITLE = S.UM_COURSE_3_TITLE,
    T.UM_COURSE_4_CREDIT = S.UM_COURSE_4_CREDIT,
    T.UM_COURSE_4_ID = S.UM_COURSE_4_ID,
    T.UM_COURSE_4_TITLE = S.UM_COURSE_4_TITLE,
    T.UM_COURSE_5_CREDIT = S.UM_COURSE_5_CREDIT,
    T.UM_COURSE_5_ID = S.UM_COURSE_5_ID,
    T.UM_COURSE_5_TITLE = S.UM_COURSE_5_TITLE,
    T.MILITARY_STATUS = S.MILITARY_STATUS,
    T.UM_ETHNIC = S.UM_ETHNIC,
    T.UM_EMPLOY_STATE = S.UM_EMPLOY_STATE,
    T.UM_LAW4 = S.UM_LAW4,
    T.UM_ADM_FAC_FNAME = S.UM_ADM_FAC_FNAME,
    T.UM_ADM_FAC_LNAME = S.UM_ADM_FAC_LNAME,
    T.UM_LAW5 = S.UM_LAW5,
    T.UM_ADM_APP_MO_FR2 = S.UM_ADM_APP_MO_FR2,
    T.UM_ADM_APP_MO_FR3 = S.UM_ADM_APP_MO_FR3,
    T.UM_ADM_APP_MO_FR4 = S.UM_ADM_APP_MO_FR4,
    T.UM_ADM_APP_MO_TO2 = S.UM_ADM_APP_MO_TO2,
    T.UM_ADM_APP_MO_TO3 = S.UM_ADM_APP_MO_TO3,
    T.UM_ADM_APP_MO_TO4 = S.UM_ADM_APP_MO_TO4,
    T.UM_HS_ID = S.UM_HS_ID,
    T.UM_ACAD_LOAD_APPR = S.UM_ACAD_LOAD_APPR,
    T.UM_ADM_ONLINE_DEGR = S.UM_ADM_ONLINE_DEGR,
    T.UM_ADM_MUSIC_INST1 = S.UM_ADM_MUSIC_INST1,
    T.UM_PRV1_ID = S.UM_PRV1_ID,
    T.UM_PRV2_ID = S.UM_PRV2_ID,
    T.UM_PRV3_ID = S.UM_PRV3_ID,
    T.UM_PRV4_ID = S.UM_PRV4_ID,
    T.UM_PHONE_TYPE = S.UM_PHONE_TYPE,
    T.UM_INTL_ZIP = S.UM_INTL_ZIP,
    T.UM_ADM_APP_MO_FR1 = S.UM_ADM_APP_MO_FR1,
    T.UM_ADM_APP_MO_TO1 = S.UM_ADM_APP_MO_TO1,
    T.UM_ADM_APP_TITLE1 = S.UM_ADM_APP_TITLE1,
    T.UM_ADM_APP_YR_FR1 = S.UM_ADM_APP_YR_FR1,
    T.UM_ADM_APP_YR_TO1 = S.UM_ADM_APP_YR_TO1,
    T.UM_ADM_APP_POS1 = S.UM_ADM_APP_POS1,
    T.UM_ADM_APP_POS2 = S.UM_ADM_APP_POS2,
    T.UM_ADM_APP_POS3 = S.UM_ADM_APP_POS3,
    T.UM_ADM_APP_ADDR1 = S.UM_ADM_APP_ADDR1,
    T.UM_ADM_APP_ADDR2 = S.UM_ADM_APP_ADDR2,
    T.UM_ADM_APP_ADDR3 = S.UM_ADM_APP_ADDR3,
    T.UM_ADM_APP_NAM1 = S.UM_ADM_APP_NAM1,
    T.UM_ADM_APP_NAM2 = S.UM_ADM_APP_NAM2,
    T.UM_ADM_APP_NAM3 = S.UM_ADM_APP_NAM3,
    T.UM_ADM_APP_CNTRY1 = S.UM_ADM_APP_CNTRY1,
    T.UM_ADM_APP_CNTRY2 = S.UM_ADM_APP_CNTRY2,
    T.UM_ADM_APP_CNTRY3 = S.UM_ADM_APP_CNTRY3,
    T.UM_ADM_APP_CNTRY4 = S.UM_ADM_APP_CNTRY4,
    T.UM_ADM_APP_STATE1 = S.UM_ADM_APP_STATE1,
    T.UM_ADM_APP_STATE2 = S.UM_ADM_APP_STATE2,
    T.UM_ADM_APP_STATE3 = S.UM_ADM_APP_STATE3,
    T.UM_ADM_APP_STATE4 = S.UM_ADM_APP_STATE4,
    T.UM_ADM_APP_PR_TRM = S.UM_ADM_APP_PR_TRM,
    T.UM_ADM_APP_EN_TRM = S.UM_ADM_APP_EN_TRM,
    T.UM_ADM_APP_PRIOR = S.UM_ADM_APP_PRIOR,
    T.UM_ADM_APP_ENRL = S.UM_ADM_APP_ENRL,
    T.UM_ADM_APP_CAR1 = S.UM_ADM_APP_CAR1,
    T.UM_ADM_APP_CAR2 = S.UM_ADM_APP_CAR2,
    T.UM_ADM_APP_CAR3 = S.UM_ADM_APP_CAR3,
    T.UM_ADM_APP_CAR4 = S.UM_ADM_APP_CAR4,
    T.UM_ADM_APP_DEG1 = S.UM_ADM_APP_DEG1,
    T.UM_ADM_APP_DEG2 = S.UM_ADM_APP_DEG2,
    T.UM_ADM_APP_DEG3 = S.UM_ADM_APP_DEG3,
    T.UM_ADM_APP_DEG4 = S.UM_ADM_APP_DEG4,
    T.UM_ADM_APP_DEGYR1 = S.UM_ADM_APP_DEGYR1,
    T.UM_ADM_APP_DEGYR2 = S.UM_ADM_APP_DEGYR2,
    T.UM_ADM_APP_DEGYR3 = S.UM_ADM_APP_DEGYR3,
    T.UM_ADM_APP_DEGYR4 = S.UM_ADM_APP_DEGYR4,
    T.UM_ADM_APP_FROM1 = S.UM_ADM_APP_FROM1,
    T.UM_ADM_APP_FROM2 = S.UM_ADM_APP_FROM2,
    T.UM_ADM_APP_FROM3 = S.UM_ADM_APP_FROM3,
    T.UM_ADM_APP_FROM4 = S.UM_ADM_APP_FROM4,
    T.UM_ADM_APP_MONTH1 = S.UM_ADM_APP_MONTH1,
    T.UM_ADM_APP_MONTH2 = S.UM_ADM_APP_MONTH2,
    T.UM_ADM_APP_MONTH3 = S.UM_ADM_APP_MONTH3,
    T.UM_ADM_APP_MONTH4 = S.UM_ADM_APP_MONTH4,
    T.UM_ADM_APP_PRV1 = S.UM_ADM_APP_PRV1,
    T.UM_ADM_APP_PRV2 = S.UM_ADM_APP_PRV2,
    T.UM_ADM_APP_PRV3 = S.UM_ADM_APP_PRV3,
    T.UM_ADM_APP_PRV4 = S.UM_ADM_APP_PRV4,
    T.UM_ADM_APP_TO1 = S.UM_ADM_APP_TO1,
    T.UM_ADM_APP_TO2 = S.UM_ADM_APP_TO2,
    T.UM_ADM_APP_TO3 = S.UM_ADM_APP_TO3,
    T.UM_ADM_APP_TO4 = S.UM_ADM_APP_TO4,
    T.UM_ADM_ASSIST = S.UM_ADM_ASSIST,
    T.UM_ADM_FELLOW = S.UM_ADM_FELLOW,
    T.MBA_ABITUS = S.MBA_ABITUS,
    T.MBA_CURRENT_STUDNT = S.MBA_CURRENT_STUDNT,
    T.MBA_DAY = S.MBA_DAY,
    T.MBA_EXCLSVE_ONLINE = S.MBA_EXCLSVE_ONLINE,
    T.MBA_WRK_PROFESNLS = S.MBA_WRK_PROFESNLS,
    T.CONTACT_NAME = S.CONTACT_NAME,
    T.UM_EMERG_COUNTRY = S.UM_EMERG_COUNTRY,
    T.UM_ADM_REL_TYPE = S.UM_ADM_REL_TYPE,
    T.UM_EMERG_CNTRY_CD = S.UM_EMERG_CNTRY_CD,
    T.UM_EMERG_PHONE = S.UM_EMERG_PHONE,
    T.CONTACT_PHONE = S.CONTACT_PHONE,
    T.CONTACT_PHONE_EXT = S.CONTACT_PHONE_EXT,
    T.UM_PARENT_NAME = S.UM_PARENT_NAME,
    T.UM_PARENT_ADDR1 = S.UM_PARENT_ADDR1,
    T.UM_PARENT_ADDR2 = S.UM_PARENT_ADDR2,
    T.UM_PARENT_ADDR3 = S.UM_PARENT_ADDR3,
    T.UM_PARENT_CITY = S.UM_PARENT_CITY,
    T.UM_PARENT_STATE = S.UM_PARENT_STATE,
    T.UM_PARENT_COUNTRY = S.UM_PARENT_COUNTRY,
    T.UM_PARENT_PHONE = S.UM_PARENT_PHONE,
    T.UM_PARENT_CNTRY_CD = S.UM_PARENT_CNTRY_CD,
    T.UM_PARENT_PHONE1 = S.UM_PARENT_PHONE1,
    T.UM_PARENT_PHONE2 = S.UM_PARENT_PHONE2,
    T.UM_PARENT_PHONE3 = S.UM_PARENT_PHONE3,
    T.UM_PARENT_TYPE = S.UM_PARENT_TYPE,
    T.ALUMNI_EVER = S.ALUMNI_EVER,
    T.HIGHEST_EDUC_LVL = S.HIGHEST_EDUC_LVL,
    T.UM_PARENT2_NAME = S.UM_PARENT2_NAME,
    T.UM_PARENT2_ADDR1 = S.UM_PARENT2_ADDR1,
    T.UM_PARENT2_ADDR2 = S.UM_PARENT2_ADDR2,
    T.UM_PARENT2_ADDR3 = S.UM_PARENT2_ADDR3,
    T.UM_PARENT2_CITY = S.UM_PARENT2_CITY,
    T.UM_PARENT2_STATE = S.UM_PARENT2_STATE,
    T.UM_PARENT2_POSTAL = S.UM_PARENT2_POSTAL,
    T.UM_PARENT2_INT_ZIP = S.UM_PARENT2_INT_ZIP,
    T.UM_PARENT2_COUNTRY = S.UM_PARENT2_COUNTRY,
    T.UM_PARENT_CNTRY_C2 = S.UM_PARENT_CNTRY_C2,
    T.UM_PARENT2_PHONE = S.UM_PARENT2_PHONE,
    T.UM_PARENT2_PHONE1 = S.UM_PARENT2_PHONE1,
    T.UM_PARENT2_PHONE2 = S.UM_PARENT2_PHONE2,
    T.UM_PARENT2_PHONE3 = S.UM_PARENT2_PHONE3,
    T.UM_PARENT2_TYPE = S.UM_PARENT2_TYPE,
    T.UM_ALUMNI_EVER_P2 = S.UM_ALUMNI_EVER_P2,
    T.UM_HIGH_EDUCLVL_P2 = S.UM_HIGH_EDUCLVL_P2,
    T.UM_GUARD_NAME = S.UM_GUARD_NAME,
    T.UM_GUARD_ADDR1 = S.UM_GUARD_ADDR1,
    T.UM_GUARD_ADDR2 = S.UM_GUARD_ADDR2,
    T.UM_GUARD_ADDR3 = S.UM_GUARD_ADDR3,
    T.UM_GUARD_CITY = S.UM_GUARD_CITY,
    T.UM_GUARD_STATE = S.UM_GUARD_STATE,
    T.UM_GUARD_POSTAL = S.UM_GUARD_POSTAL,
    T.UM_GUARD_INT_ZIP = S.UM_GUARD_INT_ZIP,
    T.UM_GUARD_COUNTRY = S.UM_GUARD_COUNTRY,
    T.UM_GUARD_CNTRY_CD = S.UM_GUARD_CNTRY_CD,
    T.UM_GUARD_PHONE = S.UM_GUARD_PHONE,
    T.UM_GUARD_PHONE1 = S.UM_GUARD_PHONE1,
    T.UM_GUARD_PHONE2 = S.UM_GUARD_PHONE2,
    T.UM_GUARD_PHONE3 = S.UM_GUARD_PHONE3,
    T.UM_MASS_RESIDENT = S.UM_MASS_RESIDENT,
    T.UM_ALUMNI_EVER_GUA = S.UM_ALUMNI_EVER_GUA,
    T.UM_HIGH_EDUCLVL_GU = S.UM_HIGH_EDUCLVL_GU,
    T.UM_CNTRY_CITIZENSH = S.UM_CNTRY_CITIZENSH,
    T.UM_BIRTHPLACE = S.UM_BIRTHPLACE,
    T.UM_ADM_ENGINEERING = S.UM_ADM_ENGINEERING,
    T.UM_ADM_O_ENGINEER = S.UM_ADM_O_ENGINEER,
    T.UM_ADM_E_DIS_NAME = S.UM_ADM_E_DIS_NAME,
    T.UM_ADM_SCIENCE_DIS = S.UM_ADM_SCIENCE_DIS,
    T.UM_ADM_S_DIS_NAME = S.UM_ADM_S_DIS_NAME,
    T.UM_COUNSELOR_FNAME = S.UM_COUNSELOR_FNAME,
    T.UM_COUNSELOR_LNAME = S.UM_COUNSELOR_LNAME,
    T.UM_COUNSELOR_EMAIL = S.UM_COUNSELOR_EMAIL,
    T.UM_ADM_APP_5YR = S.UM_ADM_APP_5YR,
    T.UM_ADM_EARLY_D = S.UM_ADM_EARLY_D,
    T.UM_ADM_REF1_FNAME = S.UM_ADM_REF1_FNAME,
    T.UM_ADM_REF1_LNAME = S.UM_ADM_REF1_LNAME,
    T.UM_ADM_REF1_MNAME = S.UM_ADM_REF1_MNAME,
    T.UM_ADM_REF2_FNAME = S.UM_ADM_REF2_FNAME,
    T.UM_ADM_REF2_LNAME = S.UM_ADM_REF2_LNAME,
    T.UM_ADM_REF2_MNAME = S.UM_ADM_REF2_MNAME,
    T.UM_ADM_REF3_FNAME = S.UM_ADM_REF3_FNAME,
    T.UM_ADM_REF3_LNAME = S.UM_ADM_REF3_LNAME,
    T.UM_ADM_REF3_MNAME = S.UM_ADM_REF3_MNAME,
    T.UM_REF_PRIVATE1 = S.UM_REF_PRIVATE1,
    T.UM_REF_PRIVATE2 = S.UM_REF_PRIVATE2,
    T.UM_REF_PRIVATE3 = S.UM_REF_PRIVATE3,
    T.UM_ADM_BA_MASTER = S.UM_ADM_BA_MASTER,
    T.UM_ADM_UMB_TEACH = S.UM_ADM_UMB_TEACH,
    T.UM_ADM_CAR_SWITCH = S.UM_ADM_CAR_SWITCH,
    T.UM_ADM_UMB_MTEL = S.UM_ADM_UMB_MTEL,
    T.UM_ADM_UMB_VISION = S.UM_ADM_UMB_VISION,
    T.UM_ADM_NATL_CERTIF = S.UM_ADM_NATL_CERTIF,
    T.UM_ADM_CERTIFICATN = S.UM_ADM_CERTIFICATN,
    T.UM_ADM_CERT_EXP_DT = S.UM_ADM_CERT_EXP_DT,
    T.UM_ADM_CNOW_LOW_IN = S.UM_ADM_CNOW_LOW_IN,
    T.UM_ADM_CNOW_FRST_G = S.UM_ADM_CNOW_FRST_G,
    T.UM_ADM_CNOW_NOT_AP = S.UM_ADM_CNOW_NOT_AP,
    T.UM_ADM_ARCHELOGY = S.UM_ADM_ARCHELOGY,
    T.UM_ADM_SCHL_NAME = S.UM_ADM_SCHL_NAME,
    T.UM_ADM_SCHL_LOC = S.UM_ADM_SCHL_LOC,
    T.UM_ADM_PREV_BCKGRD = S.UM_ADM_PREV_BCKGRD,
    T.UM_ADM_NBR_MTHS = S.UM_ADM_NBR_MTHS,
    T.UM_ADM_INIT_LICNSE = S.UM_ADM_INIT_LICNSE,
    T.UM_ADM_LICNSE_DESR = S.UM_ADM_LICNSE_DESR,
    T.UM_ADM_TEACH_SUBJ = S.UM_ADM_TEACH_SUBJ,
    T.UM_ADM_NE_REGIONAL = S.UM_ADM_NE_REGIONAL,
    T.UM_ADM_NO_VISA = S.UM_ADM_NO_VISA,
    T.UM_PARENT_EMP_COLL = S.UM_PARENT_EMP_COLL,
    T.UM_PARENT_LIVING = S.UM_PARENT_LIVING,
    T.UM_PARENT_POSTAL = S.UM_PARENT_POSTAL,
    T.UM_PARENT_INT_ZIP = S.UM_PARENT_INT_ZIP,
    T.UM_PARENT_JOBTITLE = S.UM_PARENT_JOBTITLE,
    T.UM_PARENT_GRADSCHL = S.UM_PARENT_GRADSCHL,
    T.UM_ADM_SUCCESS_DEG = S.UM_ADM_SUCCESS_DEG,
    T.UM_ADM_CRS_STR = S.UM_ADM_CRS_STR,
    T.UM_ADM_CRS_END = S.UM_ADM_CRS_END,
    T.UM_ADM_PREV_APPLD = S.UM_ADM_PREV_APPLD,
    T.UM_PARENT_EMPLOYER = S.UM_PARENT_EMPLOYER,
    T.UM_PARENT_OCCUPTN = S.UM_PARENT_OCCUPTN,
    T.UM_PARENT_EMAIL = S.UM_PARENT_EMAIL,
    T.UM_GUARD_EMAIL = S.UM_GUARD_EMAIL,
    T.UM_GUARD_EMPLOYER = S.UM_GUARD_EMPLOYER,
    T.UM_GUARD_EMP_COLL = S.UM_GUARD_EMP_COLL,
    T.UM_GUARD_GRADSCHL = S.UM_GUARD_GRADSCHL,
    T.UM_GUARD_OCCUPTN = S.UM_GUARD_OCCUPTN,
    T.UM_GUARD_JOBTITLE = S.UM_GUARD_JOBTITLE,
    T.UM_GUARD_DEGREE = S.UM_GUARD_DEGREE,
    T.UM_GUARD_DEGREE_G = S.UM_GUARD_DEGREE_G,
    T.UM_PARENT2_DEGREE = S.UM_PARENT2_DEGREE,
    T.UM_PARENT_DEGREE = S.UM_PARENT_DEGREE,
    T.UM_PARENT_DEGREE_G = S.UM_PARENT_DEGREE_G,
    T.UM_ADM_RELIANT_FA = S.UM_ADM_RELIANT_FA,
    T.UM_PARENT_CEEB_G = S.UM_PARENT_CEEB_G,
    T.UM_GUARD_CEEB = S.UM_GUARD_CEEB,
    T.UM_PARENT2_EMAIL = S.UM_PARENT2_EMAIL,
    T.UM_PARENT2_EMPCOLL = S.UM_PARENT2_EMPCOLL,
    T.UM_PARENT2_EMPLOYR = S.UM_PARENT2_EMPLOYR,
    T.UM_PARENT2_GRADSCH = S.UM_PARENT2_GRADSCH,
    T.UM_PARENT2_JOBTITL = S.UM_PARENT2_JOBTITL,
    T.UM_PARENT2_OCCUPTN = S.UM_PARENT2_OCCUPTN,
    T.UM_PARENT2_LIVING = S.UM_PARENT2_LIVING,
    T.UM_ADM_CSCE_TUITN = S.UM_ADM_CSCE_TUITN,
    T.UM_ADM_CURR_EMP = S.UM_ADM_CURR_EMP,
    T.UM_ADM_CURR_JOB = S.UM_ADM_CURR_JOB,
    T.UM_ADM_LAW_SCHL1 = S.UM_ADM_LAW_SCHL1,
    T.UM_ADM_LAW_SCHL2 = S.UM_ADM_LAW_SCHL2,
    T.UM_ADM_LAW_SCHL3 = S.UM_ADM_LAW_SCHL3,
    T.UM_ADM_LAW_SCHL4 = S.UM_ADM_LAW_SCHL4,
    T.UM_ADM_LAW_3_3_PRG = S.UM_ADM_LAW_3_3_PRG,
    T.UM_ADM_LAW_ATTD_B4 = S.UM_ADM_LAW_ATTD_B4,
    T.UM_ADM_LAW_JT_MBA = S.UM_ADM_LAW_JT_MBA,
    T.UM_ADM_LAW_PRV_APP = S.UM_ADM_LAW_PRV_APP,
    T.UM_ADM_LAW_SRV_ACC = S.UM_ADM_LAW_SRV_ACC,
    T.UM_GUARD_CEEB_G = S.UM_GUARD_CEEB_G,
    T.UM_PARENT2_CEEB = S.UM_PARENT2_CEEB,
    T.UM_PARENT2_CEEB_G = S.UM_PARENT2_CEEB_G,
    T.UM_PARENT_CEEB = S.UM_PARENT_CEEB,
    T.UM_PARENT_COLLEGE = S.UM_PARENT_COLLEGE,
    T.UM_GUARD_COLLEGE = S.UM_GUARD_COLLEGE,
    T.UM_PARENT2_COLLEGE = S.UM_PARENT2_COLLEGE,
    T.UM_PARENT2_DEGRE_G = S.UM_PARENT2_DEGRE_G,
    T.UM_ADM_RESID_HALL = S.UM_ADM_RESID_HALL,
    T.UM_ADM_HS_DUAL_ENR = S.UM_ADM_HS_DUAL_ENR,
    T.UM_ADM_SPORT = S.UM_ADM_SPORT,
    T.UM_ADM_HONORS_PRG = S.UM_ADM_HONORS_PRG,
    T.UM_ADM_PREV_CRSE = S.UM_ADM_PREV_CRSE,
    T.UM_ADM_MUSIC_INSTR = S.UM_ADM_MUSIC_INSTR,
    T.UM_ADM_EXCER_PRGM = S.UM_ADM_EXCER_PRGM,
    T.UM_ADM_MUSIC_ENSEM = S.UM_ADM_MUSIC_ENSEM,
    T.UM_ADM_BACH_PATHWY = S.UM_ADM_BACH_PATHWY,
    T.UM_ADM_UML_DISABLE = S.UM_ADM_UML_DISABLE,
    T.UM_ADM_SCHL_NAME2 = S.UM_ADM_SCHL_NAME2,
    T.UM_ADM_SCHL_LOC2 = S.UM_ADM_SCHL_LOC2,
    T.UM_HIGH_S_OR_GED = S.UM_HIGH_S_OR_GED,
    T.UM_COUNSELOR_PHONE = S.UM_COUNSELOR_PHONE,
    T.UM_HS_CNTRY1 = S.UM_HS_CNTRY1,
    T.UM_HS_DEGREE = S.UM_HS_DEGREE,
    T.UM_HS_DEGYR1 = S.UM_HS_DEGYR1,
    T.UM_HS_FROM_DT1 = S.UM_HS_FROM_DT1,
    T.UM_HS_MONTH1 = S.UM_HS_MONTH1,
    T.UM_HS_NAME = S.UM_HS_NAME,
    T.UM_HS_STATE1 = S.UM_HS_STATE1,
    T.UM_HS_TO_DT1 = S.UM_HS_TO_DT1,
    T.UM_HS_TXT = S.UM_HS_TXT,
    T.UM_ADM_PREP_CAREER = S.UM_ADM_PREP_CAREER,
    T.YEAR = S.YEAR,
    T.UM_LAW_JT_MBA_OPTN = S.UM_LAW_JT_MBA_OPTN,
    T.UM_LAW1 = S.UM_LAW1,
    T.UM_LAW2 = S.UM_LAW2,
    T.UM_LAW3 = S.UM_LAW3,
    T.UM_INTL_MAIL_ZIP = S.UM_INTL_MAIL_ZIP,
    T.UM_BUSN_PHONE2 = S.UM_BUSN_PHONE2,
    T.UM_COURSE_6_CREDIT = S.UM_COURSE_6_CREDIT,
    T.UM_COURSE_6_ID = S.UM_COURSE_6_ID,
    T.UM_COURSE_6_TITLE = S.UM_COURSE_6_TITLE,
    T.UM_CELL = S.UM_CELL,
    T.UM_DISCIPLINE = S.UM_DISCIPLINE,
    T.UM_FELONY = S.UM_FELONY,
    T.UM_ADM_RA_TA = S.UM_ADM_RA_TA,
    T.UM_ADM_MAJ1_DESCR = S.UM_ADM_MAJ1_DESCR,
    T.UM_ADM_MAJ2_DESCR = S.UM_ADM_MAJ2_DESCR,
    T.UM_CSCE_NURSE_LIC = S.UM_CSCE_NURSE_LIC,
    T.UM_REF_POSTED_SEQ = S.UM_REF_POSTED_SEQ,
    T.UM_ADM_APPL_WAIVER = S.UM_ADM_APPL_WAIVER,
    T.UM_PAY_WITH_CC = S.UM_PAY_WITH_CC,
    T.UM_ADM_WAIVER_OPTN = S.UM_ADM_WAIVER_OPTN,
    T.UM_ADM_PAY_BY_CHK = S.UM_ADM_PAY_BY_CHK,
    T.ADM_APPL_COMPLETE = S.ADM_APPL_COMPLETE,
    T.UM_CS_REQUEST_ID = S.UM_CS_REQUEST_ID,
    T.UM_REQUEST_ID = S.UM_REQUEST_ID,
    T.UM_ADM_PAY_STS = S.UM_ADM_PAY_STS,
    T.UM_CYBERSRC_ERR_CD = S.UM_CYBERSRC_ERR_CD,
    T.UM_CYBERSRC_ERR_D = S.UM_CYBERSRC_ERR_D,
    T.ADM_APPL_METHOD = S.ADM_APPL_METHOD,
    T.AMOUNT = S.AMOUNT,
    T.UM_ADM_APP_SIG = S.UM_ADM_APP_SIG,
    T.UM_ADM_APP_SIG_DT = S.UM_ADM_APP_SIG_DT,
    T.UM_ADM_APP_NAME = S.UM_ADM_APP_NAME,
    T.UM_ADM_LOWG_CLS = S.UM_ADM_LOWG_CLS,
    T.UM_ADM_DG_P_TEACH = S.UM_ADM_DG_P_TEACH,
    T.UM_ADM_DG_MAT_COM = S.UM_ADM_DG_MAT_COM,
    T.UM_ADM_DG_MAT_CONT = S.UM_ADM_DG_MAT_CONT,
    T.UM_ADM_DG_PORT_SM = S.UM_ADM_DG_PORT_SM,
    T.UM_ADM_DG_PORT_SR = S.UM_ADM_DG_PORT_SR,
    T.UM_ADM_DG_PH_STDNT = S.UM_ADM_DG_PH_STDNT,
    T.UM_ADM_DG_PH_STDN1 = S.UM_ADM_DG_PH_STDN1,
    T.UM_ADM_DG_ACKNOWLG = S.UM_ADM_DG_ACKNOWLG,
    T.UM_ADM_DG_ANALYTIC = S.UM_ADM_DG_ANALYTIC,
    T.UM_ADM_DG_BIOCHEM = S.UM_ADM_DG_BIOCHEM,
    T.UM_ADM_DG_COMPU = S.UM_ADM_DG_COMPU,
    T.UM_ADM_DG_ECOM_MTH = S.UM_ADM_DG_ECOM_MTH,
    T.UM_ADM_DG_ECOM_YR = S.UM_ADM_DG_ECOM_YR,
    T.UM_ADM_DG_INORGANI = S.UM_ADM_DG_INORGANI,
    T.UM_ADM_DG_ORGANIC = S.UM_ADM_DG_ORGANIC,
    T.UM_ADM_DG_MARINE = S.UM_ADM_DG_MARINE,
    T.UM_ADM_DG_POLYMER = S.UM_ADM_DG_POLYMER,
    T.UM_ADM_DG_PHYSICAL = S.UM_ADM_DG_PHYSICAL,
    T.UM_ADM_DG_UNDECID = S.UM_ADM_DG_UNDECID,
    T.UM_ADM_G_CHEM_YR = S.UM_ADM_G_CHEM_YR,
    T.UM_ADM_G_CHEM_CR = S.UM_ADM_G_CHEM_CR,
    T.UM_ADM_G_CHEM_GR = S.UM_ADM_G_CHEM_GR,
    T.UM_ADM_A_CHEM_YR = S.UM_ADM_A_CHEM_YR,
    T.UM_ADM_A_CHEM_CR = S.UM_ADM_A_CHEM_CR,
    T.UM_ADM_A_CHEM_GR = S.UM_ADM_A_CHEM_GR,
    T.UM_ADM_AI_CHEM_YR = S.UM_ADM_AI_CHEM_YR,
    T.UM_ADM_AI_CHEM_CR = S.UM_ADM_AI_CHEM_CR,
    T.UM_ADM_AI_CHEM_GR = S.UM_ADM_AI_CHEM_GR,
    T.UM_ADM_OR_CHEM1_YR = S.UM_ADM_OR_CHEM1_YR,
    T.UM_ADM_OR_CHEM1_CR = S.UM_ADM_OR_CHEM1_CR,
    T.UM_ADM_OR_CHEM1_GR = S.UM_ADM_OR_CHEM1_GR,
    T.UM_ADM_OR_CHEM2_YR = S.UM_ADM_OR_CHEM2_YR,
    T.UM_ADM_OR_CHEM2_CR = S.UM_ADM_OR_CHEM2_CR,
    T.UM_ADM_OR_CHEM2_GR = S.UM_ADM_OR_CHEM2_GR,
    T.UM_ADM_PHYSICS_YR = S.UM_ADM_PHYSICS_YR,
    T.UM_ADM_PHYSICS_CR = S.UM_ADM_PHYSICS_CR,
    T.UM_ADM_PHYSICS_GR = S.UM_ADM_PHYSICS_GR,
    T.UM_ADM_PHY_CHM1_YR = S.UM_ADM_PHY_CHM1_YR,
    T.UM_ADM_PHY_CHM1_CR = S.UM_ADM_PHY_CHM1_CR,
    T.UM_ADM_PHY_CHM1_GR = S.UM_ADM_PHY_CHM1_GR,
    T.UM_ADM_PHY_CHM2_YR = S.UM_ADM_PHY_CHM2_YR,
    T.UM_ADM_PHY_CHM2_CR = S.UM_ADM_PHY_CHM2_CR,
    T.UM_ADM_PHY_CHM2_GR = S.UM_ADM_PHY_CHM2_GR,
    T.UM_ADM_CALCULUS_YR = S.UM_ADM_CALCULUS_YR,
    T.UM_ADM_CALCULUS_CR = S.UM_ADM_CALCULUS_CR,
    T.UM_ADM_CALCULUS_GR = S.UM_ADM_CALCULUS_GR,
    T.UM_ADM_CHEM_E1_CRS = S.UM_ADM_CHEM_E1_CRS,
    T.UM_ADM_CHEM_E1_YR = S.UM_ADM_CHEM_E1_YR,
    T.UM_ADM_CHEM_E1_CR = S.UM_ADM_CHEM_E1_CR,
    T.UM_ADM_CHEM_E1_GR = S.UM_ADM_CHEM_E1_GR,
    T.UM_ADM_CHEM_E2_CRS = S.UM_ADM_CHEM_E2_CRS,
    T.UM_ADM_CHEM_E2_YR = S.UM_ADM_CHEM_E2_YR,
    T.UM_ADM_CHEM_E2_CR = S.UM_ADM_CHEM_E2_CR,
    T.UM_ADM_CHEM_E2_GR = S.UM_ADM_CHEM_E2_GR,
    T.UM_ADM_CHEM_E3_CRS = S.UM_ADM_CHEM_E3_CRS,
    T.UM_ADM_CHEM_E3_YR = S.UM_ADM_CHEM_E3_YR,
    T.UM_ADM_CHEM_E3_CR = S.UM_ADM_CHEM_E3_CR,
    T.UM_ADM_CHEM_E3_GR = S.UM_ADM_CHEM_E3_GR,
    T.UM_ADM_CHEM_E4_CRS = S.UM_ADM_CHEM_E4_CRS,
    T.UM_ADM_CHEM_E4_YR = S.UM_ADM_CHEM_E4_YR,
    T.UM_ADM_CHEM_E4_CR = S.UM_ADM_CHEM_E4_CR,
    T.UM_ADM_CHEM_E4_GR = S.UM_ADM_CHEM_E4_GR,
    T.UM_ADM_DG_CONCENTR = S.UM_ADM_DG_CONCENTR,
    T.UM_ADM_DG_ELEM_SCH = S.UM_ADM_DG_ELEM_SCH,
    T.UM_ADM_DG_COMM_MTH = S.UM_ADM_DG_COMM_MTH,
    T.UM_ADM_DG_COMM_YR = S.UM_ADM_DG_COMM_YR,
    T.UM_ADM_DG_CONT_YR = S.UM_ADM_DG_CONT_YR,
    T.UM_ADM_DG_CONT_MTH = S.UM_ADM_DG_CONT_MTH,
    T.UM_ADM_DG_FOUN_MTH = S.UM_ADM_DG_FOUN_MTH,
    T.UM_ADM_DG_FOUN_YR = S.UM_ADM_DG_FOUN_YR,
    T.UM_ADM_DG_MIDL_SCH = S.UM_ADM_DG_MIDL_SCH,
    T.UM_ADM_DG_SUBJ_MTH = S.UM_ADM_DG_SUBJ_MTH,
    T.UM_ADM_DG_SUBJ_YR = S.UM_ADM_DG_SUBJ_YR,
    T.UM_ADM_DG_N_APPLIC = S.UM_ADM_DG_N_APPLIC,
    T.UM_ADM_DG_MATP_ACK = S.UM_ADM_DG_MATP_ACK,
    T.UM_ADM_BG_EDC_LIC1 = S.UM_ADM_BG_EDC_LIC1,
    T.UM_ADM_BG_EDC_LIC2 = S.UM_ADM_BG_EDC_LIC2,
    T.UM_ADM_BG_EDC_LIC3 = S.UM_ADM_BG_EDC_LIC3,
    T.UM_ADM_BG_ADMIN_L = S.UM_ADM_BG_ADMIN_L,
    T.UM_ADM_BG_OTH_LIC = S.UM_ADM_BG_OTH_LIC,
    T.UM_ADM_BG_GRD_DGR = S.UM_ADM_BG_GRD_DGR,
    T.UM_ADM_BG_CERT_NP = S.UM_ADM_BG_CERT_NP,
    T.UM_ADM_BG_ADULT_NP = S.UM_ADM_BG_ADULT_NP,
    T.UM_ADM_BG_PEDI_NP = S.UM_ADM_BG_PEDI_NP,
    T.UM_ADM_BG_FACULTY1 = S.UM_ADM_BG_FACULTY1,
    T.UM_ADM_BG_FACULTY2 = S.UM_ADM_BG_FACULTY2,
    T.UM_ADM_BG_FACULTY3 = S.UM_ADM_BG_FACULTY3,
    T.UM_ADM_BG_CAR_GOAL = S.UM_ADM_BG_CAR_GOAL,
    T.UM_ADM_BG_CAR_OTH = S.UM_ADM_BG_CAR_OTH,
    T.UM_ADM_BG_DEGR_IN = S.UM_ADM_BG_DEGR_IN,
    T.UM_ADM_BG_GRAD_SER = S.UM_ADM_BG_GRAD_SER,
    T.UM_ADM_BG_SERV_HRS = S.UM_ADM_BG_SERV_HRS,
    T.UM_ADM_BG_ON_CAMP = S.UM_ADM_BG_ON_CAMP,
    T.UM_ADM_BG_ONLINE = S.UM_ADM_BG_ONLINE,
    T.UM_ADM_DU_ONLINE = S.UM_ADM_DU_ONLINE,
    T.UM_ADM_DU_CERT = S.UM_ADM_DU_CERT,
    T.UM_ADM_BG_RESEARCH = S.UM_ADM_BG_RESEARCH,
    T.UM_LOWU_CLINICAL_1 = S.UM_LOWU_CLINICAL_1,
    T.UM_ADM_BG_ADVISOR1 = S.UM_ADM_BG_ADVISOR1,
    T.UM_ADM_BG_ADVISOR2 = S.UM_ADM_BG_ADVISOR2,
    T.UM_ADM_BG_ADVISOR3 = S.UM_ADM_BG_ADVISOR3,
    T.UM_ADM_BG_SPEC_ED = S.UM_ADM_BG_SPEC_ED,
    T.UM_ADM_UMB_Z_AD_ON = S.UM_ADM_UMB_Z_AD_ON,
    T.UM_ADM_UMB_Z_F_EMP = S.UM_ADM_UMB_Z_F_EMP,
    T.UM_ADM_UMB_Z_FAM = S.UM_ADM_UMB_Z_FAM,
    T.UM_ADM_UMB_Z_OTHER = S.UM_ADM_UMB_Z_OTHER,
    T.UM_ADM_UMB_Z_PRINT = S.UM_ADM_UMB_Z_PRINT,
    T.UM_ADM_UMB_Z_RADIO = S.UM_ADM_UMB_Z_RADIO,
    T.UM_ADM_UMB_Z_TEXT = S.UM_ADM_UMB_Z_TEXT,
    T.UM_ADM_UMB_Z_TV = S.UM_ADM_UMB_Z_TV,
    T.UM_ADM_UMB_Z_WEB = S.UM_ADM_UMB_Z_WEB,
    T.UM_ADM_DU_UNIV_EXT = S.UM_ADM_DU_UNIV_EXT,
    T.UM_ADM_LG_AREA_INT = S.UM_ADM_LG_AREA_INT,
    T.UM_ADM_DU_DAY = S.UM_ADM_DU_DAY,
    T.UM_ADM_DU_NIGHT = S.UM_ADM_DU_NIGHT,
    T.UM_ADM_BG_SOC_PHD = S.UM_ADM_BG_SOC_PHD,
    T.UM_ADM_BG_BPE_QUES = S.UM_ADM_BG_BPE_QUES,
    T.UM_ADM_PAY_REDO_DT = S.UM_ADM_PAY_REDO_DT,
    T.UM_ADM_PAY_CMPL_DT = S.UM_ADM_PAY_CMPL_DT,
    T.UM_ADM_BG_DISB_LIC = S.UM_ADM_BG_DISB_LIC,
    T.UM_ADM_BG_MASTERS = S.UM_ADM_BG_MASTERS,
    T.UM_ADM_BG_LICENS = S.UM_ADM_BG_LICENS,
    T.UM_ADM_PARTNERSHIP = S.UM_ADM_PARTNERSHIP,
    T.UM_BOSG_FLEX_MBA = S.UM_BOSG_FLEX_MBA,
    T.UM_BOSG_PRO_MBA = S.UM_BOSG_PRO_MBA,
    T.UM_BOSG_ACCEL_MAST = S.UM_BOSG_ACCEL_MAST,
    T.UM_ADM_LU_DIAGN_DT = S.UM_ADM_LU_DIAGN_DT,
    T.UM_ADM_LU_MUS_AUD = S.UM_ADM_LU_MUS_AUD,
	T.UM_ADM_HEAR_US_CD = S.UM_ADM_HEAR_US_CD,
	T.UM_ADM_HEAR_TEXT = S.UM_ADM_HEAR_TEXT,
	T.UM_ENG_AS_SEC_LNG = S.UM_ENG_AS_SEC_LNG,
    T.UM_ADM_DG_PARTTIME = S.UM_ADM_DG_PARTTIME,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.EMPLID <> S.EMPLID or 
    T.ADM_APPL_NBR <> S.ADM_APPL_NBR or 
    T.UM_ADM_EMAIL <> S.UM_ADM_EMAIL or 
    T.EMAILID <> S.EMAILID or 
    T.ACAD_CAREER <> S.ACAD_CAREER or 
    T.ADM_APPL_CTR <> S.ADM_APPL_CTR or 
    T.UM_ADM_SUB_DT <> S.UM_ADM_SUB_DT or 
    T.FIRST_NAME <> S.FIRST_NAME or 
    T.MIDDLE_NAME <> S.MIDDLE_NAME or 
    T.LAST_NAME <> S.LAST_NAME or 
    T.UM_SSN <> S.UM_SSN or 
    nvl(trim(T.BIRTHDATE),0) <> nvl(trim(S.BIRTHDATE),0) or 
    T.SEX <> S.SEX or 
    T.UM_US_CITIZEN <> S.UM_US_CITIZEN or 
    T.UM_CITIZNSHIP_STAT <> S.UM_CITIZNSHIP_STAT or 
    T.UM_BIRTH_CNTRY <> S.UM_BIRTH_CNTRY or 
    T.UM_ADM_NOT_MA_RES <> S.UM_ADM_NOT_MA_RES or 
    T.VISA_PERMIT_TYPE <> S.VISA_PERMIT_TYPE or 
    T.VISA_WRKPMT_STATUS <> S.VISA_WRKPMT_STATUS or 
    T.UM_VISA_WRKPMT_NBR <> S.UM_VISA_WRKPMT_NBR or 
    nvl(trim(T.UM_ADM_VISA_START),0) <> nvl(trim(S.UM_ADM_VISA_START),0) or 
    nvl(trim(T.UM_ADM_VISA_END),0) <> nvl(trim(S.UM_ADM_VISA_END),0) or 
    T.UM_SUFFIX <> S.UM_SUFFIX or 
    T.UM_PREFIX <> S.UM_PREFIX or 
    T.UM_FORMER_FNAME1 <> S.UM_FORMER_FNAME1 or 
    T.UM_FORMER_MNAME1 <> S.UM_FORMER_MNAME1 or 
    T.UM_FORMER_LNAME1 <> S.UM_FORMER_LNAME1 or 
    T.UM_PREF_FNAME1 <> S.UM_PREF_FNAME1 or 
    T.UM_PREF_MNAME1 <> S.UM_PREF_MNAME1 or 
    T.UM_PREF_LNAME1 <> S.UM_PREF_LNAME1 or 
    T.UM_PERM_ADDRESS1 <> S.UM_PERM_ADDRESS1 or 
    T.UM_PERM_ADDRESS2 <> S.UM_PERM_ADDRESS2 or 
    T.UM_PERM_ADDRESS3 <> S.UM_PERM_ADDRESS3 or 
    T.UM_PERM_CITY <> S.UM_PERM_CITY or 
    T.UM_PERM_STATE <> S.UM_PERM_STATE or 
    T.UM_PERM_POSTAL <> S.UM_PERM_POSTAL or 
    T.COUNTRY_CODE_PERM <> S.COUNTRY_CODE_PERM or 
    T.UM_PERM_COUNTRY <> S.UM_PERM_COUNTRY or 
    nvl(trim(T.UM_VALID_UNTIL),0) <> nvl(trim(S.UM_VALID_UNTIL),0) or 
    T.UM_MAIL_ADDRESS1 <> S.UM_MAIL_ADDRESS1 or 
    T.UM_MAIL_ADDRESS2 <> S.UM_MAIL_ADDRESS2 or 
    T.UM_MAIL_ADDRESS3 <> S.UM_MAIL_ADDRESS3 or 
    T.UM_MAIL_CITY <> S.UM_MAIL_CITY or 
    T.UM_MAIL_STATE <> S.UM_MAIL_STATE or 
    T.UM_MAIL_POSTAL <> S.UM_MAIL_POSTAL or 
    T.UM_MAIL_COUNTRY <> S.UM_MAIL_COUNTRY or 
    T.COUNTRY_CD <> S.COUNTRY_CD or 
    T.COUNTRY_CODE <> S.COUNTRY_CODE or 
    T.UM_PERM_PHONE <> S.UM_PERM_PHONE or 
    T.UM_PERM_PHONE1 <> S.UM_PERM_PHONE1 or 
    T.UM_PERM_PHONE2 <> S.UM_PERM_PHONE2 or 
    T.UM_PERM_PHONE3 <> S.UM_PERM_PHONE3 or 
    T.UM_CELL1 <> S.UM_CELL1 or 
    T.UM_CELL2 <> S.UM_CELL2 or 
    T.UM_CELL3 <> S.UM_CELL3 or 
    T.UM_BUSN_CNTRY_CD <> S.UM_BUSN_CNTRY_CD or 
    T.UM_BUSN_PHONE <> S.UM_BUSN_PHONE or 
    T.UM_BUSN_PHONE1 <> S.UM_BUSN_PHONE1 or 
    T.UM_BUSN_PHONE3 <> S.UM_BUSN_PHONE3 or 
    T.UM_BUSN_EXTENSION <> S.UM_BUSN_EXTENSION or 
    T.UM_ADM_SESSION <> S.UM_ADM_SESSION or 
    T.ADMIT_TYPE <> S.ADMIT_TYPE or 
    T.UM_ADM_SUBFIELD <> S.UM_ADM_SUBFIELD or 
    T.UM_PRG_PLN_SBPLN <> S.UM_PRG_PLN_SBPLN or 
    T.UM_PRG_PLN_SBPLN2 <> S.UM_PRG_PLN_SBPLN2 or 
    T.UM_ACAD_PROG <> S.UM_ACAD_PROG or 
    T.UM_ACAD_PLAN <> S.UM_ACAD_PLAN or 
    T.UM_ACAD_SUB_PLAN <> S.UM_ACAD_SUB_PLAN or 
    T.UM_ACAD_PROG1 <> S.UM_ACAD_PROG1 or 
    T.UM_ACAD_PLAN1 <> S.UM_ACAD_PLAN1 or 
    T.UM_ACAD_SUB_PLAN1 <> S.UM_ACAD_SUB_PLAN1 or 
    T.UM_ADM_APP_FIN_AID <> S.UM_ADM_APP_FIN_AID or 
    T.UM_GRE_MO <> S.UM_GRE_MO or 
    T.UM_GRE_YR <> S.UM_GRE_YR or 
    T.UM_GRE_VERB <> S.UM_GRE_VERB or 
    T.UM_GRE_QUAN <> S.UM_GRE_QUAN or 
    T.UM_GRE_ANAL <> S.UM_GRE_ANAL or 
    T.UM_GRE_SUBJ_SC <> S.UM_GRE_SUBJ_SC or 
    T.UM_GRE_SUBJ_MO <> S.UM_GRE_SUBJ_MO or 
    T.UM_GRE_SUBJ_YR <> S.UM_GRE_SUBJ_YR or 
    T.UM_GRE_SUBJECT <> S.UM_GRE_SUBJECT or 
    T.UM_GMAT_MO <> S.UM_GMAT_MO or 
    T.UM_GMAT_YR <> S.UM_GMAT_YR or 
    T.UM_GMAT_SC <> S.UM_GMAT_SC or 
    T.UM_TOEFL_MO <> S.UM_TOEFL_MO or 
    T.UM_TOEFL_YR <> S.UM_TOEFL_YR or 
    T.UM_TOEFL_SC <> S.UM_TOEFL_SC or 
    T.UM_TEST1_MO <> S.UM_TEST1_MO or 
    T.UM_TEST1_NAME <> S.UM_TEST1_NAME or 
    T.UM_TEST1_SC <> S.UM_TEST1_SC or 
    T.UM_TEST1_YR <> S.UM_TEST1_YR or 
    T.UM_ADM_APP_GPA_ALL <> S.UM_ADM_APP_GPA_ALL or 
    T.UM_ADM_APP_GPA_GRD <> S.UM_ADM_APP_GPA_GRD or 
    T.UM_ADM_APP_GPA_MAJ <> S.UM_ADM_APP_GPA_MAJ or 
    T.UM_ADM_APP_GPA_TWO <> S.UM_ADM_APP_GPA_TWO or 
    T.UM_ACT1_MO <> S.UM_ACT1_MO or 
    T.UM_ACT1_SC <> S.UM_ACT1_SC or 
    T.UM_ACT1_YR <> S.UM_ACT1_YR or 
    T.UM_ACT2_MO <> S.UM_ACT2_MO or 
    T.UM_ACT2_SC <> S.UM_ACT2_SC or 
    T.UM_ACT2_YR <> S.UM_ACT2_YR or 
    T.UM_ACT3_MO <> S.UM_ACT3_MO or 
    T.UM_ACT3_SC <> S.UM_ACT3_SC or 
    T.UM_ACT3_YR <> S.UM_ACT3_YR or 
    T.UM_SAT1_MO <> S.UM_SAT1_MO or 
    T.UM_SAT1_SC <> S.UM_SAT1_SC or 
    T.UM_SAT1_YR <> S.UM_SAT1_YR or 
    T.UM_SAT2_MO <> S.UM_SAT2_MO or 
    T.UM_SAT2_SC <> S.UM_SAT2_SC or 
    T.UM_SAT2_YR <> S.UM_SAT2_YR or 
    T.UM_SAT3_MO <> S.UM_SAT3_MO or 
    T.UM_SAT3_SC <> S.UM_SAT3_SC or 
    T.UM_SAT3_YR <> S.UM_SAT3_YR or 
    T.UM_TOEFL1_MO <> S.UM_TOEFL1_MO or 
    T.UM_TOEFL1_SC <> S.UM_TOEFL1_SC or 
    T.UM_TOEFL1_YR <> S.UM_TOEFL1_YR or 
    T.UM_TOEFL2_MO <> S.UM_TOEFL2_MO or 
    T.UM_TOEFL2_SC <> S.UM_TOEFL2_SC or 
    T.UM_TOEFL2_YR <> S.UM_TOEFL2_YR or 
    T.UM_TOEFL3_MO <> S.UM_TOEFL3_MO or 
    T.UM_TOEFL3_SC <> S.UM_TOEFL3_SC or 
    T.UM_TOEFL3_YR <> S.UM_TOEFL3_YR or 
    T.UM_IELTS1_MO <> S.UM_IELTS1_MO or 
    T.UM_IELTS1_SC <> S.UM_IELTS1_SC or 
    T.UM_IELTS1_YR <> S.UM_IELTS1_YR or 
    T.UM_IELTS2_MO <> S.UM_IELTS2_MO or 
    T.UM_IELTS2_SC <> S.UM_IELTS2_SC or 
    T.UM_IELTS2_YR <> S.UM_IELTS2_YR or 
    T.UM_IELTS3_MO <> S.UM_IELTS3_MO or 
    T.UM_IELTS3_SC <> S.UM_IELTS3_SC or 
    T.UM_IELTS3_YR <> S.UM_IELTS3_YR or 
    T.UM_GMAT1_MO <> S.UM_GMAT1_MO or 
    T.UM_GMAT1_SC <> S.UM_GMAT1_SC or 
    T.UM_GMAT1_YR <> S.UM_GMAT1_YR or 
    T.UM_GMAT2_MO <> S.UM_GMAT2_MO or 
    T.UM_GMAT2_SC <> S.UM_GMAT2_SC or 
    T.UM_GMAT2_YR <> S.UM_GMAT2_YR or 
    T.UM_GMAT3_MO <> S.UM_GMAT3_MO or 
    T.UM_GMAT3_SC <> S.UM_GMAT3_SC or 
    T.UM_GMAT3_YR <> S.UM_GMAT3_YR or 
    T.UM_GRE_AW_MO <> S.UM_GRE_AW_MO or 
    T.UM_GRE_AW_SC <> S.UM_GRE_AW_SC or 
    T.UM_GRE_AW_YR <> S.UM_GRE_AW_YR or 
    T.UM_GRE_Q_MO <> S.UM_GRE_Q_MO or 
    T.UM_GRE_Q_SC <> S.UM_GRE_Q_SC or 
    T.UM_GRE_Q_YR <> S.UM_GRE_Q_YR or 
    T.UM_GRE_V_MO <> S.UM_GRE_V_MO or 
    T.UM_GRE_V_SC <> S.UM_GRE_V_SC or 
    T.UM_GRE_V_YR <> S.UM_GRE_V_YR or 
    T.UM_LSAT1_MO <> S.UM_LSAT1_MO or 
    T.UM_LSAT1_SC <> S.UM_LSAT1_SC or 
    T.UM_LSAT1_YR <> S.UM_LSAT1_YR or 
    T.UM_LSAT2_MO <> S.UM_LSAT2_MO or 
    T.UM_LSAT2_SC <> S.UM_LSAT2_SC or 
    T.UM_LSAT2_YR <> S.UM_LSAT2_YR or 
    T.UM_LSAT3_MO <> S.UM_LSAT3_MO or 
    T.UM_LSAT3_SC <> S.UM_LSAT3_SC or 
    T.UM_LSAT3_YR <> S.UM_LSAT3_YR or 
    T.UM_COURSE_1_CREDIT <> S.UM_COURSE_1_CREDIT or 
    T.UM_COURSE_1_ID <> S.UM_COURSE_1_ID or 
    T.UM_COURSE_1_TITLE <> S.UM_COURSE_1_TITLE or 
    T.UM_COURSE_2_CREDIT <> S.UM_COURSE_2_CREDIT or 
    T.UM_COURSE_2_ID <> S.UM_COURSE_2_ID or 
    T.UM_COURSE_2_TITLE <> S.UM_COURSE_2_TITLE or 
    T.UM_COURSE_3_CREDIT <> S.UM_COURSE_3_CREDIT or 
    T.UM_COURSE_3_ID <> S.UM_COURSE_3_ID or 
    T.UM_COURSE_3_TITLE <> S.UM_COURSE_3_TITLE or 
    T.UM_COURSE_4_CREDIT <> S.UM_COURSE_4_CREDIT or 
    T.UM_COURSE_4_ID <> S.UM_COURSE_4_ID or 
    T.UM_COURSE_4_TITLE <> S.UM_COURSE_4_TITLE or 
    T.UM_COURSE_5_CREDIT <> S.UM_COURSE_5_CREDIT or 
    T.UM_COURSE_5_ID <> S.UM_COURSE_5_ID or 
    T.UM_COURSE_5_TITLE <> S.UM_COURSE_5_TITLE or 
    T.MILITARY_STATUS <> S.MILITARY_STATUS or 
    T.UM_ETHNIC <> S.UM_ETHNIC or 
    T.UM_EMPLOY_STATE <> S.UM_EMPLOY_STATE or 
    T.UM_LAW4 <> S.UM_LAW4 or 
    T.UM_ADM_FAC_FNAME <> S.UM_ADM_FAC_FNAME or 
    T.UM_ADM_FAC_LNAME <> S.UM_ADM_FAC_LNAME or 
    T.UM_LAW5 <> S.UM_LAW5 or 
    T.UM_ADM_APP_MO_FR2 <> S.UM_ADM_APP_MO_FR2 or 
    T.UM_ADM_APP_MO_FR3 <> S.UM_ADM_APP_MO_FR3 or 
    T.UM_ADM_APP_MO_FR4 <> S.UM_ADM_APP_MO_FR4 or 
    T.UM_ADM_APP_MO_TO2 <> S.UM_ADM_APP_MO_TO2 or 
    T.UM_ADM_APP_MO_TO3 <> S.UM_ADM_APP_MO_TO3 or 
    T.UM_ADM_APP_MO_TO4 <> S.UM_ADM_APP_MO_TO4 or 
    T.UM_HS_ID <> S.UM_HS_ID or 
    T.UM_ACAD_LOAD_APPR <> S.UM_ACAD_LOAD_APPR or 
    T.UM_ADM_ONLINE_DEGR <> S.UM_ADM_ONLINE_DEGR or 
    T.UM_ADM_MUSIC_INST1 <> S.UM_ADM_MUSIC_INST1 or 
    T.UM_PRV1_ID <> S.UM_PRV1_ID or 
    T.UM_PRV2_ID <> S.UM_PRV2_ID or 
    T.UM_PRV3_ID <> S.UM_PRV3_ID or 
    T.UM_PRV4_ID <> S.UM_PRV4_ID or 
    T.UM_PHONE_TYPE <> S.UM_PHONE_TYPE or 
    T.UM_INTL_ZIP <> S.UM_INTL_ZIP or 
    T.UM_ADM_APP_MO_FR1 <> S.UM_ADM_APP_MO_FR1 or 
    T.UM_ADM_APP_MO_TO1 <> S.UM_ADM_APP_MO_TO1 or 
    T.UM_ADM_APP_TITLE1 <> S.UM_ADM_APP_TITLE1 or 
    T.UM_ADM_APP_YR_FR1 <> S.UM_ADM_APP_YR_FR1 or 
    T.UM_ADM_APP_YR_TO1 <> S.UM_ADM_APP_YR_TO1 or 
    T.UM_ADM_APP_POS1 <> S.UM_ADM_APP_POS1 or 
    T.UM_ADM_APP_POS2 <> S.UM_ADM_APP_POS2 or 
    T.UM_ADM_APP_POS3 <> S.UM_ADM_APP_POS3 or 
    T.UM_ADM_APP_ADDR1 <> S.UM_ADM_APP_ADDR1 or 
    T.UM_ADM_APP_ADDR2 <> S.UM_ADM_APP_ADDR2 or 
    T.UM_ADM_APP_ADDR3 <> S.UM_ADM_APP_ADDR3 or 
    T.UM_ADM_APP_NAM1 <> S.UM_ADM_APP_NAM1 or 
    T.UM_ADM_APP_NAM2 <> S.UM_ADM_APP_NAM2 or 
    T.UM_ADM_APP_NAM3 <> S.UM_ADM_APP_NAM3 or 
    T.UM_ADM_APP_CNTRY1 <> S.UM_ADM_APP_CNTRY1 or 
    T.UM_ADM_APP_CNTRY2 <> S.UM_ADM_APP_CNTRY2 or 
    T.UM_ADM_APP_CNTRY3 <> S.UM_ADM_APP_CNTRY3 or 
    T.UM_ADM_APP_CNTRY4 <> S.UM_ADM_APP_CNTRY4 or 
    T.UM_ADM_APP_STATE1 <> S.UM_ADM_APP_STATE1 or 
    T.UM_ADM_APP_STATE2 <> S.UM_ADM_APP_STATE2 or 
    T.UM_ADM_APP_STATE3 <> S.UM_ADM_APP_STATE3 or 
    T.UM_ADM_APP_STATE4 <> S.UM_ADM_APP_STATE4 or 
    T.UM_ADM_APP_PR_TRM <> S.UM_ADM_APP_PR_TRM or 
    T.UM_ADM_APP_EN_TRM <> S.UM_ADM_APP_EN_TRM or 
    T.UM_ADM_APP_PRIOR <> S.UM_ADM_APP_PRIOR or 
    T.UM_ADM_APP_ENRL <> S.UM_ADM_APP_ENRL or 
    T.UM_ADM_APP_CAR1 <> S.UM_ADM_APP_CAR1 or 
    T.UM_ADM_APP_CAR2 <> S.UM_ADM_APP_CAR2 or 
    T.UM_ADM_APP_CAR3 <> S.UM_ADM_APP_CAR3 or 
    T.UM_ADM_APP_CAR4 <> S.UM_ADM_APP_CAR4 or 
    T.UM_ADM_APP_DEG1 <> S.UM_ADM_APP_DEG1 or 
    T.UM_ADM_APP_DEG2 <> S.UM_ADM_APP_DEG2 or 
    T.UM_ADM_APP_DEG3 <> S.UM_ADM_APP_DEG3 or 
    T.UM_ADM_APP_DEG4 <> S.UM_ADM_APP_DEG4 or 
    T.UM_ADM_APP_DEGYR1 <> S.UM_ADM_APP_DEGYR1 or 
    T.UM_ADM_APP_DEGYR2 <> S.UM_ADM_APP_DEGYR2 or 
    T.UM_ADM_APP_DEGYR3 <> S.UM_ADM_APP_DEGYR3 or 
    T.UM_ADM_APP_DEGYR4 <> S.UM_ADM_APP_DEGYR4 or 
    T.UM_ADM_APP_FROM1 <> S.UM_ADM_APP_FROM1 or 
    T.UM_ADM_APP_FROM2 <> S.UM_ADM_APP_FROM2 or 
    T.UM_ADM_APP_FROM3 <> S.UM_ADM_APP_FROM3 or 
    T.UM_ADM_APP_FROM4 <> S.UM_ADM_APP_FROM4 or 
    T.UM_ADM_APP_MONTH1 <> S.UM_ADM_APP_MONTH1 or 
    T.UM_ADM_APP_MONTH2 <> S.UM_ADM_APP_MONTH2 or 
    T.UM_ADM_APP_MONTH3 <> S.UM_ADM_APP_MONTH3 or 
    T.UM_ADM_APP_MONTH4 <> S.UM_ADM_APP_MONTH4 or 
    T.UM_ADM_APP_PRV1 <> S.UM_ADM_APP_PRV1 or 
    T.UM_ADM_APP_PRV2 <> S.UM_ADM_APP_PRV2 or 
    T.UM_ADM_APP_PRV3 <> S.UM_ADM_APP_PRV3 or 
    T.UM_ADM_APP_PRV4 <> S.UM_ADM_APP_PRV4 or 
    T.UM_ADM_APP_TO1 <> S.UM_ADM_APP_TO1 or 
    T.UM_ADM_APP_TO2 <> S.UM_ADM_APP_TO2 or 
    T.UM_ADM_APP_TO3 <> S.UM_ADM_APP_TO3 or 
    T.UM_ADM_APP_TO4 <> S.UM_ADM_APP_TO4 or 
    T.UM_ADM_ASSIST <> S.UM_ADM_ASSIST or 
    T.UM_ADM_FELLOW <> S.UM_ADM_FELLOW or 
    T.MBA_ABITUS <> S.MBA_ABITUS or 
    T.MBA_CURRENT_STUDNT <> S.MBA_CURRENT_STUDNT or 
    T.MBA_DAY <> S.MBA_DAY or 
    T.MBA_EXCLSVE_ONLINE <> S.MBA_EXCLSVE_ONLINE or 
    T.MBA_WRK_PROFESNLS <> S.MBA_WRK_PROFESNLS or 
    T.CONTACT_NAME <> S.CONTACT_NAME or 
    T.UM_EMERG_COUNTRY <> S.UM_EMERG_COUNTRY or 
    T.UM_ADM_REL_TYPE <> S.UM_ADM_REL_TYPE or 
    T.UM_EMERG_CNTRY_CD <> S.UM_EMERG_CNTRY_CD or 
    T.UM_EMERG_PHONE <> S.UM_EMERG_PHONE or 
    T.CONTACT_PHONE <> S.CONTACT_PHONE or 
    T.CONTACT_PHONE_EXT <> S.CONTACT_PHONE_EXT or 
    T.UM_PARENT_NAME <> S.UM_PARENT_NAME or 
    T.UM_PARENT_ADDR1 <> S.UM_PARENT_ADDR1 or 
    T.UM_PARENT_ADDR2 <> S.UM_PARENT_ADDR2 or 
    T.UM_PARENT_ADDR3 <> S.UM_PARENT_ADDR3 or 
    T.UM_PARENT_CITY <> S.UM_PARENT_CITY or 
    T.UM_PARENT_STATE <> S.UM_PARENT_STATE or 
    T.UM_PARENT_COUNTRY <> S.UM_PARENT_COUNTRY or 
    T.UM_PARENT_PHONE <> S.UM_PARENT_PHONE or 
    T.UM_PARENT_CNTRY_CD <> S.UM_PARENT_CNTRY_CD or 
    T.UM_PARENT_PHONE1 <> S.UM_PARENT_PHONE1 or 
    T.UM_PARENT_PHONE2 <> S.UM_PARENT_PHONE2 or 
    T.UM_PARENT_PHONE3 <> S.UM_PARENT_PHONE3 or 
    T.UM_PARENT_TYPE <> S.UM_PARENT_TYPE or 
    T.ALUMNI_EVER <> S.ALUMNI_EVER or 
    T.HIGHEST_EDUC_LVL <> S.HIGHEST_EDUC_LVL or 
    T.UM_PARENT2_NAME <> S.UM_PARENT2_NAME or 
    T.UM_PARENT2_ADDR1 <> S.UM_PARENT2_ADDR1 or 
    T.UM_PARENT2_ADDR2 <> S.UM_PARENT2_ADDR2 or 
    T.UM_PARENT2_ADDR3 <> S.UM_PARENT2_ADDR3 or 
    T.UM_PARENT2_CITY <> S.UM_PARENT2_CITY or 
    T.UM_PARENT2_STATE <> S.UM_PARENT2_STATE or 
    T.UM_PARENT2_POSTAL <> S.UM_PARENT2_POSTAL or 
    T.UM_PARENT2_INT_ZIP <> S.UM_PARENT2_INT_ZIP or 
    T.UM_PARENT2_COUNTRY <> S.UM_PARENT2_COUNTRY or 
    T.UM_PARENT_CNTRY_C2 <> S.UM_PARENT_CNTRY_C2 or 
    T.UM_PARENT2_PHONE <> S.UM_PARENT2_PHONE or 
    T.UM_PARENT2_PHONE1 <> S.UM_PARENT2_PHONE1 or 
    T.UM_PARENT2_PHONE2 <> S.UM_PARENT2_PHONE2 or 
    T.UM_PARENT2_PHONE3 <> S.UM_PARENT2_PHONE3 or 
    T.UM_PARENT2_TYPE <> S.UM_PARENT2_TYPE or 
    T.UM_ALUMNI_EVER_P2 <> S.UM_ALUMNI_EVER_P2 or 
    T.UM_HIGH_EDUCLVL_P2 <> S.UM_HIGH_EDUCLVL_P2 or 
    T.UM_GUARD_NAME <> S.UM_GUARD_NAME or 
    T.UM_GUARD_ADDR1 <> S.UM_GUARD_ADDR1 or 
    T.UM_GUARD_ADDR2 <> S.UM_GUARD_ADDR2 or 
    T.UM_GUARD_ADDR3 <> S.UM_GUARD_ADDR3 or 
    T.UM_GUARD_CITY <> S.UM_GUARD_CITY or 
    T.UM_GUARD_STATE <> S.UM_GUARD_STATE or 
    T.UM_GUARD_POSTAL <> S.UM_GUARD_POSTAL or 
    T.UM_GUARD_INT_ZIP <> S.UM_GUARD_INT_ZIP or 
    T.UM_GUARD_COUNTRY <> S.UM_GUARD_COUNTRY or 
    T.UM_GUARD_CNTRY_CD <> S.UM_GUARD_CNTRY_CD or 
    T.UM_GUARD_PHONE <> S.UM_GUARD_PHONE or 
    T.UM_GUARD_PHONE1 <> S.UM_GUARD_PHONE1 or 
    T.UM_GUARD_PHONE2 <> S.UM_GUARD_PHONE2 or 
    T.UM_GUARD_PHONE3 <> S.UM_GUARD_PHONE3 or 
    T.UM_MASS_RESIDENT <> S.UM_MASS_RESIDENT or 
    T.UM_ALUMNI_EVER_GUA <> S.UM_ALUMNI_EVER_GUA or 
    T.UM_HIGH_EDUCLVL_GU <> S.UM_HIGH_EDUCLVL_GU or 
    T.UM_CNTRY_CITIZENSH <> S.UM_CNTRY_CITIZENSH or 
    T.UM_BIRTHPLACE <> S.UM_BIRTHPLACE or 
    T.UM_ADM_ENGINEERING <> S.UM_ADM_ENGINEERING or 
    T.UM_ADM_O_ENGINEER <> S.UM_ADM_O_ENGINEER or 
    T.UM_ADM_E_DIS_NAME <> S.UM_ADM_E_DIS_NAME or 
    T.UM_ADM_SCIENCE_DIS <> S.UM_ADM_SCIENCE_DIS or 
    T.UM_ADM_S_DIS_NAME <> S.UM_ADM_S_DIS_NAME or 
    T.UM_COUNSELOR_FNAME <> S.UM_COUNSELOR_FNAME or 
    T.UM_COUNSELOR_LNAME <> S.UM_COUNSELOR_LNAME or 
    T.UM_COUNSELOR_EMAIL <> S.UM_COUNSELOR_EMAIL or 
    T.UM_ADM_APP_5YR <> S.UM_ADM_APP_5YR or 
    T.UM_ADM_EARLY_D <> S.UM_ADM_EARLY_D or 
    T.UM_ADM_REF1_FNAME <> S.UM_ADM_REF1_FNAME or 
    T.UM_ADM_REF1_LNAME <> S.UM_ADM_REF1_LNAME or 
    T.UM_ADM_REF1_MNAME <> S.UM_ADM_REF1_MNAME or 
    T.UM_ADM_REF2_FNAME <> S.UM_ADM_REF2_FNAME or 
    T.UM_ADM_REF2_LNAME <> S.UM_ADM_REF2_LNAME or 
    T.UM_ADM_REF2_MNAME <> S.UM_ADM_REF2_MNAME or 
    T.UM_ADM_REF3_FNAME <> S.UM_ADM_REF3_FNAME or 
    T.UM_ADM_REF3_LNAME <> S.UM_ADM_REF3_LNAME or 
    T.UM_ADM_REF3_MNAME <> S.UM_ADM_REF3_MNAME or 
    T.UM_REF_PRIVATE1 <> S.UM_REF_PRIVATE1 or 
    T.UM_REF_PRIVATE2 <> S.UM_REF_PRIVATE2 or 
    T.UM_REF_PRIVATE3 <> S.UM_REF_PRIVATE3 or 
    T.UM_ADM_BA_MASTER <> S.UM_ADM_BA_MASTER or 
    T.UM_ADM_UMB_TEACH <> S.UM_ADM_UMB_TEACH or 
    T.UM_ADM_CAR_SWITCH <> S.UM_ADM_CAR_SWITCH or 
    T.UM_ADM_UMB_MTEL <> S.UM_ADM_UMB_MTEL or 
    T.UM_ADM_UMB_VISION <> S.UM_ADM_UMB_VISION or 
    T.UM_ADM_NATL_CERTIF <> S.UM_ADM_NATL_CERTIF or 
    T.UM_ADM_CERTIFICATN <> S.UM_ADM_CERTIFICATN or 
    nvl(trim(T.UM_ADM_CERT_EXP_DT),0) <> nvl(trim(S.UM_ADM_CERT_EXP_DT),0) or 
    T.UM_ADM_CNOW_LOW_IN <> S.UM_ADM_CNOW_LOW_IN or 
    T.UM_ADM_CNOW_FRST_G <> S.UM_ADM_CNOW_FRST_G or 
    T.UM_ADM_CNOW_NOT_AP <> S.UM_ADM_CNOW_NOT_AP or 
    T.UM_ADM_ARCHELOGY <> S.UM_ADM_ARCHELOGY or 
    T.UM_ADM_SCHL_NAME <> S.UM_ADM_SCHL_NAME or 
    T.UM_ADM_SCHL_LOC <> S.UM_ADM_SCHL_LOC or 
    T.UM_ADM_PREV_BCKGRD <> S.UM_ADM_PREV_BCKGRD or 
    T.UM_ADM_NBR_MTHS <> S.UM_ADM_NBR_MTHS or 
    T.UM_ADM_INIT_LICNSE <> S.UM_ADM_INIT_LICNSE or 
    T.UM_ADM_LICNSE_DESR <> S.UM_ADM_LICNSE_DESR or 
    T.UM_ADM_TEACH_SUBJ <> S.UM_ADM_TEACH_SUBJ or 
    T.UM_ADM_NE_REGIONAL <> S.UM_ADM_NE_REGIONAL or 
    T.UM_ADM_NO_VISA <> S.UM_ADM_NO_VISA or 
    T.UM_PARENT_EMP_COLL <> S.UM_PARENT_EMP_COLL or 
    T.UM_PARENT_LIVING <> S.UM_PARENT_LIVING or 
    T.UM_PARENT_POSTAL <> S.UM_PARENT_POSTAL or 
    T.UM_PARENT_INT_ZIP <> S.UM_PARENT_INT_ZIP or 
    T.UM_PARENT_JOBTITLE <> S.UM_PARENT_JOBTITLE or 
    T.UM_PARENT_GRADSCHL <> S.UM_PARENT_GRADSCHL or 
    T.UM_ADM_SUCCESS_DEG <> S.UM_ADM_SUCCESS_DEG or 
    nvl(trim(T.UM_ADM_CRS_STR),0) <> nvl(trim(S.UM_ADM_CRS_STR),0) or 
    nvl(trim(T.UM_ADM_CRS_END),0) <> nvl(trim(S.UM_ADM_CRS_END),0) or 
    T.UM_ADM_PREV_APPLD <> S.UM_ADM_PREV_APPLD or 
    T.UM_PARENT_EMPLOYER <> S.UM_PARENT_EMPLOYER or 
    T.UM_PARENT_OCCUPTN <> S.UM_PARENT_OCCUPTN or 
    T.UM_PARENT_EMAIL <> S.UM_PARENT_EMAIL or 
    T.UM_GUARD_EMAIL <> S.UM_GUARD_EMAIL or 
    T.UM_GUARD_EMPLOYER <> S.UM_GUARD_EMPLOYER or 
    T.UM_GUARD_EMP_COLL <> S.UM_GUARD_EMP_COLL or 
    T.UM_GUARD_GRADSCHL <> S.UM_GUARD_GRADSCHL or 
    T.UM_GUARD_OCCUPTN <> S.UM_GUARD_OCCUPTN or 
    T.UM_GUARD_JOBTITLE <> S.UM_GUARD_JOBTITLE or 
    T.UM_GUARD_DEGREE <> S.UM_GUARD_DEGREE or 
    T.UM_GUARD_DEGREE_G <> S.UM_GUARD_DEGREE_G or 
    T.UM_PARENT2_DEGREE <> S.UM_PARENT2_DEGREE or 
    T.UM_PARENT_DEGREE <> S.UM_PARENT_DEGREE or 
    T.UM_PARENT_DEGREE_G <> S.UM_PARENT_DEGREE_G or 
    T.UM_ADM_RELIANT_FA <> S.UM_ADM_RELIANT_FA or 
    T.UM_PARENT_CEEB_G <> S.UM_PARENT_CEEB_G or 
    T.UM_GUARD_CEEB <> S.UM_GUARD_CEEB or 
    T.UM_PARENT2_EMAIL <> S.UM_PARENT2_EMAIL or 
    T.UM_PARENT2_EMPCOLL <> S.UM_PARENT2_EMPCOLL or 
    T.UM_PARENT2_EMPLOYR <> S.UM_PARENT2_EMPLOYR or 
    T.UM_PARENT2_GRADSCH <> S.UM_PARENT2_GRADSCH or 
    T.UM_PARENT2_JOBTITL <> S.UM_PARENT2_JOBTITL or 
    T.UM_PARENT2_OCCUPTN <> S.UM_PARENT2_OCCUPTN or 
    T.UM_PARENT2_LIVING <> S.UM_PARENT2_LIVING or 
    T.UM_ADM_CSCE_TUITN <> S.UM_ADM_CSCE_TUITN or 
    T.UM_ADM_CURR_EMP <> S.UM_ADM_CURR_EMP or 
    T.UM_ADM_CURR_JOB <> S.UM_ADM_CURR_JOB or 
    T.UM_ADM_LAW_SCHL1 <> S.UM_ADM_LAW_SCHL1 or 
    T.UM_ADM_LAW_SCHL2 <> S.UM_ADM_LAW_SCHL2 or 
    T.UM_ADM_LAW_SCHL3 <> S.UM_ADM_LAW_SCHL3 or 
    T.UM_ADM_LAW_SCHL4 <> S.UM_ADM_LAW_SCHL4 or 
    T.UM_ADM_LAW_3_3_PRG <> S.UM_ADM_LAW_3_3_PRG or 
    T.UM_ADM_LAW_ATTD_B4 <> S.UM_ADM_LAW_ATTD_B4 or 
    T.UM_ADM_LAW_JT_MBA <> S.UM_ADM_LAW_JT_MBA or 
    T.UM_ADM_LAW_PRV_APP <> S.UM_ADM_LAW_PRV_APP or 
    T.UM_ADM_LAW_SRV_ACC <> S.UM_ADM_LAW_SRV_ACC or 
    T.UM_GUARD_CEEB_G <> S.UM_GUARD_CEEB_G or 
    T.UM_PARENT2_CEEB <> S.UM_PARENT2_CEEB or 
    T.UM_PARENT2_CEEB_G <> S.UM_PARENT2_CEEB_G or 
    T.UM_PARENT_CEEB <> S.UM_PARENT_CEEB or 
    T.UM_PARENT_COLLEGE <> S.UM_PARENT_COLLEGE or 
    T.UM_GUARD_COLLEGE <> S.UM_GUARD_COLLEGE or 
    T.UM_PARENT2_COLLEGE <> S.UM_PARENT2_COLLEGE or 
    T.UM_PARENT2_DEGRE_G <> S.UM_PARENT2_DEGRE_G or 
    T.UM_ADM_RESID_HALL <> S.UM_ADM_RESID_HALL or 
    T.UM_ADM_HS_DUAL_ENR <> S.UM_ADM_HS_DUAL_ENR or 
    T.UM_ADM_SPORT <> S.UM_ADM_SPORT or 
    T.UM_ADM_HONORS_PRG <> S.UM_ADM_HONORS_PRG or 
    T.UM_ADM_PREV_CRSE <> S.UM_ADM_PREV_CRSE or 
    T.UM_ADM_MUSIC_INSTR <> S.UM_ADM_MUSIC_INSTR or 
    T.UM_ADM_EXCER_PRGM <> S.UM_ADM_EXCER_PRGM or 
    T.UM_ADM_MUSIC_ENSEM <> S.UM_ADM_MUSIC_ENSEM or 
    T.UM_ADM_BACH_PATHWY <> S.UM_ADM_BACH_PATHWY or 
    T.UM_ADM_UML_DISABLE <> S.UM_ADM_UML_DISABLE or 
    T.UM_ADM_SCHL_NAME2 <> S.UM_ADM_SCHL_NAME2 or 
    T.UM_ADM_SCHL_LOC2 <> S.UM_ADM_SCHL_LOC2 or 
    T.UM_HIGH_S_OR_GED <> S.UM_HIGH_S_OR_GED or 
    T.UM_COUNSELOR_PHONE <> S.UM_COUNSELOR_PHONE or 
    T.UM_HS_CNTRY1 <> S.UM_HS_CNTRY1 or 
    T.UM_HS_DEGREE <> S.UM_HS_DEGREE or 
    T.UM_HS_DEGYR1 <> S.UM_HS_DEGYR1 or 
    T.UM_HS_FROM_DT1 <> S.UM_HS_FROM_DT1 or 
    T.UM_HS_MONTH1 <> S.UM_HS_MONTH1 or 
    T.UM_HS_NAME <> S.UM_HS_NAME or 
    T.UM_HS_STATE1 <> S.UM_HS_STATE1 or 
    T.UM_HS_TO_DT1 <> S.UM_HS_TO_DT1 or 
    T.UM_HS_TXT <> S.UM_HS_TXT or 
    T.UM_ADM_PREP_CAREER <> S.UM_ADM_PREP_CAREER or 
    T.YEAR <> S.YEAR or 
    T.UM_LAW_JT_MBA_OPTN <> S.UM_LAW_JT_MBA_OPTN or 
    T.UM_LAW1 <> S.UM_LAW1 or 
    T.UM_LAW2 <> S.UM_LAW2 or 
    T.UM_LAW3 <> S.UM_LAW3 or 
    T.UM_INTL_MAIL_ZIP <> S.UM_INTL_MAIL_ZIP or 
    T.UM_BUSN_PHONE2 <> S.UM_BUSN_PHONE2 or 
    T.UM_COURSE_6_CREDIT <> S.UM_COURSE_6_CREDIT or 
    T.UM_COURSE_6_ID <> S.UM_COURSE_6_ID or 
    T.UM_COURSE_6_TITLE <> S.UM_COURSE_6_TITLE or 
    T.UM_CELL <> S.UM_CELL or 
    T.UM_DISCIPLINE <> S.UM_DISCIPLINE or 
    T.UM_FELONY <> S.UM_FELONY or 
    T.UM_ADM_RA_TA <> S.UM_ADM_RA_TA or 
    T.UM_ADM_MAJ1_DESCR <> S.UM_ADM_MAJ1_DESCR or 
    T.UM_ADM_MAJ2_DESCR <> S.UM_ADM_MAJ2_DESCR or 
    T.UM_CSCE_NURSE_LIC <> S.UM_CSCE_NURSE_LIC or 
    T.UM_REF_POSTED_SEQ <> S.UM_REF_POSTED_SEQ or 
    T.UM_ADM_APPL_WAIVER <> S.UM_ADM_APPL_WAIVER or 
    T.UM_PAY_WITH_CC <> S.UM_PAY_WITH_CC or 
    T.UM_ADM_WAIVER_OPTN <> S.UM_ADM_WAIVER_OPTN or 
    T.UM_ADM_PAY_BY_CHK <> S.UM_ADM_PAY_BY_CHK or 
    T.ADM_APPL_COMPLETE <> S.ADM_APPL_COMPLETE or 
    T.UM_CS_REQUEST_ID <> S.UM_CS_REQUEST_ID or 
    T.UM_REQUEST_ID <> S.UM_REQUEST_ID or 
    T.UM_ADM_PAY_STS <> S.UM_ADM_PAY_STS or 
    T.UM_CYBERSRC_ERR_CD <> S.UM_CYBERSRC_ERR_CD or 
    T.UM_CYBERSRC_ERR_D <> S.UM_CYBERSRC_ERR_D or 
    T.ADM_APPL_METHOD <> S.ADM_APPL_METHOD or 
    T.AMOUNT <> S.AMOUNT or 
    T.UM_ADM_APP_SIG <> S.UM_ADM_APP_SIG or 
    T.UM_ADM_APP_SIG_DT <> S.UM_ADM_APP_SIG_DT or 
    T.UM_ADM_APP_NAME <> S.UM_ADM_APP_NAME or 
    T.UM_ADM_LOWG_CLS <> S.UM_ADM_LOWG_CLS or 
    T.UM_ADM_DG_P_TEACH <> S.UM_ADM_DG_P_TEACH or 
    T.UM_ADM_DG_MAT_COM <> S.UM_ADM_DG_MAT_COM or 
    T.UM_ADM_DG_MAT_CONT <> S.UM_ADM_DG_MAT_CONT or 
    T.UM_ADM_DG_PORT_SM <> S.UM_ADM_DG_PORT_SM or 
    T.UM_ADM_DG_PORT_SR <> S.UM_ADM_DG_PORT_SR or 
    T.UM_ADM_DG_PH_STDNT <> S.UM_ADM_DG_PH_STDNT or 
    T.UM_ADM_DG_PH_STDN1 <> S.UM_ADM_DG_PH_STDN1 or 
    T.UM_ADM_DG_ACKNOWLG <> S.UM_ADM_DG_ACKNOWLG or 
    T.UM_ADM_DG_ANALYTIC <> S.UM_ADM_DG_ANALYTIC or 
    T.UM_ADM_DG_BIOCHEM <> S.UM_ADM_DG_BIOCHEM or 
    T.UM_ADM_DG_COMPU <> S.UM_ADM_DG_COMPU or 
    T.UM_ADM_DG_ECOM_MTH <> S.UM_ADM_DG_ECOM_MTH or 
    T.UM_ADM_DG_ECOM_YR <> S.UM_ADM_DG_ECOM_YR or 
    T.UM_ADM_DG_INORGANI <> S.UM_ADM_DG_INORGANI or 
    T.UM_ADM_DG_ORGANIC <> S.UM_ADM_DG_ORGANIC or 
    T.UM_ADM_DG_MARINE <> S.UM_ADM_DG_MARINE or 
    T.UM_ADM_DG_POLYMER <> S.UM_ADM_DG_POLYMER or 
    T.UM_ADM_DG_PHYSICAL <> S.UM_ADM_DG_PHYSICAL or 
    T.UM_ADM_DG_UNDECID <> S.UM_ADM_DG_UNDECID or 
    T.UM_ADM_G_CHEM_YR <> S.UM_ADM_G_CHEM_YR or 
    T.UM_ADM_G_CHEM_CR <> S.UM_ADM_G_CHEM_CR or 
    T.UM_ADM_G_CHEM_GR <> S.UM_ADM_G_CHEM_GR or 
    T.UM_ADM_A_CHEM_YR <> S.UM_ADM_A_CHEM_YR or 
    T.UM_ADM_A_CHEM_CR <> S.UM_ADM_A_CHEM_CR or 
    T.UM_ADM_A_CHEM_GR <> S.UM_ADM_A_CHEM_GR or 
    T.UM_ADM_AI_CHEM_YR <> S.UM_ADM_AI_CHEM_YR or 
    T.UM_ADM_AI_CHEM_CR <> S.UM_ADM_AI_CHEM_CR or 
    T.UM_ADM_AI_CHEM_GR <> S.UM_ADM_AI_CHEM_GR or 
    T.UM_ADM_OR_CHEM1_YR <> S.UM_ADM_OR_CHEM1_YR or 
    T.UM_ADM_OR_CHEM1_CR <> S.UM_ADM_OR_CHEM1_CR or 
    T.UM_ADM_OR_CHEM1_GR <> S.UM_ADM_OR_CHEM1_GR or 
    T.UM_ADM_OR_CHEM2_YR <> S.UM_ADM_OR_CHEM2_YR or 
    T.UM_ADM_OR_CHEM2_CR <> S.UM_ADM_OR_CHEM2_CR or 
    T.UM_ADM_OR_CHEM2_GR <> S.UM_ADM_OR_CHEM2_GR or 
    T.UM_ADM_PHYSICS_YR <> S.UM_ADM_PHYSICS_YR or 
    T.UM_ADM_PHYSICS_CR <> S.UM_ADM_PHYSICS_CR or 
    T.UM_ADM_PHYSICS_GR <> S.UM_ADM_PHYSICS_GR or 
    T.UM_ADM_PHY_CHM1_YR <> S.UM_ADM_PHY_CHM1_YR or 
    T.UM_ADM_PHY_CHM1_CR <> S.UM_ADM_PHY_CHM1_CR or 
    T.UM_ADM_PHY_CHM1_GR <> S.UM_ADM_PHY_CHM1_GR or 
    T.UM_ADM_PHY_CHM2_YR <> S.UM_ADM_PHY_CHM2_YR or 
    T.UM_ADM_PHY_CHM2_CR <> S.UM_ADM_PHY_CHM2_CR or 
    T.UM_ADM_PHY_CHM2_GR <> S.UM_ADM_PHY_CHM2_GR or 
    T.UM_ADM_CALCULUS_YR <> S.UM_ADM_CALCULUS_YR or 
    T.UM_ADM_CALCULUS_CR <> S.UM_ADM_CALCULUS_CR or 
    T.UM_ADM_CALCULUS_GR <> S.UM_ADM_CALCULUS_GR or 
    T.UM_ADM_CHEM_E1_CRS <> S.UM_ADM_CHEM_E1_CRS or 
    T.UM_ADM_CHEM_E1_YR <> S.UM_ADM_CHEM_E1_YR or 
    T.UM_ADM_CHEM_E1_CR <> S.UM_ADM_CHEM_E1_CR or 
    T.UM_ADM_CHEM_E1_GR <> S.UM_ADM_CHEM_E1_GR or 
    T.UM_ADM_CHEM_E2_CRS <> S.UM_ADM_CHEM_E2_CRS or 
    T.UM_ADM_CHEM_E2_YR <> S.UM_ADM_CHEM_E2_YR or 
    T.UM_ADM_CHEM_E2_CR <> S.UM_ADM_CHEM_E2_CR or 
    T.UM_ADM_CHEM_E2_GR <> S.UM_ADM_CHEM_E2_GR or 
    T.UM_ADM_CHEM_E3_CRS <> S.UM_ADM_CHEM_E3_CRS or 
    T.UM_ADM_CHEM_E3_YR <> S.UM_ADM_CHEM_E3_YR or 
    T.UM_ADM_CHEM_E3_CR <> S.UM_ADM_CHEM_E3_CR or 
    T.UM_ADM_CHEM_E3_GR <> S.UM_ADM_CHEM_E3_GR or 
    T.UM_ADM_CHEM_E4_CRS <> S.UM_ADM_CHEM_E4_CRS or 
    T.UM_ADM_CHEM_E4_YR <> S.UM_ADM_CHEM_E4_YR or 
    T.UM_ADM_CHEM_E4_CR <> S.UM_ADM_CHEM_E4_CR or 
    T.UM_ADM_CHEM_E4_GR <> S.UM_ADM_CHEM_E4_GR or 
    T.UM_ADM_DG_CONCENTR <> S.UM_ADM_DG_CONCENTR or 
    T.UM_ADM_DG_ELEM_SCH <> S.UM_ADM_DG_ELEM_SCH or 
    T.UM_ADM_DG_COMM_MTH <> S.UM_ADM_DG_COMM_MTH or 
    T.UM_ADM_DG_COMM_YR <> S.UM_ADM_DG_COMM_YR or 
    T.UM_ADM_DG_CONT_YR <> S.UM_ADM_DG_CONT_YR or 
    T.UM_ADM_DG_CONT_MTH <> S.UM_ADM_DG_CONT_MTH or 
    T.UM_ADM_DG_FOUN_MTH <> S.UM_ADM_DG_FOUN_MTH or 
    T.UM_ADM_DG_FOUN_YR <> S.UM_ADM_DG_FOUN_YR or 
    T.UM_ADM_DG_MIDL_SCH <> S.UM_ADM_DG_MIDL_SCH or 
    T.UM_ADM_DG_SUBJ_MTH <> S.UM_ADM_DG_SUBJ_MTH or 
    T.UM_ADM_DG_SUBJ_YR <> S.UM_ADM_DG_SUBJ_YR or 
    T.UM_ADM_DG_N_APPLIC <> S.UM_ADM_DG_N_APPLIC or 
    T.UM_ADM_DG_MATP_ACK <> S.UM_ADM_DG_MATP_ACK or 
    T.UM_ADM_BG_EDC_LIC1 <> S.UM_ADM_BG_EDC_LIC1 or 
    T.UM_ADM_BG_EDC_LIC2 <> S.UM_ADM_BG_EDC_LIC2 or 
    T.UM_ADM_BG_EDC_LIC3 <> S.UM_ADM_BG_EDC_LIC3 or 
    T.UM_ADM_BG_ADMIN_L <> S.UM_ADM_BG_ADMIN_L or 
    T.UM_ADM_BG_OTH_LIC <> S.UM_ADM_BG_OTH_LIC or 
    T.UM_ADM_BG_GRD_DGR <> S.UM_ADM_BG_GRD_DGR or 
    T.UM_ADM_BG_CERT_NP <> S.UM_ADM_BG_CERT_NP or 
    T.UM_ADM_BG_ADULT_NP <> S.UM_ADM_BG_ADULT_NP or 
    T.UM_ADM_BG_PEDI_NP <> S.UM_ADM_BG_PEDI_NP or 
    T.UM_ADM_BG_FACULTY1 <> S.UM_ADM_BG_FACULTY1 or 
    T.UM_ADM_BG_FACULTY2 <> S.UM_ADM_BG_FACULTY2 or 
    T.UM_ADM_BG_FACULTY3 <> S.UM_ADM_BG_FACULTY3 or 
    T.UM_ADM_BG_CAR_GOAL <> S.UM_ADM_BG_CAR_GOAL or 
    T.UM_ADM_BG_CAR_OTH <> S.UM_ADM_BG_CAR_OTH or 
    T.UM_ADM_BG_DEGR_IN <> S.UM_ADM_BG_DEGR_IN or 
    T.UM_ADM_BG_GRAD_SER <> S.UM_ADM_BG_GRAD_SER or 
    T.UM_ADM_BG_SERV_HRS <> S.UM_ADM_BG_SERV_HRS or 
    T.UM_ADM_BG_ON_CAMP <> S.UM_ADM_BG_ON_CAMP or 
    T.UM_ADM_BG_ONLINE <> S.UM_ADM_BG_ONLINE or 
    T.UM_ADM_DU_ONLINE <> S.UM_ADM_DU_ONLINE or 
    T.UM_ADM_DU_CERT <> S.UM_ADM_DU_CERT or 
    T.UM_ADM_BG_RESEARCH <> S.UM_ADM_BG_RESEARCH or 
    T.UM_LOWU_CLINICAL_1 <> S.UM_LOWU_CLINICAL_1 or 
    T.UM_ADM_BG_ADVISOR1 <> S.UM_ADM_BG_ADVISOR1 or 
    T.UM_ADM_BG_ADVISOR2 <> S.UM_ADM_BG_ADVISOR2 or 
    T.UM_ADM_BG_ADVISOR3 <> S.UM_ADM_BG_ADVISOR3 or 
    T.UM_ADM_BG_SPEC_ED <> S.UM_ADM_BG_SPEC_ED or 
    T.UM_ADM_UMB_Z_AD_ON <> S.UM_ADM_UMB_Z_AD_ON or 
    T.UM_ADM_UMB_Z_F_EMP <> S.UM_ADM_UMB_Z_F_EMP or 
    T.UM_ADM_UMB_Z_FAM <> S.UM_ADM_UMB_Z_FAM or 
    T.UM_ADM_UMB_Z_OTHER <> S.UM_ADM_UMB_Z_OTHER or 
    T.UM_ADM_UMB_Z_PRINT <> S.UM_ADM_UMB_Z_PRINT or 
    T.UM_ADM_UMB_Z_RADIO <> S.UM_ADM_UMB_Z_RADIO or 
    T.UM_ADM_UMB_Z_TEXT <> S.UM_ADM_UMB_Z_TEXT or 
    T.UM_ADM_UMB_Z_TV <> S.UM_ADM_UMB_Z_TV or 
    T.UM_ADM_UMB_Z_WEB <> S.UM_ADM_UMB_Z_WEB or 
    T.UM_ADM_DU_UNIV_EXT <> S.UM_ADM_DU_UNIV_EXT or 
    T.UM_ADM_LG_AREA_INT <> S.UM_ADM_LG_AREA_INT or 
    T.UM_ADM_DU_DAY <> S.UM_ADM_DU_DAY or 
    T.UM_ADM_DU_NIGHT <> S.UM_ADM_DU_NIGHT or 
    T.UM_ADM_BG_SOC_PHD <> S.UM_ADM_BG_SOC_PHD or 
    T.UM_ADM_BG_BPE_QUES <> S.UM_ADM_BG_BPE_QUES or 
    nvl(trim(T.UM_ADM_PAY_REDO_DT),0) <> nvl(trim(S.UM_ADM_PAY_REDO_DT),0) or 
    nvl(trim(T.UM_ADM_PAY_CMPL_DT),0) <> nvl(trim(S.UM_ADM_PAY_CMPL_DT),0) or 
    T.UM_ADM_BG_DISB_LIC <> S.UM_ADM_BG_DISB_LIC or 
    T.UM_ADM_BG_MASTERS <> S.UM_ADM_BG_MASTERS or 
    T.UM_ADM_BG_LICENS <> S.UM_ADM_BG_LICENS or 
    T.UM_ADM_PARTNERSHIP <> S.UM_ADM_PARTNERSHIP or 
    T.UM_BOSG_FLEX_MBA <> S.UM_BOSG_FLEX_MBA or 
    T.UM_BOSG_PRO_MBA <> S.UM_BOSG_PRO_MBA or 
    T.UM_BOSG_ACCEL_MAST <> S.UM_BOSG_ACCEL_MAST or 
    nvl(trim(T.UM_ADM_LU_DIAGN_DT),0) <> nvl(trim(S.UM_ADM_LU_DIAGN_DT),0) or 
    T.UM_ADM_LU_MUS_AUD <> S.UM_ADM_LU_MUS_AUD or 
	T.UM_ADM_HEAR_US_CD <> S.UM_ADM_HEAR_US_CD or 
	T.UM_ADM_HEAR_TEXT <> S.UM_ADM_HEAR_TEXT or 
	T.UM_ENG_AS_SEC_LNG <> S.UM_ENG_AS_SEC_LNG or
    nvl(trim(T.UM_ADM_DG_PARTTIME),0) <> nvl(trim(S.UM_ADM_DG_PARTTIME),0) or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.UM_ADM_USERID,
    T.UM_ADM_APP_SEQ, 
    T.UM_ADM_REC_NBR, 
    T.INSTITUTION,
    T.SRC_SYS_ID, 
    T.EMPLID, 
    T.ADM_APPL_NBR, 
    T.UM_ADM_EMAIL, 
    T.EMAILID,
    T.ACAD_CAREER,
    T.ADM_APPL_CTR, 
    T.UM_ADM_SUB_DT,
    T.FIRST_NAME, 
    T.MIDDLE_NAME,
    T.LAST_NAME,
    T.UM_SSN, 
    T.BIRTHDATE,
    T.SEX,
    T.UM_US_CITIZEN,
    T.UM_CITIZNSHIP_STAT, 
    T.UM_BIRTH_CNTRY, 
    T.UM_ADM_NOT_MA_RES,
    T.VISA_PERMIT_TYPE, 
    T.VISA_WRKPMT_STATUS, 
    T.UM_VISA_WRKPMT_NBR, 
    T.UM_ADM_VISA_START,
    T.UM_ADM_VISA_END,
    T.UM_SUFFIX,
    T.UM_PREFIX,
    T.UM_FORMER_FNAME1, 
    T.UM_FORMER_MNAME1, 
    T.UM_FORMER_LNAME1, 
    T.UM_PREF_FNAME1, 
    T.UM_PREF_MNAME1, 
    T.UM_PREF_LNAME1, 
    T.UM_PERM_ADDRESS1, 
    T.UM_PERM_ADDRESS2, 
    T.UM_PERM_ADDRESS3, 
    T.UM_PERM_CITY, 
    T.UM_PERM_STATE,
    T.UM_PERM_POSTAL, 
    T.COUNTRY_CODE_PERM,
    T.UM_PERM_COUNTRY,
    T.UM_VALID_UNTIL, 
    T.UM_MAIL_ADDRESS1, 
    T.UM_MAIL_ADDRESS2, 
    T.UM_MAIL_ADDRESS3, 
    T.UM_MAIL_CITY, 
    T.UM_MAIL_STATE,
    T.UM_MAIL_POSTAL, 
    T.UM_MAIL_COUNTRY,
    T.COUNTRY_CD, 
    T.COUNTRY_CODE, 
    T.UM_PERM_PHONE,
    T.UM_PERM_PHONE1, 
    T.UM_PERM_PHONE2, 
    T.UM_PERM_PHONE3, 
    T.UM_CELL1, 
    T.UM_CELL2, 
    T.UM_CELL3, 
    T.UM_BUSN_CNTRY_CD, 
    T.UM_BUSN_PHONE,
    T.UM_BUSN_PHONE1, 
    T.UM_BUSN_PHONE3, 
    T.UM_BUSN_EXTENSION,
    T.UM_ADM_SESSION, 
    T.ADMIT_TYPE, 
    T.UM_ADM_SUBFIELD,
    T.UM_PRG_PLN_SBPLN, 
    T.UM_PRG_PLN_SBPLN2,
    T.UM_ACAD_PROG, 
    T.UM_ACAD_PLAN, 
    T.UM_ACAD_SUB_PLAN, 
    T.UM_ACAD_PROG1,
    T.UM_ACAD_PLAN1,
    T.UM_ACAD_SUB_PLAN1,
    T.UM_ADM_APP_FIN_AID, 
    T.UM_GRE_MO,
    T.UM_GRE_YR,
    T.UM_GRE_VERB,
    T.UM_GRE_QUAN,
    T.UM_GRE_ANAL,
    T.UM_GRE_SUBJ_SC, 
    T.UM_GRE_SUBJ_MO, 
    T.UM_GRE_SUBJ_YR, 
    T.UM_GRE_SUBJECT, 
    T.UM_GMAT_MO, 
    T.UM_GMAT_YR, 
    T.UM_GMAT_SC, 
    T.UM_TOEFL_MO,
    T.UM_TOEFL_YR,
    T.UM_TOEFL_SC,
    T.UM_TEST1_MO,
    T.UM_TEST1_NAME,
    T.UM_TEST1_SC,
    T.UM_TEST1_YR,
    T.UM_ADM_APP_GPA_ALL, 
    T.UM_ADM_APP_GPA_GRD, 
    T.UM_ADM_APP_GPA_MAJ, 
    T.UM_ADM_APP_GPA_TWO, 
    T.UM_ACT1_MO, 
    T.UM_ACT1_SC, 
    T.UM_ACT1_YR, 
    T.UM_ACT2_MO, 
    T.UM_ACT2_SC, 
    T.UM_ACT2_YR, 
    T.UM_ACT3_MO, 
    T.UM_ACT3_SC, 
    T.UM_ACT3_YR, 
    T.UM_SAT1_MO, 
    T.UM_SAT1_SC, 
    T.UM_SAT1_YR, 
    T.UM_SAT2_MO, 
    T.UM_SAT2_SC, 
    T.UM_SAT2_YR, 
    T.UM_SAT3_MO, 
    T.UM_SAT3_SC, 
    T.UM_SAT3_YR, 
    T.UM_TOEFL1_MO, 
    T.UM_TOEFL1_SC, 
    T.UM_TOEFL1_YR, 
    T.UM_TOEFL2_MO, 
    T.UM_TOEFL2_SC, 
    T.UM_TOEFL2_YR, 
    T.UM_TOEFL3_MO, 
    T.UM_TOEFL3_SC, 
    T.UM_TOEFL3_YR, 
    T.UM_IELTS1_MO, 
    T.UM_IELTS1_SC, 
    T.UM_IELTS1_YR, 
    T.UM_IELTS2_MO, 
    T.UM_IELTS2_SC, 
    T.UM_IELTS2_YR, 
    T.UM_IELTS3_MO, 
    T.UM_IELTS3_SC, 
    T.UM_IELTS3_YR, 
    T.UM_GMAT1_MO,
    T.UM_GMAT1_SC,
    T.UM_GMAT1_YR,
    T.UM_GMAT2_MO,
    T.UM_GMAT2_SC,
    T.UM_GMAT2_YR,
    T.UM_GMAT3_MO,
    T.UM_GMAT3_SC,
    T.UM_GMAT3_YR,
    T.UM_GRE_AW_MO, 
    T.UM_GRE_AW_SC, 
    T.UM_GRE_AW_YR, 
    T.UM_GRE_Q_MO,
    T.UM_GRE_Q_SC,
    T.UM_GRE_Q_YR,
    T.UM_GRE_V_MO,
    T.UM_GRE_V_SC,
    T.UM_GRE_V_YR,
    T.UM_LSAT1_MO,
    T.UM_LSAT1_SC,
    T.UM_LSAT1_YR,
    T.UM_LSAT2_MO,
    T.UM_LSAT2_SC,
    T.UM_LSAT2_YR,
    T.UM_LSAT3_MO,
    T.UM_LSAT3_SC,
    T.UM_LSAT3_YR,
    T.UM_COURSE_1_CREDIT, 
    T.UM_COURSE_1_ID, 
    T.UM_COURSE_1_TITLE,
    T.UM_COURSE_2_CREDIT, 
    T.UM_COURSE_2_ID, 
    T.UM_COURSE_2_TITLE,
    T.UM_COURSE_3_CREDIT, 
    T.UM_COURSE_3_ID, 
    T.UM_COURSE_3_TITLE,
    T.UM_COURSE_4_CREDIT, 
    T.UM_COURSE_4_ID, 
    T.UM_COURSE_4_TITLE,
    T.UM_COURSE_5_CREDIT, 
    T.UM_COURSE_5_ID, 
    T.UM_COURSE_5_TITLE,
    T.MILITARY_STATUS,
    T.UM_ETHNIC,
    T.UM_EMPLOY_STATE,
    T.UM_LAW4,
    T.UM_ADM_FAC_FNAME, 
    T.UM_ADM_FAC_LNAME, 
    T.UM_LAW5,
    T.UM_ADM_APP_MO_FR2,
    T.UM_ADM_APP_MO_FR3,
    T.UM_ADM_APP_MO_FR4,
    T.UM_ADM_APP_MO_TO2,
    T.UM_ADM_APP_MO_TO3,
    T.UM_ADM_APP_MO_TO4,
    T.UM_HS_ID, 
    T.UM_ACAD_LOAD_APPR,
    T.UM_ADM_ONLINE_DEGR, 
    T.UM_ADM_MUSIC_INST1, 
    T.UM_PRV1_ID, 
    T.UM_PRV2_ID, 
    T.UM_PRV3_ID, 
    T.UM_PRV4_ID, 
    T.UM_PHONE_TYPE,
    T.UM_INTL_ZIP,
    T.UM_ADM_APP_MO_FR1,
    T.UM_ADM_APP_MO_TO1,
    T.UM_ADM_APP_TITLE1,
    T.UM_ADM_APP_YR_FR1,
    T.UM_ADM_APP_YR_TO1,
    T.UM_ADM_APP_POS1,
    T.UM_ADM_APP_POS2,
    T.UM_ADM_APP_POS3,
    T.UM_ADM_APP_ADDR1, 
    T.UM_ADM_APP_ADDR2, 
    T.UM_ADM_APP_ADDR3, 
    T.UM_ADM_APP_NAM1,
    T.UM_ADM_APP_NAM2,
    T.UM_ADM_APP_NAM3,
    T.UM_ADM_APP_CNTRY1,
    T.UM_ADM_APP_CNTRY2,
    T.UM_ADM_APP_CNTRY3,
    T.UM_ADM_APP_CNTRY4,
    T.UM_ADM_APP_STATE1,
    T.UM_ADM_APP_STATE2,
    T.UM_ADM_APP_STATE3,
    T.UM_ADM_APP_STATE4,
    T.UM_ADM_APP_PR_TRM,
    T.UM_ADM_APP_EN_TRM,
    T.UM_ADM_APP_PRIOR, 
    T.UM_ADM_APP_ENRL,
    T.UM_ADM_APP_CAR1,
    T.UM_ADM_APP_CAR2,
    T.UM_ADM_APP_CAR3,
    T.UM_ADM_APP_CAR4,
    T.UM_ADM_APP_DEG1,
    T.UM_ADM_APP_DEG2,
    T.UM_ADM_APP_DEG3,
    T.UM_ADM_APP_DEG4,
    T.UM_ADM_APP_DEGYR1,
    T.UM_ADM_APP_DEGYR2,
    T.UM_ADM_APP_DEGYR3,
    T.UM_ADM_APP_DEGYR4,
    T.UM_ADM_APP_FROM1, 
    T.UM_ADM_APP_FROM2, 
    T.UM_ADM_APP_FROM3, 
    T.UM_ADM_APP_FROM4, 
    T.UM_ADM_APP_MONTH1,
    T.UM_ADM_APP_MONTH2,
    T.UM_ADM_APP_MONTH3,
    T.UM_ADM_APP_MONTH4,
    T.UM_ADM_APP_PRV1,
    T.UM_ADM_APP_PRV2,
    T.UM_ADM_APP_PRV3,
    T.UM_ADM_APP_PRV4,
    T.UM_ADM_APP_TO1, 
    T.UM_ADM_APP_TO2, 
    T.UM_ADM_APP_TO3, 
    T.UM_ADM_APP_TO4, 
    T.UM_ADM_ASSIST,
    T.UM_ADM_FELLOW,
    T.MBA_ABITUS, 
    T.MBA_CURRENT_STUDNT, 
    T.MBA_DAY,
    T.MBA_EXCLSVE_ONLINE, 
    T.MBA_WRK_PROFESNLS,
    T.CONTACT_NAME, 
    T.UM_EMERG_COUNTRY, 
    T.UM_ADM_REL_TYPE,
    T.UM_EMERG_CNTRY_CD,
    T.UM_EMERG_PHONE, 
    T.CONTACT_PHONE,
    T.CONTACT_PHONE_EXT,
    T.UM_PARENT_NAME, 
    T.UM_PARENT_ADDR1,
    T.UM_PARENT_ADDR2,
    T.UM_PARENT_ADDR3,
    T.UM_PARENT_CITY, 
    T.UM_PARENT_STATE,
    T.UM_PARENT_COUNTRY,
    T.UM_PARENT_PHONE,
    T.UM_PARENT_CNTRY_CD, 
    T.UM_PARENT_PHONE1, 
    T.UM_PARENT_PHONE2, 
    T.UM_PARENT_PHONE3, 
    T.UM_PARENT_TYPE, 
    T.ALUMNI_EVER,
    T.HIGHEST_EDUC_LVL, 
    T.UM_PARENT2_NAME,
    T.UM_PARENT2_ADDR1, 
    T.UM_PARENT2_ADDR2, 
    T.UM_PARENT2_ADDR3, 
    T.UM_PARENT2_CITY,
    T.UM_PARENT2_STATE, 
    T.UM_PARENT2_POSTAL,
    T.UM_PARENT2_INT_ZIP, 
    T.UM_PARENT2_COUNTRY, 
    T.UM_PARENT_CNTRY_C2, 
    T.UM_PARENT2_PHONE, 
    T.UM_PARENT2_PHONE1,
    T.UM_PARENT2_PHONE2,
    T.UM_PARENT2_PHONE3,
    T.UM_PARENT2_TYPE,
    T.UM_ALUMNI_EVER_P2,
    T.UM_HIGH_EDUCLVL_P2, 
    T.UM_GUARD_NAME,
    T.UM_GUARD_ADDR1, 
    T.UM_GUARD_ADDR2, 
    T.UM_GUARD_ADDR3, 
    T.UM_GUARD_CITY,
    T.UM_GUARD_STATE, 
    T.UM_GUARD_POSTAL,
    T.UM_GUARD_INT_ZIP, 
    T.UM_GUARD_COUNTRY, 
    T.UM_GUARD_CNTRY_CD,
    T.UM_GUARD_PHONE, 
    T.UM_GUARD_PHONE1,
    T.UM_GUARD_PHONE2,
    T.UM_GUARD_PHONE3,
    T.UM_MASS_RESIDENT, 
    T.UM_ALUMNI_EVER_GUA, 
    T.UM_HIGH_EDUCLVL_GU, 
    T.UM_CNTRY_CITIZENSH, 
    T.UM_BIRTHPLACE,
    T.UM_ADM_ENGINEERING, 
    T.UM_ADM_O_ENGINEER,
    T.UM_ADM_E_DIS_NAME,
    T.UM_ADM_SCIENCE_DIS, 
    T.UM_ADM_S_DIS_NAME,
    T.UM_COUNSELOR_FNAME, 
    T.UM_COUNSELOR_LNAME, 
    T.UM_COUNSELOR_EMAIL, 
    T.UM_ADM_APP_5YR, 
    T.UM_ADM_EARLY_D, 
    T.UM_ADM_REF1_FNAME,
    T.UM_ADM_REF1_LNAME,
    T.UM_ADM_REF1_MNAME,
    T.UM_ADM_REF2_FNAME,
    T.UM_ADM_REF2_LNAME,
    T.UM_ADM_REF2_MNAME,
    T.UM_ADM_REF3_FNAME,
    T.UM_ADM_REF3_LNAME,
    T.UM_ADM_REF3_MNAME,
    T.UM_REF_PRIVATE1,
    T.UM_REF_PRIVATE2,
    T.UM_REF_PRIVATE3,
    T.UM_ADM_BA_MASTER, 
    T.UM_ADM_UMB_TEACH, 
    T.UM_ADM_CAR_SWITCH,
    T.UM_ADM_UMB_MTEL,
    T.UM_ADM_UMB_VISION,
    T.UM_ADM_NATL_CERTIF, 
    T.UM_ADM_CERTIFICATN, 
    T.UM_ADM_CERT_EXP_DT, 
    T.UM_ADM_CNOW_LOW_IN, 
    T.UM_ADM_CNOW_FRST_G, 
    T.UM_ADM_CNOW_NOT_AP, 
    T.UM_ADM_ARCHELOGY, 
    T.UM_ADM_SCHL_NAME, 
    T.UM_ADM_SCHL_LOC,
    T.UM_ADM_PREV_BCKGRD, 
    T.UM_ADM_NBR_MTHS,
    T.UM_ADM_INIT_LICNSE, 
    T.UM_ADM_LICNSE_DESR, 
    T.UM_ADM_TEACH_SUBJ,
    T.UM_ADM_NE_REGIONAL, 
    T.UM_ADM_NO_VISA, 
    T.UM_PARENT_EMP_COLL, 
    T.UM_PARENT_LIVING, 
    T.UM_PARENT_POSTAL, 
    T.UM_PARENT_INT_ZIP,
    T.UM_PARENT_JOBTITLE, 
    T.UM_PARENT_GRADSCHL, 
    T.UM_ADM_SUCCESS_DEG, 
    T.UM_ADM_CRS_STR, 
    T.UM_ADM_CRS_END, 
    T.UM_ADM_PREV_APPLD,
    T.UM_PARENT_EMPLOYER, 
    T.UM_PARENT_OCCUPTN,
    T.UM_PARENT_EMAIL,
    T.UM_GUARD_EMAIL, 
    T.UM_GUARD_EMPLOYER,
    T.UM_GUARD_EMP_COLL,
    T.UM_GUARD_GRADSCHL,
    T.UM_GUARD_OCCUPTN, 
    T.UM_GUARD_JOBTITLE,
    T.UM_GUARD_DEGREE,
    T.UM_GUARD_DEGREE_G,
    T.UM_PARENT2_DEGREE,
    T.UM_PARENT_DEGREE, 
    T.UM_PARENT_DEGREE_G, 
    T.UM_ADM_RELIANT_FA,
    T.UM_PARENT_CEEB_G, 
    T.UM_GUARD_CEEB,
    T.UM_PARENT2_EMAIL, 
    T.UM_PARENT2_EMPCOLL, 
    T.UM_PARENT2_EMPLOYR, 
    T.UM_PARENT2_GRADSCH, 
    T.UM_PARENT2_JOBTITL, 
    T.UM_PARENT2_OCCUPTN, 
    T.UM_PARENT2_LIVING,
    T.UM_ADM_CSCE_TUITN,
    T.UM_ADM_CURR_EMP,
    T.UM_ADM_CURR_JOB,
    T.UM_ADM_LAW_SCHL1, 
    T.UM_ADM_LAW_SCHL2, 
    T.UM_ADM_LAW_SCHL3, 
    T.UM_ADM_LAW_SCHL4, 
    T.UM_ADM_LAW_3_3_PRG, 
    T.UM_ADM_LAW_ATTD_B4, 
    T.UM_ADM_LAW_JT_MBA,
    T.UM_ADM_LAW_PRV_APP, 
    T.UM_ADM_LAW_SRV_ACC, 
    T.UM_GUARD_CEEB_G,
    T.UM_PARENT2_CEEB,
    T.UM_PARENT2_CEEB_G,
    T.UM_PARENT_CEEB, 
    T.UM_PARENT_COLLEGE,
    T.UM_GUARD_COLLEGE, 
    T.UM_PARENT2_COLLEGE, 
    T.UM_PARENT2_DEGRE_G, 
    T.UM_ADM_RESID_HALL,
    T.UM_ADM_HS_DUAL_ENR, 
    T.UM_ADM_SPORT, 
    T.UM_ADM_HONORS_PRG,
    T.UM_ADM_PREV_CRSE, 
    T.UM_ADM_MUSIC_INSTR, 
    T.UM_ADM_EXCER_PRGM,
    T.UM_ADM_MUSIC_ENSEM, 
    T.UM_ADM_BACH_PATHWY, 
    T.UM_ADM_UML_DISABLE, 
    T.UM_ADM_SCHL_NAME2,
    T.UM_ADM_SCHL_LOC2, 
    T.UM_HIGH_S_OR_GED, 
    T.UM_COUNSELOR_PHONE, 
    T.UM_HS_CNTRY1, 
    T.UM_HS_DEGREE, 
    T.UM_HS_DEGYR1, 
    T.UM_HS_FROM_DT1, 
    T.UM_HS_MONTH1, 
    T.UM_HS_NAME, 
    T.UM_HS_STATE1, 
    T.UM_HS_TO_DT1, 
    T.UM_HS_TXT,
    T.UM_ADM_PREP_CAREER, 
    T.YEAR, 
    T.UM_LAW_JT_MBA_OPTN, 
    T.UM_LAW1,
    T.UM_LAW2,
    T.UM_LAW3,
    T.UM_INTL_MAIL_ZIP, 
    T.UM_BUSN_PHONE2, 
    T.UM_COURSE_6_CREDIT, 
    T.UM_COURSE_6_ID, 
    T.UM_COURSE_6_TITLE,
    T.UM_CELL,
    T.UM_DISCIPLINE,
    T.UM_FELONY,
    T.UM_ADM_RA_TA, 
    T.UM_ADM_MAJ1_DESCR,
    T.UM_ADM_MAJ2_DESCR,
    T.UM_CSCE_NURSE_LIC,
    T.UM_REF_POSTED_SEQ,
    T.UM_ADM_APPL_WAIVER, 
    T.UM_PAY_WITH_CC, 
    T.UM_ADM_WAIVER_OPTN, 
    T.UM_ADM_PAY_BY_CHK,
    T.ADM_APPL_COMPLETE,
    T.UM_CS_REQUEST_ID, 
    T.UM_REQUEST_ID,
    T.UM_ADM_PAY_STS, 
    T.UM_CYBERSRC_ERR_CD, 
    T.UM_CYBERSRC_ERR_D,
    T.ADM_APPL_METHOD,
    T.AMOUNT, 
    T.UM_ADM_APP_SIG, 
    T.UM_ADM_APP_SIG_DT,
    T.UM_ADM_APP_NAME,
    T.UM_ADM_LOWG_CLS,
    T.UM_ADM_DG_P_TEACH,
    T.UM_ADM_DG_MAT_COM,
    T.UM_ADM_DG_MAT_CONT, 
    T.UM_ADM_DG_PORT_SM,
    T.UM_ADM_DG_PORT_SR,
    T.UM_ADM_DG_PH_STDNT, 
    T.UM_ADM_DG_PH_STDN1, 
    T.UM_ADM_DG_ACKNOWLG, 
    T.UM_ADM_DG_ANALYTIC, 
    T.UM_ADM_DG_BIOCHEM,
    T.UM_ADM_DG_COMPU,
    T.UM_ADM_DG_ECOM_MTH, 
    T.UM_ADM_DG_ECOM_YR,
    T.UM_ADM_DG_INORGANI, 
    T.UM_ADM_DG_ORGANIC,
    T.UM_ADM_DG_MARINE, 
    T.UM_ADM_DG_POLYMER,
    T.UM_ADM_DG_PHYSICAL, 
    T.UM_ADM_DG_UNDECID,
    T.UM_ADM_G_CHEM_YR, 
    T.UM_ADM_G_CHEM_CR, 
    T.UM_ADM_G_CHEM_GR, 
    T.UM_ADM_A_CHEM_YR, 
    T.UM_ADM_A_CHEM_CR, 
    T.UM_ADM_A_CHEM_GR, 
    T.UM_ADM_AI_CHEM_YR,
    T.UM_ADM_AI_CHEM_CR,
    T.UM_ADM_AI_CHEM_GR,
    T.UM_ADM_OR_CHEM1_YR, 
    T.UM_ADM_OR_CHEM1_CR, 
    T.UM_ADM_OR_CHEM1_GR, 
    T.UM_ADM_OR_CHEM2_YR, 
    T.UM_ADM_OR_CHEM2_CR, 
    T.UM_ADM_OR_CHEM2_GR, 
    T.UM_ADM_PHYSICS_YR,
    T.UM_ADM_PHYSICS_CR,
    T.UM_ADM_PHYSICS_GR,
    T.UM_ADM_PHY_CHM1_YR, 
    T.UM_ADM_PHY_CHM1_CR, 
    T.UM_ADM_PHY_CHM1_GR, 
    T.UM_ADM_PHY_CHM2_YR, 
    T.UM_ADM_PHY_CHM2_CR, 
    T.UM_ADM_PHY_CHM2_GR, 
    T.UM_ADM_CALCULUS_YR, 
    T.UM_ADM_CALCULUS_CR, 
    T.UM_ADM_CALCULUS_GR, 
    T.UM_ADM_CHEM_E1_CRS, 
    T.UM_ADM_CHEM_E1_YR,
    T.UM_ADM_CHEM_E1_CR,
    T.UM_ADM_CHEM_E1_GR,
    T.UM_ADM_CHEM_E2_CRS, 
    T.UM_ADM_CHEM_E2_YR,
    T.UM_ADM_CHEM_E2_CR,
    T.UM_ADM_CHEM_E2_GR,
    T.UM_ADM_CHEM_E3_CRS, 
    T.UM_ADM_CHEM_E3_YR,
    T.UM_ADM_CHEM_E3_CR,
    T.UM_ADM_CHEM_E3_GR,
    T.UM_ADM_CHEM_E4_CRS, 
    T.UM_ADM_CHEM_E4_YR,
    T.UM_ADM_CHEM_E4_CR,
    T.UM_ADM_CHEM_E4_GR,
    T.UM_ADM_DG_CONCENTR, 
    T.UM_ADM_DG_ELEM_SCH, 
    T.UM_ADM_DG_COMM_MTH, 
    T.UM_ADM_DG_COMM_YR,
    T.UM_ADM_DG_CONT_YR,
    T.UM_ADM_DG_CONT_MTH, 
    T.UM_ADM_DG_FOUN_MTH, 
    T.UM_ADM_DG_FOUN_YR,
    T.UM_ADM_DG_MIDL_SCH, 
    T.UM_ADM_DG_SUBJ_MTH, 
    T.UM_ADM_DG_SUBJ_YR,
    T.UM_ADM_DG_N_APPLIC, 
    T.UM_ADM_DG_MATP_ACK, 
    T.UM_ADM_BG_EDC_LIC1, 
    T.UM_ADM_BG_EDC_LIC2, 
    T.UM_ADM_BG_EDC_LIC3, 
    T.UM_ADM_BG_ADMIN_L,
    T.UM_ADM_BG_OTH_LIC,
    T.UM_ADM_BG_GRD_DGR,
    T.UM_ADM_BG_CERT_NP,
    T.UM_ADM_BG_ADULT_NP, 
    T.UM_ADM_BG_PEDI_NP,
    T.UM_ADM_BG_FACULTY1, 
    T.UM_ADM_BG_FACULTY2, 
    T.UM_ADM_BG_FACULTY3, 
    T.UM_ADM_BG_CAR_GOAL, 
    T.UM_ADM_BG_CAR_OTH,
    T.UM_ADM_BG_DEGR_IN,
    T.UM_ADM_BG_GRAD_SER, 
    T.UM_ADM_BG_SERV_HRS, 
    T.UM_ADM_BG_ON_CAMP,
    T.UM_ADM_BG_ONLINE, 
    T.UM_ADM_DU_ONLINE, 
    T.UM_ADM_DU_CERT, 
    T.UM_ADM_BG_RESEARCH, 
    T.UM_LOWU_CLINICAL_1, 
    T.UM_ADM_BG_ADVISOR1, 
    T.UM_ADM_BG_ADVISOR2, 
    T.UM_ADM_BG_ADVISOR3, 
    T.UM_ADM_BG_SPEC_ED,
    T.UM_ADM_UMB_Z_AD_ON, 
    T.UM_ADM_UMB_Z_F_EMP, 
    T.UM_ADM_UMB_Z_FAM, 
    T.UM_ADM_UMB_Z_OTHER, 
    T.UM_ADM_UMB_Z_PRINT, 
    T.UM_ADM_UMB_Z_RADIO, 
    T.UM_ADM_UMB_Z_TEXT,
    T.UM_ADM_UMB_Z_TV,
    T.UM_ADM_UMB_Z_WEB, 
    T.UM_ADM_DU_UNIV_EXT, 
    T.UM_ADM_LG_AREA_INT, 
    T.UM_ADM_DU_DAY,
    T.UM_ADM_DU_NIGHT,
    T.UM_ADM_BG_SOC_PHD,
    T.UM_ADM_BG_BPE_QUES, 
    T.UM_ADM_PAY_REDO_DT, 
    T.UM_ADM_PAY_CMPL_DT, 
    T.UM_ADM_BG_DISB_LIC, 
    T.UM_ADM_BG_MASTERS,
    T.UM_ADM_BG_LICENS, 
    T.UM_ADM_PARTNERSHIP, 
    T.UM_BOSG_FLEX_MBA, 
    T.UM_BOSG_PRO_MBA,
    T.UM_BOSG_ACCEL_MAST, 
    T.UM_ADM_LU_DIAGN_DT, 
    T.UM_ADM_LU_MUS_AUD,
	T.UM_ADM_HEAR_US_CD,
	T.UM_ADM_HEAR_TEXT,
	T.UM_ENG_AS_SEC_LNG,
    T.UM_ADM_DG_PARTTIME, 
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
) 
values (
    S.UM_ADM_USERID,
    S.UM_ADM_APP_SEQ, 
    S.UM_ADM_REC_NBR, 
    S.INSTITUTION,
    'CS90', 
    S.EMPLID, 
    S.ADM_APPL_NBR, 
    S.UM_ADM_EMAIL, 
    S.EMAILID,
    S.ACAD_CAREER,
    S.ADM_APPL_CTR, 
    S.UM_ADM_SUB_DT,
    S.FIRST_NAME, 
    S.MIDDLE_NAME,
    S.LAST_NAME,
    S.UM_SSN, 
    S.BIRTHDATE,
    S.SEX,
    S.UM_US_CITIZEN,
    S.UM_CITIZNSHIP_STAT, 
    S.UM_BIRTH_CNTRY, 
    S.UM_ADM_NOT_MA_RES,
    S.VISA_PERMIT_TYPE, 
    S.VISA_WRKPMT_STATUS, 
    S.UM_VISA_WRKPMT_NBR, 
    S.UM_ADM_VISA_START,
    S.UM_ADM_VISA_END,
    S.UM_SUFFIX,
    S.UM_PREFIX,
    S.UM_FORMER_FNAME1, 
    S.UM_FORMER_MNAME1, 
    S.UM_FORMER_LNAME1, 
    S.UM_PREF_FNAME1, 
    S.UM_PREF_MNAME1, 
    S.UM_PREF_LNAME1, 
    S.UM_PERM_ADDRESS1, 
    S.UM_PERM_ADDRESS2, 
    S.UM_PERM_ADDRESS3, 
    S.UM_PERM_CITY, 
    S.UM_PERM_STATE,
    S.UM_PERM_POSTAL, 
    S.COUNTRY_CODE_PERM,
    S.UM_PERM_COUNTRY,
    S.UM_VALID_UNTIL, 
    S.UM_MAIL_ADDRESS1, 
    S.UM_MAIL_ADDRESS2, 
    S.UM_MAIL_ADDRESS3, 
    S.UM_MAIL_CITY, 
    S.UM_MAIL_STATE,
    S.UM_MAIL_POSTAL, 
    S.UM_MAIL_COUNTRY,
    S.COUNTRY_CD, 
    S.COUNTRY_CODE, 
    S.UM_PERM_PHONE,
    S.UM_PERM_PHONE1, 
    S.UM_PERM_PHONE2, 
    S.UM_PERM_PHONE3, 
    S.UM_CELL1, 
    S.UM_CELL2, 
    S.UM_CELL3, 
    S.UM_BUSN_CNTRY_CD, 
    S.UM_BUSN_PHONE,
    S.UM_BUSN_PHONE1, 
    S.UM_BUSN_PHONE3, 
    S.UM_BUSN_EXTENSION,
    S.UM_ADM_SESSION, 
    S.ADMIT_TYPE, 
    S.UM_ADM_SUBFIELD,
    S.UM_PRG_PLN_SBPLN, 
    S.UM_PRG_PLN_SBPLN2,
    S.UM_ACAD_PROG, 
    S.UM_ACAD_PLAN, 
    S.UM_ACAD_SUB_PLAN, 
    S.UM_ACAD_PROG1,
    S.UM_ACAD_PLAN1,
    S.UM_ACAD_SUB_PLAN1,
    S.UM_ADM_APP_FIN_AID, 
    S.UM_GRE_MO,
    S.UM_GRE_YR,
    S.UM_GRE_VERB,
    S.UM_GRE_QUAN,
    S.UM_GRE_ANAL,
    S.UM_GRE_SUBJ_SC, 
    S.UM_GRE_SUBJ_MO, 
    S.UM_GRE_SUBJ_YR, 
    S.UM_GRE_SUBJECT, 
    S.UM_GMAT_MO, 
    S.UM_GMAT_YR, 
    S.UM_GMAT_SC, 
    S.UM_TOEFL_MO,
    S.UM_TOEFL_YR,
    S.UM_TOEFL_SC,
    S.UM_TEST1_MO,
    S.UM_TEST1_NAME,
    S.UM_TEST1_SC,
    S.UM_TEST1_YR,
    S.UM_ADM_APP_GPA_ALL, 
    S.UM_ADM_APP_GPA_GRD, 
    S.UM_ADM_APP_GPA_MAJ, 
    S.UM_ADM_APP_GPA_TWO, 
    S.UM_ACT1_MO, 
    S.UM_ACT1_SC, 
    S.UM_ACT1_YR, 
    S.UM_ACT2_MO, 
    S.UM_ACT2_SC, 
    S.UM_ACT2_YR, 
    S.UM_ACT3_MO, 
    S.UM_ACT3_SC, 
    S.UM_ACT3_YR, 
    S.UM_SAT1_MO, 
    S.UM_SAT1_SC, 
    S.UM_SAT1_YR, 
    S.UM_SAT2_MO, 
    S.UM_SAT2_SC, 
    S.UM_SAT2_YR, 
    S.UM_SAT3_MO, 
    S.UM_SAT3_SC, 
    S.UM_SAT3_YR, 
    S.UM_TOEFL1_MO, 
    S.UM_TOEFL1_SC, 
    S.UM_TOEFL1_YR, 
    S.UM_TOEFL2_MO, 
    S.UM_TOEFL2_SC, 
    S.UM_TOEFL2_YR, 
    S.UM_TOEFL3_MO, 
    S.UM_TOEFL3_SC, 
    S.UM_TOEFL3_YR, 
    S.UM_IELTS1_MO, 
    S.UM_IELTS1_SC, 
    S.UM_IELTS1_YR, 
    S.UM_IELTS2_MO, 
    S.UM_IELTS2_SC, 
    S.UM_IELTS2_YR, 
    S.UM_IELTS3_MO, 
    S.UM_IELTS3_SC, 
    S.UM_IELTS3_YR, 
    S.UM_GMAT1_MO,
    S.UM_GMAT1_SC,
    S.UM_GMAT1_YR,
    S.UM_GMAT2_MO,
    S.UM_GMAT2_SC,
    S.UM_GMAT2_YR,
    S.UM_GMAT3_MO,
    S.UM_GMAT3_SC,
    S.UM_GMAT3_YR,
    S.UM_GRE_AW_MO, 
    S.UM_GRE_AW_SC, 
    S.UM_GRE_AW_YR, 
    S.UM_GRE_Q_MO,
    S.UM_GRE_Q_SC,
    S.UM_GRE_Q_YR,
    S.UM_GRE_V_MO,
    S.UM_GRE_V_SC,
    S.UM_GRE_V_YR,
    S.UM_LSAT1_MO,
    S.UM_LSAT1_SC,
    S.UM_LSAT1_YR,
    S.UM_LSAT2_MO,
    S.UM_LSAT2_SC,
    S.UM_LSAT2_YR,
    S.UM_LSAT3_MO,
    S.UM_LSAT3_SC,
    S.UM_LSAT3_YR,
    S.UM_COURSE_1_CREDIT, 
    S.UM_COURSE_1_ID, 
    S.UM_COURSE_1_TITLE,
    S.UM_COURSE_2_CREDIT, 
    S.UM_COURSE_2_ID, 
    S.UM_COURSE_2_TITLE,
    S.UM_COURSE_3_CREDIT, 
    S.UM_COURSE_3_ID, 
    S.UM_COURSE_3_TITLE,
    S.UM_COURSE_4_CREDIT, 
    S.UM_COURSE_4_ID, 
    S.UM_COURSE_4_TITLE,
    S.UM_COURSE_5_CREDIT, 
    S.UM_COURSE_5_ID, 
    S.UM_COURSE_5_TITLE,
    S.MILITARY_STATUS,
    S.UM_ETHNIC,
    S.UM_EMPLOY_STATE,
    S.UM_LAW4,
    S.UM_ADM_FAC_FNAME, 
    S.UM_ADM_FAC_LNAME, 
    S.UM_LAW5,
    S.UM_ADM_APP_MO_FR2,
    S.UM_ADM_APP_MO_FR3,
    S.UM_ADM_APP_MO_FR4,
    S.UM_ADM_APP_MO_TO2,
    S.UM_ADM_APP_MO_TO3,
    S.UM_ADM_APP_MO_TO4,
    S.UM_HS_ID, 
    S.UM_ACAD_LOAD_APPR,
    S.UM_ADM_ONLINE_DEGR, 
    S.UM_ADM_MUSIC_INST1, 
    S.UM_PRV1_ID, 
    S.UM_PRV2_ID, 
    S.UM_PRV3_ID, 
    S.UM_PRV4_ID, 
    S.UM_PHONE_TYPE,
    S.UM_INTL_ZIP,
    S.UM_ADM_APP_MO_FR1,
    S.UM_ADM_APP_MO_TO1,
    S.UM_ADM_APP_TITLE1,
    S.UM_ADM_APP_YR_FR1,
    S.UM_ADM_APP_YR_TO1,
    S.UM_ADM_APP_POS1,
    S.UM_ADM_APP_POS2,
    S.UM_ADM_APP_POS3,
    S.UM_ADM_APP_ADDR1, 
    S.UM_ADM_APP_ADDR2, 
    S.UM_ADM_APP_ADDR3, 
    S.UM_ADM_APP_NAM1,
    S.UM_ADM_APP_NAM2,
    S.UM_ADM_APP_NAM3,
    S.UM_ADM_APP_CNTRY1,
    S.UM_ADM_APP_CNTRY2,
    S.UM_ADM_APP_CNTRY3,
    S.UM_ADM_APP_CNTRY4,
    S.UM_ADM_APP_STATE1,
    S.UM_ADM_APP_STATE2,
    S.UM_ADM_APP_STATE3,
    S.UM_ADM_APP_STATE4,
    S.UM_ADM_APP_PR_TRM,
    S.UM_ADM_APP_EN_TRM,
    S.UM_ADM_APP_PRIOR, 
    S.UM_ADM_APP_ENRL,
    S.UM_ADM_APP_CAR1,
    S.UM_ADM_APP_CAR2,
    S.UM_ADM_APP_CAR3,
    S.UM_ADM_APP_CAR4,
    S.UM_ADM_APP_DEG1,
    S.UM_ADM_APP_DEG2,
    S.UM_ADM_APP_DEG3,
    S.UM_ADM_APP_DEG4,
    S.UM_ADM_APP_DEGYR1,
    S.UM_ADM_APP_DEGYR2,
    S.UM_ADM_APP_DEGYR3,
    S.UM_ADM_APP_DEGYR4,
    S.UM_ADM_APP_FROM1, 
    S.UM_ADM_APP_FROM2, 
    S.UM_ADM_APP_FROM3, 
    S.UM_ADM_APP_FROM4, 
    S.UM_ADM_APP_MONTH1,
    S.UM_ADM_APP_MONTH2,
    S.UM_ADM_APP_MONTH3,
    S.UM_ADM_APP_MONTH4,
    S.UM_ADM_APP_PRV1,
    S.UM_ADM_APP_PRV2,
    S.UM_ADM_APP_PRV3,
    S.UM_ADM_APP_PRV4,
    S.UM_ADM_APP_TO1, 
    S.UM_ADM_APP_TO2, 
    S.UM_ADM_APP_TO3, 
    S.UM_ADM_APP_TO4, 
    S.UM_ADM_ASSIST,
    S.UM_ADM_FELLOW,
    S.MBA_ABITUS, 
    S.MBA_CURRENT_STUDNT, 
    S.MBA_DAY,
    S.MBA_EXCLSVE_ONLINE, 
    S.MBA_WRK_PROFESNLS,
    S.CONTACT_NAME, 
    S.UM_EMERG_COUNTRY, 
    S.UM_ADM_REL_TYPE,
    S.UM_EMERG_CNTRY_CD,
    S.UM_EMERG_PHONE, 
    S.CONTACT_PHONE,
    S.CONTACT_PHONE_EXT,
    S.UM_PARENT_NAME, 
    S.UM_PARENT_ADDR1,
    S.UM_PARENT_ADDR2,
    S.UM_PARENT_ADDR3,
    S.UM_PARENT_CITY, 
    S.UM_PARENT_STATE,
    S.UM_PARENT_COUNTRY,
    S.UM_PARENT_PHONE,
    S.UM_PARENT_CNTRY_CD, 
    S.UM_PARENT_PHONE1, 
    S.UM_PARENT_PHONE2, 
    S.UM_PARENT_PHONE3, 
    S.UM_PARENT_TYPE, 
    S.ALUMNI_EVER,
    S.HIGHEST_EDUC_LVL, 
    S.UM_PARENT2_NAME,
    S.UM_PARENT2_ADDR1, 
    S.UM_PARENT2_ADDR2, 
    S.UM_PARENT2_ADDR3, 
    S.UM_PARENT2_CITY,
    S.UM_PARENT2_STATE, 
    S.UM_PARENT2_POSTAL,
    S.UM_PARENT2_INT_ZIP, 
    S.UM_PARENT2_COUNTRY, 
    S.UM_PARENT_CNTRY_C2, 
    S.UM_PARENT2_PHONE, 
    S.UM_PARENT2_PHONE1,
    S.UM_PARENT2_PHONE2,
    S.UM_PARENT2_PHONE3,
    S.UM_PARENT2_TYPE,
    S.UM_ALUMNI_EVER_P2,
    S.UM_HIGH_EDUCLVL_P2, 
    S.UM_GUARD_NAME,
    S.UM_GUARD_ADDR1, 
    S.UM_GUARD_ADDR2, 
    S.UM_GUARD_ADDR3, 
    S.UM_GUARD_CITY,
    S.UM_GUARD_STATE, 
    S.UM_GUARD_POSTAL,
    S.UM_GUARD_INT_ZIP, 
    S.UM_GUARD_COUNTRY, 
    S.UM_GUARD_CNTRY_CD,
    S.UM_GUARD_PHONE, 
    S.UM_GUARD_PHONE1,
    S.UM_GUARD_PHONE2,
    S.UM_GUARD_PHONE3,
    S.UM_MASS_RESIDENT, 
    S.UM_ALUMNI_EVER_GUA, 
    S.UM_HIGH_EDUCLVL_GU, 
    S.UM_CNTRY_CITIZENSH, 
    S.UM_BIRTHPLACE,
    S.UM_ADM_ENGINEERING, 
    S.UM_ADM_O_ENGINEER,
    S.UM_ADM_E_DIS_NAME,
    S.UM_ADM_SCIENCE_DIS, 
    S.UM_ADM_S_DIS_NAME,
    S.UM_COUNSELOR_FNAME, 
    S.UM_COUNSELOR_LNAME, 
    S.UM_COUNSELOR_EMAIL, 
    S.UM_ADM_APP_5YR, 
    S.UM_ADM_EARLY_D, 
    S.UM_ADM_REF1_FNAME,
    S.UM_ADM_REF1_LNAME,
    S.UM_ADM_REF1_MNAME,
    S.UM_ADM_REF2_FNAME,
    S.UM_ADM_REF2_LNAME,
    S.UM_ADM_REF2_MNAME,
    S.UM_ADM_REF3_FNAME,
    S.UM_ADM_REF3_LNAME,
    S.UM_ADM_REF3_MNAME,
    S.UM_REF_PRIVATE1,
    S.UM_REF_PRIVATE2,
    S.UM_REF_PRIVATE3,
    S.UM_ADM_BA_MASTER, 
    S.UM_ADM_UMB_TEACH, 
    S.UM_ADM_CAR_SWITCH,
    S.UM_ADM_UMB_MTEL,
    S.UM_ADM_UMB_VISION,
    S.UM_ADM_NATL_CERTIF, 
    S.UM_ADM_CERTIFICATN, 
    S.UM_ADM_CERT_EXP_DT, 
    S.UM_ADM_CNOW_LOW_IN, 
    S.UM_ADM_CNOW_FRST_G, 
    S.UM_ADM_CNOW_NOT_AP, 
    S.UM_ADM_ARCHELOGY, 
    S.UM_ADM_SCHL_NAME, 
    S.UM_ADM_SCHL_LOC,
    S.UM_ADM_PREV_BCKGRD, 
    S.UM_ADM_NBR_MTHS,
    S.UM_ADM_INIT_LICNSE, 
    S.UM_ADM_LICNSE_DESR, 
    S.UM_ADM_TEACH_SUBJ,
    S.UM_ADM_NE_REGIONAL, 
    S.UM_ADM_NO_VISA, 
    S.UM_PARENT_EMP_COLL, 
    S.UM_PARENT_LIVING, 
    S.UM_PARENT_POSTAL, 
    S.UM_PARENT_INT_ZIP,
    S.UM_PARENT_JOBTITLE, 
    S.UM_PARENT_GRADSCHL, 
    S.UM_ADM_SUCCESS_DEG, 
    S.UM_ADM_CRS_STR, 
    S.UM_ADM_CRS_END, 
    S.UM_ADM_PREV_APPLD,
    S.UM_PARENT_EMPLOYER, 
    S.UM_PARENT_OCCUPTN,
    S.UM_PARENT_EMAIL,
    S.UM_GUARD_EMAIL, 
    S.UM_GUARD_EMPLOYER,
    S.UM_GUARD_EMP_COLL,
    S.UM_GUARD_GRADSCHL,
    S.UM_GUARD_OCCUPTN, 
    S.UM_GUARD_JOBTITLE,
    S.UM_GUARD_DEGREE,
    S.UM_GUARD_DEGREE_G,
    S.UM_PARENT2_DEGREE,
    S.UM_PARENT_DEGREE, 
    S.UM_PARENT_DEGREE_G, 
    S.UM_ADM_RELIANT_FA,
    S.UM_PARENT_CEEB_G, 
    S.UM_GUARD_CEEB,
    S.UM_PARENT2_EMAIL, 
    S.UM_PARENT2_EMPCOLL, 
    S.UM_PARENT2_EMPLOYR, 
    S.UM_PARENT2_GRADSCH, 
    S.UM_PARENT2_JOBTITL, 
    S.UM_PARENT2_OCCUPTN, 
    S.UM_PARENT2_LIVING,
    S.UM_ADM_CSCE_TUITN,
    S.UM_ADM_CURR_EMP,
    S.UM_ADM_CURR_JOB,
    S.UM_ADM_LAW_SCHL1, 
    S.UM_ADM_LAW_SCHL2, 
    S.UM_ADM_LAW_SCHL3, 
    S.UM_ADM_LAW_SCHL4, 
    S.UM_ADM_LAW_3_3_PRG, 
    S.UM_ADM_LAW_ATTD_B4, 
    S.UM_ADM_LAW_JT_MBA,
    S.UM_ADM_LAW_PRV_APP, 
    S.UM_ADM_LAW_SRV_ACC, 
    S.UM_GUARD_CEEB_G,
    S.UM_PARENT2_CEEB,
    S.UM_PARENT2_CEEB_G,
    S.UM_PARENT_CEEB, 
    S.UM_PARENT_COLLEGE,
    S.UM_GUARD_COLLEGE, 
    S.UM_PARENT2_COLLEGE, 
    S.UM_PARENT2_DEGRE_G, 
    S.UM_ADM_RESID_HALL,
    S.UM_ADM_HS_DUAL_ENR, 
    S.UM_ADM_SPORT, 
    S.UM_ADM_HONORS_PRG,
    S.UM_ADM_PREV_CRSE, 
    S.UM_ADM_MUSIC_INSTR, 
    S.UM_ADM_EXCER_PRGM,
    S.UM_ADM_MUSIC_ENSEM, 
    S.UM_ADM_BACH_PATHWY, 
    S.UM_ADM_UML_DISABLE, 
    S.UM_ADM_SCHL_NAME2,
    S.UM_ADM_SCHL_LOC2, 
    S.UM_HIGH_S_OR_GED, 
    S.UM_COUNSELOR_PHONE, 
    S.UM_HS_CNTRY1, 
    S.UM_HS_DEGREE, 
    S.UM_HS_DEGYR1, 
    S.UM_HS_FROM_DT1, 
    S.UM_HS_MONTH1, 
    S.UM_HS_NAME, 
    S.UM_HS_STATE1, 
    S.UM_HS_TO_DT1, 
    S.UM_HS_TXT,
    S.UM_ADM_PREP_CAREER, 
    S.YEAR, 
    S.UM_LAW_JT_MBA_OPTN, 
    S.UM_LAW1,
    S.UM_LAW2,
    S.UM_LAW3,
    S.UM_INTL_MAIL_ZIP, 
    S.UM_BUSN_PHONE2, 
    S.UM_COURSE_6_CREDIT, 
    S.UM_COURSE_6_ID, 
    S.UM_COURSE_6_TITLE,
    S.UM_CELL,
    S.UM_DISCIPLINE,
    S.UM_FELONY,
    S.UM_ADM_RA_TA, 
    S.UM_ADM_MAJ1_DESCR,
    S.UM_ADM_MAJ2_DESCR,
    S.UM_CSCE_NURSE_LIC,
    S.UM_REF_POSTED_SEQ,
    S.UM_ADM_APPL_WAIVER, 
    S.UM_PAY_WITH_CC, 
    S.UM_ADM_WAIVER_OPTN, 
    S.UM_ADM_PAY_BY_CHK,
    S.ADM_APPL_COMPLETE,
    S.UM_CS_REQUEST_ID, 
    S.UM_REQUEST_ID,
    S.UM_ADM_PAY_STS, 
    S.UM_CYBERSRC_ERR_CD, 
    S.UM_CYBERSRC_ERR_D,
    S.ADM_APPL_METHOD,
    S.AMOUNT, 
    S.UM_ADM_APP_SIG, 
    S.UM_ADM_APP_SIG_DT,
    S.UM_ADM_APP_NAME,
    S.UM_ADM_LOWG_CLS,
    S.UM_ADM_DG_P_TEACH,
    S.UM_ADM_DG_MAT_COM,
    S.UM_ADM_DG_MAT_CONT, 
    S.UM_ADM_DG_PORT_SM,
    S.UM_ADM_DG_PORT_SR,
    S.UM_ADM_DG_PH_STDNT, 
    S.UM_ADM_DG_PH_STDN1, 
    S.UM_ADM_DG_ACKNOWLG, 
    S.UM_ADM_DG_ANALYTIC, 
    S.UM_ADM_DG_BIOCHEM,
    S.UM_ADM_DG_COMPU,
    S.UM_ADM_DG_ECOM_MTH, 
    S.UM_ADM_DG_ECOM_YR,
    S.UM_ADM_DG_INORGANI, 
    S.UM_ADM_DG_ORGANIC,
    S.UM_ADM_DG_MARINE, 
    S.UM_ADM_DG_POLYMER,
    S.UM_ADM_DG_PHYSICAL, 
    S.UM_ADM_DG_UNDECID,
    S.UM_ADM_G_CHEM_YR, 
    S.UM_ADM_G_CHEM_CR, 
    S.UM_ADM_G_CHEM_GR, 
    S.UM_ADM_A_CHEM_YR, 
    S.UM_ADM_A_CHEM_CR, 
    S.UM_ADM_A_CHEM_GR, 
    S.UM_ADM_AI_CHEM_YR,
    S.UM_ADM_AI_CHEM_CR,
    S.UM_ADM_AI_CHEM_GR,
    S.UM_ADM_OR_CHEM1_YR, 
    S.UM_ADM_OR_CHEM1_CR, 
    S.UM_ADM_OR_CHEM1_GR, 
    S.UM_ADM_OR_CHEM2_YR, 
    S.UM_ADM_OR_CHEM2_CR, 
    S.UM_ADM_OR_CHEM2_GR, 
    S.UM_ADM_PHYSICS_YR,
    S.UM_ADM_PHYSICS_CR,
    S.UM_ADM_PHYSICS_GR,
    S.UM_ADM_PHY_CHM1_YR, 
    S.UM_ADM_PHY_CHM1_CR, 
    S.UM_ADM_PHY_CHM1_GR, 
    S.UM_ADM_PHY_CHM2_YR, 
    S.UM_ADM_PHY_CHM2_CR, 
    S.UM_ADM_PHY_CHM2_GR, 
    S.UM_ADM_CALCULUS_YR, 
    S.UM_ADM_CALCULUS_CR, 
    S.UM_ADM_CALCULUS_GR, 
    S.UM_ADM_CHEM_E1_CRS, 
    S.UM_ADM_CHEM_E1_YR,
    S.UM_ADM_CHEM_E1_CR,
    S.UM_ADM_CHEM_E1_GR,
    S.UM_ADM_CHEM_E2_CRS, 
    S.UM_ADM_CHEM_E2_YR,
    S.UM_ADM_CHEM_E2_CR,
    S.UM_ADM_CHEM_E2_GR,
    S.UM_ADM_CHEM_E3_CRS, 
    S.UM_ADM_CHEM_E3_YR,
    S.UM_ADM_CHEM_E3_CR,
    S.UM_ADM_CHEM_E3_GR,
    S.UM_ADM_CHEM_E4_CRS, 
    S.UM_ADM_CHEM_E4_YR,
    S.UM_ADM_CHEM_E4_CR,
    S.UM_ADM_CHEM_E4_GR,
    S.UM_ADM_DG_CONCENTR, 
    S.UM_ADM_DG_ELEM_SCH, 
    S.UM_ADM_DG_COMM_MTH, 
    S.UM_ADM_DG_COMM_YR,
    S.UM_ADM_DG_CONT_YR,
    S.UM_ADM_DG_CONT_MTH, 
    S.UM_ADM_DG_FOUN_MTH, 
    S.UM_ADM_DG_FOUN_YR,
    S.UM_ADM_DG_MIDL_SCH, 
    S.UM_ADM_DG_SUBJ_MTH, 
    S.UM_ADM_DG_SUBJ_YR,
    S.UM_ADM_DG_N_APPLIC, 
    S.UM_ADM_DG_MATP_ACK, 
    S.UM_ADM_BG_EDC_LIC1, 
    S.UM_ADM_BG_EDC_LIC2, 
    S.UM_ADM_BG_EDC_LIC3, 
    S.UM_ADM_BG_ADMIN_L,
    S.UM_ADM_BG_OTH_LIC,
    S.UM_ADM_BG_GRD_DGR,
    S.UM_ADM_BG_CERT_NP,
    S.UM_ADM_BG_ADULT_NP, 
    S.UM_ADM_BG_PEDI_NP,
    S.UM_ADM_BG_FACULTY1, 
    S.UM_ADM_BG_FACULTY2, 
    S.UM_ADM_BG_FACULTY3, 
    S.UM_ADM_BG_CAR_GOAL, 
    S.UM_ADM_BG_CAR_OTH,
    S.UM_ADM_BG_DEGR_IN,
    S.UM_ADM_BG_GRAD_SER, 
    S.UM_ADM_BG_SERV_HRS, 
    S.UM_ADM_BG_ON_CAMP,
    S.UM_ADM_BG_ONLINE, 
    S.UM_ADM_DU_ONLINE, 
    S.UM_ADM_DU_CERT, 
    S.UM_ADM_BG_RESEARCH, 
    S.UM_LOWU_CLINICAL_1, 
    S.UM_ADM_BG_ADVISOR1, 
    S.UM_ADM_BG_ADVISOR2, 
    S.UM_ADM_BG_ADVISOR3, 
    S.UM_ADM_BG_SPEC_ED,
    S.UM_ADM_UMB_Z_AD_ON, 
    S.UM_ADM_UMB_Z_F_EMP, 
    S.UM_ADM_UMB_Z_FAM, 
    S.UM_ADM_UMB_Z_OTHER, 
    S.UM_ADM_UMB_Z_PRINT, 
    S.UM_ADM_UMB_Z_RADIO, 
    S.UM_ADM_UMB_Z_TEXT,
    S.UM_ADM_UMB_Z_TV,
    S.UM_ADM_UMB_Z_WEB, 
    S.UM_ADM_DU_UNIV_EXT, 
    S.UM_ADM_LG_AREA_INT, 
    S.UM_ADM_DU_DAY,
    S.UM_ADM_DU_NIGHT,
    S.UM_ADM_BG_SOC_PHD,
    S.UM_ADM_BG_BPE_QUES, 
    S.UM_ADM_PAY_REDO_DT, 
    S.UM_ADM_PAY_CMPL_DT, 
    S.UM_ADM_BG_DISB_LIC, 
    S.UM_ADM_BG_MASTERS,
    S.UM_ADM_BG_LICENS, 
    S.UM_ADM_PARTNERSHIP, 
    S.UM_BOSG_FLEX_MBA, 
    S.UM_BOSG_PRO_MBA,
    S.UM_BOSG_ACCEL_MAST, 
    S.UM_ADM_LU_DIAGN_DT, 
    S.UM_ADM_LU_MUS_AUD,
	S.UM_ADM_HEAR_US_CD,
	S.UM_ADM_HEAR_TEXT,
	S.UM_ENG_AS_SEC_LNG,
    S.UM_ADM_DG_PARTTIME, 
    'N',
    'S',
    sysdate,
    sysdate,
    1234);
    
    
strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;


strMessage01    := '# of PS_UM_ADM_APP_TMP rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_UM_ADM_APP_TMP',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_UM_ADM_APP_TMP';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_UM_ADM_APP_TMP';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_UM_ADM_APP_TMP';
update CSSTG_OWNER.PS_UM_ADM_APP_TMP T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select UM_ADM_USERID, UM_ADM_APP_SEQ, UM_ADM_REC_NBR, INSTITUTION
   from CSSTG_OWNER.PS_UM_ADM_APP_TMP T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_UM_ADM_APP_TMP') = 'Y'
  minus
 select UM_ADM_USERID, UM_ADM_APP_SEQ, UM_ADM_REC_NBR, INSTITUTION
   from SYSADM.PS_UM_ADM_APP_TMP@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_UM_ADM_APP_TMP') = 'Y'
   ) S
 where T.UM_ADM_USERID = S.UM_ADM_USERID 
   and T.UM_ADM_APP_SEQ = S.UM_ADM_APP_SEQ
   and T.UM_ADM_REC_NBR = S.UM_ADM_REC_NBR
   and T.INSTITUTION = S.INSTITUTION
   and T.SRC_SYS_ID = 'CS90') 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_UM_ADM_APP_TMP rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_UM_ADM_APP_TMP',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_UM_ADM_APP_TMP'
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

END PS_UM_ADM_APP_TMP_P;
/
