CREATE OR REPLACE PROCEDURE             "UM_F_STDNT_ADM_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
-- Loads table                -- UM_F_STDNT_ADM
-- V01 12/13/2018             -- srikanth ,pabbu converted to proc from sql scripts
-- V02 Case: 70378 11/02/2020  Jim Doucette
--                             Add UM_CA_FIRST_GEN to UM_F_STDNT_ADM

------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_STDNT_ADM';
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

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_STDNT_ADM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_STDNT_ADM');

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_STDNT_ADM disable constraint PK_UM_F_STDNT_ADM';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_STDNT_ADM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_STDNT_ADM';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_STDNT_ADM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_STDNT_ADM';				
INSERT /*+ APPEND */ INTO CSMRT_OWNER.UM_F_STDNT_ADM
   WITH ACAD_TAB
        AS (SELECT /*+ INLINE PARALLEL(8) */ DISTINCT PERSON_SID,
                            INSTITUTION_SID,
                            INSTITUTION_CD,
                            SRC_SYS_ID SRC_SYS_ID,
                            ACAD_CAR_SID ACAD_CAR_SID,
                            ACAD_PROG_SID,
                            ACAD_PLAN_SID,
                            ACAD_SPLAN_SID,
                            STDNT_CAR_NUM STDNT_CAR_NUM
              FROM UM_F_STDNT_ACAD_STRUCT),
        EXT
        as (select /*+ PARALLEL(8) INLINE */
            PERSON_SID, SRC_SYS_ID, EXT_ORG_SID, INSTITUTION_SID, 
            max(CLASS_RANK) CLASS_RANK, max(CLASS_SIZE) CLASS_SIZE, max(CLASS_PERCENTILE) CLASS_PERCENTILE, max(EXT_GPA) EXT_GPA, max(CONVERTED_GPA) CONVERTED_GPA,
            max(UM_CUM_CREDIT) UM_CUM_CREDIT, max(UM_CUM_GPA) UM_CUM_GPA, max(UM_CUM_QP) UM_CUM_QP, max(UM_GPA_EXCLUDE_FLG) UM_GPA_EXCLUDE_FLG, 
            max(UM_EXT_ORG_CR) UM_EXT_ORG_CR, max(UM_EXT_ORG_QP) UM_EXT_ORG_QP, max(UM_EXT_ORG_GPA) UM_EXT_ORG_GPA, max(UM_EXT_ORG_CNV_CR) UM_EXT_ORG_CNV_CR, 
            max(UM_EXT_ORG_CNV_GPA) UM_EXT_ORG_CNV_GPA, max(UM_EXT_ORG_CNV_QP) UM_EXT_ORG_CNV_QP, 
            max(UM_GPA_OVRD_FLG) UM_GPA_OVRD_FLG, max(UM_1_OVRD_HSGPA_FLG) UM_1_OVRD_HSGPA_FLG, max(UM_CONVERT_GPA) UM_CONVERT_GPA,
            max(UM_EXT_OR_MTSC_GPA) UM_EXT_OR_MTSC_GPA,    -- SMT-8300
            max(MS_CONVERT_GPA) MS_CONVERT_GPA             -- SMT-8300		
            from PS_F_EXT_ACAD_SUMM
            where DATA_ORIGIN <> 'D'
            group by PERSON_SID, SRC_SYS_ID, EXT_ORG_SID, INSTITUTION_SID),
        ADM_TAB
        AS (SELECT /*+ INLINE PARALLEL(8) */
                  ACAD.PERSON_SID,
                   ACAD.INSTITUTION_SID,
                   ACAD.ACAD_CAR_SID,
--                   ACAD.STDNT_CAR_NUM STU_CAR_NBR,
                   NVL (ADM.STU_CAR_NBR, 0) STU_CAR_NBR,    -- Sept 2016 
                   NVL (ADM_APPL_NBR, '0') ADM_APPL_NBR,
                   NVL (APPL_PROG_NBR, 0) APPL_PROG_NBR,
                   ACAD.ACAD_PROG_SID SR_ACAD_PROG_SID,
                   ACAD.ACAD_PLAN_SID SR_ACAD_PLAN_SID,
                   ACAD.ACAD_SPLAN_SID SR_ACAD_SPLAN_SID,
                   NVL (ADM.ACAD_PROG_SID, 2147483646) ADM_ACAD_PROG_SID,
                   NVL (ADM.ACAD_PLAN_SID, 2147483646) ADM_ACAD_PLAN_SID,
                   NVL (ADM.ACAD_SPLAN_SID, 2147483646) ADM_ACAD_SPLAN_SID,
                   ACAD.SRC_SYS_ID SRC_SYS_ID,
                   ACAD.INSTITUTION_CD INSTITUTION_CD,
                   NVL (ADMIT_TERM_SID, 2147483646) ADMIT_TERM_SID,
                   NVL (ADMIT_TYPE_SID, 2147483646) ADMIT_TYPE_SID,
                   NVL (ACAD_LVL_SID, 2147483646) ACAD_LVL_SID,
                   NVL (ACAD_LOAD_SID, 2147483646) ACAD_LOAD_SID,
                   NVL (PROG_STAT_SID, 2147483646) PROG_STAT_SID,
                   NVL (PROG_ACN_SID, 2147483646) PROG_ACN_SID,
                   NVL (PROG_ACN_RSN_SID, 2147483646) PROG_ACN_RSN_SID,
                   ACTION_DT,
                   APPL_DT,
                   NVL (APPL_CNTR_SID, 2147483646) APPL_CNTR_SID,
                   NVL (APPL_MTHD_SID, 2147483646) APPL_MTHD_SID,
                   NVL (FIN_AID_INTEREST, '-') FIN_AID_INTEREST,
                   NVL (HOUSING_INTEREST, '-') HOUSING_INTEREST,
                   NVL (LST_SCHL_ATTND_SID, 2147483646) LST_SCHL_ATTND_SID,
                   to_number(nvl(to_char(LST_SCHL_GRDDT,'YYYYMMDD'),'19000101')) LST_SCHL_GRDDT,
                   NVL (NOTIFICATION_PLAN, '-') NOTIFICATION_PLAN,
                   NVL (UM_BHE, 'N') UM_BHE,
                   NVL (UM_BHE_ENG, 'N') UM_BHE_ENG,
                   NVL (UM_BHE_SOCSCI, 'N') UM_BHE_SOCSCI,
                   NVL (UM_BHE_SCI, 'N') UM_BHE_SCI,
                   NVL (UM_BHE_MATH, 'N') UM_BHE_MATH,
                   NVL (UM_BHE_ELT, 'N') UM_BHE_ELT,
                   NVL (UM_BHE_FRLG, 'N') UM_BHE_FRLG,
                   NVL (UM_BHE_CMPLT, 'N') UM_BHE_CMPLT,
                   NVL (UM_BHE_EXP_VOCTEC, 'N') UM_BHE_EXP_VOCTEC,
                   NVL (UM_BHE_EXP_ESL, 'N') UM_BHE_EXP_ESL,
                   NVL (UM_BHE_EXP_INTL, 'N') UM_BHE_EXP_INTL,
                   NVL (UM_BHE_PRECOLLEGE, 'N') UM_BHE_PRECOLLEGE,
                   NVL (UM_BHE_EXP_LD, 'N') UM_BHE_EXP_LD,
--                   NVL (UM_BHE_TRANS_CR, 0) UM_BHE_TRANS_CR,
                   UM_BHE_TRANS_CR,					-- Jan 2017 
--                   NVL (UM_BHE_TRANS_GPA, 0) UM_BHE_TRANS_GPA,
                   UM_BHE_TRANS_GPA,					-- Jan 2017 
                   NVL (UM_RA_TA_INTEREST, 'N') UM_RA_TA_INTEREST,
                   ACAD.STDNT_CAR_NUM STU_CAR_NBR_SR,
                   NVL (UM_TCA_COMPLETE, 'N') UM_TCA_COMPLETE,
                   NVL (UM_TCA_CREDITS, 0) UM_TCA_CREDITS,
                   EXT.EXT_GPA,
                   EXT.CONVERTED_GPA,
                   EXT.UM_CUM_CREDIT,
                   EXT.UM_CUM_GPA,
                   EXT.UM_CUM_QP,
                   EXT.UM_GPA_EXCLUDE_FLG,
                   EXT.UM_EXT_ORG_CR,
                   EXT.UM_EXT_ORG_QP,
                   EXT.UM_EXT_ORG_GPA,
                   EXT.UM_EXT_ORG_CNV_CR,
                   EXT.UM_EXT_ORG_CNV_GPA,
                   EXT.UM_EXT_ORG_CNV_QP,
                   EXT.UM_GPA_OVRD_FLG,
                   EXT.UM_1_OVRD_HSGPA_FLG,
                   EXT.UM_CONVERT_GPA,
				   EXT.UM_EXT_OR_MTSC_GPA,
				   EXT.MS_CONVERT_GPA,
				   ADM.UM_CA_FIRST_GEN        -- Case 70378 11/02/2020
              FROM ACAD_TAB ACAD
                   LEFT OUTER JOIN UM_F_ADM_APPL_STAT ADM
                     ON ACAD.PERSON_SID = ADM.APPLCNT_SID
                    AND ACAD.SRC_SYS_ID = ADM.SRC_SYS_ID
                    AND ACAD.ACAD_CAR_SID = ADM.ACAD_CAR_SID
                    AND ACAD.STDNT_CAR_NUM = ADM.STU_CAR_NBR_SR
                    AND APPL_COUNT_ORDER = 1
                    AND MAX_TERM_FLG = 'Y'
                   left outer join EXT 
                     on ADM.APPLCNT_SID = EXT.PERSON_SID
                    and ADM.SRC_SYS_ID = EXT.SRC_SYS_ID
                    and ADM.LST_SCHL_ATTND_SID = EXT.EXT_ORG_SID
                    and ADM.INSTITUTION_SID = EXT.INSTITUTION_SID),
        ADM_LIST
        AS (SELECT /*+ INLINE PARALLEL(8) */ DISTINCT PERSON_SID,
                            LST_SCHL_ATTND_SID,
                            A.SRC_SYS_ID,
                            B.TERM_BEGIN_DT,
                            A.ADMIT_TERM_SID
              FROM ADM_TAB A, PS_D_TERM B
             WHERE A.ADMIT_TERM_SID = B.TERM_SID),
        SCORE_TAB                            -- Remove SCORE columns!!!
        AS (SELECT /*+ INLINE PARALLEL(8) */ 
                   DISTINCT ADM.PERSON_SID,
                            ADM.SRC_SYS_ID,
                            ACT_COMP_SCORE,
                            ACT_CONV_SCORE,                           -- Added
                            GMAT_TOTAL_SCORE,
                            GRE_COMB_DECILE,                          -- Added
                            GRE_ANLY_SCORE,
                            GRE_QUAN_SCORE,
                            GRE_VERB_SCORE,
                            IELTS_BAND_SCORE,
                            LSAT_COMP_SCORE,
                            SAT_COMB_DECILE,                          -- Added
                            SAT_MATH_SCORE,
                            SAT_VERB_SCORE,
                            SAT_CONV_SCORE,                           -- Added
                            TOEFL_IBTT_SCORE,
                            UMDAR_INDEX_SCORE,
                            UMLOW_INDEX_SCORE
              FROM ADM_LIST ADM
--                   LEFT OUTER JOIN UM_F_ADM_APPL_TESTSCORE_AGG SCORE
                   LEFT OUTER JOIN UM_F_EXT_TESTSCORE_AGG SCORE
                      ON     ADM.PERSON_SID = SCORE.PERSON_SID
                         AND ADM.SRC_SYS_ID = SCORE.SRC_SYS_ID),
        EXT_TAB
        AS (SELECT /*+ INLINE PARALLEL(8) */ DISTINCT
                   A.PERSON_SID,
                   A.LST_SCHL_ATTND_SID,
                   A.SRC_SYS_ID,
                   A.ADMIT_TERM_SID,
                   A.TERM_BEGIN_DT,
                   CASE
                      WHEN A.TERM_BEGIN_DT < EXT_DEG_DT THEN 2147483646
                      ELSE EXT_DEG_SID
                   END
                      EXT_DEG_SID,
                   CASE
                      WHEN A.TERM_BEGIN_DT < EXT_DEG_DT THEN 9999999
                      ELSE EXT_DEG_NBR
                   END
                      EXT_DEG_NBR,
                   CASE
                      WHEN A.TERM_BEGIN_DT < EXT_DEG_DT
                      THEN
                         TO_DATE (19000101, 'YYYYMMDD')
                      ELSE
                         EXT_DEG_DT
                   END
                      EXT_DEG_DT,
                   CASE
                      WHEN A.TERM_BEGIN_DT < EXT_DEG_DT THEN '-'
                      ELSE EXT_DEG_STAT_ID
                   END
                      EXT_DEG_STAT_ID
              FROM ADM_LIST A
                   LEFT OUTER JOIN UM_F_EXT_DEG E
                      ON     A.PERSON_SID = E.PERSON_SID
                         AND A.SRC_SYS_ID = E.SRC_SYS_ID
                         AND A.LST_SCHL_ATTND_SID = E.EXT_ORG_SID),
        EXT_DEG_TAB_MIN_NUM
        AS (SELECT /*+ INLINE PARALLEL(8) */ PERSON_SID,
                   LST_SCHL_ATTND_SID,
                   SRC_SYS_ID,
                   ADMIT_TERM_SID,
                   EXT_DEG_SID,
                   EXT_DEG_NBR,
                   EXT_DEG_DT,
                   EXT_DEG_STAT_ID,
                   ROW_NUMBER ()
                   OVER (
                      PARTITION BY PERSON_SID,
                                   LST_SCHL_ATTND_SID,
                                   ADMIT_TERM_SID
                      ORDER BY EXT_DEG_DT DESC, EXT_DEG_NBR)
                      ROW_NUM
              FROM EXT_TAB),
        XLAT
        AS
            (SELECT /*+ INLINE PARALLEL(8) */
                    FIELDNAME, FIELDVALUE, SRC_SYS_ID,
                    XLATLONGNAME, XLATSHORTNAME
               FROM UM_D_XLATITEM)
   SELECT /*+ INLINE PARALLEL(8) */ 
          ADM.PERSON_SID,        -- Add natural keys!!! 
          ADM.INSTITUTION_SID,
          ADM.ACAD_CAR_SID,
          ADM.STU_CAR_NBR,
          ADM.STU_CAR_NBR_SR,
          ADM.ADM_APPL_NBR,
          APPL_PROG_NBR,
          SR_ACAD_PROG_SID,
          SR_ACAD_PLAN_SID,
          SR_ACAD_SPLAN_SID,
          ADM_ACAD_PROG_SID,
          ADM_ACAD_PLAN_SID,
          ADM_ACAD_SPLAN_SID,
          ADM.SRC_SYS_ID,
          ADM.INSTITUTION_CD,
          ADM.ADMIT_TERM_SID,
          ADM.ADMIT_TYPE_SID,
          ADM.ACAD_LVL_SID,
          ADM.ACAD_LOAD_SID,
          ADM.PROG_STAT_SID,
          ADM.PROG_ACN_SID,
          ADM.PROG_ACN_RSN_SID,
          ACTION_DT,
          APPL_DT,
          ADM.APPL_CNTR_SID,
          ADM.APPL_MTHD_SID,
          NVL (DEG.EXT_DEG_SID, 2147483646) EXT_DEG_SID,
          DEG.EXT_DEG_DT,
          NVL (DEG.EXT_DEG_STAT_ID, ' ') EXT_DEG_STAT_ID,
--           NVL (
--               (SELECT MIN (X.XLATSHORTNAME)
--                  FROM UM_D_XLATITEM_VW X
--                 WHERE     X.FIELDNAME = 'DEGREE_STATUS'
--                       AND X.FIELDVALUE = DEG.EXT_DEG_STAT_ID),
--               ' ')                             EXT_DEG_STAT_SD,
--           NVL (
--               (SELECT MIN (X.XLATLONGNAME)
--                  FROM UM_D_XLATITEM_VW X
--                 WHERE     X.FIELDNAME = 'DEGREE_STATUS'
--                       AND X.FIELDVALUE = DEG.EXT_DEG_STAT_ID),
--               ' ')                             EXT_DEG_STAT_LD,
           NVL (X1.XLATSHORTNAME,' ')           EXT_DEG_STAT_SD,        -- June 2021 
           NVL (X1.XLATLONGNAME,' ')            EXT_DEG_STAT_LD,        -- June 2021 
          ADM.FIN_AID_INTEREST,
          ADM.HOUSING_INTEREST,
--           NVL (
--               (SELECT MIN (X.XLATSHORTNAME)
--                  FROM UM_D_XLATITEM_VW X
--                 WHERE     X.FIELDNAME = 'HOUSING_INTEREST'
--                       AND X.FIELDVALUE = ADM.HOUSING_INTEREST),
--               ' ')                             HOUSING_INTEREST_SD,
--           NVL (
--               (SELECT MIN (X.XLATLONGNAME)
--                  FROM UM_D_XLATITEM_VW X
--                 WHERE     X.FIELDNAME = 'HOUSING_INTEREST'
--                       AND X.FIELDVALUE = ADM.HOUSING_INTEREST),
--               ' ')                             HOUSING_INTEREST_LD,
           NVL (X2.XLATSHORTNAME,' ')           HOUSING_INTEREST_SD,    -- June 2021 
           NVL (X2.XLATLONGNAME,' ')            HOUSING_INTEREST_LD,    -- June 2021 
          ADM.LST_SCHL_ATTND_SID,
          ADM.LST_SCHL_GRDDT,
          ADM.NOTIFICATION_PLAN,
--           NVL (
--               (SELECT MIN (X.XLATSHORTNAME)
--                  FROM UM_D_XLATITEM_VW X
--                 WHERE     X.FIELDNAME = 'NOTIFICATION_PLAN'
--                       AND X.FIELDVALUE = ADM.NOTIFICATION_PLAN),
--               ' ')                             NOTIFICATION_PLAN_SD,
--           NVL (
--               (SELECT MIN (X.XLATLONGNAME)
--                  FROM UM_D_XLATITEM_VW X
--                 WHERE     X.FIELDNAME = 'NOTIFICATION_PLAN'
--                       AND X.FIELDVALUE = ADM.NOTIFICATION_PLAN),
--               ' ')                             NOTIFICATION_PLAN_LD,
           NVL (X3.XLATSHORTNAME,' ')           NOTIFICATION_PLAN_SD,   -- June 2021 
           NVL (X3.XLATLONGNAME,' ')            NOTIFICATION_PLAN_LD,   -- June 2021 
          ADM.UM_BHE,
--           NVL (
--               (SELECT MIN (X.XLATSHORTNAME)
--                  FROM UM_D_XLATITEM_VW X
--                 WHERE X.FIELDNAME = 'UM_BHE' AND X.FIELDVALUE = ADM.UM_BHE),
--               ' ')                             UM_BHE_SD,
--           NVL (
--               (SELECT MIN (X.XLATLONGNAME)
--                  FROM UM_D_XLATITEM_VW X
--                 WHERE X.FIELDNAME = 'UM_BHE' AND X.FIELDVALUE = ADM.UM_BHE),
--               ' ')                             UM_BHE_LD,
           NVL (X4.XLATSHORTNAME,' ')           UM_BHE_SD,              -- June 2021 
           NVL (X4.XLATLONGNAME,' ')            UM_BHE_LD,              -- June 2021 
          ADM.UM_BHE_ENG,
          ADM.UM_BHE_SOCSCI,
          ADM.UM_BHE_SCI,
          ADM.UM_BHE_MATH,
          ADM.UM_BHE_ELT,
          ADM.UM_BHE_FRLG,
          ADM.UM_BHE_CMPLT,
          ADM.UM_BHE_EXP_VOCTEC,
          ADM.UM_BHE_EXP_ESL,
          ADM.UM_BHE_EXP_INTL,
          ADM.UM_BHE_PRECOLLEGE,
          ADM.UM_BHE_EXP_LD,
          ADM.UM_BHE_TRANS_CR,
          ADM.UM_BHE_TRANS_GPA,
          ADM.UM_RA_TA_INTEREST,
--           NVL (
--               (SELECT MIN (X.XLATSHORTNAME)
--                  FROM UM_D_XLATITEM_VW X
--                 WHERE     X.FIELDNAME = 'UM_RA_TA_INTEREST'
--                       AND X.FIELDVALUE = ADM.UM_RA_TA_INTEREST),
--               ' ')                             UM_RA_TA_INTEREST_SD,
--           NVL (
--               (SELECT MIN (X.XLATLONGNAME)
--                  FROM UM_D_XLATITEM_VW X
--                 WHERE     X.FIELDNAME = 'UM_RA_TA_INTEREST'
--                       AND X.FIELDVALUE = ADM.UM_RA_TA_INTEREST),
--               ' ')                             UM_RA_TA_INTEREST_LD,
           NVL (X5.XLATSHORTNAME,' ')           UM_RA_TA_INTEREST_SD,   -- June 2021 
           NVL (X5.XLATLONGNAME,' ')            UM_RA_TA_INTEREST_LD,   -- June 2021 
          ADM.UM_TCA_COMPLETE,
          ADM.UM_TCA_CREDITS,
          ADM.EXT_GPA,
          ADM.CONVERTED_GPA,
          ADM.UM_CUM_CREDIT,
          ADM.UM_CUM_GPA,
          ADM.UM_CUM_QP,
          ADM.UM_GPA_EXCLUDE_FLG,
          ADM.UM_EXT_ORG_CR,
          ADM.UM_EXT_ORG_QP,
          ADM.UM_EXT_ORG_GPA,
          ADM.UM_EXT_ORG_CNV_CR,
          ADM.UM_EXT_ORG_CNV_GPA,
          ADM.UM_EXT_ORG_CNV_QP,
          ADM.UM_GPA_OVRD_FLG,
          ADM.UM_1_OVRD_HSGPA_FLG,
          ADM.UM_CONVERT_GPA,
          TO_DATE('01-JAN-1900') TEST_EFFDT, 
          SCORE.ACT_COMP_SCORE,            -- Remove SCORE columns!!!
          SCORE.ACT_CONV_SCORE,
          SCORE.GMAT_TOTAL_SCORE,
          SCORE.GRE_COMB_DECILE,
          SCORE.GRE_ANLY_SCORE,
          SCORE.GRE_QUAN_SCORE,
          SCORE.GRE_VERB_SCORE,
          SCORE.IELTS_BAND_SCORE,
          SCORE.LSAT_COMP_SCORE,
          SCORE.SAT_COMB_DECILE,
          SCORE.SAT_MATH_SCORE,
          SCORE.SAT_VERB_SCORE,
          SCORE.SAT_CONV_SCORE,
          SCORE.TOEFL_IBTT_SCORE,
          SCORE.UMDAR_INDEX_SCORE,
          SCORE.UMLOW_INDEX_SCORE, 
		  ADM.UM_EXT_OR_MTSC_GPA,    -- SMT-8300
		  ADM.MS_CONVERT_GPA,        -- SMT-8300
		  ADM.UM_CA_FIRST_GEN,       -- CASE-70378
		  'S',                       -- SMT-8300
          SYSDATE,                   -- SMT-8300
          SYSDATE                    -- SMT-8300
      FROM ADM_TAB ADM 
      JOIN SCORE_TAB SCORE
        ON ADM.PERSON_SID = SCORE.PERSON_SID
       AND ADM.SRC_SYS_ID = SCORE.SRC_SYS_ID
      JOIN EXT_DEG_TAB_MIN_NUM DEG
        ON ADM.PERSON_SID = DEG.PERSON_SID
       AND ADM.SRC_SYS_ID = DEG.SRC_SYS_ID
       AND ADM.LST_SCHL_ATTND_SID = DEG.LST_SCHL_ATTND_SID
       AND ADM.ADMIT_TERM_SID = DEG.ADMIT_TERM_SID
       AND DEG.ROW_NUM = 1
      LEFT OUTER JOIN XLAT X1                           -- June 2021 
        ON X1.FIELDNAME = 'DEGREE_STATUS'
       AND X1.FIELDVALUE = DEG.EXT_DEG_STAT_ID
       AND X1.SRC_SYS_ID = DEG.SRC_SYS_ID
      LEFT OUTER JOIN XLAT X2
        ON X2.FIELDNAME = 'HOUSING_INTEREST'
       AND X2.FIELDVALUE = ADM.HOUSING_INTEREST
       AND X2.SRC_SYS_ID = ADM.SRC_SYS_ID
      LEFT OUTER JOIN XLAT X3
        ON X3.FIELDNAME = 'NOTIFICATION_PLAN'
       AND X3.FIELDVALUE = ADM.NOTIFICATION_PLAN
       AND X3.SRC_SYS_ID = ADM.SRC_SYS_ID
      LEFT OUTER JOIN XLAT X4
        ON X4.FIELDNAME = 'UM_BHE'
       AND X4.FIELDVALUE = ADM.UM_BHE
       AND X4.SRC_SYS_ID = ADM.SRC_SYS_ID
      LEFT OUTER JOIN XLAT X5
        ON X5.FIELDNAME = 'UM_RA_TA_INTEREST'
       AND X5.FIELDVALUE = ADM.UM_RA_TA_INTEREST
       AND X5.SRC_SYS_ID = ADM.SRC_SYS_ID
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_STDNT_ADM rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_STDNT_ADM',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_STDNT_ADM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_STDNT_ADM enable constraint PK_UM_F_STDNT_ADM';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_STDNT_ADM');

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

END UM_F_STDNT_ADM_P;
/
