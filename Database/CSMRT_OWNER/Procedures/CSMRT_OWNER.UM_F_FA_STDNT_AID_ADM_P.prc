CREATE OR REPLACE PROCEDURE             "UM_F_FA_STDNT_AID_ADM_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--George Adams
--Loads table                -- UM_F_FA_STDNT_AID_ADM
--V01 12/12/2018             -- srikanth ,pabbu converted to proc from sql scripts
-- V01.2  SMT-8300 09/06/2017,    James Doucette
--                                Added two new fields, plus houskeeping fields.
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_FA_STDNT_AID_ADM';
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

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_FA_STDNT_AID_ADM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_FA_STDNT_AID_ADM');

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_FA_STDNT_AID_ADM disable constraint PK_UM_F_FA_STDNT_AID_ADM';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_FA_STDNT_AID_ADM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_FA_STDNT_AID_ADM';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_FA_STDNT_AID_ADM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_FA_STDNT_AID_ADM';				
insert /*+ append */ into CSMRT_OWNER.UM_F_FA_STDNT_AID_ADM
   WITH EXT
        AS (  SELECT  /*+ INLINE PARALLEL(8) */ 
                     PERSON_SID,            -- Add natural keys!!! 
                     SRC_SYS_ID,
                     EXT_ORG_SID,
                     INSTITUTION_SID,
                     MAX (CLASS_RANK) CLASS_RANK,
                     MAX (CLASS_SIZE) CLASS_SIZE,
                     MAX (CLASS_PERCENTILE) CLASS_PERCENTILE,
                     max(EXT_SUMM_TYPE_ID) EXT_SUMM_TYPE_ID,        -- Jan 2017 
                     MAX (EXT_GPA) EXT_GPA,
                     MAX (CONVERTED_GPA) CONVERTED_GPA,
                     MAX (UM_CUM_CREDIT) UM_CUM_CREDIT,
                     MAX (UM_CUM_GPA) UM_CUM_GPA,
                     MAX (UM_CUM_QP) UM_CUM_QP,
                     MAX (UM_GPA_EXCLUDE_FLG) UM_GPA_EXCLUDE_FLG,
                     MAX (UM_EXT_ORG_CR) UM_EXT_ORG_CR,
                     MAX (UM_EXT_ORG_QP) UM_EXT_ORG_QP,
                     MAX (UM_EXT_ORG_GPA) UM_EXT_ORG_GPA,
                     MAX (UM_EXT_ORG_CNV_CR) UM_EXT_ORG_CNV_CR,
                     MAX (UM_EXT_ORG_CNV_GPA) UM_EXT_ORG_CNV_GPA,
                     MAX (UM_EXT_ORG_CNV_QP) UM_EXT_ORG_CNV_QP,
                     MAX (UM_GPA_OVRD_FLG) UM_GPA_OVRD_FLG,
                     MAX (UM_1_OVRD_HSGPA_FLG) UM_1_OVRD_HSGPA_FLG,
                     MAX (UM_CONVERT_GPA) UM_CONVERT_GPA,
                     MAX (UM_EXT_OR_MTSC_GPA) UM_EXT_OR_MTSC_GPA,    -- SMT-8300 Sept 2019
                     MAX (MS_CONVERT_GPA) MS_CONVERT_GPA             -- SMT-8300 Sept 2019
                FROM PS_F_EXT_ACAD_SUMM
               WHERE DATA_ORIGIN <> 'D'
                 AND ROWNUM < 1000000000
            GROUP BY PERSON_SID,
                     SRC_SYS_ID,
                     EXT_ORG_SID,
                     INSTITUTION_SID),
        ADM
        AS (SELECT  /*+ INLINE PARALLEL(8) no_use_nl(F EXT) */ 
                   F.INSTITUTION_CD,
                   F.PERSON_ID,
                   F.INSTITUTION_SID,
                   F.APPLCNT_SID PERSON_SID,
                   F.ACAD_CAR_SID,                                      -- Jan 2017 
                   F.STU_CAR_NBR,                                       -- Jan 2017 
                   F.ADM_APPL_NBR,                                      -- Jan 2017 
                   F.APPL_PROG_NBR,                                     -- Jan 2017 
                   F.ACAD_PROG_SID,                                     -- Jan 2017 
                   F.ACAD_PLAN_SID,                                     -- Jan 2017 
                   F.ACAD_SPLAN_SID,                                    -- Jan 2017 
                   F.EFFDT,
                   F.EFFSEQ,
                   F.SRC_SYS_ID,
                   F.ACAD_CAR_CD,                                       -- Jan 2017 
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
                   TO_NUMBER (
                      NVL (TO_CHAR (LST_SCHL_GRDDT, 'YYYYMMDD'), '19000101'))
                      LST_SCHL_GRDDT,
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
                   NVL (UM_BHE_TRANS_CR, 0) UM_BHE_TRANS_CR,
                   NVL (UM_BHE_TRANS_GPA, 0) UM_BHE_TRANS_GPA,
                   NVL (UM_CA_FIRST_GEN, 'N') UM_CA_FIRST_GEN,          -- Added Mar 2017 
                   NVL (UM_RA_TA_INTEREST, 'N') UM_RA_TA_INTEREST,
                   F.STU_CAR_NBR_SR,                                    -- Added Mar 2016 
                   NVL (UM_TCA_COMPLETE, 'N') UM_TCA_COMPLETE,
                   NVL (UM_TCA_CREDITS, 0) UM_TCA_CREDITS,
                   EXT.EXT_SUMM_TYPE_ID,                                -- Jan 2017 
                   EXT.EXT_GPA,
                   decode(EXT.CONVERTED_GPA,0,NULL,EXT.CONVERTED_GPA) CONVERTED_GPA,    -- Jan 2017 
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
				   UM_EXT_OR_MTSC_GPA,    -- SMT-8300 Sept 2019
                   MS_CONVERT_GPA        -- SMT-8300 Sept 2019
              FROM UM_F_ADM_APPL_STAT F
                   LEFT OUTER JOIN EXT
                      ON     F.APPLCNT_SID = EXT.PERSON_SID
                         AND F.SRC_SYS_ID = EXT.SRC_SYS_ID
                         AND F.LST_SCHL_ATTND_SID = EXT.EXT_ORG_SID
                         AND F.INSTITUTION_SID = EXT.INSTITUTION_SID
              WHERE F.APPL_COUNT_ORDER = 1
                AND F.MAX_TERM_FLG = 'Y'),
        ADM_LIST
        AS (SELECT  /*+ INLINE PARALLEL(8) no_use_nl(A B) */ 
                   DISTINCT PERSON_SID,
                            LST_SCHL_ATTND_SID,
                            A.SRC_SYS_ID,
                            B.TERM_BEGIN_DT,
                            A.ADMIT_TERM_SID
              FROM ADM A, PS_D_TERM B
             WHERE A.ADMIT_TERM_SID = B.TERM_SID),
        EXT_TAB
        AS (SELECT  /*+ INLINE PARALLEL(8) no_use_nl(A E) */ 
                   DISTINCT
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
        AS (SELECT  /*+ INLINE PARALLEL(8) */ 
                  PERSON_SID,
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
              FROM EXT_TAB)
   SELECT  /*+ INLINE PARALLEL(8) no_use_nl(ADM SCORE DEG) */ 
          ADM.INSTITUTION_CD,
          ADM.PERSON_ID,
          ADM.INSTITUTION_SID,
          ADM.PERSON_SID,
          ADM.ACAD_CAR_SID,
          ADM.STU_CAR_NBR,
          ADM.ADM_APPL_NBR,
          ADM.APPL_PROG_NBR,
          ADM.ACAD_PROG_SID,
          ADM.ACAD_PLAN_SID,
          ADM.ACAD_SPLAN_SID,
          ADM.EFFDT,
          ADM.EFFSEQ,
          ADM.SRC_SYS_ID,
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
          NVL (
             (SELECT MIN (X.XLATSHORTNAME)
                FROM UM_D_XLATITEM_VW X
               WHERE     X.FIELDNAME = 'DEGREE_STATUS'
                     AND X.FIELDVALUE = DEG.EXT_DEG_STAT_ID),
             ' ')
             EXT_DEG_STAT_SD,
          NVL (
             (SELECT MIN (X.XLATLONGNAME)
                FROM UM_D_XLATITEM_VW X
               WHERE     X.FIELDNAME = 'DEGREE_STATUS'
                     AND X.FIELDVALUE = DEG.EXT_DEG_STAT_ID),
             ' ')
             EXT_DEG_STAT_LD,
          ADM.FIN_AID_INTEREST,
          ADM.HOUSING_INTEREST,
          NVL (
             (SELECT MIN (X.XLATSHORTNAME)
                FROM UM_D_XLATITEM_VW X
               WHERE     X.FIELDNAME = 'HOUSING_INTEREST'
                     AND X.FIELDVALUE = ADM.HOUSING_INTEREST),
             ' ')
             HOUSING_INTEREST_SD,
          NVL (
             (SELECT MIN (X.XLATLONGNAME)
                FROM UM_D_XLATITEM_VW X
               WHERE     X.FIELDNAME = 'HOUSING_INTEREST'
                     AND X.FIELDVALUE = ADM.HOUSING_INTEREST),
             ' ')
             HOUSING_INTEREST_LD,
          ADM.LST_SCHL_ATTND_SID,
          ADM.LST_SCHL_GRDDT,
          ADM.NOTIFICATION_PLAN,
          NVL (
             (SELECT MIN (X.XLATSHORTNAME)
                FROM UM_D_XLATITEM_VW X
               WHERE     X.FIELDNAME = 'NOTIFICATION_PLAN'
                     AND X.FIELDVALUE = ADM.NOTIFICATION_PLAN),
             ' ')
             NOTIFICATION_PLAN_SD,
          NVL (
             (SELECT MIN (X.XLATLONGNAME)
                FROM UM_D_XLATITEM_VW X
               WHERE     X.FIELDNAME = 'NOTIFICATION_PLAN'
                     AND X.FIELDVALUE = ADM.NOTIFICATION_PLAN),
             ' ')
             NOTIFICATION_PLAN_LD,
          ADM.UM_BHE,
          NVL (
             (SELECT MIN (X.XLATSHORTNAME)
                FROM UM_D_XLATITEM_VW X
               WHERE X.FIELDNAME = 'UM_BHE' AND X.FIELDVALUE = ADM.UM_BHE),
             ' ')
             UM_BHE_SD,
          NVL (
             (SELECT MIN (X.XLATLONGNAME)
                FROM UM_D_XLATITEM_VW X
               WHERE X.FIELDNAME = 'UM_BHE' AND X.FIELDVALUE = ADM.UM_BHE),
             ' ')
             UM_BHE_LD,
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
          ADM.UM_CA_FIRST_GEN,
          ADM.UM_RA_TA_INTEREST,
          NVL (
             (SELECT MIN (X.XLATSHORTNAME)
                FROM UM_D_XLATITEM_VW X
               WHERE     X.FIELDNAME = 'UM_RA_TA_INTEREST'
                     AND X.FIELDVALUE = ADM.UM_RA_TA_INTEREST),
             ' ')
             UM_RA_TA_INTEREST_SD,
          NVL (
             (SELECT MIN (X.XLATLONGNAME)
                FROM UM_D_XLATITEM_VW X
               WHERE     X.FIELDNAME = 'UM_RA_TA_INTEREST'
                     AND X.FIELDVALUE = ADM.UM_RA_TA_INTEREST),
             ' ')
             UM_RA_TA_INTEREST_LD,
          ADM.STU_CAR_NBR_SR,           -- Added Mar 2016 
          ADM.UM_TCA_COMPLETE,
          ADM.UM_TCA_CREDITS,
          ADM.EXT_SUMM_TYPE_ID,                        -- Jan 2017 
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
          SCORE.SAT_TOTAL_UM_SCORE,             -- Jan 2017 
          SCORE.SAT_TOTAL_1600_CONV_SCORE,      -- Jan 2017 
          SCORE.SAT_CONV_2016_SCORE,             -- Jan 2017 
		  UM_EXT_OR_MTSC_GPA,        -- SMT-8300
          MS_CONVERT_GPA,            -- SMT-8300
		  'S',                       -- SMT-8300
          SYSDATE,                   -- SMT-8300
          SYSDATE                    -- SMT-8300
     FROM ADM, 
          UM_F_ADM_APPL_TESTSCORE_AGG SCORE,     -- Jan 2017  
          EXT_DEG_TAB_MIN_NUM DEG
    WHERE ADM.PERSON_ID = SCORE.PERSON_ID
      AND ADM.ACAD_CAR_CD = SCORE.ACAD_CAR_CD
      AND ADM.STU_CAR_NBR = SCORE.STU_CAR_NBR
      AND ADM.ADM_APPL_NBR = SCORE.ADM_APPL_NBR
      AND ADM.SRC_SYS_ID = SCORE.SRC_SYS_ID
      AND ADM.PERSON_SID = DEG.PERSON_SID
      AND ADM.SRC_SYS_ID = DEG.SRC_SYS_ID
      AND ADM.LST_SCHL_ATTND_SID = DEG.LST_SCHL_ATTND_SID
      AND ADM.ADMIT_TERM_SID = DEG.ADMIT_TERM_SID
      AND DEG.ROW_NUM = 1;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_FA_STDNT_AID_ADM rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_FA_STDNT_AID_ADM',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_FA_STDNT_AID_ADM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_FA_STDNT_AID_ADM enable constraint PK_UM_F_FA_STDNT_AID_ADM';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_FA_STDNT_AID_ADM');

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

END UM_F_FA_STDNT_AID_ADM_P;
/
