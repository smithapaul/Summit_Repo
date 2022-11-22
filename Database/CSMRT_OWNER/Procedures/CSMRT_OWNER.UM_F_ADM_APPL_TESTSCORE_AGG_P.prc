DROP PROCEDURE CSMRT_OWNER.UM_F_ADM_APPL_TESTSCORE_AGG_P
/

--
-- UM_F_ADM_APPL_TESTSCORE_AGG_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_F_ADM_APPL_TESTSCORE_AGG_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--George Adams
--Loads table                -- UM_F_ADM_APPL_TESTSCORE_AGG
--V01 12/13/2018             -- srikanth ,pabbu converted to proc from sql scripts

------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_ADM_APPL_TESTSCORE_AGG';
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

strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_ADM_APPL_TESTSCORE_AGG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_ADM_APPL_TESTSCORE_AGG';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_ADM_APPL_TESTSCORE_AGG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_ADM_APPL_TESTSCORE_AGG');

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_ADM_APPL_TESTSCORE_AGG disable constraint PK_UM_F_ADM_APPL_TESTSCORE_AGG';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_ADM_APPL_TESTSCORE_AGG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_ADM_APPL_TESTSCORE_AGG';
insert /*+ append enable_parallel_dml parallel(8) */ into UM_F_ADM_APPL_TESTSCORE_AGG
   SELECT /*+ parallel(8) inline */
          DISTINCT
          A.PERSON_ID,
          A.ACAD_CAR_CD,
          A.STU_CAR_NBR,
          A.ADM_APPL_NBR,
          A.SRC_SYS_ID,
          A.INSTITUTION_CD,
          A.INSTITUTION_SID,
          A.ACAD_CAR_SID,
          A.APPLCNT_SID,
          A.UM_CA_TESTING_PLAN,
          TO_NUMBER (
             DECODE (TRIM (A.UM_CA_TESTING_PLAN),
                     'No Test', NULL,
                     ACT_COMP_SCORE))
             ACT_COMP_SCORE,
          TO_NUMBER (
             DECODE (TRIM (A.UM_CA_TESTING_PLAN),
                     'No Test', NULL,
                     ACT_MATH_SCORE))
             ACT_MATH_SCORE,
          TO_NUMBER (
             DECODE (TRIM (A.UM_CA_TESTING_PLAN),
                     'No Test', NULL,
                     ACT_VERB_SCORE))
             ACT_VERB_SCORE,
          TO_NUMBER (
             DECODE (TRIM (A.UM_CA_TESTING_PLAN),
                     'No Test', NULL,
                     ACT_WR_SCORE))
             ACT_WR_SCORE,
          TO_NUMBER (
             DECODE (TRIM (A.UM_CA_TESTING_PLAN),
                     'No Test', NULL,
                     ACT_CONV_SCORE))
             ACT_CONV_SCORE,
          GMAT_ANLY_SCORE,
          GMAT_QUAN_SCORE,
          GMAT_VERB_SCORE,
          GMAT_IR_SCORE,
          GMAT_TOTAL_SCORE,
          GRE_COMB_DECILE,
          GRE_ANLY_SCORE,
          GRE_QUAN_SCORE,
          GRE_VERB_SCORE,
          IELTS_BAND_SCORE,
          LSAT_COMP_SCORE,
          TO_NUMBER (
             DECODE (TRIM (A.UM_CA_TESTING_PLAN),
                     'No Test', NULL,
                     SAT_AHSSC_SCORE))
             SAT_AHSSC_SCORE,
          TO_NUMBER (
             DECODE (TRIM (A.UM_CA_TESTING_PLAN),
                     'No Test', NULL,
                     SAT_ASC_SCORE))
             SAT_ASC_SCORE,
          TO_NUMBER (
             DECODE (TRIM (A.UM_CA_TESTING_PLAN),
                     'No Test', NULL,
                     SAT_CE_SCORE))
             SAT_CE_SCORE,
          TO_NUMBER (
             DECODE (TRIM (A.UM_CA_TESTING_PLAN),
                     'No Test', NULL,
                     SAT_COMB_DECILE))
             SAT_COMB_DECILE,
          TO_NUMBER (
             DECODE (TRIM (A.UM_CA_TESTING_PLAN),
                     'No Test', NULL,
                     SAT_CONV_SCORE))
             SAT_CONV_SCORE,
--          TO_NUMBER (
--             DECODE (TRIM (A.UM_CA_TESTING_PLAN),
--                     'No Test', NULL,
--                     SAT_CONV_2016_SCORE))
--             SAT_CONV_2016_SCORE,                                     -- Oct 2016
          to_number(
          case when trim(A.UM_CA_TESTING_PLAN) = 'No Test' then NULL
               when A.ADMIT_TERM_CD >= '2830' then SAT_CONV_2018_SCORE
               else SAT_CONV_2016_SCORE
           end) SAT_CONV_2016_SCORE,                                    -- Dec 2018
          TO_NUMBER (
             DECODE (TRIM (A.UM_CA_TESTING_PLAN),
                     'No Test', NULL,
                     SAT_EI_SCORE))
             SAT_EI_SCORE,
          TO_NUMBER (
             DECODE (TRIM (A.UM_CA_TESTING_PLAN),
                     'No Test', NULL,
                     SAT_ERWS_SCORE))
             SAT_ERWS_SCORE,
          TO_NUMBER (
             DECODE (TRIM (A.UM_CA_TESTING_PLAN),
                     'No Test', NULL,
                     SAT_ERWS_CONV_SCORE))
             SAT_ERWS_CONV_SCORE,
          TO_NUMBER (
             DECODE (TRIM (A.UM_CA_TESTING_PLAN),
                     'No Test', NULL,
                     SAT_ESA_SCORE))
             SAT_ESA_SCORE,
          TO_NUMBER (
             DECODE (TRIM (A.UM_CA_TESTING_PLAN),
                     'No Test', NULL,
                     SAT_ESR_SCORE))
             SAT_ESR_SCORE,
          TO_NUMBER (
             DECODE (TRIM (A.UM_CA_TESTING_PLAN),
                     'No Test', NULL,
                     SAT_ESW_SCORE))
             SAT_ESW_SCORE,
          TO_NUMBER (
             DECODE (TRIM (A.UM_CA_TESTING_PLAN),
                     'No Test', NULL,
                     SAT_HA_SCORE))
             SAT_HA_SCORE,
          TO_NUMBER (
             DECODE (TRIM (A.UM_CA_TESTING_PLAN),
                     'No Test', NULL,
                     SAT_MATH_SCORE))
             SAT_MATH_SCORE,
          TO_NUMBER (
             DECODE (TRIM (A.UM_CA_TESTING_PLAN),
                     'No Test', NULL,
                     SAT_MSS_SCORE))
             SAT_MSS_SCORE,
          TO_NUMBER (
             DECODE (TRIM (A.UM_CA_TESTING_PLAN),
                     'No Test', NULL,
                     SAT_MSS_CONV_SCORE))
             SAT_MSS_CONV_SCORE,
          TO_NUMBER (
             DECODE (TRIM (A.UM_CA_TESTING_PLAN),
                     'No Test', NULL,
                     SAT_MT_SCORE))
             SAT_MT_SCORE,
          TO_NUMBER (
             DECODE (TRIM (A.UM_CA_TESTING_PLAN),
                     'No Test', NULL,
                     SAT_MT_CONV_SCORE))
             SAT_MT_CONV_SCORE,
          TO_NUMBER (
             DECODE (TRIM (A.UM_CA_TESTING_PLAN),
                     'No Test', NULL,
                     SAT_PAM_SCORE))
             SAT_PAM_SCORE,
          TO_NUMBER (
             DECODE (TRIM (A.UM_CA_TESTING_PLAN),
                     'No Test', NULL,
                     SAT_PSDA_SCORE))
             SAT_PSDA_SCORE,
          TO_NUMBER (
             DECODE (TRIM (A.UM_CA_TESTING_PLAN),
                     'No Test', NULL,
                     SAT_RT_SCORE))
             SAT_RT_SCORE,
          TO_NUMBER (
             DECODE (TRIM (A.UM_CA_TESTING_PLAN),
                     'No Test', NULL,
                     SAT_RT_CONV_SCORE))
             SAT_RT_CONV_SCORE,
          TO_NUMBER (
             DECODE (TRIM (A.UM_CA_TESTING_PLAN),
                     'No Test', NULL,
                     SAT_RWC_SCORE))
             SAT_RWC_SCORE,
          TO_NUMBER (
             DECODE (TRIM (A.UM_CA_TESTING_PLAN),
                     'No Test', NULL,
                     SAT_SEC_SCORE))
             SAT_SEC_SCORE,
          TO_NUMBER (
             DECODE (TRIM (A.UM_CA_TESTING_PLAN),
                     'No Test', NULL,
                     SAT_TOTAL_SCORE))
             SAT_TOTAL_SCORE,
          TO_NUMBER (
             DECODE (TRIM (A.UM_CA_TESTING_PLAN),
                     'No Test', NULL,
                     SAT_TOTAL_1600_CONV_SCORE))
             SAT_TOTAL_1600_CONV_SCORE,
          TO_NUMBER (
             DECODE (TRIM (A.UM_CA_TESTING_PLAN),
                     'No Test', NULL,
                     SAT_TOTAL_2400_CONV_SCORE))
             SAT_TOTAL_2400_CONV_SCORE,
          (  TO_NUMBER (
                DECODE (TRIM (A.UM_CA_TESTING_PLAN),
                        'No Test', NULL,
                        SAT_ERWS_SCORE))
           + TO_NUMBER (
                DECODE (TRIM (A.UM_CA_TESTING_PLAN),
                        'No Test', NULL,
                        SAT_MSS_SCORE)))
             SAT_TOTAL_UM_SCORE,
          TO_NUMBER (
             DECODE (TRIM (A.UM_CA_TESTING_PLAN),
                     'No Test', NULL,
                     SAT_VERB_SCORE))
             SAT_VERB_SCORE,
          TO_NUMBER (
             DECODE (TRIM (A.UM_CA_TESTING_PLAN),
                     'No Test', NULL,
                     SAT_WLT_SCORE))
             SAT_WLT_SCORE,
          TO_NUMBER (
             DECODE (TRIM (A.UM_CA_TESTING_PLAN),
                     'No Test', NULL,
                     SAT_WLT_CONV_SCORE))
             SAT_WLT_CONV_SCORE,
          TO_NUMBER (
             DECODE (TRIM (A.UM_CA_TESTING_PLAN),
                     'No Test', NULL,
                     SAT_WR_SCORE))
             SAT_WR_SCORE,
          TOEFL_COMPP_SCORE,
          TOEFL_IBTT_SCORE,
          UMDAR_INDEX_SCORE,
          UMLOW_INDEX_SCORE,
          (SELECT MAX (BEST_HS_GPA)
             FROM UM_F_ADM_APPL_EXT E
            WHERE     A.APPLCNT_SID = E.APPLCNT_SID
                  AND A.SRC_SYS_ID = E.SRC_SYS_ID
                  AND E.INSTITUTION_CD = 'UMBOS'
                  AND BEST_SUMM_TYPE_GPA_FLG = 'Y')
             UMBOS_BEST_HS_GPA,
          (SELECT MAX (BEST_HS_GPA)
             FROM UM_F_ADM_APPL_EXT E
            WHERE     A.APPLCNT_SID = E.APPLCNT_SID
                  AND A.SRC_SYS_ID = E.SRC_SYS_ID
                  AND E.INSTITUTION_CD = 'UMDAR'
                  AND BEST_SUMM_TYPE_GPA_FLG = 'Y')
             UMDAR_BEST_HS_GPA,
          (SELECT MAX (BEST_HS_GPA)
             FROM UM_F_ADM_APPL_EXT E
            WHERE     A.APPLCNT_SID = E.APPLCNT_SID
                  AND A.SRC_SYS_ID = E.SRC_SYS_ID
                  AND E.INSTITUTION_CD = 'UMLOW'
                  AND BEST_SUMM_TYPE_GPA_FLG = 'Y')
             UMLOW_BEST_HS_GPA
     FROM UM_F_ADM_APPL_STAT A
     LEFT OUTER JOIN UM_F_EXT_TESTSCORE_AGG T
       ON A.APPLCNT_SID = T.PERSON_SID
      AND A.SRC_SYS_ID = T.SRC_SYS_ID
    WHERE A.MAX_TERM_FLG = 'Y'
;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_ADM_APPL_TESTSCORE_AGG rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_ADM_APPL_TESTSCORE_AGG',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_ADM_APPL_TESTSCORE_AGG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_ADM_APPL_TESTSCORE_AGG enable constraint PK_UM_F_ADM_APPL_TESTSCORE_AGG';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_ADM_APPL_TESTSCORE_AGG');

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

END UM_F_ADM_APPL_TESTSCORE_AGG_P;
/
