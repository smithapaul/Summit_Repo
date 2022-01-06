CREATE OR REPLACE VIEW C_UM_F_EXT_TESTSCORE_AGG_VW
BEQUEATH DEFINER
AS 
SELECT PERSON_SID,
           SRC_SYS_ID,
           PERSON_ID,
           ACT_COMP_SCORE,
           ACT_MATH_SCORE,
           ACT_VERB_SCORE,
           ACT_WR_SCORE,
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
           SAT_AHSSC_SCORE,
           SAT_ASC_SCORE,
           SAT_CE_SCORE,
           SAT_COMB_DECILE,
           SAT_CONV_SCORE,
           SAT_CONV_2016_SCORE,                                    -- Oct 2016
           SAT_CONV_2018_SCORE,                                    -- Dec 2018
           SAT_EI_SCORE,
           SAT_ERWS_SCORE,
           SAT_ERWS_CONV_SCORE,
           SAT_ESA_SCORE,
           SAT_ESR_SCORE,
           SAT_ESW_SCORE,
           SAT_HA_SCORE,
           SAT_MATH_SCORE,
           SAT_MSS_SCORE,
           SAT_MSS_CONV_SCORE,
           SAT_MT_SCORE,
           SAT_MT_CONV_SCORE,
           SAT_PAM_SCORE,
           SAT_PSDA_SCORE,
           SAT_RT_SCORE,
           SAT_RT_CONV_SCORE,
           SAT_RWC_SCORE,
           SAT_SEC_SCORE,
           SAT_TOTAL_SCORE,
           SAT_TOTAL_1600_CONV_SCORE,
           SAT_TOTAL_2400_CONV_SCORE,
           (SAT_ERWS_SCORE + SAT_MSS_SCORE)
               SAT_TOTAL_UM_SCORE,
           SAT_VERB_SCORE,
           SAT_WLT_SCORE,
           SAT_WLT_CONV_SCORE,
           SAT_WR_SCORE,
           TOEFL_COMPP_SCORE,
           TOEFL_IBTT_SCORE,
           UMDAR_INDEX_SCORE,
           UMLOW_INDEX_SCORE,
           (SELECT MAX (BEST_HS_GPA)
              FROM UM_F_ADM_APPL_EXT E
             WHERE     A.PERSON_SID = E.APPLCNT_SID
                   AND A.SRC_SYS_ID = E.SRC_SYS_ID
                   AND E.INSTITUTION_CD = 'UMBOS'
                   --                  AND E.EXT_ACAD_CAR_ID = 'HS'
                   AND BEST_SUMM_TYPE_GPA_FLG = 'Y')
               UMBOS_BEST_HS_GPA,
           (SELECT MAX (BEST_HS_GPA)
              FROM UM_F_ADM_APPL_EXT E
             WHERE     A.PERSON_SID = E.APPLCNT_SID
                   AND A.SRC_SYS_ID = E.SRC_SYS_ID
                   AND E.INSTITUTION_CD = 'UMDAR'
                   --                  AND E.EXT_ACAD_CAR_ID = 'HS'
                   AND BEST_SUMM_TYPE_GPA_FLG = 'Y')
               UMDAR_BEST_HS_GPA,
           (SELECT MAX (BEST_HS_GPA)
              FROM UM_F_ADM_APPL_EXT E
             WHERE     A.PERSON_SID = E.APPLCNT_SID
                   AND A.SRC_SYS_ID = E.SRC_SYS_ID
                   AND E.INSTITUTION_CD = 'UMLOW'
                   --                  AND E.EXT_ACAD_CAR_ID = 'HS'
                   AND BEST_SUMM_TYPE_GPA_FLG = 'Y')
               UMLOW_BEST_HS_GPA
      FROM UM_F_EXT_TESTSCORE_AGG A
     WHERE PERSON_ID <> 'D'
	   AND ROWNUM < 10000000;
