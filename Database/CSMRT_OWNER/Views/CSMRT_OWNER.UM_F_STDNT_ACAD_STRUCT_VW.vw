DROP VIEW CSMRT_OWNER.UM_F_STDNT_ACAD_STRUCT_VW
/

--
-- UM_F_STDNT_ACAD_STRUCT_VW  (View) 
--
CREATE OR REPLACE VIEW CSMRT_OWNER.UM_F_STDNT_ACAD_STRUCT_VW
BEQUEATH DEFINER
AS 
SELECT /*+ OPT_ESTIMATE(TABLE UM_F_STDNT_ACAD_STRUCT MIN=100000) */
          TERM_SID,
          PERSON_SID,
          STDNT_CAR_NUM,
          ACAD_PLAN_SID,
          ACAD_SPLAN_SID,
          SRC_SYS_ID,
          INSTITUTION_CD,
          ACAD_CAR_CD,
          TERM_CD,
          PERSON_ID,
          ACAD_PROG_CD,
          ACAD_PLAN_CD,
          ACAD_SPLAN_CD,
          EFFDT,
          EFFSEQ,
          ACTION_DT,
          ACAD_CAR_SID,
          ACAD_PROG_SID,
          ADMIT_TERM_SID,
          CAMPUS_SID,
          COMPL_TERM_SID,
          EXP_GRAD_TERM_SID,
          INSTITUTION_SID,
          CALC_TERM_LIMIT_SID,                               -- Added Feb 2016
          NEW_TERM_LIMIT_SID,                                -- Added Feb 2016
          PROG_STAT_SID,
          PROG_ACN_SID,
          PROG_ACN_RSN_SID,
          REQ_TERM_SID,
          STACK_BEGIN_TERM_SID,
          STACK_READMIT_TERM_SID,
          PLAN_COMPL_TERM_SID,
          PLAN_REQ_TERM_SID,
          SPLAN_REQ_TERM_SID,
          ADM_APPL_NBR,
          CERTIFICATE_ONLY_FLG,
          D_RANK,
          D_RANK_PTYPE,
          D_RANK_SPLAN,
          DATA_FROM_ADM_APPL_FLG,
          DEGR_CHKOUT_STAT,
          DEGR_CHKOUT_STAT_SD,
          DEGR_CHKOUT_STAT_LD,
          DEGR_CHKOUT_LAST,                                 -- Added Sept 2015
          DEGR_CHKOUT_LAST_SD,                              -- Added Sept 2015
          DEGR_CHKOUT_LAST_LD,                              -- Added Sept 2015
          DEGR_CHKOUT_LAST_EGT,                              -- Added Dec 2015
          DEGR_CHKOUT_LAST_EGT_SD,                           -- Added Dec 2015
          DEGR_CHKOUT_LAST_EGT_LD,                           -- Added Dec 2015
          DEGR_CHKOUT_LAST_EGT_EFFDT,                        -- Added Dec 2015
          DEGR_CHKOUT_LAST_EGT_ORDER,                        -- Added Dec 2015
          DEGREE_NBR,                                        -- Added Oct 2017  
          DEGREE_SEEKING_FLG,
          ED_LVL_RANK,
          MAJOR_RANK,                                       -- Added Sept 2015
          MINOR_RANK,                                       -- Added Sept 2015
          SPLAN_RANK,                                       -- Added Sept 2015
          PRIM_STACK_CAREER_RANK,                           -- Added Sept 2015
          PRIM_STACK_STDNT_RANK,                            -- Added Sept 2015
          MIN_PROG_STAT_CTGRY,
          MISSING_PROG_PLAN_FLG,
          PROGRAM_CATGRY,
          PLAN_ADVIS_STAT,
          PLAN_DECLARE_DT,
          PLAN_GPA,                                          -- Added Mar 2017
          PLAN_GPA_DT,                                       -- Added Mar 2017
          PLAN_SEQUENCE,
          PLAN_DEGR_CHKOUT_STAT,
          PLAN_STDNT_DEGR_CD,
          SPLAN_DECLARE_DT,
          STACK_BEGIN_FLG,
          STACK_CONTINUE_FLG,
          STACK_READMIT_EFFDT,                               -- Added Dec 2015
          STACK_READMIT_FLG,
          STACK_LAST_UPD_TERM_CD,                            -- Added APR 2015
          STACK_LAST_UPD_TERM_SID,                           -- Added Oct 2017   
          TERM_BEGIN_DT,                                     -- Added Oct 2017   
          TERM_END_DT,                                       -- Added Oct 2017  
          DEG_LIMIT_EFFDT,                                   -- Added Feb 2016
          NEW_TERM_LIMIT_CD,                                 -- Added Feb 2016
          CALC_TERM_LIMIT_CD,                                -- Added Feb 2016
          DEG_LIMIT_UM_OVRRIDE_EXTENSN,                      -- Added Feb 2016
          DEG_LIMIT_YEARS,                                   -- Added Feb 2016
          DEG_LIMIT_COMMENTS_MSGS,                           -- Added Feb 2016
          UMDAR_ED_LVL,
          UMDAR_ED_LVL_SD,
          UMDAR_ED_LVL_LD,
          PROG_CNT,
          (CASE WHEN MAJOR_RANK = 1 THEN 1 ELSE 0 END) MAJOR_1_CNT, -- Added Sept 2015
          (CASE WHEN MAJOR_RANK = 1 AND SPLAN_RANK = 1 THEN 1 ELSE 0 END) MAJOR_1_SPLAN_1_CNT,                           -- Added Sept 2015
          (CASE WHEN MAJOR_RANK = 1 AND SPLAN_RANK = 2 THEN 1 ELSE 0 END) MAJOR_1_SPLAN_2_CNT,                           -- Added Sept 2015
          (CASE WHEN MAJOR_RANK = 1 AND SPLAN_RANK = 3 THEN 1 ELSE 0 END) MAJOR_1_SPLAN_3_CNT,                           -- Added Sept 2015
          (CASE WHEN MAJOR_RANK = 2 THEN 1 ELSE 0 END) MAJOR_2_CNT, -- Added Sept 2015
          (CASE WHEN MAJOR_RANK = 2 AND SPLAN_RANK = 1 THEN 1 ELSE 0 END) MAJOR_2_SPLAN_1_CNT,                           -- Added Sept 2015
          (CASE WHEN MAJOR_RANK = 2 AND SPLAN_RANK = 2 THEN 1 ELSE 0 END) MAJOR_2_SPLAN_2_CNT,                           -- Added Sept 2015
          (CASE WHEN MAJOR_RANK = 3 THEN 1 ELSE 0 END) MAJOR_3_CNT, -- Added Sept 2015
          (CASE WHEN MAJOR_RANK = 3 AND SPLAN_RANK = 1 THEN 1 ELSE 0 END) MAJOR_3_SPLAN_1_CNT,                           -- Added Sept 2015
          (CASE WHEN MAJOR_RANK = 3 AND SPLAN_RANK = 2 THEN 1 ELSE 0 END) MAJOR_3_SPLAN_2_CNT,                           -- Added Sept 2015
          (CASE WHEN MINOR_RANK = 1 THEN 1 ELSE 0 END) MINOR_1_CNT, -- Added Sept 2015
          (CASE WHEN MINOR_RANK = 2 THEN 1 ELSE 0 END) MINOR_2_CNT, -- Added Sept 2015
          (CASE WHEN MINOR_RANK = 3 THEN 1 ELSE 0 END) MINOR_3_CNT, -- Added Sept 2015
          (CASE WHEN MAJOR_RANK = 4 THEN 1 ELSE 0 END) OTHER_PLAN_CNT, -- Added Sept 2015
          PRIM_PROG_MAJOR_1_CNT,
          PRIM_PROG_MAJOR_2_CNT,
          PRIM_PROG_MINOR_1_CNT,
          PRIM_PROG_MINOR_2_CNT,
          PRIM_PROG_OTHER_PLAN_CNT,
          SEC_PROG_MAJOR_1_CNT,
          SEC_PROG_MAJOR_2_CNT,
          SEC_PROG_MINOR_1_CNT,
          SEC_PROG_MINOR_2_CNT,
          SEC_PROG_OTHER_PLAN_CNT,
          PP_SUB_PLAN_11_CNT,
          PP_SUB_PLAN_12_CNT,
          PP_SUB_PLAN_21_CNT,
          PP_SUB_PLAN_22_CNT,
          SP_SUB_PLAN_11_CNT,
          SP_SUB_PLAN_12_CNT,
          SP_SUB_PLAN_21_CNT,
          SP_SUB_PLAN_22_CNT,
          UNDUP_STDNT_CNT,                               -- Added Oct 2017    
          PRIM_PROG_MAJOR1_ORDER
     FROM UM_F_STDNT_ACAD_STRUCT
/
