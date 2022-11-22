DROP VIEW CSMRT_OWNER.UM_D_ACAD_PLAN_VW
/

--
-- UM_D_ACAD_PLAN_VW  (View) 
--
CREATE OR REPLACE VIEW CSMRT_OWNER.UM_D_ACAD_PLAN_VW
BEQUEATH DEFINER
AS 
SELECT 
           ACAD_PLAN_SID,
           EFFDT,
           INSTITUTION_CD,
           ACAD_PLAN_CD,
           SRC_SYS_ID,
           EFFDT_START,
           EFFDT_END,
           EFFDT_ORDER,
           EFF_STAT_CD,
           ACAD_PLAN_SD,
           ACAD_PLAN_LD,
           ACAD_PLAN_CD_DESC,
           ACAD_CAR_SID,
           ACAD_PROG_SID,
           DEG_SID,
           INSTITUTION_SID,
           ACAD_PLAN_TYPE_CD,
           ACAD_PLAN_TYPE_SD,
           ACAD_PLAN_TYPE_LD,
           ACAD_PLAN_TYPE_CD_DESC,
           CIP_CD,
           CIP_LD,
           DIPLOMA_DESCR,
           DIPLOMA_PRINT_FLG,
           EDU_LVL_CTGRY,
           EVALUATE_PLAN_FLG,
           PLAN_REQTRM_DFLT,
           PLAN_REQTRM_DFLT_SD,
           PLAN_REQTRM_DFLT_LD,
           SAA_WHIF_DISP_ADVR_FLG,
           SAA_WHIF_DISP_PREM_FLG,
           SAA_WHIF_DISP_STD_FLG,
           SSR_NSC_CRD_LVL,
           SSR_NSC_CRD_LVL_SD,
           SSR_NSC_CRD_LVL_LD,
           SSR_NSC_INCL_PLAN_FLG,
           SSR_PROG_LEN_TYPE,
           SSR_PROG_LEN_TYPE_SD,
           SSR_PROG_LEN_TYPE_LD,
           SSR_PROG_LENGTH,
           SEV_VALID_CIP_FLG,
           TRNSCR_DESCR,
           TRNSCR_PRINT_FLG,
           UM_STEM_FLG,
           DATA_ORIGIN,
           CREATED_EW_DTTM,
           LASTUPD_EW_DTTM
      FROM UM_D_ACAD_PLAN
/
