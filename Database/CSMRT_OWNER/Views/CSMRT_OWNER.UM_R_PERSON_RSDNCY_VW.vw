DROP VIEW CSMRT_OWNER.UM_R_PERSON_RSDNCY_VW
/

--
-- UM_R_PERSON_RSDNCY_VW  (View) 
--
CREATE OR REPLACE VIEW CSMRT_OWNER.UM_R_PERSON_RSDNCY_VW
BEQUEATH DEFINER
AS 
SELECT /*+ OPT_ESTIMATE(TABLE UM_R_PERSON_RSDNCY MIN=10000000) */
           INSTITUTION_CD,
           ACAD_CAR_CD,
           EFF_TERM_CD,
           PERSON_ID,
           SRC_SYS_ID,
           INSTITUTION_SID,
           ACAD_CAR_SID,
           EFF_TERM_SID,
           PERSON_SID,
           RSDNCY_SID,
           RSDNCY_ID,
           RSDNCY_LD,
           ADM_RSDNCY_SID,
           ADM_RSDNCY_ID,
           ADM_RSDNCY_LD,
           FA_FED_RSDNCY_SID,
           FA_FED_RSDNCY_ID,
           FA_FED_RSDNCY_LD,
           FA_ST_RSDNCY_SID,
           FA_ST_RSDNCY_ID,
           FA_ST_RSDNCY_LD,
           TUITION_RSDNCY_SID,
           TUITION_RSDNCY_ID,
           TUITION_RSDNCY_LD,
           RSDNCY_TERM_SID,
           RSDNCY_TERM_CD,
           ADM_EXCPT_SID,
           ADM_RSDNCY_EXCPTN,
           ADM_RSDNCY_EXCPTN_LD,
           FA_FED_EXCPT_SID,
           FA_FED_RSDNCY_EXCPTN,
           FA_FED_RSDNCY_EXCPTN_LD,
           FA_ST_EXCPT_SID,
           FA_ST_RSDNCY_EXCPTN,
           FA_ST_RSDNCY_EXCPTN_LD,
           TUITION_EXCPT_SID,
           TUITION_RSDNCY_EXCPTN,
           TUITION_RSDNCY_EXCPTN_LD,
           RSDNCY_DT,
           APPEAL_EFFDT,
           APPEAL_STATUS,
           APPEAL_STATUS_SD,
           APPEAL_STATUS_LD,
           APPEAL_COMMENTS,
           DATA_ORIGIN,
           CREATED_EW_DTTM,
           LASTUPD_EW_DTTM
      FROM UM_R_PERSON_RSDNCY
     where ROWNUM < 100000000
/
