DROP VIEW CSMRT_OWNER.C_UM_R_PERSON_RSDNCY_VW
/

--
-- C_UM_R_PERSON_RSDNCY_VW  (View) 
--
CREATE OR REPLACE VIEW CSMRT_OWNER.C_UM_R_PERSON_RSDNCY_VW
BEQUEATH DEFINER
AS 
SELECT EFF_TERM_SID, 
           PERSON_SID, 
           SRC_SYS_ID, 
           INSTITUTION_CD, 
           ACAD_CAR_CD, 
           EFF_TERM_CD, 
           PERSON_ID, 
           INSTITUTION_SID, 
           ACAD_CAR_SID, 
           RSDNCY_SID, 
           ADM_RSDNCY_SID, 
           FA_FED_RSDNCY_SID, 
           FA_ST_RSDNCY_SID, 
           TUITION_RSDNCY_SID, 
           RSDNCY_TERM_SID, 
           ADM_EXCPT_SID, 
           FA_FED_EXCPT_SID, 
           FA_ST_EXCPT_SID, 
           TUITION_EXCPT_SID, 
           RSDNCY_DT, 
           APPEAL_EFFDT, 
           APPEAL_STATUS, 
           APPEAL_STATUS_SD, 
           APPEAL_STATUS_LD, 
           APPEAL_COMMENTS, 
           CAST('N' AS VARCHAR2(1))             LOAD_ERROR, 
           DATA_ORIGIN, 
           CREATED_EW_DTTM, 
           LASTUPD_EW_DTTM, 
           CAST(1234 AS NUMBER(10))             BATCH_SID
	 FROM  CSMRT_OWNER.UM_R_PERSON_RSDNCY
/
