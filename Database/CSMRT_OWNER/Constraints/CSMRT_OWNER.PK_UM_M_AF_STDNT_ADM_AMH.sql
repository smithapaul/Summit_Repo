ALTER TABLE CSMRT_OWNER.UM_M_AF_STDNT_ADM_AMH
  DROP CONSTRAINT PK_UM_M_AF_STDNT_ADM_AMH
/

ALTER TABLE CSMRT_OWNER.UM_M_AF_STDNT_ADM_AMH ADD (
  CONSTRAINT PK_UM_M_AF_STDNT_ADM_AMH
  PRIMARY KEY
  (INSTITUTION_CD, ACAD_CAR_CD, ACAD_PROG_CD, ACAD_PLAN_CD, ADMIT_TERM_CD, PERSON_ID, ADM_APPL_NBR, SLATE_ID, EXT_ADM_APPL_NBR, SRC_SYS_ID)
  RELY
  ENABLE VALIDATE)
/
