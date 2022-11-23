ALTER TABLE CSMRT_OWNER.UM_F_ADM_APPL_EXT_DEG
  DROP CONSTRAINT PK_UM_F_ADM_APPL_EXT_DEG
/

ALTER TABLE CSMRT_OWNER.UM_F_ADM_APPL_EXT_DEG ADD (
  CONSTRAINT PK_UM_F_ADM_APPL_EXT_DEG
  PRIMARY KEY
  (APPLCNT_SID, INSTITUTION_SID, EXT_ORG_SID, EXT_ACAD_CAR_SID, EXT_DATA_NBR, EXT_DEG_NBR, SRC_SYS_ID)
  RELY
  ENABLE VALIDATE)
/
