ALTER TABLE CSMRT_OWNER.UM_S_SF_ITEM
  DROP CONSTRAINT PK_UM_S_SF_ITEM
/

ALTER TABLE CSMRT_OWNER.UM_S_SF_ITEM ADD (
  CONSTRAINT PK_UM_S_SF_ITEM
  PRIMARY KEY
  (INSTITUTION_CD, PERSON_ID, SA_ID_TYPE, ITEM_NBR, SRC_SYS_ID, UM_FISCAL_YEAR)
  RELY
  ENABLE VALIDATE)
/
