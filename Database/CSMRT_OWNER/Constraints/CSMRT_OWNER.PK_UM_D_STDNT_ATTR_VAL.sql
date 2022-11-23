ALTER TABLE CSMRT_OWNER.UM_D_STDNT_ATTR_VAL
  DROP CONSTRAINT PK_UM_D_STDNT_ATTR_VAL
/

ALTER TABLE CSMRT_OWNER.UM_D_STDNT_ATTR_VAL ADD (
  CONSTRAINT PK_UM_D_STDNT_ATTR_VAL
  PRIMARY KEY
  (PERSON_ID, ACAD_CAR_CD, STDNT_CAR_NUM, STDNT_ATTR, STDNT_ATTR_VALUE, SRC_SYS_ID)
  RELY
  DISABLE NOVALIDATE)
/
