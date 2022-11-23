ALTER TABLE CSMRT_OWNER.UM_F_FA_STDNT_AWRD_ACTV
  DROP CONSTRAINT PK_UM_F_FA_STDNT_AWRD_ACTV
/

ALTER TABLE CSMRT_OWNER.UM_F_FA_STDNT_AWRD_ACTV ADD (
  CONSTRAINT PK_UM_F_FA_STDNT_AWRD_ACTV
  PRIMARY KEY
  (INSTITUTION_CD, AID_YEAR, PERSON_ID, ITEM_TYPE, ACAD_CAR_CD, ACTION_DTTM, SRC_SYS_ID)
  RELY
  ENABLE VALIDATE)
/
