ALTER TABLE CSMRT_OWNER.UM_R_FA_UMBOS_SF_TREE
  DROP CONSTRAINT PK_UM_R_FA_UMBOS_SF_TREE
/

ALTER TABLE CSMRT_OWNER.UM_R_FA_UMBOS_SF_TREE ADD (
  CONSTRAINT PK_UM_R_FA_UMBOS_SF_TREE
  PRIMARY KEY
  (INSTITUTION_CD, ACAD_CAR_CD, TERM_CD, PERSON_ID, SELECT_VALUE, ACCOUNT_TYPE_SF, WAIVER_CODE, ITEM_TYPE, TREE_NAME, TREE_NODE, TREE_NODE_NUM, SRC_SYS_ID)
  RELY
  ENABLE VALIDATE)
/
