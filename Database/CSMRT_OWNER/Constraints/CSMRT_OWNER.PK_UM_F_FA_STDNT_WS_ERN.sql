ALTER TABLE CSMRT_OWNER.UM_F_FA_STDNT_WS_ERN
  DROP CONSTRAINT PK_UM_F_FA_STDNT_WS_ERN
/

ALTER TABLE CSMRT_OWNER.UM_F_FA_STDNT_WS_ERN ADD (
  CONSTRAINT PK_UM_F_FA_STDNT_WS_ERN
  PRIMARY KEY
  (EMPLID, INSTITUTION, AID_YEAR, PAY_END_DT, EMPL_RCD, ACCT_CD, DEPTID, ITEM_TYPE, LAST_RUN_DT, ERN_BEGIN_DT, ERN_END_DT, SRC_SYS_ID)
  RELY
  ENABLE VALIDATE)
/
