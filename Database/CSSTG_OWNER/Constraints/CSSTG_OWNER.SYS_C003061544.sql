ALTER TABLE CSSTG_OWNER.PS_AGGR_LIMIT_TBL MODIFY 
  SFA_LIFETM_ELG_MAX NULL
/

ALTER TABLE CSSTG_OWNER.PS_AGGR_LIMIT_TBL MODIFY 
  SFA_LIFETM_ELG_MAX NOT NULL
  ENABLE VALIDATE
/
