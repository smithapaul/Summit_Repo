ALTER TABLE CSSTG_OWNER.PS_T_ENRL_REQ_DETAIL MODIFY 
  ENRL_REQ_DETL_STAT NULL
/

ALTER TABLE CSSTG_OWNER.PS_T_ENRL_REQ_DETAIL MODIFY 
  ENRL_REQ_DETL_STAT NOT NULL
  ENABLE VALIDATE
/
