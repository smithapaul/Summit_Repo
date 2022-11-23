ALTER TABLE CSMRT_OWNER.PS_D_ENRL_HDR_STAT
  DROP CONSTRAINT PK_PS_D_ENRL_HDR_STAT
/

ALTER TABLE CSMRT_OWNER.PS_D_ENRL_HDR_STAT ADD (
  CONSTRAINT PK_PS_D_ENRL_HDR_STAT
  PRIMARY KEY
  (ENRL_HDR_STAT_SID)
  RELY
  DISABLE NOVALIDATE)
/
