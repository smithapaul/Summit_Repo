ALTER TABLE CSMRT_OWNER.UM_F_STDNT_TERM_ENRL_MV MODIFY 
  T_TERM_ACTV_MAX_TERM_SID NULL
/

ALTER TABLE CSMRT_OWNER.UM_F_STDNT_TERM_ENRL_MV MODIFY 
  T_TERM_ACTV_MAX_TERM_SID NOT NULL
  ENABLE VALIDATE
/
