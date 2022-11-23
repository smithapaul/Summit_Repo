DROP INDEX CSMRT_OWNER.PK_UM_F_STDNT_ENRL_REQ
/

--
-- PK_UM_F_STDNT_ENRL_REQ  (Index) 
--
CREATE UNIQUE INDEX CSMRT_OWNER.PK_UM_F_STDNT_ENRL_REQ ON CSMRT_OWNER.UM_F_STDNT_ENRL_REQ
(ENRL_REQUEST_ID, ENRL_REQ_DETL_SEQ, SRC_SYS_ID)
/
