DROP INDEX CSSTG_OWNER.PK_PS_ACAD_STACTN_TBL
/

--
-- PK_PS_ACAD_STACTN_TBL  (Index) 
--
CREATE UNIQUE INDEX CSSTG_OWNER.PK_PS_ACAD_STACTN_TBL ON CSSTG_OWNER.PS_ACAD_STACTN_TBL
(INSTITUTION, ACAD_CAREER, EFFDT, ACAD_STNDNG_ACTN, SRC_SYS_ID)
/
