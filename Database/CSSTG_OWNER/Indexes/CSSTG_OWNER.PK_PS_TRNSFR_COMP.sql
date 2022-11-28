DROP INDEX CSSTG_OWNER.PK_PS_TRNSFR_COMP
/

--
-- PK_PS_TRNSFR_COMP  (Index) 
--
CREATE UNIQUE INDEX CSSTG_OWNER.PK_PS_TRNSFR_COMP ON CSSTG_OWNER.PS_TRNSFR_COMP
(INSTITUTION, TRNSFR_SRC_ID, COMP_SUBJECT_AREA, EFFDT, TRNSFR_EQVLNCY_CMP, 
SRC_SYS_ID)
/
