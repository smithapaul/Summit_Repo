DROP INDEX CSMRT_OWNER.PK_PS_F_TERM_ENRLMT
/

--
-- PK_PS_F_TERM_ENRLMT  (Index) 
--
CREATE UNIQUE INDEX CSMRT_OWNER.PK_PS_F_TERM_ENRLMT ON CSMRT_OWNER.PS_F_TERM_ENRLMT
(INSTITUTION_CD, ACAD_CAR_CD, TERM_CD, PERSON_ID, SRC_SYS_ID)
/
