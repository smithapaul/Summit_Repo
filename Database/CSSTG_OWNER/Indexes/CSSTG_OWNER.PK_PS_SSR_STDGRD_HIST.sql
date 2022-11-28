DROP INDEX CSSTG_OWNER.PK_PS_SSR_STDGRD_HIST
/

--
-- PK_PS_SSR_STDGRD_HIST  (Index) 
--
CREATE UNIQUE INDEX CSSTG_OWNER.PK_PS_SSR_STDGRD_HIST ON CSSTG_OWNER.PS_SSR_STDGRD_HIST
(EMPLID, INSTITUTION, ACAD_CAREER, STDNT_CAR_NBR, ACAD_PROG, 
EXP_GRAD_TERM, DEGREE, SSR_GRAD_REV_DTTM, SRC_SYS_ID)
/
