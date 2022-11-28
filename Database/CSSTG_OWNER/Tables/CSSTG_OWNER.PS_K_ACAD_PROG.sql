DROP TABLE CSSTG_OWNER.PS_K_ACAD_PROG CASCADE CONSTRAINTS
/

--
-- PS_K_ACAD_PROG  (Table) 
--
CREATE TABLE CSSTG_OWNER.PS_K_ACAD_PROG
(
  EMPLID         VARCHAR2(11 BYTE)              NOT NULL,
  ACAD_CAREER    VARCHAR2(4 BYTE)               NOT NULL,
  STDNT_CAR_NBR  INTEGER                        NOT NULL,
  EFFDT          DATE                           NOT NULL,
  EFFSEQ         INTEGER                        NOT NULL,
  SRC_SYS_ID     VARCHAR2(5 BYTE)               NOT NULL
)
NOCOMPRESS
/
