DROP VIEW CSMRT_OWNER.C_PS_D_PROG_ACN_RSN_VW
/

--
-- C_PS_D_PROG_ACN_RSN_VW  (View) 
--
CREATE OR REPLACE VIEW CSMRT_OWNER.C_PS_D_PROG_ACN_RSN_VW
BEQUEATH DEFINER
AS 
WITH
        PACN
        AS
            (SELECT PROG_ACN_SID,
                    SETID,
                    PROG_ACN_CD,
                    PROG_ACN_SD,
                    PROG_ACN_LD
               FROM csmrt_owner.PS_D_PROG_ACN
			  WHERE DATA_ORIGIN <> 'D')
    SELECT D.PROG_ACN_RSN_SID,
           D.SETID,
           NVL(P.PROG_ACN_SID, 2147483646)          PROG_ACN_SID,
           D.PROG_ACN_CD,
           NVL(P.PROG_ACN_SD, '-')                  PROG_ACN_SD,
           NVL(P.PROG_ACN_LD, '-')                  PROG_ACN_LD,
           D.PROG_ACN_RSN_CD,
           D.EFFDT,
           D.EFF_STAT_CD,
           D.PROG_ACN_RSN_SD,
           D.PROG_ACN_RSN_LD,
           D.SETID                                  SRC_SETID,
           TO_DATE ('1/1/1753', 'MM/DD/YYYY')       EFF_START_DT,
           TO_DATE ('12/31/9999', 'MM/DD/YYYY')     EFF_END_DT,
           CAST ('Y' AS VARCHAR2 (1))               CURRENT_IND,
           D.SRC_SYS_ID,
           CAST ('N' AS VARCHAR2 (1))               LOAD_ERROR,
           D.DATA_ORIGIN,
           D.CREATED_EW_DTTM,
           D.LASTUPD_EW_DTTM,
           CAST (1234 AS NUMBER (10))               BATCH_SID
      FROM csmrt_owner.PS_D_PROG_ACN_RSN  D
      LEFT OUTER JOIN PACN P 
	    ON D.PROG_ACN_CD = P.PROG_ACN_CD
	   AND D.SETID = P.SETID
	 WHERE D.DATA_ORIGIN <> 'D'
/
