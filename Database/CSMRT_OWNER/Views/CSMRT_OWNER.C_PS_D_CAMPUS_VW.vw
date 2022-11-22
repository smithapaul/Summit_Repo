DROP VIEW CSMRT_OWNER.C_PS_D_CAMPUS_VW
/

--
-- C_PS_D_CAMPUS_VW  (View) 
--
CREATE OR REPLACE VIEW CSMRT_OWNER.C_PS_D_CAMPUS_VW
BEQUEATH DEFINER
AS 
WITH STG AS (
               select INSTITUTION, CAMPUS, EFFDT, SRC_SYS_ID, LOCATION,
                      row_number() over (partition by INSTITUTION, CAMPUS, SRC_SYS_ID
                                         order by DATA_ORIGIN desc, 
										 (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1900') 
										 else EFFDT end) desc) Q_ORDER
                 from CSSTG_OWNER.PS_CAMPUS_TBL), 

 INST as (
	          SELECT INSTITUTION_SID, INSTITUTION_CD, INSTITUTION_SD, INSTITUTION_LD
	            FROM CSMRT_OWNER.PS_D_INSTITUTION
	),
 LOC as (
	          SELECT LOC_SID, SETID, LOC_ID, SRC_SYS_ID, LOC_SD, LOC_LD
	            FROM CSMRT_OWNER.PS_D_LOCATION
	)
SELECT  D.CAMPUS_SID, 
        I.INSTITUTION_SID, 
		D.INSTITUTION_CD, 
		I.INSTITUTION_SD, 
		I.INSTITUTION_LD, 
		D.CAMPUS_CD, 
		D.EFFDT, 
		D.EFF_STAT_CD, 
		NVL(D.CAMPUS_SD, '-')                     CAMPUS_SD,
		NVL(D.CAMPUS_LD, '-')                     CAMPUS_LD,
		CAST(NVL(TRIM(L.LOC_SID), 2147483646) AS INTEGER)   LOC_SID, 
		NVL(S.LOCATION, '-')                      LOC_ID, 
		NVL(L.LOC_SD, '-')                        LOC_SD, 
		NVL(L.LOC_LD, '-')                        LOC_LD,
        TO_DATE ('1/1/1753', 'MM/DD/YYYY')       EFF_START_DT,
        TO_DATE ('12/31/9999', 'MM/DD/YYYY')     EFF_END_DT,
        CAST('Y' AS VARCHAR2(1))                 CURRENT_IND,
        D.SRC_SYS_ID,
        CAST('N' AS VARCHAR2(1))                 LOAD_ERROR,
        D.DATA_ORIGIN,
        D.CREATED_EW_DTTM,
        D.LASTUPD_EW_DTTM,
        CAST(1234 AS NUMBER(10))                 BATCH_SID
      FROM csmrt_owner.PS_D_CAMPUS D
LEFT OUTER JOIN INST I 
	ON D.INSTITUTION_CD = I.INSTITUTION_CD	
LEFT OUTER JOIN STG S
    ON D.CAMPUS_CD = S.CAMPUS
   AND D.INSTITUTION_CD = S.INSTITUTION
   AND S.Q_ORDER = 1
LEFT OUTER JOIN LOC L 
	ON S.LOCATION = L.LOC_ID
   AND D.INSTITUTION_CD = S.INSTITUTION
 WHERE D.DATA_ORIGIN <> 'D'
/
