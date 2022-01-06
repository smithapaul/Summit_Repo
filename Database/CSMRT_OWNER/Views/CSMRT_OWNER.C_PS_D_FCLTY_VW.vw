CREATE OR REPLACE VIEW C_PS_D_FCLTY_VW
BEQUEATH DEFINER
AS 
WITH FACTBL AS (
               select SETID, FACILITY_ID, EFFDT, SRC_SYS_ID, ACAD_ORG,
                      row_number() over (partition by SETID, FACILITY_ID, SRC_SYS_ID
                                         order by DATA_ORIGIN desc, 
										 (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1900') 
										 else EFFDT end) desc) Q_ORDER
                 from CSSTG_OWNER.PS_FACILITY_TBL),
       AORG AS (
                   SELECT ACAD_ORG_SID, ACAD_ORG_CD, INSTITUTION_CD, SRC_SYS_ID, 
                          ACAD_ORG_SD, ACAD_ORG_LD
                     FROM CSMRT_OWNER.PS_D_ACAD_ORG
                    WHERE EFFDT_ORDER = 1
               )
SELECT D.FCLTY_SID, 
	   D.SETID, 
	   D.FCLTY_ID, 
	   D.EFFDT, 
	   D.EFF_STAT_CD, 
	   D.FCLTY_SD, 
	   D.FCLTY_LD, 
	   D.FCLTY_TYPE_CD, 
	   D.FCLTY_TYPE_SD, 
	   D.FCLTY_TYPE_LD, 
	   CAST('N' AS VARCHAR2(1))             FCLTY_GRP_FLG, 
	   D.BLDG_CD, 
	   D.BLDG_SD, 
	   D.BLDG_LD, 
	   D.LOC_SID, 
	   L.LOC_ID, 
	   L.LOC_SD, 
	   L.LOC_LD, 
	   D.ROOM_NM, 
	   D.ROOM_CAPACITY_NUM, 
	   nvl(A.ACAD_ORG_SID,2147483646)          ACAD_ORG_SID,
	   nvl(F.ACAD_ORG ,'-')                    ACAD_ORG, 
	   nvl(A.ACAD_ORG_SD ,'-')                 ACAD_ORG_SD, 
	   nvl(A.ACAD_ORG_LD,'-')                  ACAD_ORG_LD, 
	   D.SETID                                  SRC_SETID,	   
	   TO_DATE ('1/1/1753', 'MM/DD/YYYY')       EFF_START_DT,
       TO_DATE ('12/31/9999', 'MM/DD/YYYY')     EFF_END_DT,	
	   CAST('Y' AS VARCHAR2(1))                 CURRENT_IND,
	   D.SRC_SYS_ID,
       CAST('N' AS VARCHAR2(1))                 LOAD_ERROR,			
	   D.DATA_ORIGIN,
       CAST(1234 AS NUMBER(10))                 BATCH_SID,	   
	   D.CREATED_EW_DTTM,
	   D.LASTUPD_EW_DTTM
 FROM  CSMRT_OWNER.PS_D_FCLTY D
  left outer join PS_D_LOCATION L  
    on D.LOC_SID = L.LOC_SID 
  left outer join FACTBL F
    on D.SETID = F.SETID
   and D.FCLTY_ID = F.FACILITY_ID
   and D.SRC_SYS_ID = F.SRC_SYS_ID
   and F.Q_ORDER = 1
  left outer join AORG A
    on F.ACAD_ORG = A.ACAD_ORG_CD
   and D.SETID = A.INSTITUTION_CD
   and D.SRC_SYS_ID = A.SRC_SYS_ID
 WHERE D.DATA_ORIGIN <> 'D';
