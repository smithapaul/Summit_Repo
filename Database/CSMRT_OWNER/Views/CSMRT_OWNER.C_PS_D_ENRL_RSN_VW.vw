CREATE OR REPLACE VIEW C_PS_D_ENRL_RSN_VW
BEQUEATH DEFINER
AS 
SELECT CAST(ENRL_ACT_RSN_SID AS NUMBER(10))  ENRL_ACT_RSN_SID,
           SETID, 
		   SETID        ENRL_SETID,
		   ACAD_CAR_CD  ACAD_CAREER, 
		   ENRL_ACTION, 
           ENRL_ACTION ENRL_REQ_ACTION,		   
		   ENRL_ACT_RSN,   
		   EFFDT, 
		   EFF_STAT_CD, 
		   ENRL_ACT_RSN_LD,
		   ENRL_ACT_RSN_SD,
		   CAST('-' AS VARCHAR2(3))		            TIME_PERIOD,
		   CAST('N' AS VARCHAR2(1))                 DEFAULT_DROP_RSN,            		   
		   SRC_SYS_ID,		   
		   TO_DATE ('1/1/1753', 'MM/DD/YYYY')       EFF_START_DT,
		   TO_DATE ('12/31/9999', 'MM/DD/YYYY')     EFF_END_DT,		   
		   CAST('Y' AS VARCHAR2(1))                 CURRENT_IND,		   
           CAST('N' AS VARCHAR2(1))                 LOAD_ERROR,
           DATA_ORIGIN,
           CREATED_EW_DTTM,
           LASTUPD_EW_DTTM,
           CAST(1234 AS NUMBER(10))                 BATCH_SID
      FROM csmrt_owner.PS_D_ENRL_RSN
	 WHERE DATA_ORIGIN <> 'D';
