CREATE OR REPLACE VIEW C_PS_D_DEG_VW
BEQUEATH DEFINER
AS 
SELECT  
			CAST(DEG_SID AS NUMBER(10))       DEG_SID,           
			DEG_CD,            
			SRC_SYS_ID,        
			EFFDT,             
			EFF_STAT_CD,       
			DEG_SD,            
			DEG_LD,            
			DEG_FD,            
			EDU_LVL_CD,        
			EDU_LVL_SD,        
			EDU_LVL_LD,        
			INTERNAL_DEG_FLG,  
			YRS_OF_EDU_NUM,
			TO_DATE ('1/1/1753', 'MM/DD/YYYY')       EFF_START_DT,
			TO_DATE ('12/31/9999', 'MM/DD/YYYY')     EFF_END_DT,		
			CAST('Y' AS VARCHAR2(1))                 CURRENT_IND,
            CAST('N' AS VARCHAR2(1))         LOAD_ERROR,			
			DATA_ORIGIN,        
--			CAST('N' AS VARCHAR2(1))                 LOAD_ERROR,
			CAST(1234 AS NUMBER(10))                 BATCH_SID,
			CREATED_EW_DTTM,
			LASTUPD_EW_DTTM
	   FROM CSMRT_OWNER.PS_D_DEG
	  WHERE DATA_ORIGIN <> 'D';
