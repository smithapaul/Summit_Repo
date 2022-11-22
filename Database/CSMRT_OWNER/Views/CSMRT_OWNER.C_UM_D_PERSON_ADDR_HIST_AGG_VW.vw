DROP VIEW CSMRT_OWNER.C_UM_D_PERSON_ADDR_HIST_AGG_VW
/

--
-- C_UM_D_PERSON_ADDR_HIST_AGG_VW  (View) 
--
CREATE OR REPLACE VIEW CSMRT_OWNER.C_UM_D_PERSON_ADDR_HIST_AGG_VW
BEQUEATH DEFINER
AS 
SELECT PERSON_SID,
           CAST (PERSON_ID AS VARCHAR2 (15))     EMPLID,
           ADDRESS_TYPE,
           EFFDT,
           SRC_SYS_ID,
           EFFDT_END                             EFF_END_DT, -- Is this the same as EFFDT_END?
           EFF_STATUS,
           ADDRESS_TYPE_SD,
           ADDRESS_TYPE_LD,
           ADDRESS1,
           ADDRESS2,
           ADDRESS3,
           ADDRESS4,
           CITY,
           COUNTY,
           STATE,
           STATE_LD,
           POSTAL,
           POSTAL3_USA_CD,
           POSTAL5_USA_CD,
           POSTAL_PLUS4_USA_CD,
           CAST ('-' AS VARCHAR2 (30))           GEO_CODE,
           UMLOW_GRAD_PROXIMITY,
           UMLOW_UGRD_PROXIMITY,
           CAST ('-' AS VARCHAR2 (1))            IN_CITY_LIMIT,
           COUNTRY,
           COUNTRY_SD,
           COUNTRY_LD,
           COUNTRY_2CHAR,
           EU_MEMBER_STATE,
           CAST ('-' AS VARCHAR2 (1))            CSW_ADDR_PREF_FLG,
           MLP_ADDR_ORDER,
           MPL_ADDR_ORDER,
           PML_ADDR_ORDER,
           DMP_ADDR_ORDER,
           DPM_ADDR_ORDER,
           PERM_ADDR_ORDER,
           MAIL_ADDR_ORDER,
           RESH_ADDR_ORDER,
           RESH_UMBOS_ORDER,
           RESH_UMDAR_ORDER,
           RESH_UMLOW_ORDER,
           ADDR_ORDER,
           DMLP_ADDR_ORDER,
           BMLP_ADDR_ORDER,
           LASTUPDDTTM,
           LASTUPDOPRID,
           CAST ('N' AS VARCHAR2 (1))            LOAD_ERROR,
           DATA_ORIGIN,
           CREATED_EW_DTTM,
           LASTUPD_EW_DTTM,
           CAST (1234 AS NUMBER (10))            BATCH_SID
      FROM CSMRT_OWNER.UM_D_PERSON_ADDR
     WHERE ADDR_ORDER = 1
	   AND DATA_ORIGIN <> 'D'
/
