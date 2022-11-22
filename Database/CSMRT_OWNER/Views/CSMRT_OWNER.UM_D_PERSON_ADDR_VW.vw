DROP VIEW CSMRT_OWNER.UM_D_PERSON_ADDR_VW
/

--
-- UM_D_PERSON_ADDR_VW  (View) 
--
CREATE OR REPLACE VIEW CSMRT_OWNER.UM_D_PERSON_ADDR_VW
BEQUEATH DEFINER
AS 
SELECT PERSON_ID,
           ADDRESS_TYPE,
           EFFDT,
           SRC_SYS_ID,
           EFFDT_START,
           EFFDT_END,
           EFFDT_ORDER,
           EFF_STATUS,
           EFFDT_END EFF_END_DT,    -- Keep until RPD has changed
           PERSON_ID EMPLID,        -- Keep until RPD has changed 
           PERSON_SID,
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
           UMLOW_GRAD_PROXIMITY,
           UMLOW_UGRD_PROXIMITY,
           COUNTRY,
           COUNTRY_SD,
           COUNTRY_LD,
           COUNTRY_2CHAR,
           EU_MEMBER_STATE,
           BMLP_ADDR_ORDER,
           MLP_ADDR_ORDER,
           MPL_ADDR_ORDER,
           PML_ADDR_ORDER,
           DMLP_ADDR_ORDER,
           DMP_ADDR_ORDER,
           DPM_ADDR_ORDER,
           PERM_ADDR_ORDER,
           MAIL_ADDR_ORDER,
           RESH_ADDR_ORDER,
           RESH_UMBOS_ORDER,
           RESH_UMBOS_FLG,
           RESH_UMDAR_ORDER,
           RESH_UMDAR_FLG,
           RESH_UMLOW_ORDER,
           RESH_UMLOW_FLG,
           PML_HIST_ORDER,
           ADDR_ORDER,
           LASTUPDDTTM,
           LASTUPDOPRID,
           DATA_ORIGIN,
           CREATED_EW_DTTM,
           LASTUPD_EW_DTTM
      FROM UM_D_PERSON_ADDR
/
