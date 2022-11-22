DROP VIEW CSMRT_OWNER.UM_D_PERSON_ADDR_HIST_VW
/

--
-- UM_D_PERSON_ADDR_HIST_VW  (View) 
--
CREATE OR REPLACE VIEW CSMRT_OWNER.UM_D_PERSON_ADDR_HIST_VW
BEQUEATH DEFINER
AS 
SELECT PERSON_SID,
           PERSON_ID     EMPLID,
           PERSON_ID,
           ADDRESS_TYPE,
           EFFDT,
           SRC_SYS_ID,
           EFFDT_END     EFF_END_DT,
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
           --           '-' GEO_CODE,            -- Still need???
           UMLOW_GRAD_PROXIMITY,
           UMLOW_UGRD_PROXIMITY,
           --           '-' IN_CITY_LIMIT,       -- Still need????
           COUNTRY,
           COUNTRY_SD,
           COUNTRY_LD,
           COUNTRY_2CHAR,
           EU_MEMBER_STATE,
           --           '-' CSW_ADDR_PREF_FLG,   -- Still need???
           BMLP_ADDR_ORDER,                                        -- Dec 2016
           MLP_ADDR_ORDER,
           MPL_ADDR_ORDER,
           PML_ADDR_ORDER,
           DMLP_ADDR_ORDER,                                       -- July 2016
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
           ADDR_ORDER,
           LASTUPDDTTM,
           LASTUPDOPRID,
           DATA_ORIGIN,
           CREATED_EW_DTTM,
           LASTUPD_EW_DTTM
      FROM UM_D_PERSON_ADDR
/
