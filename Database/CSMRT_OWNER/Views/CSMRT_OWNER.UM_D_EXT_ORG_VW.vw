DROP VIEW CSMRT_OWNER.UM_D_EXT_ORG_VW
/

--
-- UM_D_EXT_ORG_VW  (View) 
--
CREATE OR REPLACE VIEW CSMRT_OWNER.UM_D_EXT_ORG_VW
BEQUEATH DEFINER
AS 
SELECT EXT_ORG_SID,
           EXT_ORG_ID,
           SRC_SYS_ID,
           EFFDT,
           EFF_STAT_CD,
           EXT_ORG_SD,
           EXT_ORG_LD,
           EXT_ORG_FD,
           EXT_ORG_TYPE_ID,
           EXT_ORG_TYPE_SD,
           EXT_ORG_TYPE_LD,
              EXT_ORG_LD
           || CASE
                  WHEN CITY_NM <> '-' AND STATE_ID <> '-'
                  THEN
                      ' (' || CITY_NM || ', ' || STATE_ID || ')'
                  ELSE
                      ''
              END    EXT_ORG_LD_LOC,
           SCHOOL_TYPE_ID,
           SCHOOL_TYPE_SD,
           SCHOOL_TYPE_LD,
           ADDR1_LD,
           ADDR2_LD,
           ADDR3_LD,
           ADDR4_LD,
           CITY_NM,
           CNTY_NM,
           STATE_ID,
           STATE_LD,
           POSTAL_CD,
           CNTRY_ID,
           CNTRY_SD,
           CNTRY_LD,
           ACCREDITED_FLG,
           ATP_CD,
           EXT_CAREER,
           EXT_CAREER_SD,
           EXT_CAREER_LD,
           EXT_TERM_TYPE,
           EXT_TERM_TYPE_SD,
           EXT_TERM_TYPE_LD,
           OFFERS_COURSES_FLG,
           ORG_LOCATION,
           PROPRIETORSHIP,
           PROPRIETORSHIP_SD,
           PROPRIETORSHIP_LD,
           SHARED_CATALOG_FLG,
           UNT_TYPE,
           UNT_TYPE_SD,
           UNT_TYPE_LD,
           DATA_ORIGIN,
           INACTIVE_FLAG,
           CREATED_EW_DTTM,
           LASTUPD_EW_DTTM
      FROM PS_D_EXT_ORG
/
