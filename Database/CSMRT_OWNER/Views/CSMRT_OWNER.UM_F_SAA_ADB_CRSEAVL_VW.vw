CREATE OR REPLACE VIEW UM_F_SAA_ADB_CRSEAVL_VW
BEQUEATH DEFINER
AS 
SELECT /*+ OPT_ESTIMATE(TABLE UM_F_SAA_ADB_CRSEAVL MIN=100000) */
         EMPLID,
          ANALYSIS_DB_SEQ,
          SAA_CAREER_RPT,
          SAA_ENTRY_SEQ,
          SAA_COURSE_SEQ,
          SRC_SYS_ID,
          INSTITUTION_CD,
          CRSE_SID,                                         -- Added Feb 2017
          PLAN_TERM_SID,                                    -- Added Feb 2017
          COURSE_LIST,
          CRS_TOPIC_ID,
          EARN_CREDIT,
          PRE_REQ_MET_FLG,
          ENROLL_FLG,                                       -- Added Mar 2017 
          DEMAND_COUNT,                                     -- Added Feb 2017
          PLAN_COUNT,                                       -- Added Feb 2017
          CREATED_EW_DTTM
     FROM UM_F_SAA_ADB_CRSEAVL
--    WHERE ROWNUM < 100000000
;
