CREATE OR REPLACE VIEW UM_R_PERSON_RSDNCY_VW2
BEQUEATH DEFINER
AS 
SELECT /*+ OPT_ESTIMATE(TABLE UM_R_PERSON_RSDNCY MIN=100000) */
           EFF_TERM_SID,
           PERSON_SID,
           SRC_SYS_ID,
           INSTITUTION_CD,
           ACAD_CAR_CD,
           EFF_TERM_CD,
           PERSON_ID,
           INSTITUTION_SID,
           ACAD_CAR_SID,
           RSDNCY_SID,
           CAST ('-' AS VARCHAR (5))      RSDNCY_ID,           -- new 9/9/2019
           CAST ('-' AS VARCHAR (30))     RSDNCY_LD,           -- new 9/9/2019
           ADM_RSDNCY_SID,
           CAST ('-' AS VARCHAR (5))      ADM_RSDNCY_ID,       -- new 9/9/2019
           CAST ('-' AS VARCHAR (30))     ADM_RSDNCY_LD,       -- new 9/9/2019
           FA_FED_RSDNCY_SID,
           CAST ('-' AS VARCHAR (5))      FA_FED_RSDNCY_ID,    -- new 9/9/2019
           CAST ('-' AS VARCHAR (30))     FA_FED_RSDNCY_LD,    -- new 9/9/2019
           FA_ST_RSDNCY_SID,
           CAST ('-' AS VARCHAR (5))      FA_ST_RSDNCY_ID,     -- new 9/9/2019
           CAST ('-' AS VARCHAR (30))     FA_ST_RSDNCY_LD,     -- new 9/9/2019
           TUITION_RSDNCY_SID,
           CAST ('-' AS VARCHAR (5))      TUITION_RSDNCY_ID,   -- new 9/9/2019
           CAST ('-' AS VARCHAR (30))     TUITION_RSDNCY_LD,   -- new 9/9/2019
           RSDNCY_TERM_SID,
           CAST ('-' AS VARCHAR (4))      RSDNCY_TERM_CD,      -- new 9/9/2019
           ADM_EXCPT_SID,
           CAST ('-' AS VARCHAR (5))      ADM_RSDNCY_EXCPTN,   -- new 9/9/2019
           CAST ('-' AS VARCHAR (30))     ADM_RSDNCY_EXCPTN_LD, -- new 9/9/2019
           FA_FED_EXCPT_SID,
           CAST ('-' AS VARCHAR (5))      FA_FED_RSDNCY_EXCPTN, -- new 9/9/2019
           CAST ('-' AS VARCHAR (30))     FA_FED_RSDNCY_EXCPTN_LD, -- new 9/9/2019
           FA_ST_EXCPT_SID,
           CAST ('-' AS VARCHAR (5))      FA_ST_RSDNCY_EXCPTN, -- new 9/9/2019
           CAST ('-' AS VARCHAR (30))     FA_ST_RSDNCY_EXCPTN_LD, -- new 9/9/2019
           TUITION_EXCPT_SID,
           CAST ('-' AS VARCHAR (5))      TUITION_RSDNCY_EXCPTN, -- new 9/9/2019
           CAST ('-' AS VARCHAR (30))     TUITION_RSDNCY_EXCPTN_LD, -- new 9/9/2019
           RSDNCY_DT,
           APPEAL_EFFDT,
           APPEAL_STATUS,
           APPEAL_STATUS_SD,
           APPEAL_STATUS_LD,
           APPEAL_COMMENTS,
           LOAD_ERROR,
           DATA_ORIGIN,
           CREATED_EW_DTTM,
           LASTUPD_EW_DTTM,
           BATCH_SID
      FROM UM_R_PERSON_RSDNCY
     where ROWNUM < 100000000;
