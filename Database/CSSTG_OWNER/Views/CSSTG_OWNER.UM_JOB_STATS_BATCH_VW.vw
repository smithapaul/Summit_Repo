DROP VIEW CSSTG_OWNER.UM_JOB_STATS_BATCH_VW
/

--
-- UM_JOB_STATS_BATCH_VW  (View) 
--
CREATE OR REPLACE VIEW CSSTG_OWNER.UM_JOB_STATS_BATCH_VW
BEQUEATH DEFINER
AS 
with S1 as (
   SELECT   PROJECT,
            JOB_NBR,
            JOB_NAME,
            JOB_STATUS,
            trunc(TO_DATE (START_TIME, 'YYYY-MM-DD HH24:MI:SS') + (case when substr(START_TIME,12,2) >= '17' then .5 else 0 end)) BATCH_DT, 
            START_TIME,
            TO_DATE (SUBSTR (START_TIME, 1, 10), 'YYYY-MM-DD') START_DT,
            TO_DATE (START_TIME, 'YYYY-MM-DD HH24:MI:SS') START_DT_TIME,
            END_TIME,
            TO_DATE (SUBSTR (END_TIME, 1, 10), 'YYYY-MM-DD') END_DT,
            TO_DATE (END_TIME, 'YYYY-MM-DD HH24:MI:SS') END_DT_TIME,
            ELAPSED_TIME,
            CATEGORY,
            JOB_LOG
     FROM   UM_JOB_STATS
    WHERE   substr(START_TIME,1,10) >= '2010-11-05'
      AND   substr(JOB_NAME,1,7) = 'UM_MSEQ' 
      AND   JOB_NAME not like '%RDS%'
      AND   JOB_NAME not like '%UAT%' )
   SELECT   PROJECT,
            JOB_NBR,
            JOB_NAME,
            JOB_STATUS,
            BATCH_DT,
            ROW_NUMBER () OVER (PARTITION BY BATCH_DT
                                    ORDER BY BATCH_DT, START_DT_TIME) BATCH_JOB_ORDER,
            ROW_NUMBER () OVER (PARTITION BY BATCH_DT
                                    ORDER BY BATCH_DT, START_DT_TIME desc) BATCH_JOB_REV_ORDER,
            START_TIME,
            START_DT,
            START_DT_TIME,
            END_TIME,
            END_DT,
            END_DT_TIME,
            ELAPSED_TIME,
            CATEGORY,
            JOB_LOG
from S1
/
