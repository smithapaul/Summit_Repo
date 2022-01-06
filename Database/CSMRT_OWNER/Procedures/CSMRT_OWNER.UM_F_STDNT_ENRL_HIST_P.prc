CREATE OR REPLACE PROCEDURE             "UM_F_STDNT_ENRL_HIST_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--George Adams
--Loads table                -- UM_F_STDNT_ENRL_HIST
--V01 12/12/2018             -- srikanth ,pabbu converted to proc from sql scripts

------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_STDNT_ENRL_HIST';
        intProcessSid                   Integer;
        dtProcessStart                  Date            := SYSDATE;
        strMessage01                    Varchar2(4000);
        strMessage02                    Varchar2(512);
        strMessage03                    Varchar2(512)   :='';
        strNewLine                      Varchar2(2)     := chr(13) || chr(10);
        strSqlCommand                   Varchar2(32767) :='';
        strSqlDynamic                   Varchar2(32767) :='';
        strClientInfo                   Varchar2(100);
        intRowCount                     Integer;
        intTotalRowCount                Integer         := 0;
        numSqlCode                      Number;
        strSqlErrm                      Varchar2(4000);
        intTries                        Integer;

BEGIN

strSqlCommand := 'DBMS_APPLICATION_INFO.SET_CLIENT_INFO';
DBMS_APPLICATION_INFO.SET_CLIENT_INFO (strProcessName);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_INIT';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_INIT
        (
                i_MartId                => strMartId,
                i_ProcessName           => strProcessName,
                i_ProcessStartTime      => dtProcessStart,
                o_ProcessSid            => intProcessSid
        );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_STDNT_ENRL_HIST';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_STDNT_ENRL_HIST');

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_STDNT_ENRL_HIST disable constraint PK_UM_F_STDNT_ENRL_HIST';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_STDNT_ENRL_HIST';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_STDNT_ENRL_HIST';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_STDNT_ENRL_HIST';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_STDNT_ENRL_HIST';				
insert into UM_F_STDNT_ENRL_HIST
   WITH 
        R1
        as (       
            SELECT /*+ inline parallel(8) */
                   R.INSTITUTION_CD,
                   R.ACAD_CAREER,
                   R.TERM_CD,
                   R.PERSON_ID,
                   decode(R.ENRL_REQ_ACTION, 'S', R.CLASS_NBR_CHG_TO, R.CLASS_NBR) CLASS_NBR,
                   TRUNC(R.DTTM_STAMP_SEC) ENRL_DT,
                   R.SRC_SYS_ID,
                   'Y' REQ_FLG,
                   R.TERM_SID,
                   R.PERSON_SID,
                   R.ENRL_REQUEST_ID,       -- Oct 2019 
                   R.ENRL_REQ_DETL_SEQ,     -- Oct 2019 
                   trunc(R.DTTM_STAMP_SEC) DTTM_STAMP,  -- Sep 2017  
                   R.DTTM_STAMP_SEC DTTM_STAMP_SEC,     -- Sep 2017 
                   1 CLASS_ENRL_CNT,
                   R.UNT_TAKEN TAKEN_UNIT
              FROM UM_F_STDNT_ENRL_REQ R
             WHERE R.ENRL_REQ_ACTION IN ('E', 'S')
               AND R.ENRL_REQ_DETL_STAT IN ('S', 'M')
               and not exists (select 1
                                 from UM_D_CLASS_ATTR_VAL V
                                where R.CLASS_SID = V.CLASS_SID
                                  and V.CRSE_ATTR = 'LPLA' 
                                  and V.CRSE_ATTR_VALUE = 'LPLA')
               and not exists (select 1 
                                 from CSSTG_OWNER.PS_ENRL_MSG_LOG M
                                where R.ENRL_REQUEST_ID = M.ENRL_REQUEST_ID
                                  and R.ENRL_REQ_DETL_SEQ = M.ENRL_REQ_DETL_SEQ
                                  and R.SRC_SYS_ID = M.SRC_SYS_ID
                                  and M.MESSAGE_SET_NBR = 14641
                                  and M.MESSAGE_NBR in (68, 154, 215)   -- Sep 2017 
                                  and M.DATA_ORIGIN <> 'D')
             union all
            SELECT /*+ inline parallel(8) */
                   R.INSTITUTION_CD,
                   R.ACAD_CAREER,
                   R.TERM_CD,
                   R.PERSON_ID,
                   case when R.ENRL_REQ_ACTION = 'E' and R.DROP_CLASS_IF_ENRL > 0 then R.DROP_CLASS_IF_ENRL else R.CLASS_NBR end CLASS_NBR,
                   TRUNC(R.DTTM_STAMP_SEC) ENRL_DT,
                   R.SRC_SYS_ID,
                   'Y' REQ_FLG,
                   R.TERM_SID,
                   R.PERSON_SID,
                   R.ENRL_REQUEST_ID,       -- Oct 2019 
                   R.ENRL_REQ_DETL_SEQ,     -- Oct 2019 
                   trunc(R.DTTM_STAMP_SEC) DTTM_STAMP,  -- Sep 2017  
                   R.DTTM_STAMP_SEC DTTM_STAMP_SEC,     -- Sep 2017 
                   -1 CLASS_ENRL_CNT,
                   (-1 * UNT_TAKEN) TAKEN_UNIT
              FROM UM_F_STDNT_ENRL_REQ R
             WHERE ((R.ENRL_REQ_ACTION IN ('D', 'S')) or (R.ENRL_REQ_ACTION = 'E' and R.DROP_CLASS_IF_ENRL > 0))
               AND R.ENRL_REQ_DETL_STAT IN ('S', 'M')
               and not exists (select 1
                                 from UM_D_CLASS_ATTR_VAL V
                                where R.CLASS_SID = V.CLASS_SID
                                  and V.CRSE_ATTR = 'LPLA' 
                                  and V.CRSE_ATTR_VALUE = 'LPLA')
               and not exists (select 1 
                                 from CSSTG_OWNER.PS_ENRL_MSG_LOG M
                                where R.ENRL_REQUEST_ID = M.ENRL_REQUEST_ID
                                  and R.ENRL_REQ_DETL_SEQ = M.ENRL_REQ_DETL_SEQ
                                  and R.SRC_SYS_ID = M.SRC_SYS_ID
                                  and M.MESSAGE_SET_NBR = 14641
                                  and M.MESSAGE_NBR in (68, 154, 215)   -- Sep 2017 
                                  and M.DATA_ORIGIN <> 'D')
            ),
        R2
        as (       
            SELECT /*+ inline parallel(8) */
                   R1.INSTITUTION_CD,
                   R1.ACAD_CAREER,
                   R1.TERM_CD,
                   R1.PERSON_ID,
                   R1.CLASS_NBR,
                   R1.ENRL_DT,
                   R1.SRC_SYS_ID,
                   R1.REQ_FLG,
                   R1.TERM_SID,
                   R1.PERSON_SID,
                   R1.DTTM_STAMP,       -- Sep 2017  
                   R1.DTTM_STAMP_SEC,   -- Sep 2017 
                   R1.CLASS_ENRL_CNT,
                   R1.TAKEN_UNIT,
                   nvl(sum(R1.CLASS_ENRL_CNT) over (partition by R1.INSTITUTION_CD, R1.ACAD_CAREER, R1.TERM_CD, R1.PERSON_ID, R1.CLASS_NBR, R1.SRC_SYS_ID
--                                                        order by R1.DTTM_STAMP_SEC
                                                        order by R1.DTTM_STAMP_SEC, R1.ENRL_REQUEST_ID, R1.ENRL_REQ_DETL_SEQ        -- Oct 2019 
                                                    rows between unbounded preceding and 1 preceding),-1) CLASS_ENRL_CNT_PREV
              from R1),
        E1
        AS (SELECT /*+ inline parallel(8) */
                   F.INSTITUTION_CD,
                   F.ACAD_CAR_CD,
                   F.TERM_CD,
                   F.PERSON_ID,
                   F.CLASS_NUM,
--                   F.ENRL_ADD_DT ENRL_DT,
                   decode(R1.ENRLMT_REAS_ID,'EWAT',F.LAST_ENRL_DT_STMP,ENRL_ADD_DT) ENRL_DT,     -- Sep 2017 
                   F.SRC_SYS_ID,
                   'N' REQ_FLG,
                   F.TERM_SID,
                   F.PERSON_SID,
--                   decode(TAKEN_UNIT,0,0,1) CLASS_ENRL_CNT,
--                   F.TAKEN_UNIT
                   decode(decode(S1.ENRLMT_STAT_ID,'W',0,F.TAKEN_UNIT),0,0,1) CLASS_ENRL_CNT,    -- Aug 2016 
                   decode(S1.ENRLMT_STAT_ID,'W',0,F.TAKEN_UNIT) TAKEN_UNIT,                      -- Aug 2016 
                   max(decode(decode(S1.ENRLMT_STAT_ID,'E',F.TAKEN_UNIT,0),0,0,1)) 
                         over (partition by F.TERM_SID, F.PERSON_SID, F.SRC_SYS_ID) TERM_ENRL_CNT     -- Sep 2017 
              FROM UM_F_CLASS_ENRLMT F
              JOIN PS_D_ENRLMT_STAT S1                      -- Aug 2016  
                ON F.ENRLMT_STAT_SID = S1.ENRLMT_STAT_SID   -- Aug 2016 
              JOIN PS_D_ENRLMT_REAS R1                      -- Sep 2017  
                ON F.ENRLMT_REAS_SID = R1.ENRLMT_REAS_SID   -- Sep 2017 
             where not exists (select 1
                                 from UM_D_CLASS_ATTR_VAL V
                                where F.CLASS_SID = V.CLASS_SID
                                  and V.CRSE_ATTR = 'LPLA' 
                                  and V.CRSE_ATTR_VALUE = 'LPLA')
            UNION ALL
            SELECT F.INSTITUTION_CD,
                   F.ACAD_CAR_CD,
                   F.TERM_CD,
                   F.PERSON_ID,
                   F.CLASS_NUM,
                   F.ENRL_DROP_DT ENRL_DT,
                   F.SRC_SYS_ID,
                   'N' REQ_FLG,
                   F.TERM_SID,
                   F.PERSON_SID,
                   decode(TAKEN_UNIT,0,0,-1) CLASS_ENRL_CNT, 
                   (-1 * TAKEN_UNIT) TAKEN_UNIT,
                   0 TERM_ENRL_CNT                  -- Sep 2017 
              FROM UM_F_CLASS_ENRLMT F
              JOIN PS_D_ENRLMT_STAT S1
                ON F.ENRLMT_STAT_SID = S1.ENRLMT_STAT_SID
               AND S1.ENRLMT_STAT_ID = 'D'
             where not exists (select 1
                                 from UM_D_CLASS_ATTR_VAL V
                                where F.CLASS_SID = V.CLASS_SID
                                  and V.CRSE_ATTR = 'LPLA' 
                                  and V.CRSE_ATTR_VALUE = 'LPLA')
            UNION ALL
            SELECT R2.INSTITUTION_CD,
                   R2.ACAD_CAREER,
                   R2.TERM_CD,
                   R2.PERSON_ID,
                   R2.CLASS_NBR,
                   R2.ENRL_DT,
                   R2.SRC_SYS_ID,
                   R2.REQ_FLG,
                   R2.TERM_SID,
                   R2.PERSON_SID,
--                   decode(R2.TAKEN_UNIT,0,0,R2.CLASS_ENRL_CNT) CLASS_ENRL_CNT,
                   (case when R2.CLASS_ENRL_CNT <= 0 then R2.CLASS_ENRL_CNT     -- Some drops have zero credits 
                         when R2.TAKEN_UNIT = 0 then 0
                         else R2.CLASS_ENRL_CNT end) CLASS_ENRL_CNT,
                   R2.TAKEN_UNIT,
                   0 TERM_ENRL_CNT          -- Sep 2017 
              FROM R2
             where not (R2.CLASS_ENRL_CNT = -1 and R2.CLASS_ENRL_CNT_PREV <= -1) 
               and not exists (SELECT 1
--                                 from PS_F_CLASS_ENRLMT E
                                 from UM_F_CLASS_ENRLMT E
                                where E.TERM_SID = R2.TERM_SID  
                                  and E.PERSON_SID = R2.PERSON_SID
                                  and E.CLASS_NUM = R2.CLASS_NBR
                                  and E.SRC_SYS_ID = R2.SRC_SYS_ID
--                                  and E.ENRL_ADD_DT <= R2.DTTM_STAMP_SEC)       -- Sep 2017 
                                  and E.ENRL_ADD_DT <= R2.DTTM_STAMP)       -- Sep 2017 
                                  ),
        E2
        AS (SELECT /*+ inline parallel(8) */
                   INSTITUTION_CD,
                   ACAD_CAR_CD,
                   TERM_CD,
                   PERSON_ID,
                   CLASS_NUM,
                   ENRL_DT,
                   SRC_SYS_ID,
                   REQ_FLG,
                   MIN (CLASS_ENRL_CNT)
                      OVER (PARTITION BY INSTITUTION_CD,
                                         ACAD_CAR_CD,
                                         TERM_CD,
                                         PERSON_ID,
                                         CLASS_NUM,
                                         SRC_SYS_ID
--                            ORDER BY ENRL_DT, CLASS_ENRL_CNT desc 
                            ORDER BY ENRL_DT, CLASS_ENRL_CNT desc, REQ_FLG, TAKEN_UNIT desc     -- May 2017 
                            ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING)
                      PREV_ENRL_CNT,
                   MIN (REQ_FLG)
                      OVER (PARTITION BY INSTITUTION_CD,
                                         ACAD_CAR_CD,
                                         TERM_CD,
                                         PERSON_ID,
                                         SRC_SYS_ID)
                      REQ_ONLY_FLG,
                   SUM (decode(REQ_FLG,'N',CLASS_ENRL_CNT,0))
                      OVER (PARTITION BY INSTITUTION_CD,
                                         ACAD_CAR_CD,
                                         TERM_CD,
                                         PERSON_ID,
                                         SRC_SYS_ID)
                      CLASS_ENRL_ONLY_SUM,
                   TERM_SID,
                   PERSON_SID,
                   CLASS_ENRL_CNT,
                   TAKEN_UNIT,
                   MAX (TERM_ENRL_CNT)
                      OVER (PARTITION BY INSTITUTION_CD,
                                         ACAD_CAR_CD,
                                         TERM_CD,
                                         PERSON_ID,
                                         SRC_SYS_ID)
                   TERM_ENRL_CNT      -- Sep 2017 
              FROM E1),
        E3
        AS (  SELECT /*+ inline parallel(8) */
                     INSTITUTION_CD,
                     ACAD_CAR_CD,
                     TERM_CD,
                     PERSON_ID,
                     ENRL_DT,
                     SRC_SYS_ID,
                     REQ_ONLY_FLG,
                     CLASS_ENRL_ONLY_SUM,
                     TERM_SID,
                     PERSON_SID,
--                     SUM (CLASS_ENRL_CNT) CLASS_ENRL_CNT,
                     SUM (case when REQ_FLG = 'Y' and nvl(PREV_ENRL_CNT,9) = 0 then 0 else CLASS_ENRL_CNT end) CLASS_ENRL_CNT,
                     SUM (TAKEN_UNIT) TAKEN_UNIT,
                     max(TERM_ENRL_CNT) TERM_ENRL_CNT      -- Sep 2017 
                FROM E2
               where (CLASS_ENRL_CNT <> PREV_ENRL_CNT or PREV_ENRL_CNT is NULL) 
                 and not(PREV_ENRL_CNT is NULL and CLASS_ENRL_CNT < 0)
            GROUP BY INSTITUTION_CD,
                     ACAD_CAR_CD,
                     TERM_CD,
                     PERSON_ID,
                     ENRL_DT,
                     SRC_SYS_ID,
                     REQ_ONLY_FLG,
                     CLASS_ENRL_ONLY_SUM,
                     TERM_SID,
                     PERSON_SID),
        E4
        AS (SELECT /*+ inline parallel(8) */
                   INSTITUTION_CD,
                   ACAD_CAR_CD,
                   TERM_CD,
                   PERSON_ID,
                   ENRL_DT,
                   SRC_SYS_ID,
                   REQ_ONLY_FLG,
                   CLASS_ENRL_ONLY_SUM,
                   TERM_SID,
                   PERSON_SID,
                   NVL (
                      SUM (CLASS_ENRL_CNT)
                      OVER (PARTITION BY INSTITUTION_CD,
                                         ACAD_CAR_CD,
                                         TERM_CD,
                                         PERSON_ID,
                                         SRC_SYS_ID
--                            ORDER BY ENRL_DT
                            ORDER BY ENRL_DT, CLASS_ENRL_CNT desc, TAKEN_UNIT desc, CLASS_ENRL_ONLY_SUM     -- May 2017 
                            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
                      0)
                      CLASS_ENRL_CNT_PREV_SUM,
                   CLASS_ENRL_CNT,
                   NVL (
                      SUM (CLASS_ENRL_CNT)
                      OVER (PARTITION BY INSTITUTION_CD,
                                         ACAD_CAR_CD,
                                         TERM_CD,
                                         PERSON_ID,
                                         SRC_SYS_ID
--                            ORDER BY ENRL_DT
                            ORDER BY ENRL_DT, CLASS_ENRL_CNT desc, TAKEN_UNIT desc, CLASS_ENRL_ONLY_SUM     -- May 2017 
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
                      0)
                      CLASS_ENRL_CNT_SUM,
                   NVL (
                      SUM (TAKEN_UNIT)
                      OVER (PARTITION BY INSTITUTION_CD,
                                         ACAD_CAR_CD,
                                         TERM_CD,
                                         PERSON_ID,
                                         SRC_SYS_ID
--                            ORDER BY ENRL_DT
                            ORDER BY ENRL_DT, CLASS_ENRL_CNT desc, TAKEN_UNIT desc, CLASS_ENRL_ONLY_SUM     -- May 2017 
                            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
                      0)
                      TAKEN_UNIT_PREV_SUM,
                   TAKEN_UNIT,
                   NVL (
                      SUM (TAKEN_UNIT)
                      OVER (PARTITION BY INSTITUTION_CD,
                                         ACAD_CAR_CD,
                                         TERM_CD,
                                         PERSON_ID,
                                         SRC_SYS_ID
--                            ORDER BY ENRL_DT
                            ORDER BY ENRL_DT, CLASS_ENRL_CNT desc, TAKEN_UNIT desc, CLASS_ENRL_ONLY_SUM     -- May 2017 
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
                      0)
                      TAKEN_UNIT_SUM,
                   NVL (
                      MAX (TERM_ENRL_CNT)
                      OVER (PARTITION BY INSTITUTION_CD,
                                         ACAD_CAR_CD,
                                         TERM_CD,
                                         PERSON_ID,
                                         SRC_SYS_ID), 0) TERM_ENRL_CNT      -- Sep 2017 
              FROM E3
              ),
   E5 as (
   SELECT /*+ inline parallel(8) */
          INSTITUTION_CD,
          ACAD_CAR_CD,
          TERM_CD,
          PERSON_ID,
          ENRL_DT,
          SRC_SYS_ID,
          REQ_ONLY_FLG,
          CLASS_ENRL_ONLY_SUM,
          TERM_SID,
          PERSON_SID,
          CLASS_ENRL_CNT_PREV_SUM,
          CLASS_ENRL_CNT,
          CLASS_ENRL_CNT_SUM,
          TAKEN_UNIT_PREV_SUM,
          TAKEN_UNIT,
          TAKEN_UNIT_SUM,
          TERM_ENRL_CNT,      -- Sep 2017 
          ROW_NUMBER ()
             OVER (PARTITION BY INSTITUTION_CD,
                                ACAD_CAR_CD,
                                TERM_CD,
                                PERSON_ID,
                                SRC_SYS_ID
                   ORDER BY ENRL_DT)
             ENRL_FIRST_ORDER,
          ROW_NUMBER ()
             OVER (PARTITION BY INSTITUTION_CD,
                                ACAD_CAR_CD,
                                TERM_CD,
                                PERSON_ID,
                                SRC_SYS_ID
                   ORDER BY ENRL_DT DESC)
             ENRL_LAST_ORDER
     FROM E4
    WHERE (   (CLASS_ENRL_CNT_PREV_SUM <> CLASS_ENRL_CNT_SUM)
           OR (TAKEN_UNIT_PREV_SUM <> TAKEN_UNIT_SUM))
      and ENRL_DT is not NULL
           )
    select /*+ parallel(8) */
    INSTITUTION_CD, ACAD_CAR_CD, TERM_CD, PERSON_ID, ENRL_DT, SRC_SYS_ID, 
    TERM_SID, PERSON_SID,  
    CLASS_ENRL_CNT_PREV_SUM, 
    CLASS_ENRL_CNT, 
    CLASS_ENRL_CNT_SUM, 
    TAKEN_UNIT_PREV_SUM, 
    case when CLASS_ENRL_CNT_SUM = 0 then -1 * TAKEN_UNIT_PREV_SUM else TAKEN_UNIT end TAKEN_UNIT,      -- May 2017  
    case when CLASS_ENRL_CNT_SUM = 0 then 0 else TAKEN_UNIT_SUM end TAKEN_UNIT_SUM,                     -- May 2017 
    TERM_ENRL_CNT,      -- Sep 2017 
    ENRL_FIRST_ORDER, ENRL_LAST_ORDER
    from E5
    where PERSON_SID <> 2147483646          -- Oct 2020 
    union all
    select /*+ parallel(8) */
    E5.INSTITUTION_CD, E5.ACAD_CAR_CD, E5.TERM_CD, E5.PERSON_ID, 
--    to_date(T.WDN_DT_SID,'YYYYMMDD')+1 ENRL_DT, 
    E5.ENRL_DT +1 ENRL_DT, 
    E5.SRC_SYS_ID, 
    E5.TERM_SID, E5.PERSON_SID,  
    E5.CLASS_ENRL_CNT_SUM CLASS_ENRL_CNT_PREV_SUM, 
    -1*E5.CLASS_ENRL_CNT_SUM CLASS_ENRL_CNT, 
    0 CLASS_ENRL_CNT_SUM, 
    E5.TAKEN_UNIT_SUM TAKEN_UNIT_PREV_SUM, 
    -1*E5.TAKEN_UNIT_SUM TAKEN_UNIT, 
    0 TAKEN_UNIT_SUM, 
    TERM_ENRL_CNT,      -- Sep 2017 
    E5.ENRL_FIRST_ORDER+1, 
    E5.ENRL_LAST_ORDER-1
    from E5
    JOIN PS_F_TERM_ENRLMT T
      ON E5.TERM_SID = T.TERM_SID
     AND E5.PERSON_SID = T.PERSON_SID
     AND E5.SRC_SYS_ID = T.SRC_SYS_ID
     AND E5.ENRL_LAST_ORDER = 1
     AND (E5.REQ_ONLY_FLG = 'Y' or (E5.REQ_ONLY_FLG = 'N' and E5.CLASS_ENRL_ONLY_SUM = 0))
     AND (E5.CLASS_ENRL_CNT_SUM <> 0 or E5.TAKEN_UNIT_SUM <> 0)  
     AND T.WITHDRAW_CODE in ('CAN','WDR')
--     AND T.WDN_DT_SID > 0
     AND T.WDN_DT is not NULL
    where E5.PERSON_SID <> 2147483646       -- Oct 2020 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_STDNT_ENRL_HIST rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_STDNT_ENRL_HIST',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

--strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_STDNT_ENRL_HIST';
--COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_STDNT_ENRL_HIST enable constraint PK_UM_F_STDNT_ENRL_HIST';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );
				
--COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_STDNT_ENRL_HIST');

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := strProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

EXCEPTION
    WHEN OTHERS THEN
        COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_EXCEPTION
                (
                        i_SqlCommand   => strSqlCommand,
                        i_SqlCode      => SQLCODE,
                        i_SqlErrm      => SQLERRM
                );

END UM_F_STDNT_ENRL_HIST_P;
/
