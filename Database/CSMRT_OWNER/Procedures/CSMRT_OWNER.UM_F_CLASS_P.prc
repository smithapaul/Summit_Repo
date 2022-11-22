DROP PROCEDURE CSMRT_OWNER.UM_F_CLASS_P
/

--
-- UM_F_CLASS_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_F_CLASS_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--George Adams
--Loads table                -- UM_F_CLASS
--V01 12/11/2018             -- srikanth ,pabbu converted to proc from sql scripts

------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_CLASS';
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

strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_CLASS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_CLASS';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_CLASS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_CLASS');

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_CLASS disable constraint PK_UM_F_CLASS';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_CLASS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_CLASS';
insert /*+ append enable_parallel_dml parallel(8) */ into UM_F_CLASS
with PAT as (
select /*+ INLINE PARALLEL(8) */
        CLASS_MTG_PAT_ORDER,
        CLASS_MTG_PAT_SID,
        CLASS_MTG_NUM,
        CLASS_SID,
        FCLTY_SID,
        MEETING_TIME_START,
        MEETING_TIME_END,
        MON,
        TUES,
        WED,
        THURS,
        FRI,
        SAT,
        SUN,
        MTG_PAT_CD,
        START_DT,
        START_TIME,
        END_DT,
        END_TIME,
        MEETING_TIME,
        MTG_PAT_CRSE_TOPIC_ID,
        DESCR,
        STND_MTG_PAT,
        PRINT_TOPIC_ON_XCR,
        SRC_SYS_ID
  from UM_D_CLASS_MTG_PAT
 where CLASS_SID <> 2147483646),
INS as (
select /*+ INLINE PARALLEL(8) */
       I.CLASS_INSTRCTR_ORDER,
       I.CLASS_INSTRCTR_SID,
       I.INSTRCTR_ASGN_NUM,
       I.CLASS_MTG_PAT_SID,
       I.PERSON_SID INSTRCTR_SID,
       I.INSTRCTR_ROLE_SID,
       I.AUTOCALC_WRKLD_FLG,
--       I.ASSIGN_TYPE,
--       I.WEEK_WRKLD_HR_CNT,
--       I.ASGN_PCT,
       I.GRADE_RSTR_ACCESS,
       I.GRADE_RSTR_ACCESS_SD,
       I.GRADE_RSTR_ACCESS_LD,
       I.CONTACT_MINUTES,
       I.SCHED_PRINT_INSTR,
       I.INSTR_LOAD_FACTOR INSTRCTR_LOAD_PCT,
       I.SRC_SYS_ID
  from UM_D_CLASS_INSTRCTR I
 where I.CLASS_SID <> 2147483646
   and I.CLASS_MTG_PAT_SID <> 2147483646),
E1 as (
select /*+ parallel(8) inline */
       C.TERM_SID, C.CRSE_SID, R.PERSON_SID,
       max(R.WAIT_CNT) WAIT_PERS_CNT,
       max(R.ENROLL_CNT) ENROLL_PERS_CNT
  from UM_F_CLASS_ENRLMT R, UM_D_CLASS C
 where R.CLASS_SID = C.CLASS_SID
   and C.DATA_ORIGIN <> 'D'
 group by C.TERM_SID, C.CRSE_SID, R.PERSON_SID),
E2 as (
select /*+ parallel(8) inline */
       TERM_SID, CRSE_SID,
       sum(WAIT_PERS_CNT) WAIT_CRSE_CNT,
       sum(ENROLL_PERS_CNT) ENROLL_CRSE_CNT
  from E1
 group by TERM_SID, CRSE_SID),
E3 as (
select /*+ parallel(8) inline */
       TERM_SID, CRSE_SID,
       sum(WAIT_PERS_CNT) WAIT_ENRL_DIST_CRSE_CNT
  from E1
 where WAIT_PERS_CNT > 0
   and ENROLL_PERS_CNT > 0
 group by TERM_SID, CRSE_SID),
ENR as (
select /*+ INLINE PARALLEL(8) */
       R.CLASS_SID, R.SRC_SYS_ID,
       SUM(R.CE_CREDITS) CE_CREDITS,
       SUM(R.CE_FTE) CE_FTE,
       SUM(R.DAY_CREDITS) DAY_CREDITS,
       SUM(R.DAY_FTE) DAY_FTE,
       SUM(R.IFTE_CNT) IFTE_CNT,
--       SUM(R.ENROLL_CNT) ENROLL_CNT,
       SUM(R.HEAD_CNT) ENROLL_CNT,      -- May 2017
--       SUM(R.ENROLL_CNT) OVER (PARTITION BY R.TERM_SID, D_CLASS.CRSE_SID, R.PERSON_SID) ENROLL_PERS_CNT,    -- Dec 2016
       0 ENROLL_CRSE_CNT,                 -- Currently used?
       SUM(R.DROP_CNT) DROP_CNT,
       0 DROP_CRSE_CNT,                   -- Currently used?
       0 TAKEN_UNIT_SUM,                    -- Currently used?
       0 TAKEN_UNIT_OL_SUM,                 -- Currently used?
       SUM(R.WAIT_CNT) WAIT_CNT,
--       SUM(R.WAIT_CNT) OVER (PARTITION BY R.TERM_SID, D_CLASS.CRSE_SID, R.PERSON_SID) WAIT_PERS_CNT,        -- Dec 2016
       0 WAIT_CRSE_CNT,                   -- Currently used?
       sum(case when R.WAIT_CNT = 1 and E1.WAIT_PERS_CNT is not NULL then R.WAIT_CNT else 0 end) WAIT_ENRL_CNT,                        -- Jan 2017
       0 AS "WAIT_WAIT_CNT",                -- Currently used?
       min(case when E3.WAIT_ENRL_DIST_CRSE_CNT is not NULL then E3.WAIT_ENRL_DIST_CRSE_CNT else 0 end) WAIT_ENRL_DIST_CRSE_CNT,       -- Jan 2017
--       COUNT(DISTINCT CASE WHEN R.WAIT_CNT = 1
--                           THEN R.PERSON_SID
--                           ELSE NULL
--                       END) OVER (PARTITION BY C.CLASS_SID) WAIT_DIST_CLASS_CNT,     -- Add back at CLASS_SID grain!!!!!!!!!!!!!!!!!!!!!!!!!
--       0 WAIT_DIST_CLASS_CNT,                     -- OLD!!!
       min(case when E2.WAIT_CRSE_CNT is not NULL then E2.WAIT_CRSE_CNT else 0 end) WAIT_DIST_CRSE_CNT        -- Jan 2017
  from UM_F_CLASS_ENRLMT R
  join UM_D_CLASS C
    on R.CLASS_SID = C.CLASS_SID
   and C.DATA_ORIGIN <> 'D'
  left outer join E1
    on R.TERM_SID = E1.TERM_SID
   and C.CRSE_SID = E1.CRSE_SID
   and R.PERSON_SID = E1.PERSON_SID
   and E1.WAIT_PERS_CNT > 0
   and E1.ENROLL_PERS_CNT > 0
  left outer join E2
    on R.TERM_SID = E2.TERM_SID
   and C.CRSE_SID = E2.CRSE_SID
  left outer join E3
    on R.TERM_SID = E3.TERM_SID
   and C.CRSE_SID = E3.CRSE_SID
 group by R.CLASS_SID, R.SRC_SYS_ID),
CLS1 as (
select /*+ INLINE PARALLEL(8) */
            C.CLASS_SID,
            C.CRSE_CD,
            C.CRSE_OFFER_NUM,
            C.TERM_CD,
            C.SESSION_CD,
            C.CLASS_SECTION_CD,
            C.SRC_SYS_ID,
            C.INSTITUTION_SID,
            C.INSTITUTION_CD,
            NVL (P.CLASS_MTG_PAT_ORDER, 1) CLASS_MTG_PAT_ORDER,
            DECODE (NVL (P.CLASS_MTG_PAT_ORDER, 1), 1, 'Y', 'N') PRI_CLASS_MTG_PAT_FLAG,
            P.CLASS_MTG_PAT_SID,
            P.CLASS_MTG_NUM,
            NVL (P.FCLTY_SID, 2147483646) FCLTY_SID,
            P.MEETING_TIME_START,
            P.MEETING_TIME_END,
            P.MON,
            P.TUES,
            P.WED,
            P.THURS,
            P.FRI,
            P.SAT,
            P.SUN,
            P.MTG_PAT_CD,
        P.START_DT,
        P.START_TIME,
        P.END_DT,
        P.END_TIME,
        P.MEETING_TIME,
            P.MTG_PAT_CRSE_TOPIC_ID,
            P.DESCR,
            P.STND_MTG_PAT,
            P.PRINT_TOPIC_ON_XCR,
       CASE WHEN P.FCLTY_SID < 2147483646
             AND TRIM(P.MTG_PAT_CD) IS NOT NULL
             AND TRIM(TO_CHAR(P.MEETING_TIME_START, 'HH24:MI')) IS NOT NULL
             AND TO_CHAR(P.MEETING_TIME_START, 'HH24:MI') <> '00:00'
             AND L.FCLTY_ID NOT IN ('-','ON-LINE','TBA','TBA-NORTH')
             AND L.ROOM_NM NOT IN ('-','GROUP STUD','LMS CHAT','OFF-CAMP','OFFCAMPUS','TBD')
            THEN ORA_HASH (C.INSTITUTION_CD||C.TERM_CD||DECODE(C.SESSION_CD,'CE1','1',C.SESSION_CD)||P.FCLTY_SID||P.MTG_PAT_CD||TO_CHAR(P.MEETING_TIME_START, 'HH24:MI'))
            ELSE NULL
        END MTG_PAT_HASH,
            NVL (I.CLASS_INSTRCTR_ORDER, 1) CLASS_INSTRCTR_ORDER,
            DECODE (NVL (I.CLASS_INSTRCTR_ORDER, 1), 1, 'Y', 'N') PRI_CLASS_INSTRCTR_ORDER_FLAG,
            NVL (I.CLASS_INSTRCTR_SID, 2147483646) CLASS_INSTRCTR_SID,
            I.INSTRCTR_ASGN_NUM,
            NVL (I.INSTRCTR_SID, 2147483646) INSTRCTR_SID,
            NVL (I.INSTRCTR_ROLE_SID, 2147483646) INSTRCTR_ROLE_SID,
            I.AUTOCALC_WRKLD_FLG,
--            I.ASSIGN_TYPE,
--            I.WEEK_WRKLD_HR_CNT,
            I.INSTRCTR_LOAD_PCT,
--            I.ASGN_PCT,
            I.GRADE_RSTR_ACCESS,
            I.GRADE_RSTR_ACCESS_SD,
            I.GRADE_RSTR_ACCESS_LD,
            I.CONTACT_MINUTES,
            I.SCHED_PRINT_INSTR,
            NVL (F.CE_CREDITS, 0) CE_CREDITS,
            NVL (F.CE_FTE, 0) CE_FTE,
            NVL (F.DAY_CREDITS, 0) DAY_CREDITS,
            NVL (F.DAY_FTE, 0) DAY_FTE,
            NVL ( (F.CE_CREDITS + F.DAY_CREDITS), 0) TOTAL_CREDITS,
            NVL (F.ENROLL_CNT, 0) ENROLL_CNT,
            NVL (F.ENROLL_CRSE_CNT, 0) ENROLL_CRSE_CNT,     -- Always zero?
            NVL (F.DROP_CNT, 0) DROP_CNT,
            NVL (F.DROP_CRSE_CNT, 0) DROP_CRSE_CNT,         -- Always zero?
            NVL (F.WAIT_CNT, 0) WAIT_CNT,
            NVL (F.WAIT_CRSE_CNT, 0) WAIT_CRSE_CNT,         -- Always zero?
            F.IFTE_CNT,                                     -- Nov 2016
            NVL (F.TAKEN_UNIT_SUM, 0) TAKEN_UNIT_SUM,       -- Always zero?
            NVL (F.TAKEN_UNIT_OL_SUM, 0) TAKEN_UNIT_OL_SUM, -- Always zero?
            NVL (F.WAIT_ENRL_CNT, 0) WAIT_ENRL_CNT,
            NVL (F.WAIT_WAIT_CNT, 0) WAIT_WAIT_CNT,         -- Always zero?
            NVL (F.WAIT_ENRL_DIST_CRSE_CNT, 0) WAIT_ENRL_DIST_CRSE_CNT,
            NVL (F.WAIT_DIST_CRSE_CNT, 0) WAIT_DIST_CRSE_CNT,
            C.DATA_ORIGIN
  from UM_D_CLASS C
  left outer join PAT P
    on C.CLASS_SID = P.CLASS_SID
   and C.SRC_SYS_ID = P.SRC_SYS_ID
  left outer join INS I
    on nvl(P.CLASS_MTG_PAT_SID,2147483646) = I.CLASS_MTG_PAT_SID
   and C.SRC_SYS_ID = I.SRC_SYS_ID
  left outer join ENR F
    on C.CLASS_SID = F.CLASS_SID
   and C.SRC_SYS_ID = F.SRC_SYS_ID
  left outer join PS_D_FCLTY L
    on nvl(P.FCLTY_SID,2147483646) = L.FCLTY_SID
 where C.DATA_ORIGIN <> 'D'),
CLS2 as (
select /*+ INLINE PARALLEL(8) */
       CLASS_SID,
       CRSE_CD,
       CRSE_OFFER_NUM,
       TERM_CD,
       SESSION_CD,
       CLASS_SECTION_CD,
       SRC_SYS_ID,
       INSTITUTION_SID,
       INSTITUTION_CD,
       CLASS_MTG_PAT_ORDER,
       PRI_CLASS_MTG_PAT_FLAG,
       CLASS_MTG_PAT_SID,
       MIN(DECODE(CLASS_MTG_PAT_ORDER, 1, CLASS_MTG_PAT_SID, 2147483646)) OVER (PARTITION BY CLASS_SID) CLASS_MTG_PAT_PRIM_SID,
       MIN(DECODE(CLASS_MTG_PAT_ORDER, 2, CLASS_MTG_PAT_SID, 2147483646)) OVER (PARTITION BY CLASS_SID) CLASS_MTG_PAT_SECOND_SID,
       CLASS_MTG_NUM,
       FCLTY_SID,
       MEETING_TIME_START,
       MEETING_TIME_END,
       MON,
       TUES,
       WED,
       THURS,
       FRI,
       SAT,
       SUN,
       MTG_PAT_CD,
       START_DT,
       START_TIME,
       END_DT,
       END_TIME,
       MEETING_TIME,
       MTG_PAT_CRSE_TOPIC_ID,
       DESCR,
       STND_MTG_PAT,
       PRINT_TOPIC_ON_XCR,
       MTG_PAT_HASH,
       SUM(DECODE(PRI_CLASS_INSTRCTR_ORDER_FLAG, 'Y', 1, 0)) OVER (PARTITION BY MTG_PAT_HASH) MTG_PAT_HASH_CNT,
       CLASS_INSTRCTR_ORDER,
       PRI_CLASS_INSTRCTR_ORDER_FLAG,
       CLASS_INSTRCTR_SID,
       INSTRCTR_ASGN_NUM,
       INSTRCTR_SID,
       INSTRCTR_ROLE_SID,
       AUTOCALC_WRKLD_FLG,
--       ASSIGN_TYPE,
--       WEEK_WRKLD_HR_CNT,
       INSTRCTR_LOAD_PCT,
--       ASGN_PCT,
       GRADE_RSTR_ACCESS,
       GRADE_RSTR_ACCESS_SD,
       GRADE_RSTR_ACCESS_LD,
       CONTACT_MINUTES,
       SCHED_PRINT_INSTR,
       CE_CREDITS,
       CE_FTE,
       DAY_CREDITS,
       DAY_FTE,
       TOTAL_CREDITS,
       ENROLL_CNT,
       ENROLL_CRSE_CNT,         -- Always zero?
       DROP_CNT,
       DROP_CRSE_CNT,           -- Always zero?
       WAIT_CNT,
       WAIT_CRSE_CNT,           -- Always zero?
       IFTE_CNT,                -- Nov 2016
       TAKEN_UNIT_SUM,          -- Always zero?
       TAKEN_UNIT_OL_SUM,       -- Always zero?
       WAIT_ENRL_CNT,
       WAIT_WAIT_CNT,           -- Always zero?
       WAIT_ENRL_DIST_CRSE_CNT,
       WAIT_DIST_CRSE_CNT,
       DATA_ORIGIN
  from CLS1
)
select /*+ INLINE PARALLEL(8) */
       CLASS_SID,
       CRSE_CD,
       CRSE_OFFER_NUM,
       TERM_CD,
       SESSION_CD,
       CLASS_SECTION_CD,
       SRC_SYS_ID,
       INSTITUTION_SID,
       INSTITUTION_CD,
       CLASS_MTG_PAT_ORDER,
       PRI_CLASS_MTG_PAT_FLAG,
--       CLASS_MTG_PAT_SID,
       nvl(CLASS_MTG_PAT_SID,2147483646) CLASS_MTG_PAT_SID,
       CLASS_MTG_PAT_PRIM_SID,
       CLASS_MTG_PAT_SECOND_SID,
       CLASS_MTG_NUM,
       FCLTY_SID,
       MEETING_TIME_START,
       MEETING_TIME_END,
       MON,
       TUES,
       WED,
       THURS,
       FRI,
       SAT,
       SUN,
       MTG_PAT_CD,
       START_DT,
       START_TIME,
       END_DT,
       END_TIME,
       MEETING_TIME,
       MTG_PAT_CRSE_TOPIC_ID,
       DESCR,
       STND_MTG_PAT,
       PRINT_TOPIC_ON_XCR,
       (CASE WHEN MTG_PAT_HASH_CNT > 1 THEN MTG_PAT_HASH ELSE NULL END) MTG_PAT_HASH,
       (CASE WHEN MTG_PAT_HASH_CNT > 1 THEN MTG_PAT_HASH_CNT ELSE NULL END) MTG_PAT_HASH_CNT,
       CLASS_INSTRCTR_ORDER,
       PRI_CLASS_INSTRCTR_ORDER_FLAG,
--       CLASS_INSTRCTR_SID,
       nvl(CLASS_INSTRCTR_SID,2147483646) CLASS_INSTRCTR_SID,
       INSTRCTR_ASGN_NUM,
       INSTRCTR_SID,
       INSTRCTR_ROLE_SID,
       AUTOCALC_WRKLD_FLG,
--       ASSIGN_TYPE,
--       WEEK_WRKLD_HR_CNT,
       INSTRCTR_LOAD_PCT,
--       ASGN_PCT,
       GRADE_RSTR_ACCESS,
       GRADE_RSTR_ACCESS_SD,
       GRADE_RSTR_ACCESS_LD,
       CONTACT_MINUTES,
       SCHED_PRINT_INSTR,
       CE_CREDITS,
       CE_FTE,
       DAY_CREDITS,
       DAY_FTE,
       TOTAL_CREDITS,
       ENROLL_CNT,
       ENROLL_CRSE_CNT,         -- Always zero?
       DROP_CNT,
       DROP_CRSE_CNT,           -- Always zero?
       WAIT_CNT,
       WAIT_CRSE_CNT,           -- Always zero?
       IFTE_CNT,                -- Nov 2016
       TAKEN_UNIT_SUM,          -- Always zero?
       TAKEN_UNIT_OL_SUM,       -- Always zero?
       WAIT_ENRL_CNT,
       WAIT_WAIT_CNT,           -- Always zero?
       WAIT_ENRL_DIST_CRSE_CNT,
       WAIT_DIST_CRSE_CNT,
       'N' LOAD_ERROR,
       DATA_ORIGIN,
       SYSDATE CREATED_EW_DTTM,
       SYSDATE LASTUPD_EW_DTTM,
       1234 BATCH_SID
  from CLS2
 where CLASS_MTG_PAT_ORDER = 1
   and CLASS_INSTRCTR_ORDER = 1
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_CLASS rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_CLASS',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_CLASS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_CLASS enable constraint PK_UM_F_CLASS';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_CLASS');

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

END UM_F_CLASS_P;
/
