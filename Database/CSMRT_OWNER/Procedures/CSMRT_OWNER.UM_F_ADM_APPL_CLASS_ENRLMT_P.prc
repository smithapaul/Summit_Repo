DROP PROCEDURE CSMRT_OWNER.UM_F_ADM_APPL_CLASS_ENRLMT_P
/

--
-- UM_F_ADM_APPL_CLASS_ENRLMT_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_F_ADM_APPL_CLASS_ENRLMT_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--George Adams
--Loads table                -- UM_F_ADM_APPL_CLASS_ENRLMT
--V01 12/12/2018             -- srikanth ,pabbu converted to proc from sql scripts

------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_ADM_APPL_CLASS_ENRLMT';
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

strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_ADM_APPL_CLASS_ENRLMT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_ADM_APPL_CLASS_ENRLMT';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_ADM_APPL_CLASS_ENRLMT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_ADM_APPL_CLASS_ENRLMT');

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_ADM_APPL_CLASS_ENRLMT disable constraint PK_UM_F_ADM_APPL_CLASS_ENRLMT';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_ADM_APPL_CLASS_ENRLMT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_ADM_APPL_CLASS_ENRLMT';
insert /*+ append enable_parallel_dml parallel(8) */ into UM_F_ADM_APPL_CLASS_ENRLMT
   WITH PAT
        AS (SELECT CLASS_MTG_PAT_SID,
                   CLASS_SID,
                   ROW_NUMBER ()
                   OVER (PARTITION BY CLASS_SID
                         ORDER BY CLASS_SID, CLASS_MTG_NUM)
                      PAT_ORDER
              FROM UM_D_CLASS_MTG_PAT),
        INSTR
        AS (  SELECT CLASS_INSTRCTR_SID,
                     CLASS_MTG_PAT_SID,
                     ROW_NUMBER ()
                     OVER (
                        PARTITION BY I.CLASS_MTG_PAT_SID
                        ORDER BY
                           DECODE (R.INSTRCTR_ROLE_CD, 'PI', 0, 1),
                           I.INSTRCTR_ASGN_NUM)
                        INSTR_ORDER
                FROM UM_D_CLASS_INSTRCTR I, PS_D_INSTRCTR_ROLE R
               WHERE I.INSTRCTR_ROLE_SID = R.INSTRCTR_ROLE_SID
            ORDER BY 3 DESC)
   SELECT /*+ INLINE PARALLEL(8) */
          A.ADM_APPL_SID,
          NVL(F.SESSION_SID, 2147483646) SESSION_SID,
          A.APPLCNT_SID PERSON_SID,
          NVL (F.CLASS_NUM, 0) CLASS_NUM,
          A.SRC_SYS_ID,
          A.INSTITUTION_SID,
          A.INSTITUTION_CD,
          A.ACAD_CAR_SID,
          A.ADMIT_TERM_SID TERM_SID,
          NVL (F.CLASS_SID, 2147483646) CLASS_SID,
          NVL (P1.CLASS_MTG_PAT_SID, 2147483646) CLASS_MTG_PAT_SID_P1,
          NVL (P2.CLASS_MTG_PAT_SID, 2147483646) CLASS_MTG_PAT_SID_P2,
          NVL (F.ENRLMT_REAS_SID, 2147483646) ENRLMT_REAS_SID,
          NVL (F.ENRLMT_STAT_SID, 2147483646) ENRLMT_STAT_SID,
          NVL (F.GRADE_SID, 2147483646) GRADE_SID,
          NVL (I.CLASS_INSTRCTR_SID, 2147483646) PRI_CLASS_INSTRCTR_SID,
          NVL (F.REPEAT_SID, 2147483646) REPEAT_SID,
          F.ENRL_ADD_DT,
          F.ENRL_DROP_DT,
          TO_DATE (19000101, 'YYYYMMDD') ENRLMT_STAT_DT,
          TO_DATE (19000101, 'YYYYMMDD') GRADE_DT,
          TO_DATE (19000101, 'YYYYMMDD') GRADE_BASIS_DT,
          TO_DATE (19000101, 'YYYYMMDD') REPEAT_DT,
          F.REPEAT_FLG,
          TO_CHAR (NVL (F.CLASS_NUM, 0)) CLASS_CD,
          F.CLASS_SECTION_CD,
          F.GRADE_PTS,
          F.BILLING_UNIT,
          F.TAKEN_UNIT,
          F.PRGRS_UNIT,
          --            0 PRGRS_FA_UNIT,
          F.ERN_UNIT,
          F.CE_CREDITS,
          F.CE_FTE,
          F.DAY_CREDITS,
          F.DAY_FTE,
          F.ENROLL_CNT,
          --            F.ENROLL_CRSE_CNT,
          F.DROP_CNT,
          --            F.DROP_CRSE_CNT,
          F.WAIT_CNT,
          --            F.WAIT_CRSE_CNT,
          F.IFTE_CNT
     --            F.TAKEN_UNIT_SUM
     --            F.TAKEN_UNIT_OL_SUM
     FROM UM_F_ADM_APPL_STAT A
          JOIN UM_F_ADM_APPL_ENRL AE ON A.ADM_APPL_SID = AE.ADM_APPL_SID
          LEFT OUTER JOIN UM_F_ACAD_PROG S
             ON     A.APPLCNT_SID = S.PERSON_SID
                --      AND A.ADMIT_TERM_SID = S.TERM_SID
                AND (CASE
                        WHEN NOT (    AE.INSTITUTION_CD = 'UMLOW'
                                  AND AE.ACAD_CAR_CD IN ('CSCE', 'GRAD')
                                  AND AE.ENROLL_CNT = 0)
                        THEN
                           A.ADMIT_TERM_SID
                        WHEN     SUBSTR (AE.ADMIT_TERM_CD, -2, 2) = '10'
                             AND AE.PREV_TERM_SID IS NOT NULL
                             AND AE.PREV_ENROLL_CNT > 0
                        THEN
                           AE.PREV_TERM_SID
                        WHEN     SUBSTR (AE.ADMIT_TERM_CD, -2, 2) IN ('40',
                                                                      '50')
                             AND AE.NEXT_TERM_SID IS NOT NULL
                             AND AE.NEXT_ENROLL_CNT > 0
                        THEN
                           AE.NEXT_TERM_SID
                        ELSE
                           A.ADMIT_TERM_SID
                     END) = S.TERM_SID
                AND A.SRC_SYS_ID = S.SRC_SYS_ID
                AND NVL (A.STU_CAR_NBR_SR, -1) = S.STDNT_CAR_NUM
          LEFT OUTER JOIN UM_F_CLASS_ENRLMT F
             ON     S.PERSON_SID = F.PERSON_SID
                AND S.TERM_SID = F.TERM_SID
                AND S.SRC_SYS_ID = F.SRC_SYS_ID
          LEFT OUTER JOIN PAT P1
             ON F.CLASS_SID = P1.CLASS_SID AND P1.PAT_ORDER = 1
          LEFT OUTER JOIN PAT P2
             ON F.CLASS_SID = P2.CLASS_SID AND P2.PAT_ORDER = 2
          LEFT OUTER JOIN INSTR I
             ON     P1.CLASS_MTG_PAT_SID = I.CLASS_MTG_PAT_SID
                AND P1.PAT_ORDER = 1
                AND I.INSTR_ORDER = 1
   where A.APPLCNT_SID <> 2147483646        -- Mar 2020
--   where NVL(F.SESSION_SID, 2147483646) <> 2147483646
--     and A.APPLCNT_SID <> 2147483646
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_ADM_APPL_CLASS_ENRLMT rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_ADM_APPL_CLASS_ENRLMT',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_ADM_APPL_CLASS_ENRLMT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_ADM_APPL_CLASS_ENRLMT enable constraint PK_UM_F_ADM_APPL_CLASS_ENRLMT';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_ADM_APPL_CLASS_ENRLMT');

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

END UM_F_ADM_APPL_CLASS_ENRLMT_P;
/
