DROP PROCEDURE CSMRT_OWNER.UM_F_STDNT_TERM_P
/

--
-- UM_F_STDNT_TERM_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_F_STDNT_TERM_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--George Adams
--Loads table                -- UM_F_STDNT_TERM
--V01 12/11/2018             -- srikanth ,pabbu converted to proc from sql scripts
--V02 04/01/2019  SMT-8215   -- Doucette ,James added 4 additional fields.

------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_STDNT_TERM';
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

strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_STDNT_TERM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_STDNT_TERM';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_STDNT_TERM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_STDNT_TERM');

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_STDNT_TERM disable constraint PK_UM_F_STDNT_TERM';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_STDNT_TERM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_STDNT_TERM';
insert /*+ append enable_parallel_dml parallel(8) */ into UM_F_STDNT_TERM
WITH
    TERM_ENRL1
    AS
        (SELECT /*+ INLINE PARALLEL(8) */
                TERM_SID,
                PERSON_SID,
                SRC_SYS_ID,
                INSTITUTION_CD,
                ACAD_CAR_CD,
                TERM_CD,
                PERSON_ID,
                RESET_CUM_STATS_FLG,
                MAX_AUDIT_UNIT,
                MAX_WAIT_UNIT,
                MAX_TOT_UNIT,
                MAX_NON_GPA_UNIT,
                NVL (
                    MIN (TERM_CD)
                        OVER (
                            PARTITION BY INSTITUTION_CD,
                                         ACAD_CAR_CD,
                                         PERSON_SID,
                                         SRC_SYS_ID
                            ORDER BY TERM_CD
                            ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING),
                    '9999')
                    NEXT_TERM_CD
           FROM PS_F_TERM_ENRLMT),
    AGG1
    AS
        (SELECT /*+ INLINE PARALLEL(8) */
                A.TERM_SID,
                A.PERSON_SID,
                A.SRC_SYS_ID,
                A.INSTITUTION_CD,
                A.ACAD_CAR_CD,
                A.TERM_CD,
                A.PERSON_ID,
                A.INSTITUTION_SID,
                A.ACAD_CAR_SID,
                A.PS_STDNT_CAR_NUM,
                A.PS_PROG_SID,
                A.PS_EFFDT,
                A.PS_PROG_STAT_SID,
                A.PS_PROG_ACN_SID,
                A.PS_PROG_ACN_RSN_SID,
                A.PS_ADMIT_TERM_SID,
                A.PS_EXP_GRAD_TERM_SID,
                A.PS_DEGR_CHKOUT_LAST_EGT,
                A.PS_DEGR_CHKOUT_LAST_EGT_EFFDT,
                A.STACK_BEGIN_FLG,
                A.STACK_CONTINUE_FLG,
                A.STACK_READMIT_FLG,                               -- Mar 2018
                A.MAJ1_ACAD_PLAN_SID,
                A.MAJ1_PLAN_SEQUENCE,
                A.MAJ1_SPLAN1_SID,
                A.MAJ1_SPLAN2_SID,
                A.MAJ1_SPLAN3_SID,
                A.MAJ1_SPLAN4_SID,
                A.MAJ2_ACAD_PLAN_SID,
                A.MAJ2_SPLAN1_SID,
                A.MAJ3_ACAD_PLAN_SID,
                A.MAJ3_SPLAN1_SID,
                A.MAJ4_ACAD_PLAN_SID,
                A.MIN1_ACAD_PLAN_SID,
                A.MIN2_ACAD_PLAN_SID,
                A.MIN3_ACAD_PLAN_SID,
                A.MIN4_ACAD_PLAN_SID,
                A.OTH1_ACAD_PLAN_SID,
                A.OTH2_ACAD_PLAN_SID,
                A.ED_LVL_RANK,
                A.PREV_DEG_FLG,
                A.UMBOS_HON_FLG,
                MAX (A.UMDAR_DCE_FLG)
                    OVER (
                        PARTITION BY A.ACAD_CAR_SID,
                                     A.PERSON_SID,
                                     A.SRC_SYS_ID)
                    UMDAR_DCE_FLG,
                A.UMDAR_UGRD_SECOND_DEGR_FLG,
                A.UMLOW_UGRD_SECOND_DEGR_FLG,
                F.RESET_CUM_STATS_FLG,
                A.UNDUP_STDNT_CNT,
                MAX (F.TERM_CD)
                    OVER (
                        PARTITION BY A.TERM_SID, A.PERSON_SID, A.SRC_SYS_ID)
                    MAX_TERM_CD,
                CASE WHEN A.TERM_CD = F.TERM_CD THEN 'Y' ELSE 'N' END
                    TERM_ACTV_FLAG,
                  DENSE_RANK ()
                      OVER (PARTITION BY A.INSTITUTION_CD,
                                         A.ACAD_CAR_CD,
                                         A.PERSON_SID,
                                         A.SRC_SYS_ID
                            ORDER BY A.TERM_CD)
                - 1
                    TERM_ORDER
           FROM UM_R_STDNT_AGG  A
                LEFT OUTER JOIN TERM_ENRL1 F -- Join to PS_F_TERM_ENRLMT twice? This time only to get MAX_TERM_CD and RESET_CUM_STATS_FLG
                    ON     A.PERSON_SID = F.PERSON_SID
                       AND A.INSTITUTION_CD = F.INSTITUTION_CD
                       AND A.ACAD_CAR_CD = F.ACAD_CAR_CD
                       AND A.SRC_SYS_ID = F.SRC_SYS_ID
                       AND A.TERM_CD >= F.TERM_CD -- Does this add rows when no match to PS_F_TERM_ENRLMT? Needed for RESET_CUM_STATS_FLG!!!
                       AND A.TERM_CD < F.NEXT_TERM_CD),
    AGG2
    AS
        (SELECT /*+ INLINE PARALLEL(8) */
                TERM_SID,
                PERSON_SID,
                SRC_SYS_ID,
                INSTITUTION_CD,
                ACAD_CAR_CD,
                TERM_CD,
                PERSON_ID,
                INSTITUTION_SID,
                ACAD_CAR_SID,
                PS_STDNT_CAR_NUM,
                PS_PROG_SID,
                PS_EFFDT,
                PS_PROG_STAT_SID,
                PS_PROG_ACN_SID,
                PS_PROG_ACN_RSN_SID,
                PS_ADMIT_TERM_SID,
                PS_EXP_GRAD_TERM_SID,
                PS_DEGR_CHKOUT_LAST_EGT,
                PS_DEGR_CHKOUT_LAST_EGT_EFFDT,
                STACK_BEGIN_FLG,
                STACK_CONTINUE_FLG,
                STACK_READMIT_FLG,                                 -- Mar 2018
                MAJ1_ACAD_PLAN_SID,
                MAJ1_PLAN_SEQUENCE,
                MAJ1_SPLAN1_SID,
                MAJ1_SPLAN2_SID,
                MAJ1_SPLAN3_SID,
                MAJ1_SPLAN4_SID,
                MAJ2_ACAD_PLAN_SID,
                MAJ2_SPLAN1_SID,
                MAJ3_ACAD_PLAN_SID,
                MAJ3_SPLAN1_SID,
                MAJ4_ACAD_PLAN_SID,
                MIN1_ACAD_PLAN_SID,
                MIN2_ACAD_PLAN_SID,
                MIN3_ACAD_PLAN_SID,
                MIN4_ACAD_PLAN_SID,
                OTH1_ACAD_PLAN_SID,
                OTH2_ACAD_PLAN_SID,
                ED_LVL_RANK,
                PREV_DEG_FLG,
                UMBOS_HON_FLG,
                UMDAR_DCE_FLG,
                UMDAR_UGRD_SECOND_DEGR_FLG,
                UMLOW_UGRD_SECOND_DEGR_FLG,
                RESET_CUM_STATS_FLG,             -- Written to UM_F_STDNT_TERM
                MAX_TERM_CD, -- Used to derive RESET_CUM_STATS_FLG and many counts and amounts
                UNDUP_STDNT_CNT,
                TERM_ACTV_FLAG,                  -- Written to UM_F_STDNT_TERM
                TERM_ORDER,                         -- Used to calc CUM counts
                MAX (
                    DECODE (
                        DECODE (TERM_CD,
                                MAX_TERM_CD, RESET_CUM_STATS_FLG,
                                'N'),
                        'Y', TERM_ORDER,
                        0))
                    OVER (PARTITION BY INSTITUTION_CD,
                                       ACAD_CAR_CD,
                                       PERSON_SID,
                                       SRC_SYS_ID
                          ORDER BY TERM_CD
                          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                    RESET_TERM_ORDER,               -- Used to calc CUM counts
                ROW_NUMBER ()
                    OVER (PARTITION BY INSTITUTION_CD,
                                       ACAD_CAR_CD,
                                       MAX_TERM_CD,
                                       PERSON_ID,
                                       SRC_SYS_ID
                          ORDER BY TERM_CD)
                    TERM_ENRL_ORDER                         -- Added July 2016
           FROM AGG1),
    ATHL
    AS
        (SELECT /*+ INLINE PARALLEL(8) */
                DISTINCT PERSON_SID, SUBSTR (SPORT, 1, 1) INST
           FROM UM_D_PERSON_ATHL
          WHERE DATA_ORIGIN <> 'D'),
    ISIR
    AS
        (SELECT /*+ INLINE PARALLEL(8) */
                INSTITUTION
                    INSTITUTION_CD,
                AID_YEAR,
                EMPLID
                    PERSON_ID,
                SRC_SYS_ID,
                'Y'
                    FA_APPL_FLG,
                DECODE (PELL_ELIGIBILITY, 'Y', 'Y', 'N')
                    PELL_ELIGIBILITY,                              -- Apr 2018
                ROW_NUMBER ()
                    OVER (PARTITION BY INSTITUTION,
                                       AID_YEAR,
                                       EMPLID,
                                       SRC_SYS_ID
                          ORDER BY EFFDT DESC, EFFSEQ DESC)
                    ISIR_ORDER
           FROM CSSTG_OWNER.PS_ISIR_CONTROL
          WHERE DATA_ORIGIN <> 'D'),
    PELL
    AS
        (  SELECT /*+ INLINE PARALLEL(8) */
                  A.INSTITUTION_CD,
                  A.ACAD_CAR_CD,
                  A.TERM_CD,
                  A.PERSON_ID,
                  A.SRC_SYS_ID,
                  CASE
                      WHEN SUM (
                               CASE
                                   WHEN I.AGGREGATE_AREA IN ('PELL')
                                   THEN
                                       DISBURSED_BALANCE
                                   ELSE
                                       0
                               END) >
                           0
                      THEN
                          'Y'
                      ELSE
                          'N'
                  END
                      PELL_DISB_FLAG,
                  CASE
                      WHEN SUM (
                               CASE
                                   WHEN I.AGGREGATE_AREA IN
                                            ('DL-SUB', 'DL-UNSUB')
                                   THEN
                                       DISBURSED_BALANCE
                                   ELSE
                                       0
                               END) >
                           0
                      THEN
                          'Y'
                      ELSE
                          'N'
                  END
                      LOAN_DISB_FLAG
             FROM CSMRT_OWNER.UM_F_FA_AWARD_DISB A
                  JOIN CSMRT_OWNER.UM_D_FA_ITEM_TYPE I
                      ON     A.ITEM_TYPE_SID = I.ITEM_TYPE_SID
                         AND I.AGGREGATE_AREA IN ('DL-SUB', 'DL-UNSUB', 'PELL')
         GROUP BY A.INSTITUTION_CD,
                  A.ACAD_CAR_CD,
                  A.TERM_CD,
                  A.PERSON_ID,
                  A.SRC_SYS_ID),
    SRVC
    AS
        (SELECT /*+ INLINE PARALLEL(8) */
                DISTINCT PERSON_SID, INSTITUTION_CD
           FROM UM_D_PERSON_SRVC_IND
          WHERE DATA_ORIGIN <> 'D'),
    STND1
    AS
        (SELECT /*+ INLINE PARALLEL(8) */
                INSTITUTION,
                ACAD_CAREER,
                STRM,
                EMPLID,
                SRC_SYS_ID,
                STRM || ACAD_STNDNG_ACTN
                    ACAD_STNDNG_ACTN,
                ROW_NUMBER ()
                    OVER (
                        PARTITION BY INSTITUTION,
                                     ACAD_CAREER,
                                     STRM,
                                     EMPLID,
                                     SRC_SYS_ID
                        ORDER BY
                            DECODE (DATA_ORIGIN, 'D', 9, 0),
                            EFFDT DESC,
                            EFFSEQ DESC)
                    STND_ORDER                                     -- May 2018
           FROM CSSTG_OWNER.PS_ACAD_STDNG_ACTN
          WHERE DATA_ORIGIN <> 'D'),
    STND2
    AS
        (SELECT /*+ INLINE PARALLEL(8) */
                F.INSTITUTION_CD,
                F.ACAD_CAR_CD,
                F.TERM_CD,
                F.PERSON_ID,
                F.SRC_SYS_ID,
                NVL (
                    SUBSTR (
                        MAX (STND1.ACAD_STNDNG_ACTN)
                            OVER (PARTITION BY F.INSTITUTION_CD,
                                               F.ACAD_CAR_CD,
                                               F.PERSON_ID,
                                               F.SRC_SYS_ID
                                  ORDER BY F.TERM_CD
                                  ROWS BETWEEN CURRENT ROW AND CURRENT ROW),
                        5,
                        4),
                    '-')
                    ACAD_STNDNG_ACTN,                             -- June 2018
                NVL (
                    MAX (STRM)
                        OVER (
                            PARTITION BY F.INSTITUTION_CD,
                                         F.ACAD_CAR_CD,
                                         F.PERSON_ID,
                                         F.SRC_SYS_ID
                            ORDER BY F.TERM_CD
                            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
                    '-')
                    PREV_STRM,
                NVL (
                    SUBSTR (
                        MAX (STND1.ACAD_STNDNG_ACTN)
                            OVER (
                                PARTITION BY F.INSTITUTION_CD,
                                             F.ACAD_CAR_CD,
                                             F.PERSON_ID,
                                             F.SRC_SYS_ID
                                ORDER BY F.TERM_CD
                                ROWS BETWEEN UNBOUNDED PRECEDING
                                     AND     1 PRECEDING),
                        5,
                        4),
                    '-')
                    PREV_ACAD_STNDNG_ACTN
           FROM AGG1  F
                LEFT OUTER JOIN STND1
                    ON     F.INSTITUTION_CD = STND1.INSTITUTION
                       AND F.ACAD_CAR_CD = STND1.ACAD_CAREER
                       AND F.TERM_CD = STND1.STRM
                       AND F.PERSON_ID = STND1.EMPLID
                       AND F.SRC_SYS_ID = STND1.SRC_SYS_ID
                       AND STND1.STND_ORDER = 1),
    TERM_ENRL
    AS
        (SELECT /*+ INLINE PARALLEL(8) */
                AGG2.TERM_SID,
                AGG2.PERSON_SID,
                AGG2.SRC_SYS_ID,
                AGG2.INSTITUTION_CD,
                AGG2.ACAD_CAR_CD,
                AGG2.TERM_CD,
                AGG2.PERSON_ID,
                AGG2.INSTITUTION_SID,
                AGG2.ACAD_CAR_SID,
                AGG2.PS_STDNT_CAR_NUM,
                AGG2.PS_PROG_SID,
                AGG2.PS_EFFDT,
                AGG2.PS_PROG_STAT_SID,
                AGG2.PS_PROG_ACN_SID,
                AGG2.PS_PROG_ACN_RSN_SID,
                AGG2.PS_ADMIT_TERM_SID,
                AGG2.PS_EXP_GRAD_TERM_SID,
                AGG2.PS_DEGR_CHKOUT_LAST_EGT,
                AGG2.PS_DEGR_CHKOUT_LAST_EGT_EFFDT,
                AGG2.STACK_BEGIN_FLG,
                AGG2.STACK_CONTINUE_FLG,
                AGG2.STACK_READMIT_FLG,                            -- Mar 2018
                AGG2.MAJ1_ACAD_PLAN_SID,
                AGG2.MAJ1_PLAN_SEQUENCE,
                AGG2.MAJ1_SPLAN1_SID,
                AGG2.MAJ1_SPLAN2_SID,
                AGG2.MAJ1_SPLAN3_SID,
                AGG2.MAJ1_SPLAN4_SID,
                AGG2.MAJ2_ACAD_PLAN_SID,
                AGG2.MAJ2_SPLAN1_SID,
                AGG2.MAJ3_ACAD_PLAN_SID,
                AGG2.MAJ3_SPLAN1_SID,
                AGG2.MAJ4_ACAD_PLAN_SID,
                AGG2.MIN1_ACAD_PLAN_SID,
                AGG2.MIN2_ACAD_PLAN_SID,
                AGG2.MIN3_ACAD_PLAN_SID,
                AGG2.MIN4_ACAD_PLAN_SID,
                AGG2.OTH1_ACAD_PLAN_SID,
                AGG2.OTH2_ACAD_PLAN_SID,
                AGG2.ED_LVL_RANK,
                AGG2.PREV_DEG_FLG,
                AGG2.UMBOS_HON_FLG,
                AGG2.UMDAR_DCE_FLG,
                AGG2.UMDAR_UGRD_SECOND_DEGR_FLG,
                AGG2.UMLOW_UGRD_SECOND_DEGR_FLG,
                NVL (F.ACAD_GRP_ADVIS_SID, 2147483646)
                    ACAD_GRP_ADVIS_SID,
                NVL (F.ACAD_LOAD_APPR_SID, 2147483646)
                    ACAD_LOAD_APPR_SID,
                NVL (F.ACAD_LOAD_SID, 2147483646)
                    ACAD_LOAD_SID,
                NVL (F.STRT_ACAD_LVL_SID, 2147483646)
                    STRT_ACAD_LVL_SID,
                NVL (F.END_ACAD_LVL_SID, 2147483646)
                    END_ACAD_LVL_SID,
                NVL (F.PRJTD_ACAD_LVL_SID, 2147483646)
                    PRJTD_ACAD_LVL_SID,
                NVL (F.PRI_ACAD_PROG_SID, 2147483646)
                    PRI_ACAD_PROG_SID,
                --nvl(F.ACAD_STNDNG_SID,2147483646) ACAD_STNDNG_SID,
                NVL (D2.ACAD_STNDNG_SID, 2147483646)
                    ACAD_STNDNG_SID,                               -- May 2018
                NVL (T.TERM_CD_DESC, '-')
                    ACAD_STNDNG_TERM_CD_DESC,                      -- May 2018
                NVL (D1.ACAD_STNDNG_SID, 2147483646)
                    TERM_ACAD_STNDNG_SID,                          -- May 2018
                NVL (F.BILL_CAR_SID, 2147483646)
                    BILL_CAR_SID,
                NVL (F.FA_LOAD_SID, 2147483646)
                    FA_LOAD_SID,
                NVL (ATHL.PERSON_SID, 2147483646)
                    PERSON_ATHL_SID,
                NVL (SRVC.PERSON_SID, 2147483646)
                    PERSON_SRVC_IND_SID,
                NVL (F.ACAD_CAR_FIRST_FLG, '-')
                    ACAD_CAR_FIRST_FLG,
                --F.ACAD_LOAD_DT_SID,
                F.ACAD_LOAD_DT,
                NVL (F.ACAD_YR_SID, 1900)
                    ACAD_YR_SID,
                NVL (F.CLASS_RANK_NUM, 0)
                    CLASS_RANK_NUM,
                NVL (F.CLASS_RANK_TOT, 0)
                    CLASS_RANK_TOT,
                NVL (F.COUNTRY, '-')
                    COUNTRY,
                NVL (F.ELIG_TO_ENROLL_FLG, '-')
                    ELIG_TO_ENROLL_FLG,
                --F.ENRL_ON_TRN_DT_SID,
                F.ENRL_ON_TRN_DT,
                NVL (F.EXT_ORG_ID, '-')
                    EXT_ORG_ID,
                NVL (F.FA_ELIG_FLG, '-')
                    FA_ELIG_FLG,
                NVL (F.FA_STATS_CALC_REQ_FLG, '-')
                    FA_STATS_CALC_REQ_FLG,
                NVL (F.FA_STATS_CALC_DTTM, TO_DATE (19000101, 'YYYYMMDD'))
                    FA_STATS_CALC_DTTM,
                NVL (F.FORM_OF_STUDY, '-')
                    FORM_OF_STUDY,
                --F.FULLY_ENRL_DT_SID,
                F.FULLY_ENRL_DT,
                --F.FULLY_GRADED_DT_SID,
                F.FULLY_GRADED_DT,
                --F.LAST_ATTND_DT_SID,
                F.LAST_ATTND_DT,
                NVL (F.LOCK_IN_AMT, 0)
                    LOCK_IN_AMT,
                --F.LOCK_IN_DT_SID,
                F.LOCK_IN_DT,
                NVL (F.MAX_CRSE_CNT, 0)
                    MAX_CRSE_CNT,
                NVL (F.NSLDS_LOAN_YEAR, 0)
                    NSLDS_LOAN_YEAR,
                NVL (F.OVRD_ACAD_LVL_PROJ_FLG, '-')
                    OVRD_ACAD_LVL_PROJ_FLG,
                NVL (F.OVRD_ACAD_LVL_ALL_FLG, '-')
                    OVRD_ACAD_LVL_ALL_FLG,
                NVL (F.OVRD_BILL_UNITS_FLG, '-')
                    OVRD_BILL_UNITS_FLG,
                NVL (F.OVRD_INIT_ADD_FEE_FLG, '-')
                    OVRD_INIT_ADD_FEE_FLG,
                NVL (F.OVRD_INIT_ENR_FEE_FLG, '-')
                    OVRD_INIT_ENR_FEE_FLG,
                NVL (F.OVRD_MAX_UNITS_FLG, '-')
                    OVRD_MAX_UNITS_FLG,
                NVL (F.OVRD_TUIT_GROUP, '-')
                    OVRD_TUIT_GROUP,
                NVL (F.OVRD_WDRW_SCHED, '-')
                    OVRD_WDRW_SCHED,
                NVL (F.PRJTD_BILL_UNIT, 0)
                    PRJTD_BILL_UNIT,
                NVL (F.PRO_RATA_ELIG_FLG, '-')
                    PRO_RATA_ELIG_FLG,
                NVL (F.REFUND_PCT, 0)
                    REFUND_PCT,
                NVL (F.REFUND_SCHEME, '-')
                    REFUND_SCHEME,
                --F.REG_CARD_DT_SID,
                F.REG_CARD_DT,
                NVL (F.REG_FLG, '-')
                    REG_FLG,
                NVL (
                    DECODE (AGG2.TERM_CD,
                            AGG2.MAX_TERM_CD, F.RESET_CUM_STATS_FLG,
                            'N'),
                    'N')
                    RESET_CUM_STATS_FLG,
                NVL (F.SEL_GROUP, '-')
                    SEL_GROUP,
                --F.SSR_ACTV_DT_SID,
                F.SSR_ACTV_DT,
                --F.STATS_ON_TRN_DT_SID,
                F.STATS_ON_TRN_DT,
                NVL (F.STDNT_CAR_NUM, 0)
                    STDNT_CAR_NUM,
                NVL (F.STUDY_AGREEMENT, '-')
                    STUDY_AGREEMENT,
                NVL (F.TERM_TYPE, '-')
                    TERM_TYPE,
                NVL (F.TUIT_CALC_REQ_FLG, '-')
                    TUIT_CALC_REQ_FLG,
                NVL (F.TUIT_CALC_DTTM, TO_DATE (19000101, 'YYYYMMDD'))
                    TUIT_CALC_DTTM,
                --F.UNTPRG_CHG_NSLC_DT_SID,
                F.UNTPRG_CHG_NSLC_DT,
                NVL (F.UNIT_MULTIPLIER, 0)
                    UNIT_MULTIPLIER,
                --F.WDN_DT_SID,
                F.WDN_DT,
                NVL (F.WITHDRAW_CODE, '-')
                    WITHDRAW_CODE,
                NVL (F.WITHDRAW_REASON, '-')
                    WITHDRAW_REASON,
                AGG2.UNDUP_STDNT_CNT,
                AGG2.TERM_ENRL_ORDER,                       -- Added July 2016
                DECODE (AGG2.TERM_CD, AGG2.MAX_TERM_CD, F.TAKEN_GPA_UNIT, 0)
                    UNIT_TAKEN_GPA,
                DECODE (AGG2.TERM_CD,
                        AGG2.MAX_TERM_CD, F.TAKEN_NON_GPA_UNIT,
                        0)
                    UNIT_TAKEN_NOGPA,
                DECODE (AGG2.TERM_CD, AGG2.MAX_TERM_CD, F.GRADE_PTS, 0)
                    GRADE_PTS,
                DECODE (AGG2.TERM_CD, AGG2.MAX_TERM_CD, F.CUR_GPA_PTS, 0)
                    CUR_GPA,
                DECODE (AGG2.TERM_CD, AGG2.MAX_TERM_CD, F.PASSD_GPA_UNIT, 0)
                    UNIT_PASSED_GPA,
                DECODE (AGG2.TERM_CD,
                        AGG2.MAX_TERM_CD, F.PASSD_NON_GPA_UNIT,
                        0)
                    UNIT_PASSED_NOGPA,
                DECODE (AGG2.TERM_CD, AGG2.MAX_TERM_CD, F.PRGRS_GPA_UNIT, 0)
                    UNIT_INPROG_GPA,
                DECODE (AGG2.TERM_CD,
                        AGG2.MAX_TERM_CD, F.PRGRS_NON_GPA_UNIT,
                        0)
                    UNIT_INPROG_NOGPA,
                DECODE (AGG2.TERM_CD,
                        AGG2.MAX_TERM_CD, F.TAKEN_PRGRS_UNIT,
                        0)
                    UNIT_TAKEN_PROGRESS,
                DECODE (AGG2.TERM_CD,
                        AGG2.MAX_TERM_CD, F.PASSD_PRGRS_UNIT,
                        0)
                    UNIT_PASSED_PROGRESS,
                DECODE (AGG2.TERM_CD, AGG2.MAX_TERM_CD, F.AUDIT_UNIT, 0)
                    UNIT_AUDIT,
                DECODE (AGG2.TERM_CD, AGG2.MAX_TERM_CD, F.TRF_TAKEN_GPA, 0)
                    TRF_UNIT_TAKEN_GPA,
                DECODE (AGG2.TERM_CD, AGG2.MAX_TERM_CD, F.TRF_TAKEN_NOGPA, 0)
                    TRF_UNIT_TAKEN_NOGPA,
                DECODE (AGG2.TERM_CD,
                        AGG2.MAX_TERM_CD, F.TRF_GRADE_POINTS,
                        0)
                    TRF_GRADE_PTS,
                DECODE (AGG2.TERM_CD, AGG2.MAX_TERM_CD, F.SSR_TRF_CUR_GPA, 0)
                    TRF_CUR_GPA,
                DECODE (AGG2.TERM_CD, AGG2.MAX_TERM_CD, F.TRF_PASSED_GPA, 0)
                    TRF_UNIT_PASSED_GPA,
                DECODE (AGG2.TERM_CD,
                        AGG2.MAX_TERM_CD, F.TRF_PASSED_NOGPA,
                        0)
                    TRF_UNIT_PASSED_NOGPA,
                DECODE (AGG2.TERM_CD, AGG2.MAX_TERM_CD, F.TC_UNITS_ADJUST, 0)
                    TRF_UNIT_ADJUST,
                DECODE (AGG2.TERM_CD, AGG2.MAX_TERM_CD, F.UNT_TEST_CREDIT, 0)
                    TRF_UNIT_TEST_CREDIT,
                DECODE (AGG2.TERM_CD, AGG2.MAX_TERM_CD, F.XFER_UNIT, 0)
                    TRF_UNIT_TRANSFER,
                DECODE (AGG2.TERM_CD, AGG2.MAX_TERM_CD, F.OTH_UNIT, 0)
                    TRF_UNIT_OTHER,
                DECODE (AGG2.TERM_CD,
                        AGG2.MAX_TERM_CD, F.SSR_COMB_CUR_GPA,
                        0)
                    COMB_CUR_GPA,
                DECODE (AGG2.TERM_CD, AGG2.MAX_TERM_CD, F.TOT_TERM_UNIT, 0)
                    COMB_UNIT_TOT,
                AGG2.TERM_ACTV_FLAG,
                MAX (DECODE (AGG2.TERM_ACTV_FLAG, 'Y', F.TERM_CD, '-'))
                    OVER (PARTITION BY AGG2.INSTITUTION_CD,
                                       AGG2.ACAD_CAR_CD,
                                       AGG2.PERSON_SID,
                                       AGG2.SRC_SYS_ID)
                    TERM_ACTV_MAX_TERM_CD,                   -- Added APR 2015
                F.SSR_TOT_EN_TKNGPA
                    CUM_UNIT_TAKEN_GPA,
                SUM (
                    DECODE (AGG2.TERM_CD,
                            AGG2.MAX_TERM_CD, F.TAKEN_NON_GPA_UNIT,
                            0))
                    OVER (
                        PARTITION BY AGG2.INSTITUTION_CD,
                                     AGG2.ACAD_CAR_CD,
                                     AGG2.PERSON_SID,
                                     AGG2.SRC_SYS_ID
                        ORDER BY AGG2.TERM_CD
                        ROWS BETWEEN (AGG2.TERM_ORDER - AGG2.RESET_TERM_ORDER)
                                     PRECEDING
                             AND     CURRENT ROW)
                    CUM_UNIT_TAKEN_NOGPA,
                F.SSR_TOT_EN_GRDPTS
                    CUM_GRADE_PTS,
                F.SSR_CUM_EN_GPA
                    CUM_CUR_GPA,
                SUM (
                    DECODE (AGG2.TERM_CD,
                            AGG2.MAX_TERM_CD, F.PASSD_GPA_UNIT,
                            0))
                    OVER (
                        PARTITION BY AGG2.INSTITUTION_CD,
                                     AGG2.ACAD_CAR_CD,
                                     AGG2.PERSON_SID,
                                     AGG2.SRC_SYS_ID
                        ORDER BY AGG2.TERM_CD
                        ROWS BETWEEN (AGG2.TERM_ORDER - AGG2.RESET_TERM_ORDER)
                                     PRECEDING
                             AND     CURRENT ROW)
                    CUM_UNIT_PASSED_GPA,
                SUM (
                    DECODE (AGG2.TERM_CD,
                            AGG2.MAX_TERM_CD, F.PASSD_NON_GPA_UNIT,
                            0))
                    OVER (
                        PARTITION BY AGG2.INSTITUTION_CD,
                                     AGG2.ACAD_CAR_CD,
                                     AGG2.PERSON_SID,
                                     AGG2.SRC_SYS_ID
                        ORDER BY AGG2.TERM_CD
                        ROWS BETWEEN (AGG2.TERM_ORDER - AGG2.RESET_TERM_ORDER)
                                     PRECEDING
                             AND     CURRENT ROW)
                    CUM_UNIT_PASSED_NOGPA,
                F.TOT_INPROG_GPA
                    CUM_UNIT_INPROG_GPA,
                F.TOT_INPROG_NOGPA
                    CUM_UNIT_INPROG_NOGPA,
                SUM (
                    DECODE (AGG2.TERM_CD,
                            AGG2.MAX_TERM_CD, F.TAKEN_PRGRS_UNIT,
                            0))
                    OVER (
                        PARTITION BY AGG2.INSTITUTION_CD,
                                     AGG2.ACAD_CAR_CD,
                                     AGG2.PERSON_SID,
                                     AGG2.SRC_SYS_ID
                        ORDER BY AGG2.TERM_CD
                        ROWS BETWEEN (AGG2.TERM_ORDER - AGG2.RESET_TERM_ORDER)
                                     PRECEDING
                             AND     CURRENT ROW)
                    CUM_UNIT_TAKEN_PROGRESS,
                SUM (
                    DECODE (AGG2.TERM_CD,
                            AGG2.MAX_TERM_CD, F.PASSD_PRGRS_UNIT,
                            0))
                    OVER (
                        PARTITION BY AGG2.INSTITUTION_CD,
                                     AGG2.ACAD_CAR_CD,
                                     AGG2.PERSON_SID,
                                     AGG2.SRC_SYS_ID
                        ORDER BY AGG2.TERM_CD
                        ROWS BETWEEN (AGG2.TERM_ORDER - AGG2.RESET_TERM_ORDER)
                                     PRECEDING
                             AND     CURRENT ROW)
                    CUM_UNIT_PASSED_PROGRESS,
                F.TOT_AUDIT
                    CUM_UNIT_AUDIT,
                F.SSR_TOT_TR_TKNGPA
                    CUM_TRF_UNIT_TAKEN_GPA,
                SUM (
                    DECODE (AGG2.TERM_CD,
                            AGG2.MAX_TERM_CD, F.TRF_TAKEN_NOGPA,
                            0))
                    OVER (
                        PARTITION BY AGG2.INSTITUTION_CD,
                                     AGG2.ACAD_CAR_CD,
                                     AGG2.PERSON_SID,
                                     AGG2.SRC_SYS_ID
                        ORDER BY AGG2.TERM_CD
                        ROWS BETWEEN (AGG2.TERM_ORDER - AGG2.RESET_TERM_ORDER)
                                     PRECEDING
                             AND     CURRENT ROW)
                    CUM_TRF_UNIT_TAKEN_NOGPA,
                F.SSR_TOT_TR_GRDPTS
                    CUM_TRF_GRADE_PTS,
                F.SSR_CUM_TR_GPA
                    CUM_TRF_CUR_GPA,
                SUM (
                    DECODE (AGG2.TERM_CD,
                            AGG2.MAX_TERM_CD, F.TRF_PASSED_GPA,
                            0))
                    OVER (
                        PARTITION BY AGG2.INSTITUTION_CD,
                                     AGG2.ACAD_CAR_CD,
                                     AGG2.PERSON_SID,
                                     AGG2.SRC_SYS_ID
                        ORDER BY AGG2.TERM_CD
                        ROWS BETWEEN (AGG2.TERM_ORDER - AGG2.RESET_TERM_ORDER)
                                     PRECEDING
                             AND     CURRENT ROW)
                    CUM_TRF_UNIT_PASSED_GPA,
                SUM (
                    DECODE (AGG2.TERM_CD,
                            AGG2.MAX_TERM_CD, F.TRF_PASSED_NOGPA,
                            0))
                    OVER (
                        PARTITION BY AGG2.INSTITUTION_CD,
                                     AGG2.ACAD_CAR_CD,
                                     AGG2.PERSON_SID,
                                     AGG2.SRC_SYS_ID
                        ORDER BY AGG2.TERM_CD
                        ROWS BETWEEN (AGG2.TERM_ORDER - AGG2.RESET_TERM_ORDER)
                                     PRECEDING
                             AND     CURRENT ROW)
                    CUM_TRF_UNIT_PASSED_NOGPA,
                SUM (
                    DECODE (AGG2.TERM_CD,
                            AGG2.MAX_TERM_CD, F.TC_UNITS_ADJUST,
                            0))
                    OVER (
                        PARTITION BY AGG2.INSTITUTION_CD,
                                     AGG2.ACAD_CAR_CD,
                                     AGG2.PERSON_SID,
                                     AGG2.SRC_SYS_ID
                        ORDER BY AGG2.TERM_CD
                        ROWS BETWEEN (AGG2.TERM_ORDER - AGG2.RESET_TERM_ORDER)
                                     PRECEDING
                             AND     CURRENT ROW)
                    CUM_TRF_UNIT_ADJUST,
                F.TOT_TEST_CREDIT
                    CUM_TRF_UNIT_TEST_CREDIT,
                F.TOT_TRNSFR
                    CUM_TRF_UNIT_TRANSFER,
                F.TOT_OTHER
                    CUM_TRF_UNIT_OTHER,
                F.TOT_TAKEN_GPA
                    CUM_COMB_UNIT_TAKEN_GPA,
                F.TOT_TAKEN_NOGPA
                    CUM_COMB_UNIT_TAKEN_NOGPA,
                F.TOT_GRADE_POINTS
                    CUM_COMB_GRADE_PTS,
                F.CUM_GPA_PTS
                    CUM_COMB_CUR_GPA,
                F.TOT_PASSD_GPA
                    CUM_COMB_UNIT_PASSED_GPA,
                F.TOT_PASSD_NOGPA
                    CUM_COMB_UNIT_PASSED_NOGPA,
--                F.TOT_PASSD_PRGRSS
                TOT_PASSD_GPA + TOT_PASSD_NOGPA     -- Jan 2020
                    CUM_COMB_UNIT_PASSED,
                F.TOT_CUMULATIVE
                    CUM_COMB_UNIT_TOT,      -- SMT-8215
                F.MAX_AUDIT_UNIT
					MAX_UNIT_AUDIT,
                F.MAX_NON_GPA_UNIT
					MAX_UNIT_NOGPA,
				F.MAX_TOT_UNIT
					MAX_UNIT_TOT,
				F.MAX_WAIT_UNIT
					MAX_UNIT_WAIT           -- SMT-8215
           FROM AGG2
                LEFT OUTER JOIN PS_F_TERM_ENRLMT F -- Joined twice? This time with AGG2.MAX_TERM_CD = F.TERM_CD
                    ON     AGG2.PERSON_SID = F.PERSON_SID
                       AND AGG2.SRC_SYS_ID = F.SRC_SYS_ID
                       AND AGG2.INSTITUTION_CD = F.INSTITUTION_CD
                       AND AGG2.ACAD_CAR_CD = F.ACAD_CAR_CD
                       AND AGG2.MAX_TERM_CD = F.TERM_CD
                LEFT OUTER JOIN ATHL
                    ON     ATHL.PERSON_SID = AGG2.PERSON_SID
                       AND ATHL.INST = SUBSTR (AGG2.INSTITUTION_CD, 3, 1)
                LEFT OUTER JOIN SRVC
                    ON     SRVC.PERSON_SID = AGG2.PERSON_SID
                       AND SRVC.INSTITUTION_CD = AGG2.INSTITUTION_CD
                LEFT OUTER JOIN STND2
                    ON     AGG2.INSTITUTION_CD = STND2.INSTITUTION_CD
                       AND AGG2.ACAD_CAR_CD = STND2.ACAD_CAR_CD
                       AND AGG2.TERM_CD = STND2.TERM_CD
                       AND AGG2.PERSON_ID = STND2.PERSON_ID
                       AND AGG2.SRC_SYS_ID = STND2.SRC_SYS_ID
                LEFT OUTER JOIN PS_D_ACAD_STNDNG D1
                    ON     STND2.INSTITUTION_CD = D1.INSTITUTION_CD
                       AND STND2.ACAD_CAR_CD = D1.ACAD_CAR_CD
                       AND STND2.ACAD_STNDNG_ACTN = D1.ACAD_STNDNG_ACN_CD
                       AND STND2.SRC_SYS_ID = D1.SRC_SYS_ID
                LEFT OUTER JOIN PS_D_ACAD_STNDNG D2
                    ON     STND2.INSTITUTION_CD = D2.INSTITUTION_CD
                       AND STND2.ACAD_CAR_CD = D2.ACAD_CAR_CD
                       AND STND2.PREV_ACAD_STNDNG_ACTN =
                           D2.ACAD_STNDNG_ACN_CD
                       AND STND2.SRC_SYS_ID = D2.SRC_SYS_ID
                LEFT OUTER JOIN PS_D_TERM T
                    ON     STND2.INSTITUTION_CD = T.INSTITUTION_CD
                       AND STND2.ACAD_CAR_CD = T.ACAD_CAR_CD
                       AND STND2.PREV_STRM = T.TERM_CD
                       AND STND2.SRC_SYS_ID = T.SRC_SYS_ID),
    ATTR
    AS
        (SELECT /*+ INLINE PARALLEL(8) */
                PERSON_SID,
                ACAD_CAR_SID,
                STDNT_CAR_NUM,
                SRC_SYS_ID,
                SUBSTR (STDNT_ATTR, 2, 2)
                    ATTR,
                STDNT_ATTR_VALUE,
                DECODE (STDNT_ATTR_VAL_SD,
                        '-', STDNT_ATTR_VALUE,
                        STDNT_ATTR_VAL_SD)
                    STDNT_ATTR_VAL_SD,                            -- Sept 2016
                DECODE (STDNT_ATTR_VAL_LD,
                        '-', STDNT_ATTR_VALUE,
                        STDNT_ATTR_VAL_LD)
                    STDNT_ATTR_VAL_LD,                            -- Sept 2016
                --       ROW_NUMBER() OVER (PARTITION BY PERSON_SID, ACAD_CAR_SID, STDNT_CAR_NUM, SRC_SYS_ID, substr(STDNT_ATTR,2,2)
                ROW_NUMBER ()
                    OVER (PARTITION BY PERSON_SID,
                                       ACAD_CAR_SID,
                                       STDNT_CAR_NUM,
                                       SRC_SYS_ID,
                                       SUBSTR (STDNT_ATTR, -2, 2)  -- Nov 2016
                          --                              ORDER BY STDNT_ATTR_VALUE desc) ATTR_ORDER,
                          ORDER BY EFFDT DESC, EFFSEQ DESC)
                    ATTR_ORDER,                                   -- Sept 2016
                --       max(case when INSTITUTION_CD = 'UMBOS' and STDNT_ATTR_VALUE = '2NDDEG' then 'Y' else 'N' end) OVER (PARTITION BY PERSON_SID, ACAD_CAR_SID, STDNT_CAR_NUM, SRC_SYS_ID) UMBOS_UGRD_SECOND_DEGR_FLG
                MAX (
                    CASE
                        WHEN     INSTITUTION_CD = 'UMBOS'
                             AND STDNT_ATTR_VALUE = '2NDDEG'
                        THEN
                            'Y'
                        ELSE
                            'N'
                    END)
                    OVER (PARTITION BY PERSON_SID, ACAD_CAR_SID, SRC_SYS_ID)
                    UMBOS_UGRD_SECOND_DEGR_FLG                     -- Nov 2016
           FROM UM_D_STDNT_ATTR_VAL
          WHERE DATA_ORIGIN <> 'D'),
    DCE
    AS
        (SELECT /*+ INLINE PARALLEL(8) */
                DISTINCT D.INSTITUTION,
                         D.ACAD_CAREER,
                         D.EMPLID,
                         S.STDNT_CAR_NBR_SR,
                         D.SRC_SYS_ID
           FROM CSSTG_OWNER.PS_ADM_APPL_DATA  D
                JOIN CSSTG_OWNER.PS_ADM_APP_CAR_SEQ S
                    ON     D.EMPLID = S.EMPLID
                       AND D.ACAD_CAREER = S.ACAD_CAREER
                       AND D.STDNT_CAR_NBR = S.STDNT_CAR_NBR
                       AND D.ADM_APPL_NBR = S.ADM_APPL_NBR
                       AND D.SRC_SYS_ID = S.SRC_SYS_ID
                       AND S.DATA_ORIGIN <> 'D'
          WHERE     D.DATA_ORIGIN <> 'D'
                AND D.INSTITUTION = 'UMDAR'
                AND D.ADM_APPL_CTR = 'DCE'
                AND S.CREATE_PROG_STATUS = 'S')
SELECT /*+ INLINE PARALLEL(8) */
       F.TERM_SID,
       F.PERSON_SID,
       F.SRC_SYS_ID,
       F.INSTITUTION_CD,
       F.ACAD_CAR_CD,
       F.TERM_CD,
       F.PERSON_ID,
       F.INSTITUTION_SID,
       F.ACAD_CAR_SID,
          TO_CHAR (F.PERSON_SID)
       || '|'
       || TO_CHAR (F.TERM_SID)
       || '|'
       || F.SRC_SYS_ID
           STDNT_TERM_KEY,
       F.PS_STDNT_CAR_NUM,
       F.PS_PROG_SID,
       F.PS_EFFDT,
       F.PS_PROG_STAT_SID,
       F.PS_PROG_ACN_SID,
       F.PS_PROG_ACN_RSN_SID,
       F.PS_ADMIT_TERM_SID,
       F.PS_EXP_GRAD_TERM_SID,
       F.PS_DEGR_CHKOUT_LAST_EGT,
       NVL (
           (SELECT MIN (X.XLATLONGNAME)
              FROM UM_D_XLATITEM_VW X
             WHERE     X.FIELDNAME = 'DEGR_CHKOUT_STAT'
                   AND X.FIELDVALUE = F.PS_DEGR_CHKOUT_LAST_EGT),
           '')
           PS_DEGR_CHKOUT_LAST_EGT_LD,
       F.PS_DEGR_CHKOUT_LAST_EGT_EFFDT,
       F.MAJ1_ACAD_PLAN_SID,
       F.MAJ1_PLAN_SEQUENCE,
       F.MAJ1_SPLAN1_SID,
       F.MAJ1_SPLAN2_SID,
       F.MAJ1_SPLAN3_SID,
       F.MAJ1_SPLAN4_SID,
       F.MAJ2_ACAD_PLAN_SID,
       F.MAJ2_SPLAN1_SID,
       F.MAJ3_ACAD_PLAN_SID,
       F.MAJ3_SPLAN1_SID,
       F.MAJ4_ACAD_PLAN_SID,
       F.MIN1_ACAD_PLAN_SID,
       F.MIN2_ACAD_PLAN_SID,
       F.MIN3_ACAD_PLAN_SID,
       F.MIN4_ACAD_PLAN_SID,
       F.OTH1_ACAD_PLAN_SID,
       F.OTH2_ACAD_PLAN_SID,
       F.ACAD_GRP_ADVIS_SID,
       F.ACAD_LOAD_APPR_SID,
       F.ACAD_LOAD_SID,
       F.STRT_ACAD_LVL_SID,
       F.END_ACAD_LVL_SID,
       F.PRJTD_ACAD_LVL_SID,
       F.PRI_ACAD_PROG_SID,
       F.ACAD_STNDNG_SID,
       F.ACAD_STNDNG_TERM_CD_DESC,                                 -- May 2018
       F.TERM_ACAD_STNDNG_SID,                                     -- May 2018
       F.BILL_CAR_SID,
       F.FA_LOAD_SID,
       F.PERSON_ATHL_SID,
       F.PERSON_SRVC_IND_SID,
--       NVL (RES.RSDNCY_SID, 2147483646)
--           RSDNCY_SID,
--       NVL (RES.ADM_RSDNCY_SID, 2147483646)
--           ADM_RSDNCY_SID,
--       NVL (RES.FA_FED_RSDNCY_SID, 2147483646)
--           FA_FED_RSDNCY_SID,
--       NVL (RES.FA_ST_RSDNCY_SID, 2147483646)
--           FA_ST_RSDNCY_SID,
--       NVL (RES.TUITION_RSDNCY_SID, 2147483646)
--           TUITION_RSDNCY_SID,
--       NVL (RES.RSDNCY_TERM_SID, 2147483646)
--           RSDNCY_TERM_SID,
       F.ACAD_CAR_FIRST_FLG,
       --TO_DATE(F.ACAD_LOAD_DT_SID,'YYYYMMDD') ACAD_LOAD_DT,
       F.ACAD_LOAD_DT,
       F.ACAD_YR_SID,
       --nvl(AD.STDNT_ATTR_VALUE,'-') ADMIT_TERM_CD,
       '-'
           ADMIT_TERM_CD,                                          -- Nov 2016
       --nvl(AD.STDNT_ATTR_VAL_SD,'-') ADMIT_TERM_SD,
       '-'
           ADMIT_TERM_SD,                                          -- Nov 2016
       --(case when AD.STDNT_ATTR_VALUE is not null then AD.STDNT_ATTR_VALUE||' ('||AD.STDNT_ATTR_VAL_SD||')' else '-' end) ADMIT_TERM_CD_DESC,
       '-'
           ADMIT_TERM_CD_DESC,                                     -- Nov 2016
       --nvl(AT.STDNT_ATTR_VAL_LD,'-') ADMIT_TYPE_LD,
       '-'
           ADMIT_TYPE_LD,                                          -- Nov 2016
       (CASE
            WHEN F.INSTITUTION_CD = 'UMBOS' AND F.ACAD_CAR_CD = 'CENC'
            THEN
                'Y'
            --      when F.INSTITUTION_CD = 'UMDAR' and F.UMDAR_DCE_FLG = 'Y'
            WHEN     F.INSTITUTION_CD = 'UMDAR'
                 AND EXISTS
                         (SELECT 1
                            FROM DCE
                           WHERE     DCE.INSTITUTION = F.INSTITUTION_CD
                                 AND DCE.ACAD_CAREER = F.ACAD_CAR_CD
                                 AND DCE.EMPLID = F.PERSON_ID
                                 AND DCE.STDNT_CAR_NBR_SR =
                                     F.PS_STDNT_CAR_NUM
                                 AND DCE.SRC_SYS_ID = F.SRC_SYS_ID) -- Oct 2017
            THEN
                'Y'
            WHEN F.INSTITUTION_CD = 'UMLOW'      -- Need to get UMLOW specs!!!
            THEN
                '-'
            ELSE
                'N'
        END)
           CE_ADMIT_FLG,
       F.CLASS_RANK_NUM,
       F.CLASS_RANK_TOT,
       F.COUNTRY,
       F.ELIG_TO_ENROLL_FLG,
       --TO_DATE(F.ENRL_ON_TRN_DT_SID, 'YYYYMMDD') ENRL_ON_TRN_DT,
       F.ENRL_ON_TRN_DT,
       F.EXT_ORG_ID,
       --NVL((SELECT 'Y'
       --       FROM CSSTG_OWNER.PS_STUDENT_AID A
       --      WHERE F.PERSON_ID = A.EMPLID
       --        AND F.INSTITUTION_CD = A.INSTITUTION
       --        AND (CASE WHEN F.TERM_CD >= '1010' THEN '20'||TRIM(TO_CHAR((TO_NUMBER (SUBSTR (F.TERM_CD, 1, 2), '99') - 9),'09')) ELSE '' end) = A.AID_YEAR
       --        AND F.SRC_SYS_ID = A.SRC_SYS_ID),'N') FA_APPL_FLG,                         -- Temp sub-query!!!
       NVL (ISIR.FA_APPL_FLG, 'N')
           FA_APPL_FLG,
       F.FA_ELIG_FLG,
       --nvl(ISIR.PELL_ELIGIBILITY,'N') FA_PELL_ELIGIBILITY,     -- Apr 2018
       DECODE (F.INSTITUTION_CD,
               'UMLOW', '-',
               NVL (ISIR.PELL_ELIGIBILITY, 'N'))
           FA_PELL_ELIGIBILITY,                                    -- Apr 2018
       --nvl(PELL.PELL_DISB_FLAG,'N') FA_PELL_DISB_FLAG,         -- Apr 2018
       DECODE (F.INSTITUTION_CD,
               'UMLOW', '-',
               NVL (PELL.PELL_DISB_FLAG, 'N'))
           FA_PELL_DISB_FLAG,                                      -- Apr 2018
       --nvl(PELL.LOAN_DISB_FLAG,'N') FA_LOAN_DISB_FLAG,         -- Apr 2018
       DECODE (F.INSTITUTION_CD,
               'UMLOW', '-',
               NVL (PELL.LOAN_DISB_FLAG, 'N'))
           FA_LOAN_DISB_FLAG,                                      -- Apr 2018
       F.FA_STATS_CALC_REQ_FLG,
       F.FA_STATS_CALC_DTTM,
       F.FORM_OF_STUDY,
       NVL (
           (SELECT MIN (X.XLATSHORTNAME)
              FROM UM_D_XLATITEM_VW X
             WHERE     X.FIELDNAME = 'FORM_OF_STUDY'
                   AND X.FIELDVALUE = F.FORM_OF_STUDY),
           ' ')
           FORM_OF_STUDY_SD,
       NVL (
           (SELECT MIN (X.XLATLONGNAME)
              FROM UM_D_XLATITEM_VW X
             WHERE     X.FIELDNAME = 'FORM_OF_STUDY'
                   AND X.FIELDVALUE = F.FORM_OF_STUDY),
           ' ')
           FORM_OF_STUDY_LD,
       --TO_DATE(F.FULLY_ENRL_DT_SID, 'YYYYMMDD') FULLY_ENRL_DT,
       F.FULLY_ENRL_DT,
       --TO_DATE(F.FULLY_GRADED_DT_SID, 'YYYYMMDD') FULLY_GRADED_DT,
       F.FULLY_GRADED_DT,
       --TO_DATE(F.LAST_ATTND_DT_SID, 'YYYYMMDD') LAST_ATTND_DT,
       F.LAST_ATTND_DT,
       F.LOCK_IN_AMT,
       --TO_DATE(F.LOCK_IN_DT_SID, 'YYYYMMDD') LOCK_IN_DT,
       F.LOCK_IN_DT,
       F.MAX_CRSE_CNT,
       F.NSLDS_LOAN_YEAR,
       NVL (
           (SELECT MIN (X.XLATSHORTNAME)
              FROM UM_D_XLATITEM_VW X
             WHERE     X.FIELDNAME = 'NSLDS_LOAN_YEAR'
                   AND X.FIELDVALUE = F.NSLDS_LOAN_YEAR),
           ' ')
           NSLDS_LOAN_YEAR_SD,
       NVL (
           (SELECT MIN (X.XLATLONGNAME)
              FROM UM_D_XLATITEM_VW X
             WHERE     X.FIELDNAME = 'NSLDS_LOAN_YEAR'
                   AND X.FIELDVALUE = F.NSLDS_LOAN_YEAR),
           ' ')
           NSLDS_LOAN_YEAR_LD,
       F.OVRD_ACAD_LVL_PROJ_FLG,
       F.OVRD_ACAD_LVL_ALL_FLG,
       F.OVRD_BILL_UNITS_FLG,
       F.OVRD_INIT_ADD_FEE_FLG,
       F.OVRD_INIT_ENR_FEE_FLG,
       F.OVRD_MAX_UNITS_FLG,
       F.OVRD_TUIT_GROUP,
       F.OVRD_WDRW_SCHED,
       F.PRJTD_BILL_UNIT,
       F.PRO_RATA_ELIG_FLG,
       F.REFUND_PCT,
       F.REFUND_SCHEME,
       --TO_DATE(F.REG_CARD_DT_SID, 'YYYYMMDD') REG_CARD_DT,
       F.REG_CARD_DT,
       F.REG_FLG,
       F.RESET_CUM_STATS_FLG,
       F.SEL_GROUP,
       --TO_DATE(F.SSR_ACTV_DT_SID, 'YYYYMMDD') SSR_ACTV_DT,
       F.SSR_ACTV_DT,
       F.STACK_BEGIN_FLG,                                          -- Mar 2018
       F.STACK_CONTINUE_FLG,                                       -- Mar 2018
       F.STACK_READMIT_FLG,                                        -- Mar 2018
       --TO_DATE(F.STATS_ON_TRN_DT_SID, 'YYYYMMDD') STATS_ON_TRN_DT,
       F.STATS_ON_TRN_DT,
       F.STDNT_CAR_NUM,
       F.STUDY_AGREEMENT,
       F.TERM_ACTV_FLAG
           AS TERM_ACTV_FLG,
       --max(decode(F.TERM_ACTV_FLAG, 'Y', F.TERM_CD, '-'))
       --    over (partition by F.INSTITUTION_CD, F.ACAD_CAR_CD, F.PERSON_SID, F.SRC_SYS_ID) TERM_ACTV_MAX_TERM_CD,      -- Added APR 2015
       F.TERM_ACTV_MAX_TERM_CD,                              -- Added APR 2015
       NVL (T.TERM_SID, 2147483646)
           TERM_ACTV_MAX_TERM_SID,                          -- Added July 2016
       T0.TERM_BEGIN_DT,                                           -- Mar 2018
       T0.TERM_END_DT,                                             -- Mar 2018
       F.TERM_TYPE,
       F.TUIT_CALC_REQ_FLG,
       F.TUIT_CALC_DTTM,
       --greatest(nvl(ATTR2.UMBOS_UGRD_SECOND_DEGR_FLG,'N'), nvl(F.UMDAR_UGRD_SECOND_DEGR_FLG,'N')) UGRD_SECOND_DEGR_FLG,   -- Added March 2015    -- UMBOS and UMDAR only
       CASE
           WHEN     F.INSTITUTION_CD = 'UMBOS'
                AND NVL (ATTR2.UMBOS_UGRD_SECOND_DEGR_FLG, 'N') = 'Y'
           THEN
               'Y'
           WHEN     F.INSTITUTION_CD = 'UMDAR'
                AND (   F.UMDAR_UGRD_SECOND_DEGR_FLG = 'Y'
                     OR F.PREV_DEG_FLG = 'Y')
           THEN
               'Y'
           WHEN     F.INSTITUTION_CD = 'UMLOW'
                AND F.PREV_DEG_FLG = 'Y'
                AND F.ED_LVL_RANK = 87
           THEN
               'Y'
           WHEN     F.INSTITUTION_CD = 'UMLOW'
                AND F.UMLOW_UGRD_SECOND_DEGR_FLG = 'Y'
           THEN
               'Y'
           ELSE
               'N'
       END
           UGRD_SECOND_DEGR_FLG,
       NVL (F.UMBOS_HON_FLG, 'N')
           UMBOS_HON_FLG,                                  -- Added March 2015
       --TO_DATE(case when F.UNTPRG_CHG_NSLC_DT_SID < 19000101 then 19000101 else F.UNTPRG_CHG_NSLC_DT_SID end, 'YYYYMMDD') UNTPRG_CHG_NSLC_DT,
       CASE
           WHEN F.UNTPRG_CHG_NSLC_DT IS NULL THEN TO_DATE ('01-JAN-1900')
           ELSE F.UNTPRG_CHG_NSLC_DT
       END
           UNTPRG_CHG_NSLC_DT,
       F.UNIT_MULTIPLIER,
       --TO_DATE(F.WDN_DT_SID, 'YYYYMMDD') WDN_DT,
       F.WDN_DT,
       F.WITHDRAW_CODE,
       NVL (
           (SELECT MIN (X.XLATSHORTNAME)
              FROM UM_D_XLATITEM_VW X
             WHERE     X.FIELDNAME = 'WITHDRAW_CODE'
                   AND X.FIELDVALUE = F.WITHDRAW_CODE),
           ' ')
           WITHDRAW_CODE_SD,
       NVL (
           (SELECT MIN (X.XLATLONGNAME)
              FROM UM_D_XLATITEM_VW X
             WHERE     X.FIELDNAME = 'WITHDRAW_CODE'
                   AND X.FIELDVALUE = F.WITHDRAW_CODE),
           ' ')
           WITHDRAW_CODE_LD,
       F.WITHDRAW_REASON,
       NVL (
           (SELECT MIN (X.XLATSHORTNAME)
              FROM UM_D_XLATITEM_VW X
             WHERE     X.FIELDNAME = 'WITHDRAW_REASON'
                   AND X.FIELDVALUE = F.WITHDRAW_REASON),
           ' ')
           WITHDRAW_REASON_SD,
       NVL (
           (SELECT MIN (X.XLATLONGNAME)
              FROM UM_D_XLATITEM_VW X
             WHERE     X.FIELDNAME = 'WITHDRAW_REASON'
                   AND X.FIELDVALUE = F.WITHDRAW_REASON),
           ' ')
           WITHDRAW_REASON_LD,
       F.UNDUP_STDNT_CNT,
       F.TERM_ENRL_ORDER,                                   -- Added July 2016
       F.UNIT_TAKEN_GPA,
       F.UNIT_TAKEN_NOGPA,
       F.GRADE_PTS,
       F.CUR_GPA,
       F.UNIT_PASSED_GPA,
       F.UNIT_PASSED_NOGPA,
       F.UNIT_INPROG_GPA,
       F.UNIT_INPROG_NOGPA,
       F.UNIT_TAKEN_PROGRESS,
       F.UNIT_PASSED_PROGRESS,
       F.UNIT_AUDIT,
       F.TRF_UNIT_TAKEN_GPA,
       F.TRF_UNIT_TAKEN_NOGPA,
       F.TRF_GRADE_PTS,
       F.TRF_CUR_GPA,
       F.TRF_UNIT_PASSED_GPA,
       F.TRF_UNIT_PASSED_NOGPA,
       (F.TRF_UNIT_TAKEN_GPA + F.TRF_UNIT_TAKEN_NOGPA)
           TRF_UNIT_TOT_GRADED,
       (F.TRF_UNIT_TRANSFER + F.TRF_UNIT_TEST_CREDIT + F.TRF_UNIT_OTHER)
           TRF_UNIT_TOT,
       F.TRF_UNIT_ADJUST,
       (  (  F.TRF_UNIT_TAKEN_GPA
           + F.TRF_UNIT_TAKEN_NOGPA
           + F.TRF_UNIT_TRANSFER
           + F.TRF_UNIT_TEST_CREDIT
           + F.TRF_UNIT_OTHER)
        - F.TRF_UNIT_ADJUST)
           TRF_UNIT_TOT_ADJUSTED,
       F.TRF_UNIT_TEST_CREDIT,
       F.TRF_UNIT_TRANSFER,
       F.TRF_UNIT_OTHER,
       (F.UNIT_TAKEN_GPA + F.TRF_UNIT_TAKEN_GPA)
           COMB_UNIT_TAKEN_GPA,
       (F.UNIT_TAKEN_NOGPA + F.TRF_UNIT_TAKEN_NOGPA)
           COMB_UNIT_TAKEN_NOGPA,
       (F.GRADE_PTS + F.TRF_GRADE_PTS)
           COMB_GRADE_PTS,
       F.COMB_CUR_GPA,
       (F.UNIT_PASSED_GPA + F.TRF_UNIT_PASSED_GPA)
           COMB_UNIT_PASSED_GPA,
       (F.UNIT_PASSED_NOGPA + F.TRF_UNIT_PASSED_NOGPA)
           COMB_UNIT_PASSED_NOGPA,
       (  F.UNIT_PASSED_GPA
        + F.TRF_UNIT_PASSED_GPA
        + F.UNIT_PASSED_NOGPA
        + F.TRF_UNIT_PASSED_NOGPA)
           COMB_UNIT_PASSED,
       F.COMB_UNIT_TOT,
       F.CUM_UNIT_TAKEN_GPA,
       F.CUM_UNIT_TAKEN_NOGPA,
       F.CUM_GRADE_PTS,
       F.CUM_CUR_GPA,
       F.CUM_UNIT_PASSED_GPA,
       F.CUM_UNIT_PASSED_NOGPA,
       F.CUM_UNIT_INPROG_GPA,
       F.CUM_UNIT_INPROG_NOGPA,
       F.CUM_UNIT_TAKEN_PROGRESS,
       F.CUM_UNIT_PASSED_PROGRESS,
       F.CUM_UNIT_AUDIT,
       F.CUM_TRF_UNIT_TAKEN_GPA,
       F.CUM_TRF_UNIT_TAKEN_NOGPA,
       F.CUM_TRF_GRADE_PTS,
       F.CUM_TRF_CUR_GPA,
       F.CUM_TRF_UNIT_PASSED_GPA,
       F.CUM_TRF_UNIT_PASSED_NOGPA,
       (F.CUM_TRF_UNIT_TAKEN_GPA + F.CUM_TRF_UNIT_TAKEN_NOGPA)
           CUM_TRF_UNIT_TOT_GRADED,
       (  F.CUM_TRF_UNIT_TRANSFER
        + F.CUM_TRF_UNIT_TEST_CREDIT
        + F.CUM_TRF_UNIT_OTHER)
           CUM_TRF_UNIT_TOT,
       F.CUM_TRF_UNIT_ADJUST,
       (  (  F.CUM_TRF_UNIT_TAKEN_GPA
           + F.CUM_TRF_UNIT_TAKEN_NOGPA
           + F.CUM_TRF_UNIT_TRANSFER
           + F.CUM_TRF_UNIT_TEST_CREDIT
           + F.CUM_TRF_UNIT_OTHER)
        - F.CUM_TRF_UNIT_ADJUST)
           CUM_TRF_UNIT_TOT_ADJUSTED,
       F.CUM_TRF_UNIT_TEST_CREDIT,
       F.CUM_TRF_UNIT_TRANSFER,
       F.CUM_TRF_UNIT_OTHER,
       F.CUM_COMB_UNIT_TAKEN_GPA,
       F.CUM_COMB_UNIT_TAKEN_NOGPA,
       F.CUM_COMB_GRADE_PTS,
       F.CUM_COMB_CUR_GPA,
       F.CUM_COMB_UNIT_PASSED_GPA,
       F.CUM_COMB_UNIT_PASSED_NOGPA,
       F.CUM_COMB_UNIT_PASSED,
       F.CUM_COMB_UNIT_TOT,     --	SMT-8215
       F.MAX_UNIT_AUDIT,
	   F.MAX_UNIT_NOGPA,
	   F.MAX_UNIT_TOT,
	   F.MAX_UNIT_WAIT,         --  SMT-8215
       'N'
           LOAD_ERROR,
       'S'
           DATA_ORIGIN,
       SYSDATE
           CREATED_EW_DTTM,
       SYSDATE
           LASTUPD_EW_DTTM,
       1234
           BATCH_SID
  FROM TERM_ENRL  F
       LEFT OUTER JOIN (SELECT DISTINCT PERSON_SID,
                                        ACAD_CAR_SID,
                                        STDNT_CAR_NUM,
                                        SRC_SYS_ID,
                                        UMBOS_UGRD_SECOND_DEGR_FLG
                          FROM ATTR) ATTR2
           ON     F.PERSON_SID = ATTR2.PERSON_SID
              AND F.ACAD_CAR_SID = ATTR2.ACAD_CAR_SID
              AND F.STDNT_CAR_NUM = ATTR2.STDNT_CAR_NUM
              AND F.SRC_SYS_ID = ATTR2.SRC_SYS_ID
       LEFT OUTER JOIN ISIR
           ON     F.INSTITUTION_CD = ISIR.INSTITUTION_CD
              AND F.PERSON_ID = ISIR.PERSON_ID
              AND (CASE
                       WHEN F.TERM_CD >= '1010'
                       THEN
                              '20'
                           || TRIM (
                                  TO_CHAR (
                                      (  TO_NUMBER (SUBSTR (F.TERM_CD, 1, 2),
                                                    '99')
                                       - 9),
                                      '09'))
                       ELSE
                           '-'
                   END) =
                  ISIR.AID_YEAR
              AND F.SRC_SYS_ID = ISIR.SRC_SYS_ID
              AND ISIR.ISIR_ORDER = 1
       LEFT OUTER JOIN PELL
           ON     F.INSTITUTION_CD = PELL.INSTITUTION_CD
              AND F.ACAD_CAR_CD = PELL.ACAD_CAR_CD
              --   and (CASE WHEN F.TERM_CD >= '1010' THEN '20'||TRIM(TO_CHAR((TO_NUMBER(SUBSTR(F.TERM_CD,1,2),'99') - 9),'09')) ELSE '-' end) = PELL.AID_YEAR
              AND F.TERM_CD = PELL.TERM_CD
              AND F.PERSON_ID = PELL.PERSON_ID
              AND F.SRC_SYS_ID = PELL.SRC_SYS_ID
--       LEFT OUTER JOIN UM_R_PERSON_RSDNCY RES
--           ON     F.TERM_SID = RES.EFF_TERM_SID
--              AND F.PERSON_SID = RES.PERSON_SID
--              AND F.SRC_SYS_ID = RES.SRC_SYS_ID
       LEFT OUTER JOIN PS_D_TERM T
           ON     F.INSTITUTION_CD = T.INSTITUTION_CD
              AND F.ACAD_CAR_CD = T.ACAD_CAR_CD
              AND F.TERM_ACTV_MAX_TERM_CD = T.TERM_CD
              AND F.SRC_SYS_ID = T.SRC_SYS_ID
       LEFT OUTER JOIN PS_D_TERM T0
           ON     F.INSTITUTION_CD = T0.INSTITUTION_CD
              AND F.ACAD_CAR_CD = T0.ACAD_CAR_CD
              AND F.TERM_CD = T0.TERM_CD
              AND F.SRC_SYS_ID = T0.SRC_SYS_ID
 WHERE F.TERM_SID < 2147483646;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_STDNT_TERM rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_STDNT_TERM',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

insert into UM_F_STDNT_TERM
with EMPL as (
select distinct EMPLID, INSTITUTION, SRC_SYS_ID
from CSSTG_OWNER.PS_STDNT_GRPS
where substr(STDNT_GROUP,1,2) in ('CF','CS','TF','TS')
  and substr(STDNT_GROUP,3,1) between '0' and '9'
  and DATA_ORIGIN <> 'D'
minus
select distinct EMPLID, INSTITUTION, SRC_SYS_ID
from CSSTG_OWNER.PS_ACAD_PROG
where DATA_ORIGIN <> 'D'
),
GRP as (
select distinct
EMPLID, INSTITUTION,
'2'||substr(STDNT_GROUP,3,1)||decode(substr(STDNT_GROUP,2,1),'F','10','30') STRM,
SRC_SYS_ID
from CSSTG_OWNER.PS_STDNT_GRPS
where substr(STDNT_GROUP,1,2) in ('CF','CS','TF','TS')
  and substr(STDNT_GROUP,3,1) between '0' and '9'
  and DATA_ORIGIN <> 'D'
),
GRP2 as (
select EMPL.EMPLID, GRP.INSTITUTION, GRP.STRM, GRP.SRC_SYS_ID
from EMPL
join GRP
  on EMPL.EMPLID = GRP.EMPLID
 and EMPL.INSTITUTION = GRP.INSTITUTION
 and EMPL.SRC_SYS_ID = GRP.SRC_SYS_ID
),
ADM as (
select distinct EMPLID, ACAD_CAREER, INSTITUTION, SRC_SYS_ID
from CSSTG_OWNER.PS_ADM_APPL_PROG
where DATA_ORIGIN <> 'D'
)
select distinct
T.TERM_SID, P.PERSON_SID, GRP2.SRC_SYS_ID,
GRP2.INSTITUTION INSTITUTION_CD, ADM.ACAD_CAREER ACAD_CAR_CD, GRP2.STRM TERM_CD, GRP2.EMPLID PERSON_ID, T.INSTITUTION_SID, T.ACAD_CAR_SID,
TO_CHAR(P.PERSON_SID)||'|'||TO_CHAR(T.TERM_SID)||'|'||GRP2.SRC_SYS_ID STDNT_TERM_KEY,
0 PS_STDNT_CAR_NUM, 2147483646 PS_PROG_SID, NULL PS_EFFDT,
2147483646 PS_PROG_STAT_SID, 2147483646 PS_PROG_ACN_SID, 2147483646 PS_PROG_ACN_RSN_SID,
2147483646 PS_ADMIT_TERM_SID, 2147483646 PS_EXP_GRAD_TERM_SID,
NULL PS_DEGR_CHKOUT_LAST_EGT, NULL PS_DEGR_CHKOUT_LAST_EGT_LD, NULL PS_DEGR_CHKOUT_LAST_EGT_EFFDT,
2147483646 MAJ1_ACAD_PLAN_SID, 2147483646 MAJ1_PLAN_SEQUENCE, 2147483646 MAJ1_SPLAN1_SID, 2147483646 MAJ1_SPLAN2_SID, 2147483646 MAJ1_SPLAN3_SID, 2147483646 MAJ1_SPLAN4_SID,
2147483646 MAJ2_ACAD_PLAN_SID, 2147483646 MAJ2_SPLAN1_SID, 2147483646 MAJ3_ACAD_PLAN_SID, 2147483646 MAJ3_SPLAN1_SID, 2147483646 MAJ4_ACAD_PLAN_SID,
2147483646 MIN1_ACAD_PLAN_SID, 2147483646 MIN2_ACAD_PLAN_SID, 2147483646 MIN3_ACAD_PLAN_SID, 2147483646 MIN4_ACAD_PLAN_SID,
2147483646 OTH1_ACAD_PLAN_SID, 2147483646 OTH2_ACAD_PLAN_SID,
2147483646 ACAD_GRP_ADVIS_SID, 2147483646 ACAD_LOAD_APPR_SID, 2147483646 ACAD_LOAD_SID, 2147483646 STRT_ACAD_LVL_SID, 2147483646 END_ACAD_LVL_SID, 2147483646 PRJTD_ACAD_LVL_SID,
2147483646 PRI_ACAD_PROG_SID, 2147483646 ACAD_STNDNG_SID, '-' ACAD_STNDNG_TERM_CD_DESC, 2147483646 TERM_ACAD_STNDNG_SID, 2147483646 BILL_CAR_SID,
2147483646 FA_LOAD_SID, 2147483646 PERSON_ATHL_SID, 2147483646 PERSON_SRVC_IND_SID,
--2147483646 RSDNCY_SID, 2147483646 ADM_RSDNCY_SID, 2147483646 FA_FED_RSDNCY_SID, 2147483646 FA_ST_RSDNCY_SID, 2147483646 TUITION_RSDNCY_SID, 2147483646 RSDNCY_TERM_SID,
'-' ACAD_CAR_FIRST_FLG, NULL ACAD_LOAD_DT, 0 ACAD_YR_SID, '-' ADMIT_TERM_CD, '-' ADMIT_TERM_SD, '-' ADMIT_TERM_CD_DESC, '-' ADMIT_TYPE_LD,
'-' CE_ADMIT_FLG, 0 CLASS_RANK_NUM, 0 CLASS_RANK_TOT, '-' COUNTRY, '-' ELIG_TO_ENROLL_FLG, NULL ENRL_ON_TRN_DT,
'-' EXT_ORG_ID, '-' FA_APPL_FLG, '-' FA_ELIG_FLG, '-' FA_PELL_ELIGIBILITY, '-' FA_PELL_DISB_FLAG, '-' FA_LOAN_DISB_FLAG, '-' FA_STATS_CALC_REQ_FLG, NULL FA_STATS_CALC_DTTM,
'-' FORM_OF_STUDY, '-' FORM_OF_STUDY_SD, '-' FORM_OF_STUDY_LD, NULL FULLY_ENRL_DT, NULL FULLY_GRADED_DT,
NULL LAST_ATTND_DT, 0 LOCK_IN_AMT, NULL LOCK_IN_DT, 0 MAX_CRSE_CNT, '-' NSLDS_LOAN_YEAR, '-' NSLDS_LOAN_YEAR_SD, '-' NSLDS_LOAN_YEAR_LD,
'-' OVRD_ACAD_LVL_PROJ_FLG, '-' OVRD_ACAD_LVL_ALL_FLG, '-' OVRD_BILL_UNITS_FLG, '-' OVRD_INIT_ADD_FEE_FLG, '-' OVRD_INIT_ENR_FEE_FLG, '-' OVRD_MAX_UNITS_FLG, '-' OVRD_TUIT_GROUP, '-' OVRD_WDRW_SCHED,
0 PRJTD_BILL_UNIT, '-' PRO_RATA_ELIG_FLG, 0 REFUND_PCT, '-' REFUND_SCHEME, NULL REG_CARD_DT, '-' REG_FLG, '-' RESET_CUM_STATS_FLG, '-' SEL_GROUP, NULL SSR_ACTV_DT,
'-' STACK_BEGIN_FLG, '-' STACK_CONTINUE_FLG, '-' STACK_READMIT_FLG, NULL STATS_ON_TRN_DT, 0 STDNT_CAR_NUM,
'-' STUDY_AGREEMENT, '-' TERM_ACTV_FLG, '-' TERM_ACTV_MAX_TERM_CD, 2147483646 TERM_ACTV_MAX_TERM_SID,
NULL TERM_BEGIN_DT, NULL TERM_END_DT,
'-' TERM_TYPE, '-' TUIT_CALC_REQ_FLG, NULL TUIT_CALC_DTTM, '-' UGRD_SECOND_DEGR_FLG, '-' UMBOS_HON_FLG, NULL UNTPRG_CHG_NSLC_DT, 0 UNIT_MULTIPLIER,
NULL WDN_DT, '-' WITHDRAW_CODE, '-' WITHDRAW_CODE_SD, '-' WITHDRAW_CODE_LD, '-' WITHDRAW_REASON, '-' WITHDRAW_REASON_SD, '-' WITHDRAW_REASON_LD, 0 UNDUP_STDNT_CNT, 1 TERM_ENRL_ORDER,
NULL UNIT_TAKEN_GPA, NULL UNIT_TAKEN_NOGPA, NULL GRADE_PTS, NULL CUR_GPA, NULL UNIT_PASSED_GPA, NULL UNIT_PASSED_NOGPA, NULL UNIT_INPROG_GPA, NULL UNIT_INPROG_NOGPA, NULL UNIT_TAKEN_PROGRESS, NULL UNIT_PASSED_PROGRESS, NULL UNIT_AUDIT,
NULL TRF_UNIT_TAKEN_GPA, NULL TRF_UNIT_TAKEN_NOGPA, NULL TRF_GRADE_PTS, NULL TRF_CUR_GPA, NULL TRF_UNIT_PASSED_GPA, NULL TRF_UNIT_PASSED_NOGPA,
NULL TRF_UNIT_TOT_GRADED, NULL TRF_UNIT_TOT, NULL TRF_UNIT_ADJUST, NULL TRF_UNIT_TOT_ADJUSTED, NULL TRF_UNIT_TEST_CREDIT, NULL TRF_UNIT_TRANSFER, NULL TRF_UNIT_OTHER,
NULL COMB_UNIT_TAKEN_GPA, NULL COMB_UNIT_TAKEN_NOGPA, NULL COMB_GRADE_PTS, NULL COMB_CUR_GPA, NULL COMB_UNIT_PASSED_GPA, NULL COMB_UNIT_PASSED_NOGPA, NULL COMB_UNIT_PASSED, NULL COMB_UNIT_TOT,
NULL CUM_UNIT_TAKEN_GPA, NULL CUM_UNIT_TAKEN_NOGPA, NULL CUM_GRADE_PTS, NULL CUM_CUR_GPA, NULL CUM_UNIT_PASSED_GPA, NULL CUM_UNIT_PASSED_NOGPA,
NULL CUM_UNIT_INPROG_GPA, NULL CUM_UNIT_INPROG_NOGPA, NULL CUM_UNIT_TAKEN_PROGRESS, NULL CUM_UNIT_PASSED_PROGRESS, NULL CUM_UNIT_AUDIT,
NULL CUM_TRF_UNIT_TAKEN_GPA, NULL CUM_TRF_UNIT_TAKEN_NOGPA, NULL CUM_TRF_GRADE_PTS, NULL CUM_TRF_CUR_GPA, NULL CUM_TRF_UNIT_PASSED_GPA, NULL CUM_TRF_UNIT_PASSED_NOGPA,
NULL CUM_TRF_UNIT_TOT_GRADED, NULL CUM_TRF_UNIT_TOT, NULL CUM_TRF_UNIT_ADJUST, NULL CUM_TRF_UNIT_TOT_ADJUSTED, NULL CUM_TRF_UNIT_TEST_CREDIT, NULL CUM_TRF_UNIT_TRANSFER, NULL CUM_TRF_UNIT_OTHER,
NULL CUM_COMB_UNIT_TAKEN_GPA, NULL CUM_COMB_UNIT_TAKEN_NOGPA, NULL CUM_COMB_GRADE_PTS, NULL CUM_COMB_CUR_GPA, NULL CUM_COMB_UNIT_PASSED_GPA, NULL CUM_COMB_UNIT_PASSED_NOGPA, NULL CUM_COMB_UNIT_PASSED, NULL CUM_COMB_UNIT_TOT,
-- SMT-8215
NULL MAX_UNIT_AUDIT, NULL MAX_UNIT_NOGPA, NULL MAX_UNIT_TOT, NULL MAX_UNIT_WAIT,
-- SMT-8215
'N' LOAD_ERROR, 'S' DATA_ORIGIN, sysdate CREATED_EW_DTTM, sysdate LASTUPD_EW_DTTM, 1234 BATCH_SID
from GRP2
join ADM
  on GRP2.EMPLID = ADM.EMPLID
 and GRP2.INSTITUTION = ADM.INSTITUTION
 and GRP2.SRC_SYS_ID = ADM.SRC_SYS_ID
join PS_D_TERM T
  on GRP2.INSTITUTION = T.INSTITUTION_CD
 and ADM.ACAD_CAREER = T.ACAD_CAR_CD
 and GRP2.STRM = T.TERM_CD
 and GRP2.SRC_SYS_ID = ADM.SRC_SYS_ID
join PS_D_PERSON P
  on GRP2.EMPLID = P.PERSON_ID
 and GRP2.SRC_SYS_ID = P.SRC_SYS_ID
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_STDNT_TERM rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_STDNT_TERM',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

Insert into CSMRT_OWNER.UM_F_STDNT_TERM
   (TERM_SID, PERSON_SID, SRC_SYS_ID, INSTITUTION_CD, ACAD_CAR_CD,
    TERM_CD, PERSON_ID, INSTITUTION_SID, ACAD_CAR_SID, STDNT_TERM_KEY,
    PS_STDNT_CAR_NUM, PS_PROG_SID, PS_EFFDT, PS_PROG_STAT_SID, PS_PROG_ACN_SID,
    PS_PROG_ACN_RSN_SID, PS_ADMIT_TERM_SID, PS_EXP_GRAD_TERM_SID, PS_DEGR_CHKOUT_LAST_EGT, MAJ1_ACAD_PLAN_SID,
    MAJ1_PLAN_SEQUENCE, MAJ1_SPLAN1_SID, MAJ1_SPLAN2_SID, MAJ1_SPLAN3_SID, MAJ1_SPLAN4_SID,
    MAJ2_ACAD_PLAN_SID, MAJ2_SPLAN1_SID, MAJ3_ACAD_PLAN_SID, MAJ3_SPLAN1_SID, MAJ4_ACAD_PLAN_SID,
    MIN1_ACAD_PLAN_SID, MIN2_ACAD_PLAN_SID, MIN3_ACAD_PLAN_SID, MIN4_ACAD_PLAN_SID, OTH1_ACAD_PLAN_SID,
    OTH2_ACAD_PLAN_SID, ACAD_GRP_ADVIS_SID, ACAD_LOAD_APPR_SID, ACAD_LOAD_SID, STRT_ACAD_LVL_SID,
    END_ACAD_LVL_SID, PRJTD_ACAD_LVL_SID, PRI_ACAD_PROG_SID, ACAD_STNDNG_SID, ACAD_STNDNG_TERM_CD_DESC, TERM_ACAD_STNDNG_SID, BILL_CAR_SID,
    FA_LOAD_SID, PERSON_ATHL_SID, PERSON_SRVC_IND_SID,
--    RSDNCY_SID, ADM_RSDNCY_SID, FA_FED_RSDNCY_SID, FA_ST_RSDNCY_SID, TUITION_RSDNCY_SID, RSDNCY_TERM_SID,
    ACAD_CAR_FIRST_FLG, ACAD_YR_SID, ADMIT_TERM_CD, ADMIT_TERM_SD, ADMIT_TERM_CD_DESC, ADMIT_TYPE_LD,
    CE_ADMIT_FLG, CLASS_RANK_NUM, CLASS_RANK_TOT, COUNTRY, ELIG_TO_ENROLL_FLG,
    ENRL_ON_TRN_DT, EXT_ORG_ID, FA_APPL_FLG, FA_ELIG_FLG, FA_PELL_ELIGIBILITY, FA_PELL_DISB_FLAG, FA_LOAN_DISB_FLAG, FA_STATS_CALC_REQ_FLG,
    FA_STATS_CALC_DTTM, FORM_OF_STUDY, FORM_OF_STUDY_SD, FORM_OF_STUDY_LD, FULLY_ENRL_DT,
    FULLY_GRADED_DT, LOCK_IN_AMT, MAX_CRSE_CNT, NSLDS_LOAN_YEAR, NSLDS_LOAN_YEAR_SD,
    NSLDS_LOAN_YEAR_LD, OVRD_ACAD_LVL_PROJ_FLG, OVRD_ACAD_LVL_ALL_FLG, OVRD_BILL_UNITS_FLG, OVRD_INIT_ADD_FEE_FLG,
    OVRD_INIT_ENR_FEE_FLG, OVRD_MAX_UNITS_FLG, OVRD_TUIT_GROUP, OVRD_WDRW_SCHED, PRJTD_BILL_UNIT,
    PRO_RATA_ELIG_FLG, REFUND_PCT, REFUND_SCHEME, REG_FLG, RESET_CUM_STATS_FLG,
    SEL_GROUP, SSR_ACTV_DT,
    STACK_BEGIN_FLG, STACK_CONTINUE_FLG, STACK_READMIT_FLG,
    STATS_ON_TRN_DT, STDNT_CAR_NUM, STUDY_AGREEMENT,
    TERM_ACTV_FLG, TERM_ACTV_MAX_TERM_CD, TERM_ACTV_MAX_TERM_SID,
    TERM_BEGIN_DT, TERM_END_DT,
    TERM_TYPE, TUIT_CALC_REQ_FLG,
    TUIT_CALC_DTTM, UGRD_SECOND_DEGR_FLG, UMBOS_HON_FLG, UNIT_MULTIPLIER, WITHDRAW_CODE,
    WITHDRAW_CODE_SD, WITHDRAW_CODE_LD, WITHDRAW_REASON, WITHDRAW_REASON_SD, WITHDRAW_REASON_LD,
    UNDUP_STDNT_CNT, TERM_ENRL_ORDER, UNIT_TAKEN_GPA, UNIT_TAKEN_NOGPA, GRADE_PTS,
    CUR_GPA, UNIT_PASSED_GPA, UNIT_PASSED_NOGPA, UNIT_INPROG_GPA, UNIT_INPROG_NOGPA,
    UNIT_TAKEN_PROGRESS, UNIT_PASSED_PROGRESS, UNIT_AUDIT, TRF_UNIT_TAKEN_GPA, TRF_UNIT_TAKEN_NOGPA,
    TRF_GRADE_PTS, TRF_CUR_GPA, TRF_UNIT_PASSED_GPA, TRF_UNIT_PASSED_NOGPA, TRF_UNIT_TOT_GRADED,
    TRF_UNIT_TOT, TRF_UNIT_ADJUST, TRF_UNIT_TOT_ADJUSTED, TRF_UNIT_TEST_CREDIT, TRF_UNIT_TRANSFER,
    TRF_UNIT_OTHER, COMB_UNIT_TAKEN_GPA, COMB_UNIT_TAKEN_NOGPA, COMB_GRADE_PTS, COMB_CUR_GPA,
    COMB_UNIT_PASSED_GPA, COMB_UNIT_PASSED_NOGPA, COMB_UNIT_PASSED, COMB_UNIT_TOT, CUM_UNIT_TAKEN_GPA,
    CUM_UNIT_TAKEN_NOGPA, CUM_GRADE_PTS, CUM_CUR_GPA, CUM_UNIT_PASSED_GPA, CUM_UNIT_PASSED_NOGPA,
    CUM_UNIT_INPROG_GPA, CUM_UNIT_INPROG_NOGPA, CUM_UNIT_TAKEN_PROGRESS, CUM_UNIT_PASSED_PROGRESS, CUM_UNIT_AUDIT,
    CUM_TRF_UNIT_TAKEN_GPA, CUM_TRF_UNIT_TAKEN_NOGPA, CUM_TRF_GRADE_PTS, CUM_TRF_CUR_GPA, CUM_TRF_UNIT_PASSED_GPA,
    CUM_TRF_UNIT_PASSED_NOGPA, CUM_TRF_UNIT_TOT_GRADED, CUM_TRF_UNIT_TOT, CUM_TRF_UNIT_ADJUST, CUM_TRF_UNIT_TOT_ADJUSTED,
    CUM_TRF_UNIT_TEST_CREDIT, CUM_TRF_UNIT_TRANSFER, CUM_TRF_UNIT_OTHER, CUM_COMB_UNIT_TAKEN_GPA, CUM_COMB_UNIT_TAKEN_NOGPA,
    CUM_COMB_GRADE_PTS, CUM_COMB_CUR_GPA, CUM_COMB_UNIT_PASSED_GPA, CUM_COMB_UNIT_PASSED_NOGPA, CUM_COMB_UNIT_PASSED,
    CUM_COMB_UNIT_TOT, MAX_UNIT_AUDIT, MAX_UNIT_NOGPA, MAX_UNIT_TOT, MAX_UNIT_WAIT,
	LOAD_ERROR, DATA_ORIGIN, CREATED_EW_DTTM, LASTUPD_EW_DTTM,
    BATCH_SID)
 Values
   (2147483646, 2147483646, 'CS90', '-', '-',
    '-', '-', 2147483646, 2147483646, '-',
    NULL, 2147483646, TO_DATE(NULL), 2147483646, 2147483646,
    2147483646, 2147483646, 2147483646, '', 2147483646,
    NULL, 2147483646, 2147483646, 2147483646, 2147483646,
    2147483646, 2147483646, 2147483646, 2147483646, 2147483646,
    2147483646, 2147483646, 2147483646, 2147483646, 2147483646,
    2147483646, 2147483646, 2147483646, 2147483646, 2147483646,
    2147483646, 2147483646, 2147483646, 2147483646, '-', 2147483646, 2147483646,
    2147483646, 2147483646,
--    2147483646, 2147483646, 2147483646, 2147483646, 2147483646, 2147483646,
    2147483646, '-', NULL, '', '', '', '',
    '', NULL, NULL, '', '',
    TO_DATE(NULL), '', '', '', '', '', '', '',
    TO_DATE(NULL), '', '', '', TO_DATE(NULL),
    TO_DATE(NULL), NULL, NULL, '', '',
    '', '', '', '', '',
    '', '', '', '', NULL,
    '', NULL, '', '', '',
    '', TO_DATE(NULL),
    '','','',
    TO_DATE(NULL), NULL, '',
    '', '', 2147483646, '', '',
    TO_DATE(NULL), '', '',
    TO_DATE(NULL), TO_DATE(NULL),
    NULL, '',
    '', '', '', '', '',
    NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL,
    'N', 'S', SYSDATE, SYSDATE, 1234);

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_STDNT_TERM rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_STDNT_TERM',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_STDNT_TERM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_STDNT_TERM enable constraint PK_UM_F_STDNT_TERM';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_STDNT_TERM');

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

END UM_F_STDNT_TERM_P;
/
