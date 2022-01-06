CREATE OR REPLACE PROCEDURE             "UM_M_AF_STDNT_ENRL_BDL_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- James Doucette
--
-- Loads table UM_M_AF_STDNT_ENRL_BDL.
--
 --V01  SMT-xxxx 09/10/2018,    James Doucette
--                              New Process
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_M_AF_STDNT_ENRL_BDL';
        intProcessSid                   Integer;
		strInstance                     VARCHAR2(100);
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

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_M_AF_STDNT_ENRL_BDL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_M_AF_STDNT_ENRL_BDL');

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_M_AF_STDNT_ENRL_BDL disable constraint PK_UM_M_AF_STDNT_ENRL_BDL';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Truncating table CSMRT_OWNER.UM_M_AF_STDNT_ENRL_BDL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_M_AF_STDNT_ENRL_BDL';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_M_AF_STDNT_ENRL_BDL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_M_AF_STDNT_ENRL_BDL';
insert /*+ append parallel(16) */ into UM_M_AF_STDNT_ENRL_BDL
  with CTERM as (
select /*+ inline OPT_ESTIMATE(TABLE UM_D_TERM_VW ROWS=1000) */
       INSTITUTION_CD, ACAD_CAR_CD, SRC_SYS_ID,
       min(AID_YEAR)-4 AID_YEAR_BEGIN,
       max(AID_YEAR)+1 AID_YEAR_END
  from UM_D_TERM_VW
 where CURRENT_TERM_FLG = 'Y'
--   and substr(TERM_CD,3,2) not in ('20','90')
   and substr(TERM_CD,3,2) not in ('90')    -- Jan 2020
 group by INSTITUTION_CD, ACAD_CAR_CD, SRC_SYS_ID
)
select  /*+ parallel(16) OPT_ESTIMATE(TABLE T992910 ROWS=1000) */
        T577578.INSTITUTION_CD,
        T577578.ACAD_CAR_CD,
        T577578.TERM_CD,
--        T572353.PERSON_ID,
        T771448.PERSON_ID,
        'CS90' SRC_SYS_ID,
        T577578.ACAD_YR_SID,
        T577578.AID_YEAR,
        T577578.TERM_LD,
        '-' ACAD_ORG_CD,
        '-' ACAD_ORG_LD,
        LVL.ACAD_LVL_CD,        -- Jan 2021
        T1024861.ACAD_PROG_CD,
        T1024861.ACAD_PROG_LD,
        T1024861.CIP_CD PROG_CIP_CD,
        T1024857.ACAD_PLAN_CD,
        T1024857.ACAD_PLAN_LD,
        T1024857.CIP_CD PLAN_CIP_CD,
        T771448.CE_ONLY_FLG,
        CASE WHEN T771448.STACK_BEGIN_FLG = 'Y'
             THEN 'New'
             ELSE 'Continuing'
         END NEW_CONT_IND,
        CASE WHEN T771448.ONLINE_CREDITS > 0 AND T771448.ONLINE_CREDITS <> T771448.TOT_CREDITS
             THEN 'Y'
             ELSE 'N'
         END ONLINE_HYBRID_FLG,
        T771448.ONLINE_ONLY_FLG,
        T992902.RSDNCY_ID,
        T992902.RSDNCY_LD,
--        CASE WHEN T993278.RSDNCY_ID = 'IS' THEN 'Y'
        CASE WHEN T992902.RSDNCY_ID = 'IS' THEN 'Y'     -- Oct 2018
             ELSE 'N'
         END IS_RSDNCY_FLG,
        CASE WHEN T577578.ACAD_CAR_CD IN ('CSCE','LAW','UGRD')
             THEN T771448.ONLINE_CREDITS/15
             ELSE T771448.ONLINE_CREDITS/9
         END ONLINE_FTE,
        T771448.TOT_FTE,
        T771448.ONLINE_CREDITS,
        T771448.CE_ONLINE_CREDITS,      -- Nov 2020
        T771448.TOT_CREDITS - T771448.ONLINE_CREDITS NON_ONLINE_CREDITS,
        T771448.CE_CREDITS,
        T771448.TOT_CREDITS - T771448.CE_CREDITS NON_CE_CREDITS,
        T771448.TOT_CREDITS,
        T771448.ENROLL_CNT,
        T771448.ONLINE_CNT,
        NULL CE_CNT,
        SYSDATE CREATED_EW_DTTM
   FROM CSMRT_OWNER.UM_D_ACAD_PROG_VW T1024861,     /* SR/FA/CD - D_ACAD_PROG - Primary */
        CSMRT_OWNER.UM_D_ACAD_PLAN_VW T1024857,     /* SR/FA/CD - D_ACAD_PLAN - Major 1 */
--        CSMRT_OWNER.UM_D_RSDNCY_VW T993278,         /* D_RSDNCY */
        CSMRT_OWNER.UM_D_RSDNCY_VW T992902,         /* D_RSDNCY - Tuition */
--        CSMRT_OWNER.UM_D_PERSON_CS_VW T572353,      /* D_PERSON */
        CSMRT_OWNER.UM_D_TERM_VW T577578,           /* D_TERM */
        CSMRT_OWNER.UM_F_STDNT_TERM_VW T771448,     /* F_STDNT_TERM */
        CSMRT_OWNER.UM_R_PERSON_RSDNCY_VW T992910,  /* R_PERSON_RSDNCY */
        CSMRT_OWNER.UM_D_ACAD_LVL_VW LVL,           -- Jan 2021
        CTERM
  WHERE (    T1024857.ACAD_PLAN_SID = NVL (T771448.MAJ1_ACAD_PLAN_SID, 2147483646)
         AND T1024857.EFFDT_END >= NVL (T771448.TERM_END_DT, TRUNC (SYSDATE))
         AND NVL (T771448.TERM_END_DT, TRUNC (SYSDATE)) >= T1024857.EFFDT_START
         AND T1024861.ACAD_PROG_SID = NVL (T771448.PS_PROG_SID, 2147483646)
         AND NVL (T771448.TERM_END_DT, TRUNC (SYSDATE)) BETWEEN T1024861.EFFDT_START AND T1024861.EFFDT_END
--         AND T992910.RSDNCY_SID = T993278.RSDNCY_SID
         AND T992902.RSDNCY_SID = T992910.TUITION_RSDNCY_SID
--         AND T572353.PERSON_SID = T771448.PERSON_SID
         AND T771448.STRT_ACAD_LVL_SID = LVL.ACAD_LVL_SID         -- Jan 2021 -- Should it be STRT_ACAD_LVL_SID???
         AND T577578.TERM_SID = T771448.TERM_SID
         AND T771448.TERM_SID = T992910.EFF_TERM_SID
         AND T771448.SRC_SYS_ID = T992910.SRC_SYS_ID
         AND T771448.PERSON_SID = T992910.PERSON_SID
         AND T771448.ENROLL_FLG = 'Y'
         AND T771448.UNDUP_STDNT_CNT = 1
         AND SUBSTR (T577578.TERM_CD, 3, 2) <> '20'
         AND SUBSTR (T577578.TERM_CD, 3, 2) <> '90'
         AND (T1024861.INSTITUTION_CD IN ('-', 'UMBOS','UMDAR'))
         AND (T1024857.INSTITUTION_CD IN ('-', 'UMBOS','UMDAR'))
         AND (T577578.INSTITUTION_CD IN ('-', 'UMBOS','UMDAR'))
         AND (T771448.INSTITUTION_CD IN ('-', 'UMBOS','UMDAR'))
         AND (T992910.INSTITUTION_CD IN ('-', 'UMBOS','UMDAR'))
         and T577578.INSTITUTION_CD = CTERM.INSTITUTION_CD
         and T577578.ACAD_CAR_CD = CTERM.ACAD_CAR_CD
         and T577578.SRC_SYS_ID = CTERM.SRC_SYS_ID
         and T577578.AID_YEAR between CTERM.AID_YEAR_BEGIN and CTERM.AID_YEAR_END)
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_M_AF_STDNT_ENRL_BDL rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_M_AF_STDNT_ENRL_BDL',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_M_AF_STDNT_ENRL_BDL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_M_AF_STDNT_ENRL_BDL';
insert /*+ append parallel(16) */ into CSMRT_OWNER.UM_M_AF_STDNT_ENRL_BDL
    WITH
        CTERM
        AS
            (  SELECT /*+ parallel(16)) inline */       -- Dec 2019
                      INSTITUTION_CD,
                      ACAD_CAR_CD,
                      SRC_SYS_ID,
                      MIN (AID_YEAR) - 4     AID_YEAR_BEGIN,
                      MAX (AID_YEAR) + 1     AID_YEAR_END
                 FROM UM_D_TERM_VW
                WHERE     CURRENT_TERM_FLG = 'Y'
--                      AND SUBSTR (TERM_CD, 3, 2) NOT IN ('20', '90')
                      AND SUBSTR (TERM_CD, 3, 2) NOT IN ('90')  -- Jan 2020
                      and ROWNUM < 100000       -- Dec 2019
             GROUP BY INSTITUTION_CD, ACAD_CAR_CD, SRC_SYS_ID),
        DATASET
        AS
            (SELECT /*+ parallel(16)) inline use_hash(T967235, T992910) */      -- Dec 2019
                 DISTINCT
                 T577578.INSTITUTION_CD,
                 T577578.ACAD_CAR_CD,
                 T577578.TERM_CD,
                 T771448.PERSON_ID,
                 T577578.ACAD_YR_SID,
                 T577578.AID_YEAR,
                 T577578.TERM_LD,
                 LVL.ACAD_LVL_CD,        -- Jan 2021
                 T955761.ACAD_PROG_CD,
                 T955761.ACAD_PROG_LD,
                 T955761.CIP_CD AS prog_cip,
                 T955758.ACAD_PLAN_CD,
                 T955758.ACAD_PLAN_LD,
                 T955758.CIP_CD AS plan_cip,
                 T771448.CE_ONLY_FLG,
                 T771448.ONLINE_ONLY_FLG,
                 T992902.RSDNCY_ID,
                 T992902.RSDNCY_LD,
                 case when T992902.RSDNCY_ID = 'IS' then 'Y' else 'N' end as IS_RSDNCY_FLG,
                 case when T577578.ACAD_CAR_CD = 'GRAD' then T771448.ONLINE_CREDITS/9 else T771448.ONLINE_CREDITS/15 end ONLINE_FTE,
                 T771448.ONLINE_CREDITS,
                 T771448.CE_ONLINE_CREDITS,      -- Nov 2020
                 T771448.CE_CREDITS,
                 T771482.TAKEN_UNIT,
                 T960115.CRSE_CD,
                 T960115.CLASS_SECTION_CD,
                 T960115.SBJCT_CD,
                 T771482.CLASS_NUM,
                 T967231.ADMIT_TYPE_ID,
                 T967239.TERM_CD as admit_term,
                 T577578.PREV_TERM,
                 T577578.PREV_TERM_2,
                 T771448.ONLINE_CNT,
                --NEW_CONTINUING
                CASE
                WHEN SUBSTR(T577578.TERM_CD,3,2) = '10'
                     AND (T967231.ADMIT_TYPE_ID NOT IN ('ITR','ICE')
                     AND (T967239.TERM_CD = T577578.TERM_CD OR T967239.TERM_CD = T577578.PREV_TERM OR T967239.TERM_CD = T577578.PREV_TERM_2)) THEN 'New'
                WHEN SUBSTR(T577578.TERM_CD,3,2) = '30'
                     AND (T967231.ADMIT_TYPE_ID NOT IN ('ITR','ICE')
                     AND (T967239.TERM_CD = T577578.TERM_CD)) THEN 'New'
                WHEN SUBSTR(T577578.TERM_CD,3,2) IN ('40','50') THEN 'Continuing'
                ELSE 'Continuing'
                END NEW_CONT_IND
   FROM CSMRT_OWNER.UM_D_CLASS_VW T960115          /* SR/FA Class - D_CLASS */,
        CSMRT_OWNER.UM_D_ENRLMT_STAT_VW T777426 /* SR/FA Enrollment - D_ENRLMT_STAT */,
        CSMRT_OWNER.UM_D_RSDNCY_VW T992902            /* D_RSDNCY - Tuition */ ,
        CSMRT_OWNER.UM_D_ACAD_PROG_VW T955761 /* SR Structure - D_ACAD_PROG */,
        CSMRT_OWNER.UM_D_ACAD_PLAN_VW T955758 /* SR Structure - D_ACAD_PLAN */,
        CSMRT_OWNER.UM_D_TERM_VW T577578                          /* D_TERM */,
        CSMRT_OWNER.UM_F_STDNT_ACAD_STRUCT_VW T771456 /* F_STDNT_ACAD_STRUCT */,
        CSMRT_OWNER.UM_F_STDNT_TERM_VW T771448              /* F_STDNT_TERM */,
        CSMRT_OWNER.UM_F_STDNT_ENRL_VW T771482              /* F_STDNT_ENRL */ ,
        CSMRT_OWNER.UM_R_PERSON_RSDNCY_VW T992910        /* R_PERSON_RSDNCY */,
        CSMRT_OWNER.UM_F_STDNT_ADM_VW T967235                /* F_STDNT_ADM */,
        CSMRT_OWNER.UM_D_ADMIT_TYPE_VW T967231 /* FA And SR Admissions - D_ADMIT_TYPE */,
        CSMRT_OWNER.UM_D_TERM_VW T967239 /* FA And SR Admissions - D_ADMIT_TERM */,
        CSMRT_OWNER.UM_D_ACAD_LVL_VW LVL,           -- Jan 2021
        CTERM
  WHERE  T771482.CLASS_SID = T960115.CLASS_SID
         AND T771456.ACAD_PLAN_SID = T955758.ACAD_PLAN_SID
         AND T771482.ENRLMT_STAT_SID = T777426.ENRLMT_STAT_SID
         AND T992902.RSDNCY_SID = T992910.TUITION_RSDNCY_SID
         AND T577578.TERM_SID = T771448.TERM_SID
         AND T577578.TERM_SID = T992910.EFF_TERM_SID                                            -- Added
         AND T771456.ACAD_PROG_SID = T955761.ACAD_PROG_SID
         AND T967235.ADMIT_TERM_SID = T967239.TERM_SID
         AND T967231.ADMIT_TYPE_SID = T967235.ADMIT_TYPE_SID
         AND T771456.ACAD_CAR_SID = T967235.ACAD_CAR_SID
         AND T771456.SRC_SYS_ID = T967235.SRC_SYS_ID
         AND T771456.STDNT_CAR_NUM = T967235.STU_CAR_NBR_SR
         AND T771456.PERSON_SID = T967235.PERSON_SID
         AND T955758.EFFDT_END >= NVL (T771456.TERM_END_DT, TRUNC (SYSDATE))
        --AND T771448.PERSON_ID = '00298398'                                                     -- Temp!!!
         AND T771448.ENROLL_FLG = 'Y'
         AND T771456.UNDUP_STDNT_CNT = 1
         AND T777426.ENRLMT_STAT_ID = 'E'
         --AND T577578.TERM_CD = '2810'                                                           -- Temp!!!
         AND T771448.END_ACAD_LVL_SID = LVL.ACAD_LVL_SID         -- Jan 2021 -- Should it be STRT_ACAD_LVL_SID???
         AND T771448.ACAD_CAR_SID = T771456.ACAD_CAR_SID
         AND T771448.TERM_SID = T771456.TERM_SID
         AND T771448.INSTITUTION_SID = T771456.INSTITUTION_SID
         AND T771448.SRC_SYS_ID = T771456.SRC_SYS_ID
         AND T771448.PERSON_SID = T771456.PERSON_SID
         AND T771448.TERM_SID = T992910.EFF_TERM_SID                                -- Put this back in
         AND T771448.SRC_SYS_ID = T992910.SRC_SYS_ID
         AND T771448.PERSON_SID = T992910.PERSON_SID
         AND T771448.ACAD_CAR_SID = T771482.ACAD_CAR_SID
         AND T771448.TERM_SID = T771482.TERM_SID
         AND T771448.INSTITUTION_SID = T771482.INSTITUTION_SID
         AND T771448.SRC_SYS_ID = T771482.SRC_SYS_ID
         AND T771448.PERSON_SID = T771482.PERSON_SID
         AND NVL (T771456.TERM_END_DT, TRUNC (SYSDATE)) >= T955758.EFFDT_START
         AND (T771448.INSTITUTION_CD IN ('-', 'UMLOW'))
         AND (T955758.INSTITUTION_CD IN ('-', 'UMLOW'))
         AND (T955761.INSTITUTION_CD IN ('-', 'UMLOW'))
         AND (T960115.INSTITUTION_CD IN ('-', 'UMLOW'))
         AND (T967235.INSTITUTION_CD IN ('-', 'UMLOW'))
         AND NVL (T771456.TERM_END_DT, TRUNC (SYSDATE)) BETWEEN T955761.EFFDT_START AND T955761.EFFDT_END
         and T577578.INSTITUTION_CD = CTERM.INSTITUTION_CD
         and T577578.ACAD_CAR_CD = CTERM.ACAD_CAR_CD
         and T577578.SRC_SYS_ID = CTERM.SRC_SYS_ID
         and T577578.AID_YEAR between CTERM.AID_YEAR_BEGIN and CTERM.AID_YEAR_END
         and CTERM.INSTITUTION_CD IN ('-', 'UMLOW')
         and T577578.INSTITUTION_CD IN ('-', 'UMLOW')
         and T967239.INSTITUTION_CD IN ('-', 'UMLOW')
),
DATASET2 AS (
SELECT /*+ parallel(16)) inline */ distinct
      INSTITUTION_CD,
      ACAD_CAR_CD,
      TERM_CD,
      PERSON_ID,
      ACAD_YR_SID,
      AID_YEAR,
      TERM_LD,
      ACAD_LVL_CD,        -- Jan 2021
      ACAD_PROG_CD,
      ACAD_PROG_LD,
      PROG_CIP,
      ACAD_PLAN_CD,
      ACAD_PLAN_LD,
      PLAN_CIP,
      CE_ONLY_FLG,
      ONLINE_ONLY_FLG,
      RSDNCY_ID,
      RSDNCY_LD,
      IS_RSDNCY_FLG,
      ONLINE_FTE,
      ONLINE_CREDITS,
      CE_ONLINE_CREDITS,      -- Nov 2020
      CE_CREDITS,
      TAKEN_UNIT,
      CRSE_CD,
      CLASS_SECTION_CD,
      SBJCT_CD,
      CLASS_NUM,
--      NEW_CONT_IND,
      max(NEW_CONT_IND) over (PARTITION BY INSTITUTION_CD, ACAD_CAR_CD, TERM_CD, PERSON_ID) NEW_CONT_IND,
      ONLINE_CNT,
      COUNT(DISTINCT case when CRSE_CD = '039094' and CLASS_SECTION_CD in ('HES','HES2','PAR') then CLASS_NUM END) OVER (PARTITION BY INSTITUTION_CD, ACAD_CAR_CD, TERM_CD, PERSON_ID) STUDY_ABROAD_CNT,
      COUNT(DISTINCT case when SBJCT_CD = 'NONC'  then  CLASS_NUM END) OVER (PARTITION BY INSTITUTION_CD, ACAD_CAR_CD, TERM_CD, PERSON_ID) NONC_CNT,
      COUNT(DISTINCT CLASS_NUM) OVER (PARTITION BY INSTITUTION_CD, ACAD_CAR_CD, TERM_CD, PERSON_ID) ALL_CLASS_CNT
 FROM DATASET
),
DATASET3 AS (
SELECT /*+ parallel(16)) inline */ distinct
      INSTITUTION_CD,
      ACAD_CAR_CD,
      TERM_CD,
      PERSON_ID,
      ACAD_YR_SID,
      AID_YEAR,
      TERM_LD,
      ACAD_LVL_CD,        -- Jan 2021
      ACAD_PROG_CD,
      ACAD_PROG_LD,
      PROG_CIP,
      ACAD_PLAN_CD,
      ACAD_PLAN_LD,
      PLAN_CIP,
      CE_ONLY_FLG,
      ONLINE_ONLY_FLG,
      RSDNCY_ID,
      RSDNCY_LD,
      IS_RSDNCY_FLG,
      ONLINE_FTE,
      ONLINE_CREDITS,
      CE_ONLINE_CREDITS,      -- Nov 2020
      CE_CREDITS,
      NEW_CONT_IND,
      ONLINE_CNT,
      STUDY_ABROAD_CNT,
      NONC_CNT,
      ALL_CLASS_CNT,
      SUM(case when SBJCT_CD <> 'NONC' AND NOT(CRSE_CD = '039094' and CLASS_SECTION_CD in ('HES','HES2','PAR'))
               then TAKEN_UNIT END) OVER (PARTITION BY INSTITUTION_CD, ACAD_CAR_CD, TERM_CD, PERSON_ID) UML_CREDITS
 from DATASET2)
SELECT /*+ parallel(16)) inline */
        INSTITUTION_CD,
        ACAD_CAR_CD,
        TERM_CD,
        PERSON_ID,
        'CS90' SRC_SYS_ID,
        ACAD_YR_SID,
        AID_YEAR,
        TERM_LD,
        '-' ACAD_ORG_CD,
        '-' ACAD_ORG_LD,
        ACAD_LVL_CD,        -- Jan 2021
        ACAD_PROG_CD,
        ACAD_PROG_LD,
        PROG_CIP,
        ACAD_PLAN_CD,
        ACAD_PLAN_LD,
        PLAN_CIP,
        CE_ONLY_FLG,
        NEW_CONT_IND,
        case when ONLINE_CREDITS > 0 and ONLINE_ONLY_FLG = 'N' then 'Y' else 'N' end ONLINE_HYBRID_FLG,
        ONLINE_ONLY_FLG,
        RSDNCY_ID,
        RSDNCY_LD,
        IS_RSDNCY_FLG,
        ONLINE_FTE,
        case when ACAD_CAR_CD = 'GRAD' then UML_CREDITS/9 else UML_CREDITS/15 end TOT_FTE,
        ONLINE_CREDITS,
        CE_ONLINE_CREDITS,      -- Nov 2020
        UML_CREDITS - ONLINE_CREDITS NON_ONLINE_CREDITS,
        CE_CREDITS,
        UML_CREDITS - CE_CREDITS NON_CE_CREDITS,
        UML_CREDITS TOT_CREDITS,
        ALL_CLASS_CNT ENROLL_CNT,
        ONLINE_CNT,
        NULL CE_CNT,
        SYSDATE CREATED_EW_DTTM
 FROM DATASET3
WHERE ALL_CLASS_CNT <> NONC_CNT
  AND ALL_CLASS_CNT <> STUDY_ABROAD_CNT
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_M_AF_STDNT_ENRL_BDL rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_M_AF_STDNT_ENRL_BDL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_M_AF_STDNT_ENRL_BDL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
--alter table UM_M_AF_STDNT_ENRL_BDL enable constraint PK_UM_M_AF_STDNT_ENRL_BDL;

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_M_AF_STDNT_ENRL_BDL enable constraint PK_UM_M_AF_STDNT_ENRL_BDL';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_M_AF_STDNT_ENRL_BDL');

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

END UM_M_AF_STDNT_ENRL_BDL_P;
/
