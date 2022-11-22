DROP PROCEDURE CSMRT_OWNER.UM_F_ADM_APPL_ACAD_STRUCT_P
/

--
-- UM_F_ADM_APPL_ACAD_STRUCT_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_F_ADM_APPL_ACAD_STRUCT_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--George Adams
--Loads table                -- UM_F_ADM_APPL_ACAD_STRUCT
--V01 12/13/2018             -- srikanth ,pabbu converted to proc from sql scripts

------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_ADM_APPL_ACAD_STRUCT';
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

strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_ADM_APPL_ACAD_STRUCT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_ADM_APPL_ACAD_STRUCT';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_ADM_APPL_ACAD_STRUCT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_ADM_APPL_ACAD_STRUCT');

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_ADM_APPL_ACAD_STRUCT disable constraint PK_UM_F_ADM_APPL_ACAD_STRUCT';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );
				
strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_ADM_APPL_ACAD_STRUCT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_ADM_APPL_ACAD_STRUCT';				
insert /*+ append enable_parallel_dml parallel(8) */ into UM_F_ADM_APPL_ACAD_STRUCT
   SELECT /*+ INLINE PARALLEL(8) */ A.ADM_APPL_SID,
          NVL (F.TERM_SID, A.ADMIT_TERM_SID) TERM_SID,
          NVL (F.PERSON_SID, A.APPLCNT_SID) PERSON_SID,
          NVL (STDNT_CAR_NUM, 0) STDNT_CAR_NUM,
          NVL (F.ACAD_PLAN_SID, 2147483646) ACAD_PLAN_SID,
          NVL (F.ACAD_SPLAN_SID, 2147483646) ACAD_SPLAN_SID,
          NVL (F.SRC_SYS_ID, A.SRC_SYS_ID) SRC_SYS_ID,
          NVL (F.INSTITUTION_CD, '-') INSTITUTION_CD,
          NVL (F.ACAD_CAR_CD, '-') ACAD_CAR_CD,
          NVL (F.TERM_CD, '-') TERM_CD,
          NVL (F.PERSON_ID, '-') PERSON_ID,
          NVL (F.ACAD_PROG_CD, '-') ACAD_PROG_CD,
          NVL (F.ACAD_PLAN_CD, '-') ACAD_PLAN_CD,
          NVL (F.ACAD_SPLAN_CD, '-') ACAD_SPLAN_CD,
          F.EFFDT,
          F.EFFSEQ,
          F.ACTION_DT,
          NVL (F.ACAD_CAR_SID, 2147483646) ACAD_CAR_SID,
          NVL (F.ACAD_PROG_SID, 2147483646) ACAD_PROG_SID,
          NVL (F.ADMIT_TERM_SID, 2147483646) ADMIT_TERM_SID,
          NVL (F.CAMPUS_SID, 2147483646) CAMPUS_SID,
          NVL (F.COMPL_TERM_SID, 2147483646) COMPL_TERM_SID,
          NVL (F.EXP_GRAD_TERM_SID, 2147483646) EXP_GRAD_TERM_SID,
          NVL (F.INSTITUTION_SID, A.INSTITUTION_SID) INSTITUTION_SID,
          NVL (F.REQ_TERM_SID, 2147483646) REQ_TERM_SID,
          NVL (F.PROG_STAT_SID, 2147483646) PROG_STAT_SID,
          NVL (F.PROG_ACN_SID, 2147483646) PROG_ACN_SID,
          NVL (F.PROG_ACN_RSN_SID, 2147483646) PROG_ACN_RSN_SID,
          NVL (PLAN_COMPL_TERM_SID, 2147483646) PLAN_COMPL_TERM_SID,
          NVL (PLAN_REQ_TERM_SID, 2147483646) PLAN_REQ_TERM_SID,
          NVL (SPLAN_REQ_TERM_SID, 2147483646) SPLAN_REQ_TERM_SID,
          NVL (STACK_BEGIN_TERM_SID, 2147483646) STACK_BEGIN_TERM_SID,
          NVL (STACK_READMIT_TERM_SID, 2147483646) STACK_READMIT_TERM_SID,
          --          NVL (F.RSDNCY_SID, 2147483646) RSDNCY_SID                   -- Moved to UM_F_STDNT_TERM !!!
          F.ADM_APPL_NBR,
          F.CERTIFICATE_ONLY_FLG,
          F.D_RANK,
          F.D_RANK_PTYPE,
          F.D_RANK_SPLAN,
          F.DATA_FROM_ADM_APPL_FLG,
          F.DEGR_CHKOUT_STAT,
          F.DEGR_CHKOUT_STAT_SD,
          F.DEGR_CHKOUT_STAT_LD,
          F.DEGREE_SEEKING_FLG,
          F.ED_LVL_RANK,
          F.MIN_PROG_STAT_CTGRY,
          F.MISSING_PROG_PLAN_FLG,
          F.PROGRAM_CATGRY,
          F.PLAN_ADVIS_STAT,
          F.PLAN_DECLARE_DT,
          F.PLAN_SEQUENCE,
          F.PLAN_DEGR_CHKOUT_STAT,
          F.PLAN_STDNT_DEGR_CD,
          F.SPLAN_DECLARE_DT,
          F.STACK_BEGIN_FLG,
          F.STACK_CONTINUE_FLG,
          F.STACK_READMIT_FLG,
          F.UMDAR_ED_LVL,
          F.UMDAR_ED_LVL_SD,
          F.UMDAR_ED_LVL_LD,
          F.PROG_CNT,
          F.PRIM_PROG_MAJOR_1_CNT,
          F.PRIM_PROG_MAJOR_2_CNT,
          F.PRIM_PROG_MINOR_1_CNT,
          F.PRIM_PROG_MINOR_2_CNT,
          F.PRIM_PROG_OTHER_PLAN_CNT,
          F.SEC_PROG_MAJOR_1_CNT,
          F.SEC_PROG_MAJOR_2_CNT,
          F.SEC_PROG_MINOR_1_CNT,
          F.SEC_PROG_MINOR_2_CNT,
          F.SEC_PROG_OTHER_PLAN_CNT,
          F.PP_SUB_PLAN_11_CNT,
          F.PP_SUB_PLAN_12_CNT,
          F.PP_SUB_PLAN_21_CNT,
          F.PP_SUB_PLAN_22_CNT,
          F.SP_SUB_PLAN_11_CNT,
          F.SP_SUB_PLAN_12_CNT,
          F.SP_SUB_PLAN_21_CNT,
          F.SP_SUB_PLAN_22_CNT,
          DECODE (F.STACK_READMIT_FLG, 'Y', 1, 0) READMIT_STUDENT_CNT,
          DECODE (F.STACK_CONTINUE_FLG, 'Y', 1, 0) CONTINUING_STUDENT_CNT,
          DECODE (F.STACK_BEGIN_FLG, 'Y', 1, 0) NEW_STUDENT_CNT,
          F.PRIM_PROG_MAJOR1_ORDER,
          A.TERM_BEGIN_DT,
          A.TERM_END_DT
     FROM UM_F_ADM_APPL_STAT A
          JOIN UM_F_ADM_APPL_ENRL AE ON A.ADM_APPL_SID = AE.ADM_APPL_SID
          LEFT OUTER JOIN UM_F_STDNT_ACAD_STRUCT F
             ON     A.APPLCNT_SID = F.PERSON_SID
                --      AND A.ADMIT_TERM_SID = F.TERM_SID
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
                     END) = F.TERM_SID
                AND A.STU_CAR_NBR_SR = F.STDNT_CAR_NUM
                AND A.SRC_SYS_ID = F.SRC_SYS_ID
--                AND F.PRIM_PROG_MAJOR1_ORDER = 1
                and F.MAJOR_RANK = 1        -- Oct 2017 
 ;
strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_ADM_APPL_ACAD_STRUCT rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_ADM_APPL_ACAD_STRUCT',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_ADM_APPL_ACAD_STRUCT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_ADM_APPL_ACAD_STRUCT enable constraint PK_UM_F_ADM_APPL_ACAD_STRUCT';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_ADM_APPL_ACAD_STRUCT');

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

END UM_F_ADM_APPL_ACAD_STRUCT_P;
/
