DROP PROCEDURE CSMRT_OWNER.UM_F_ADM_APPL_PRSPCT_CPPS_P
/

--
-- UM_F_ADM_APPL_PRSPCT_CPPS_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_F_ADM_APPL_PRSPCT_CPPS_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--George Adams
--Loads table                -- UM_F_ADM_APPL_PRSPCT_CPPS
--V01 12/13/2018             -- srikanth ,pabbu converted to proc from sql scripts

------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_ADM_APPL_PRSPCT_CPPS';
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

strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_ADM_APPL_PRSPCT_CPPS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_ADM_APPL_PRSPCT_CPPS';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_ADM_APPL_PRSPCT_CPPS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_ADM_APPL_PRSPCT_CPPS');

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_ADM_APPL_PRSPCT_CPPS disable constraint PK_UM_F_ADM_APPL_PRSPCT_CPPS';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_ADM_APPL_PRSPCT_CPPS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_ADM_APPL_PRSPCT_CPPS';
insert /*+ append enable_parallel_dml parallel(8) */ into UM_F_ADM_APPL_PRSPCT_CPPS
with DSP as (
select /*+ INLINE PARALLEL(8) */ distinct R.INSTITUTION_CD, R.ACAD_CAR_CD, R.PERSON_ID, R.ADM_APPL_NBR, R.SRC_SYS_ID, 1 DSP_CNT
  from PS_R_ADM_RECRTR R
 where R.RECRT_CTGRY_ID = 'DSP')
   SELECT /*+ INLINE PARALLEL(8) */ distinct
          A.ADM_APPL_SID,
          NVL (C.PRSPCT_CAR_SID, 2147483646) PRSPCT_CAR_SID,
          A.SRC_SYS_ID,
          NVL (A.INSTITUTION_CD, '-') INSTITUTION_CD,
          NVL (C.ACAD_CAR_CD, '-') ACAD_CAR_CD,
          NVL (C.ADMIT_TERM_SID, 2147483646) CAR_ADMIT_TERM_SID,
          NVL (C.ADMIT_TERM, '-') ADMIT_TERM,
          NVL (C.EMPLID, '-') EMPLID,
          NVL (A.INSTITUTION_SID, 2147483646) INSTITUTION_SID,
          NVL (A.ACAD_CAR_SID, 2147483646) ACAD_CAR_SID,
          NVL (A.ADMIT_TERM_SID, 2147483646) ADMIT_TERM_SID,
          NVL (A.APPLCNT_SID, 2147483646) PERSON_SID,
          NVL (PROG.ACAD_PROG_SID, 2147483646) ACAD_PROG_SID,
          NVL (PLAN.ACAD_PLAN_SID, 2147483646) ACAD_PLAN_SID,
          NVL (SPLAN.ACAD_SPLAN_SID, 2147483646) ACAD_SPLAN_SID,
--          NVL (
--             (SELECT MIN (1)
--                FROM UM_F_PRSPCT_RFRL R
--               WHERE C.PRSPCT_CAR_SID = R.PRSPCT_CAR_SID
--                 AND C.INSTITUTION_SID = A.INSTITUTION_SID
--                 AND C.ACAD_CAR_SID = A.ACAD_CAR_SID
--                 AND C.ADMIT_TERM_SID = A.ADMIT_TERM_SID
--                 AND C.PERSON_SID = A.APPLCNT_SID
--                 AND R.RFRL_DTL = 'DSP'),
--             0)
          nvl(DSP.DSP_CNT,0) PRSPCT_DSP_CNT         -- Jan 2017
     FROM UM_F_ADM_APPL_STAT A
          LEFT OUTER JOIN UM_D_PRSPCT_CAR C
             ON     A.APPLCNT_SID = C.PERSON_SID
                --       AND A.ADMIT_TERM_SID = C.ADMIT_TERM_SID
                AND A.INSTITUTION_SID = C.INSTITUTION_SID
                AND A.ACAD_CAR_SID = C.ACAD_CAR_SID
                AND A.SRC_SYS_ID = C.SRC_SYS_ID
                AND NVL (C.DATA_ORIGIN, '-') <> 'D'
          LEFT OUTER JOIN UM_D_PRSPCT_PROG PROG
             ON     PROG.PRSPCT_CAR_SID = C.PRSPCT_CAR_SID
                AND NVL (PROG.DATA_ORIGIN, '-') <> 'D'
          LEFT OUTER JOIN UM_D_PRSPCT_PLAN PLAN
             ON     PLAN.PRSPCT_PROG_SID = PROG.PRSPCT_PROG_SID
                AND NVL (PLAN.DATA_ORIGIN, '-') <> 'D'
          LEFT OUTER JOIN UM_D_PRSPCT_SBPLAN SPLAN
             ON     SPLAN.PRSPCT_PLAN_SID = PLAN.PRSPCT_PLAN_SID
                AND NVL (SPLAN.DATA_ORIGIN, '-') <> 'D'
          left outer join DSP
            on A.INSTITUTION_CD = DSP.INSTITUTION_CD
           and A.ACAD_CAR_CD = DSP.ACAD_CAR_CD
           and A.PERSON_ID = DSP.PERSON_ID
           and A.ADM_APPL_NBR = DSP.ADM_APPL_NBR
           and A.SRC_SYS_ID = DSP.SRC_SYS_ID
          ;
strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_ADM_APPL_PRSPCT_CPPS rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_ADM_APPL_PRSPCT_CPPS',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_ADM_APPL_PRSPCT_CPPS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_ADM_APPL_PRSPCT_CPPS enable constraint PK_UM_F_ADM_APPL_PRSPCT_CPPS';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_ADM_APPL_PRSPCT_CPPS');

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

END UM_F_ADM_APPL_PRSPCT_CPPS_P;
/
