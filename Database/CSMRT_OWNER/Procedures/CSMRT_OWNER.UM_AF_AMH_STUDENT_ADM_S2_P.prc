CREATE OR REPLACE PROCEDURE             UM_AF_AMH_STUDENT_ADM_S2_P AUTHID CURRENT_USER IS

   ------------------------------------------------------------------------
   -- Kieu Tran
   --
   -- Loads stage table UM_AF_AMH_STUDENT_ADM_S2.
   --
   -- V01  SMT-xxxx 07/14/2021,    Kieu Tran
   --
   ------------------------------------------------------------------------

           strMartId                       Varchar2(50)    := 'CSW';
           strProcessName                  Varchar2(100)   := 'UM_AF_AMH_STUDENT_ADM_S2_P';
           str_TargetTableName             Varchar2(100)   := 'UM_AF_AMH_STUDENT_ADM_S2';
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

   strMessage01    := 'Disabling Indexes for table CSSTG_OWNER.UM_AF_AMH_STUDENT_ADM_S2';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
   COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSSTG_OWNER','UM_AF_AMH_STUDENT_ADM_S2');

   strSqlDynamic   := 'alter table CSSTG_OWNER.UM_AF_AMH_STUDENT_ADM_S2 disable constraint PK_UM_AF_AMH_STUDENT_ADM_S2';
   strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
   COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                   (
                   i_SqlStatement          => strSqlDynamic,
                   i_MaxTries              => 10,
                   i_WaitSeconds           => 10,
                   o_Tries                 => intTries
                   );

   strMessage01    := 'Truncating table CSSTG_OWNER.UM_AF_AMH_STUDENT_ADM_S2';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

   strSqlDynamic   := 'truncate table CSSTG_OWNER.UM_AF_AMH_STUDENT_ADM_S2';
   strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
   COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                   (
                   i_SqlStatement          => strSqlDynamic,
                   i_MaxTries              => 10,
                   i_WaitSeconds           => 10,
                   o_Tries                 => intTries
                   );

   strMessage01    := 'Inserting data into CSSTG_OWNER.UM_AF_AMH_STUDENT_ADM_S2';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

   strSqlCommand   := 'insert into CSSTG_OWNER.UM_AF_AMH_STUDENT_ADM_S2';
   insert /*+ append */ into CSSTG_OWNER.UM_AF_AMH_STUDENT_ADM_S2
   with Q1 as (
   select NVL(TRIM(COLUMN_001), '-') INSTITUTION_CD,
          to_char(replace(COLUMN_002,CHR(9),'')) INSTITUTION_LD,
          NVL(TRIM(COLUMN_003), '-') ADMIT_TERM_CD,
          COLUMN_004 ADMIT_TERM_LD,
          COLUMN_005 REPORTING_TERM_CD,
          COLUMN_006 REPORTING_TERM_LD,
          COLUMN_007 ACAD_YR,
          COLUMN_008 FISCAL_YR,
          NVL(TRIM(COLUMN_009), '-') PERSON_ID,
          NVL(TRIM(COLUMN_010), '-') ADM_APPL_NBR,
          NVL(TRIM(COLUMN_011), '-') SLATE_ID,
          NVL(TRIM(COLUMN_012), '-') EXT_ADM_APPL_NBR,
          COLUMN_013 APPL_CNTR_ID,
          NVL(TRIM(COLUMN_014), '-') ACAD_CAR_CD,
          COLUMN_015 ACAD_CAR_LD,
          COLUMN_016 CE_APPL_FLG,
          COLUMN_017 ADMIT_TYPE_ID,
          COLUMN_018 ADMIT_TYPE_LD,
          COLUMN_019 ADMIT_TYPE_GRP,
          NVL(TRIM(COLUMN_020), '-') ACAD_PROG_CD,
          COLUMN_021 ACAD_PROG_LD,
          NVL(TRIM(COLUMN_022), '-') ACAD_PLAN_CD,
          COLUMN_023 ACAD_PLAN_LD,
          COLUMN_024 PLAN_CIP_CD,
          COLUMN_025 PLAN_CIP_LD,
          COLUMN_026 EDU_LVL_CD,
          COLUMN_027 EDU_LVL_LD,
          COLUMN_028 RSDNCY_ID,
          COLUMN_029 RSDNCY_LD,
          COLUMN_030 IS_RSDNCY_FLG,
          to_number(COLUMN_031) APPL_CNT,
          to_number(COLUMN_032) ADMIT_CNT,
          to_number(COLUMN_033) DENY_CNT,
          to_number(COLUMN_034) DEPOSIT_CNT,
          to_number(COLUMN_035) ENROLL_CNT,
          to_number(COLUMN_036) ENROLL_SUBSEQ_CNT,
          SYSDATE CREATED_EW_DTTM
     from COMMON_OWNER.UPLOAD_S1_VW@SMTPROD        -- Reads from apprpriate database with DB link
    where UPLOAD_ID = 'STUDENT_ADMISSIONS_AMH'
      and COLUMN_001 = 'UMAMH'
      and RECORD_NUMBER > 1)
   select 
    INSTITUTION_CD,
    INSTITUTION_LD,
    ADMIT_TERM_CD,
    ADMIT_TERM_LD,
    REPORTING_TERM_CD,
    REPORTING_TERM_LD,
    ACAD_YR,
    FISCAL_YR,
    PERSON_ID,
    ADM_APPL_NBR,
    SLATE_ID,
    EXT_ADM_APPL_NBR,
    APPL_CNTR_ID,
    ACAD_CAR_CD,
    ACAD_CAR_LD,
    CE_APPL_FLG,
    ADMIT_TYPE_ID,
    ADMIT_TYPE_LD,
    ADMIT_TYPE_GRP,
    ACAD_PROG_CD,
    ACAD_PROG_LD,
    ACAD_PLAN_CD,
    ACAD_PLAN_LD,
    PLAN_CIP_CD,
    PLAN_CIP_LD,
    EDU_LVL_CD,
    EDU_LVL_LD,
    RSDNCY_ID,
    RSDNCY_LD,
    IS_RSDNCY_FLG,
    APPL_CNT,
    ADMIT_CNT,
    DENY_CNT,
    DEPOSIT_CNT,
    ENROLL_CNT,
    ENROLL_SUBSEQ_CNT,
    CREATED_EW_DTTM
  from Q1
   ;

   strSqlCommand   := 'SET intRowCount';
   intRowCount     := SQL%ROWCOUNT;

   strSqlCommand := 'commit';
   commit;

   strMessage01    := '# of UM_AF_AMH_STUDENT_ADM_S2 rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
           (
                   i_TargetTableName   => 'UM_AF_AMH_STUDENT_ADM_S2',
                   i_Action            => 'INSERT',
                   i_RowCount          => intRowCount
           );

   strMessage01    := 'Enabling Indexes for table CSSTG_OWNER.UM_AF_AMH_STUDENT_ADM_S2';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

   strSqlDynamic   := 'alter table CSSTG_OWNER.UM_AF_AMH_STUDENT_ADM_S2 enable constraint PK_UM_AF_AMH_STUDENT_ADM_S2';
   strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
   COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                   (
                   i_SqlStatement          => strSqlDynamic,
                   i_MaxTries              => 10,
                   i_WaitSeconds           => 10,
                   o_Tries                 => intTries
                   );

   COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSSTG_OWNER','UM_AF_AMH_STUDENT_ADM_S2');

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

   END UM_AF_AMH_STUDENT_ADM_S2_P;
/
