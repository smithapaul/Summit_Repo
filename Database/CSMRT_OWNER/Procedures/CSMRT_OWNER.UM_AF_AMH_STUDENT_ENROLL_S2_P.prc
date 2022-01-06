CREATE OR REPLACE PROCEDURE             UM_AF_AMH_STUDENT_ENROLL_S2_P AUTHID CURRENT_USER IS

   ------------------------------------------------------------------------
   -- George Adams
   --
   -- Loads stage table UM_AF_AMH_STUDENT_ENROLL_S2.
   --
   -- V01  SMT-xxxx 03/29/2017,    George Adams
   --
   ------------------------------------------------------------------------

           strMartId                       Varchar2(50)    := 'CSW';
           strProcessName                  Varchar2(100)   := 'UM_AF_AMH_STUDENT_ENROLL_S2_P';
           str_TargetTableName             Varchar2(100)   := 'UM_AF_AMH_STUDENT_ENROLL_S2';
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

   strMessage01    := 'Disabling Indexes for table CSSTG_OWNER.UM_AF_AMH_STUDENT_ENROLL_S2';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
   COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSSTG_OWNER','UM_AF_AMH_STUDENT_ENROLL_S2');

   strSqlDynamic   := 'alter table CSSTG_OWNER.UM_AF_AMH_STUDENT_ENROLL_S2 disable constraint PK_UM_AF_AMH_STUDENT_ENROLL_S2';
   strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
   COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                   (
                   i_SqlStatement          => strSqlDynamic,
                   i_MaxTries              => 10,
                   i_WaitSeconds           => 10,
                   o_Tries                 => intTries
                   );

   strMessage01    := 'Truncating table CSSTG_OWNER.UM_AF_AMH_STUDENT_ENROLL_S2';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

   strSqlDynamic   := 'truncate table CSSTG_OWNER.UM_AF_AMH_STUDENT_ENROLL_S2';
   strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
   COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                   (
                   i_SqlStatement          => strSqlDynamic,
                   i_MaxTries              => 10,
                   i_WaitSeconds           => 10,
                   o_Tries                 => intTries
                   );

   strMessage01    := 'Inserting data into CSSTG_OWNER.UM_AF_AMH_STUDENT_ENROLL_S2';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

   strSqlCommand   := 'insert into CSSTG_OWNER.UM_AF_AMH_STUDENT_ENROLL_S2';
   insert /*+ append */ into CSSTG_OWNER.UM_AF_AMH_STUDENT_ENROLL_S2
   with Q1 as (
   select COLUMN_001 INSTITUTION_CD,
          to_char(replace(COLUMN_002,CHR(9),'')) INSTITUTION_LD,   -- Need replace for any columns???
   --       to_date(COLUMN_003,'DD-MON-YYYY') EFFDT,
          COLUMN_003 TERM_CD,
          COLUMN_004 TERM_LD,
          COLUMN_005 ACAD_YR,
          COLUMN_006 AID_YEAR,
          COLUMN_007 PERSON_ID,
          COLUMN_008 ACAD_CAR_CD,
          COLUMN_009 ACAD_CAR_LD,
          COLUMN_010 CE_ONLY_FLG,
   --       COLUMN_011 ACAD_ORG_CD,
   --       COLUMN_012 ACAD_ORG_LD,
          COLUMN_011 ACAD_PROG_CD,
          COLUMN_012 ACAD_PROG_LD,
          COLUMN_013 PROG_CIP_CD,
          COLUMN_014 PROG_CIP_LD,
          COLUMN_015 ACAD_PLAN_CD,
          COLUMN_016 ACAD_PLAN_LD,
          COLUMN_017 PLAN_CIP_CD,
          COLUMN_018 PLAN_CIP_LD,
          COLUMN_019 NEW_CONT_IND,
          COLUMN_020 RSDNCY_ID,
          COLUMN_021 RSDNCY_LD,
          COLUMN_022 IS_RSDNCY_FLG,
          COLUMN_023 ONLINE_ONLY_FLG,
          COLUMN_024 ONLINE_HYBRID_FLG,
          to_number(COLUMN_025) TOT_FTE,
          to_number(COLUMN_026) ONLINE_FTE,
          to_number(COLUMN_027) TOT_CREDITS,
          to_number(COLUMN_028) ONLINE_CREDITS,
          to_number(COLUMN_029) NON_ONLINE_CREDITS,
          to_number(COLUMN_030) CE_CREDITS,
          to_number(COLUMN_031) NON_CE_CREDITS,
          to_number(COLUMN_032) ENROLL_CNT,
          to_number(COLUMN_033) ONLINE_CNT,
          to_number(COLUMN_034) CE_CNT,
          COLUMN_035 ACAD_ORG_CD,
          COLUMN_036 ACAD_ORG_LD,
          COLUMN_037 ACAD_LEVEL_BOT,       -- Jan 2021
          SYSDATE CREATED_EW_DTTM
     from COMMON_OWNER.UPLOAD_S1_VW        -- Reads from appropriate database with DB link
    where UPLOAD_ID = 'STUDENT_ENROLL_AMH'
      and COLUMN_001 = 'UMAMH')
   select INSTITUTION_CD, INSTITUTION_LD, TERM_CD, TERM_LD, ACAD_YR, AID_YEAR, PERSON_ID,
          ACAD_CAR_CD, ACAD_CAR_LD, CE_ONLY_FLG, ACAD_ORG_CD, ACAD_ORG_LD, ACAD_LEVEL_BOT,         -- Jan 2021
          ACAD_PROG_CD, ACAD_PROG_LD, PROG_CIP_CD, PROG_CIP_LD, ACAD_PLAN_CD, ACAD_PLAN_LD, PLAN_CIP_CD, PLAN_CIP_LD,
          NEW_CONT_IND, RSDNCY_ID, RSDNCY_LD, IS_RSDNCY_FLG, ONLINE_ONLY_FLG, ONLINE_HYBRID_FLG,
          TOT_FTE, ONLINE_FTE, TOT_CREDITS, ONLINE_CREDITS, NON_ONLINE_CREDITS, CE_CREDITS, NON_CE_CREDITS, ENROLL_CNT, ONLINE_CNT, CE_CNT,
          CREATED_EW_DTTM
     from Q1
   ;

   strSqlCommand   := 'SET intRowCount';
   intRowCount     := SQL%ROWCOUNT;

   strSqlCommand := 'commit';
   commit;

   strMessage01    := '# of UM_AF_AMH_STUDENT_ENROLL_S2 rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
           (
                   i_TargetTableName   => 'UM_AF_AMH_STUDENT_ENROLL_S2',
                   i_Action            => 'INSERT',
                   i_RowCount          => intRowCount
           );

   strMessage01    := 'Enabling Indexes for table CSSTG_OWNER.UM_AF_AMH_STUDENT_ENROLL_S2';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

   strSqlDynamic   := 'alter table CSSTG_OWNER.UM_AF_AMH_STUDENT_ENROLL_S2 enable constraint PK_UM_AF_AMH_STUDENT_ENROLL_S2';
   strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
   COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                   (
                   i_SqlStatement          => strSqlDynamic,
                   i_MaxTries              => 10,
                   i_WaitSeconds           => 10,
                   o_Tries                 => intTries
                   );

   COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSSTG_OWNER','UM_AF_AMH_STUDENT_ENROLL_S2');

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

   END UM_AF_AMH_STUDENT_ENROLL_S2_P;
/
