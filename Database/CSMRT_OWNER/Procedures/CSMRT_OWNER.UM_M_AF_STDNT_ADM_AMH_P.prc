DROP PROCEDURE CSMRT_OWNER.UM_M_AF_STDNT_ADM_AMH_P
/

--
-- UM_M_AF_STDNT_ADM_AMH_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_M_AF_STDNT_ADM_AMH_P" AUTHID CURRENT_USER IS

   ------------------------------------------------------------------------
   -- Kieu Tran
   --
   -- Loads table UM_M_AF_STDNT_ADM_AMH_P.
   --
   --V01  Case - xxxx 07/1212021,    Kieu Tran
   --                                New Process
   --
   ------------------------------------------------------------------------

           strMartId                       Varchar2(50)    := 'CSW';
           strProcessName                  Varchar2(100)   := 'UM_M_AF_STDNT_ADM_AMH';
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

   strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_M_AF_STDNT_ADM_AMH';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
   COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_M_AF_STDNT_ADM_AMH');

   strSqlDynamic   := 'alter table CSMRT_OWNER.UM_M_AF_STDNT_ADM_AMH disable constraint PK_UM_M_AF_STDNT_ADM_AMH';
   strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
   COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                   (
                   i_SqlStatement          => strSqlDynamic,
                   i_MaxTries              => 10,
                   i_WaitSeconds           => 10,
                   o_Tries                 => intTries
                   );

   strMessage01    := 'Truncating table CSMRT_OWNER.UM_M_AF_STDNT_ADM_AMH';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

   strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_M_AF_STDNT_ADM_AMH';
   strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
   COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                   (
                   i_SqlStatement          => strSqlDynamic,
                   i_MaxTries              => 10,
                   i_WaitSeconds           => 10,
                   o_Tries                 => intTries
                   );

   strMessage01    := 'Inserting data into CSMRT_OWNER.UM_M_AF_STDNT_ADM_AMH';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

   strSqlCommand   := 'insert into CSMRT_OWNER.UM_M_AF_STDNT_ADM_AMH';
   insert /*+ append */ into CSMRT_OWNER.UM_M_AF_STDNT_ADM_AMH
   select 
        INSTITUTION_CD,
        ACAD_CAR_CD,
        ACAD_PROG_CD,
        ACAD_PLAN_CD,
        ADMIT_TERM_CD,
        PERSON_ID,
        ADM_APPL_NBR,
        SLATE_ID,
        EXT_ADM_APPL_NBR,
        'CS90' AS SRC_SYS_ID,
        INSTITUTION_LD,
        ACAD_CAR_LD,
        ACAD_PROG_LD,
        ACAD_PLAN_LD,
        ADMIT_TERM_LD,
        REPORTING_TERM_CD,
        REPORTING_TERM_LD,
        ACAD_YR,
        FISCAL_YR,
        '-' AS ACAD_ORG_CD,
        '-' AS ACAD_ORG_LD,
        ADMIT_TYPE_ID,
        ADMIT_TYPE_LD,
        ADMIT_TYPE_GRP,
        APPL_CNTR_ID,
        CE_APPL_FLG,
        EDU_LVL_CD,
        EDU_LVL_LD,
        IS_RSDNCY_FLG,
        PLAN_CIP_CD,
        PLAN_CIP_LD,
        RSDNCY_ID,
        RSDNCY_LD,
        APPL_CNT,
        ADMIT_CNT,
        DENY_CNT,
        DEPOSIT_CNT,
        ENROLL_CNT,
        ENROLL_SUBSEQ_CNT,
        DECODE(ROW_NUMBER() OVER (PARTITION BY REPLACE(ADM_APPL_NBR,'-',SLATE_ID) ORDER BY ENROLL_CNT DESC, ADMIT_CNT DESC, ACAD_PLAN_CD),1,1,0) UNDUP_CNT,        
        SYSDATE CREATED_EW_DTTM
  from CSSTG_OWNER.UM_AF_AMH_STUDENT_ADM_S2
   ;

   strSqlCommand   := 'SET intRowCount';
   intRowCount     := SQL%ROWCOUNT;

   strSqlCommand := 'commit';
   commit;

   strMessage01    := '# of UM_M_AF_STDNT_ENRL_AMH rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
           (
                   i_TargetTableName   => 'UM_M_AF_STDNT_ADM_AMH',
                   i_Action            => 'INSERT',
                   i_RowCount          => intRowCount
           );

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
           (
                   i_TargetTableName   => 'UM_M_AF_STDNT_ADM_AMH',
                   i_Action            => 'UPDATE',
                   i_RowCount          => intRowCount
           );

   strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_M_AF_STDNT_ADM_AMH';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

   strSqlDynamic   := 'alter table CSMRT_OWNER.UM_M_AF_STDNT_ADM_AMH enable constraint PK_UM_M_AF_STDNT_ADM_AMH';
   strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
   COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                   (
                   i_SqlStatement          => strSqlDynamic,
                   i_MaxTries              => 10,
                   i_WaitSeconds           => 10,
                   o_Tries                 => intTries
                   );

   COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_M_AF_STDNT_ADM_AMH');

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

   END UM_M_AF_STDNT_ADM_AMH_P;
/
