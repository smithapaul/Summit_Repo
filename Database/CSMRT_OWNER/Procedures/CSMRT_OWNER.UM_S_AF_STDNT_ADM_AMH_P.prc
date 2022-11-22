DROP PROCEDURE CSMRT_OWNER.UM_S_AF_STDNT_ADM_AMH_P
/

--
-- UM_S_AF_STDNT_ADM_AMH_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_S_AF_STDNT_ADM_AMH_P"
           (
                   i_EFFDT      in  Varchar2    Default SYSDATE
           )

   IS

/******************************************************************************
   NAME:       UM_S_AF_STDNT_ADM_AMH_P
   PURPOSE:    

   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
   1.0        9/8/2021   Smitha Paul      1. Created this procedure.

   NOTES: Daily Load process to merge the amherst admissions data and 
   adjust the effective start and end date accordingly


******************************************************************************/


           strMartId                       Varchar2(50)    := 'CSW';
           strProcessName                  Varchar2(100)   := 'UM_S_AF_STDNT_ADM_AMH';
           intProcessSid                   Integer;
   		strInstance                     VARCHAR2(100);
           dtProcessStart                  Date            := SYSDATE;
           dtEFFDT                         Date            := to_date(i_EFFDT);
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
           ProcessedStatus                 Varchar2(50);
           M_LoadStatus                    Varchar2(50);
           

   BEGIN
    
   --verifying the job is not running twice on the same day and checking whether UM_S_AF_STDNT_ADM_AMH table loaded with the current day's file 
   
    select decode(count(*),0,'NOTPROCESSED','PROCESSED') into ProcessedStatus from (
    select  max(effdt_start) STARTDATE, max(effdt_end) ENDDATE  from CSMRT_OWNER.UM_S_AF_STDNT_ADM_AMH
    )
    where (STARTDATE = trunc(dtEFFDT) OR ENDDATE = trunc(dtEFFDT));

    select decode(count(*),0,'SOURCEEMPTY','SOURCELOADED') into M_LoadStatus from CSMRT_OWNER.UM_M_AF_STDNT_ADM_AMH  where trunc(created_ew_dttm) = trunc(sysdate);
   
   
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

   If  ProcessedStatus = 'NOTPROCESSED' AND M_LoadStatus = 'SOURCELOADED'   THEN

   strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_S_AF_STDNT_ADM_AMH';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
   COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_S_AF_STDNT_ADM_AMH');

   strSqlDynamic   := 'alter table CSMRT_OWNER.UM_S_AF_STDNT_ADM_AMH disable constraint PK_UM_S_AF_STDNT_ADM_AMH';
   strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
   COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                   (
                   i_SqlStatement          => strSqlDynamic,
                   i_MaxTries              => 10,
                   i_WaitSeconds           => 10,
                   o_Tries                 => intTries
                   );

   strMessage01    := 'Merging data into CSMRT_OWNER.UM_S_AF_STDNT_ADM_AMH';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

   strSqlCommand   := 'merge into CSMRT_OWNER.UM_S_AF_STDNT_ADM_AMH';
   merge into CSMRT_OWNER.UM_S_AF_STDNT_ADM_AMH T
    using (
       with Q1 as (
   select /*+ inline parallel(8) */
          INSTITUTION_CD, ACAD_CAR_CD,ACAD_PROG_CD, ACAD_PLAN_CD, ADMIT_TERM_CD, PERSON_ID, ADM_APPL_NBR,SLATE_ID, EXT_ADM_APPL_NBR,SRC_SYS_ID,
          max(EFFDT_START) MAX_EFFDT_START,
          max(EFFDT_END) MAX_EFFDT_END
     from CSMRT_OWNER.UM_S_AF_STDNT_ADM_AMH
    group by INSTITUTION_CD, ACAD_CAR_CD,ACAD_PROG_CD, ACAD_PLAN_CD, ADMIT_TERM_CD, PERSON_ID, ADM_APPL_NBR,SLATE_ID, EXT_ADM_APPL_NBR,SRC_SYS_ID),
          Q2 as (
   select max(EFFDT_END) MAX_START_DT
     from CSMRT_OWNER.UM_S_AF_STDNT_ADM_AMH)
   select /*+ inline parallel(8) */
            S.INSTITUTION_CD, S.ACAD_CAR_CD, S.ACAD_PROG_CD, 
            S.ACAD_PLAN_CD, S.ADMIT_TERM_CD, S.PERSON_ID, 
            S.ADM_APPL_NBR, S.SLATE_ID, S.EXT_ADM_APPL_NBR, 
            S.SRC_SYS_ID, --S.EFFDT_START, S.EFFDT_END, 
            S.INSTITUTION_LD, S.ACAD_CAR_LD, S.ACAD_PROG_LD, 
            S.ACAD_PLAN_LD, S.ADMIT_TERM_LD, S.REPORTING_TERM_CD, 
            S.REPORTING_TERM_LD, S.ACAD_YR, S.FISCAL_YR, 
            S.ACAD_ORG_CD, S.ACAD_ORG_LD, S.ADMIT_TYPE_ID, 
            S.ADMIT_TYPE_LD, S.ADMIT_TYPE_GRP, S.APPL_CNTR_ID, 
            S.CE_APPL_FLG, S.EDU_LVL_CD, S.EDU_LVL_LD, 
            S.IS_RSDNCY_FLG, S.PLAN_CIP_CD, S.PLAN_CIP_LD, 
            S.RSDNCY_ID, S.RSDNCY_LD, S.APPL_CNT, 
            S.ADMIT_CNT, S.DENY_CNT, S.DEPOSIT_CNT, 
            S.ENROLL_CNT, S.ENROLL_SUBSEQ_CNT, S.UNDUP_CNT, 
            nvl(Q1.MAX_EFFDT_START,trunc(dtEFFDT)) MAX_EFFDT_START, nvl(Q1.MAX_EFFDT_END,trunc(dtEFFDT)) MAX_EFFDT_END, Q2.MAX_START_DT
from CSMRT_OWNER.UM_M_AF_STDNT_ADM_AMH S
     left outer join Q1
       on S.INSTITUTION_CD = Q1.INSTITUTION_CD
      and S.ACAD_CAR_CD = Q1.ACAD_CAR_CD
      and S.ACAD_PROG_CD = Q1.ACAD_PROG_CD
      and S.ACAD_PLAN_CD = Q1.ACAD_PLAN_CD
      and S.ADMIT_TERM_CD = Q1.ADMIT_TERM_CD
      and S.PERSON_ID = Q1.PERSON_ID
      and S.ADM_APPL_NBR = Q1.ADM_APPL_NBR
      and S.SLATE_ID = Q1.SLATE_ID
      and S.EXT_ADM_APPL_NBR = Q1.EXT_ADM_APPL_NBR
      and S.SRC_SYS_ID = Q1.SRC_SYS_ID
     join Q2
       on 1 = 1) S
        /* Formatted on 9/8/2021 2:06:53 PM (QP5 v5.354) */
        ON (nvl(trim(T.INSTITUTION_CD),'-') = nvl(trim(S.INSTITUTION_CD),'-')
        AND  nvl(trim(T.ACAD_CAR_CD),'-') = nvl(trim(S.ACAD_CAR_CD),'-')
        AND  nvl(trim(T.ACAD_PROG_CD),'-') = nvl(trim(S.ACAD_PROG_CD),'-')
        AND  nvl(trim(T.ACAD_PLAN_CD),'-') = nvl(trim(S.ACAD_PLAN_CD),'-')
        AND  nvl(trim(T.ADMIT_TERM_CD),'-') = nvl(trim(S.ADMIT_TERM_CD),'-')
        AND  nvl(trim(T.PERSON_ID),'-') = nvl(trim(S.PERSON_ID),'-')
        AND  nvl(trim(T.ADM_APPL_NBR),'-') = nvl(trim(S.ADM_APPL_NBR),'-')
        AND  nvl(trim(T.SLATE_ID),'-') = nvl(trim(S.SLATE_ID),'-')
        AND  nvl(trim(T.EXT_ADM_APPL_NBR),'-') = nvl(trim(S.EXT_ADM_APPL_NBR),'-')
        AND  nvl(trim(T.SRC_SYS_ID),'-') = nvl(trim(S.SRC_SYS_ID),'-')
        AND  nvl(trim(T.EFFDT_START),to_date('01-JAN-1900')) = nvl(trim(S.MAX_EFFDT_START),to_date('01-JAN-1900'))
        AND  nvl(trim(S.MAX_EFFDT_END),to_date('01-JAN-1900')) = nvl(trim(S.MAX_START_DT),to_date('01-JAN-1900'))     -- FOR gap problem
        AND  nvl(trim(T.INSTITUTION_LD),'-') = nvl(trim(S.INSTITUTION_LD),'-') 
        AND  nvl(trim(T.ACAD_CAR_LD),'-') = nvl(trim(S.ACAD_CAR_LD),'-')
        AND  nvl(trim(T.ACAD_PROG_LD),'-') = nvl(trim(S.ACAD_PROG_LD),'-')
        AND  nvl(trim(T.ACAD_PLAN_LD),'-') = nvl(trim(S.ACAD_PLAN_LD),'-')
        AND  nvl(trim(T.ADMIT_TERM_LD),'-') = nvl(trim(S.ADMIT_TERM_LD),'-')
        AND  nvl(trim(T.REPORTING_TERM_CD),'-') = nvl(trim(S.REPORTING_TERM_CD),'-')
        AND  nvl(trim(T.REPORTING_TERM_LD),'-') = nvl(trim(S.REPORTING_TERM_LD),'-')
        AND  nvl(trim(T.ACAD_YR),'-') = nvl(trim(S.ACAD_YR),'-')
        AND  nvl(trim(T.FISCAL_YR),'-') = nvl(trim(S.FISCAL_YR),'-')
        AND  nvl(trim(T.ACAD_ORG_CD),'-') = nvl(trim(S.ACAD_ORG_CD),'-')
        AND  nvl(trim(T.ACAD_ORG_LD),'-') = nvl(trim(S.ACAD_ORG_LD),'-')
        AND  nvl(trim(T.ADMIT_TYPE_ID),'-') = nvl(trim(S.ADMIT_TYPE_ID),'-')
        AND  nvl(trim(T.ADMIT_TYPE_LD),'-') = nvl(trim(S.ADMIT_TYPE_LD),'-')
        AND  nvl(trim(T.ADMIT_TYPE_GRP),'-') = nvl(trim(S.ADMIT_TYPE_GRP),'-')
        AND  nvl(trim(T.APPL_CNTR_ID),'-') = nvl(trim(S.APPL_CNTR_ID),'-')
        AND  nvl(trim(T.CE_APPL_FLG),'-') = nvl(trim(S.CE_APPL_FLG),'-')
        AND  nvl(trim(T.EDU_LVL_CD),'-') = nvl(trim(S.EDU_LVL_CD),'-')
        AND  nvl(trim(T.EDU_LVL_LD),'-') = nvl(trim(S.EDU_LVL_LD),'-')  
        AND  nvl(trim(T.IS_RSDNCY_FLG),'-') = nvl(trim(S.IS_RSDNCY_FLG),'-')  
        AND  nvl(trim(T.PLAN_CIP_CD),'-') = nvl(trim(S.PLAN_CIP_CD),'-')  
        AND  nvl(trim(T.PLAN_CIP_LD),'-') = nvl(trim(S.PLAN_CIP_LD),'-')  
        AND  nvl(trim(T.RSDNCY_ID),'-') = nvl(trim(S.RSDNCY_ID),'-')  
        AND  nvl(trim(T.RSDNCY_LD),'-') = nvl(trim(S.RSDNCY_LD),'-')  
        AND  nvl(round(T.APPL_CNT,9),0) = nvl(round(S.APPL_CNT,9),0)
        AND  nvl(round(T.ADMIT_CNT,9),0) = nvl(round(S.ADMIT_CNT,9),0)
        AND  nvl(round(T.DENY_CNT,9),0) = nvl(round(S.DENY_CNT,9),0)
        AND  nvl(round(T.DEPOSIT_CNT,9),0) = nvl(round(S.DEPOSIT_CNT,9),0)
        AND  nvl(round(T.ENROLL_CNT,9),0) = nvl(round(S.ENROLL_CNT,9),0)
        AND  nvl(round(T.ENROLL_SUBSEQ_CNT,9),0) = nvl(round(S.ENROLL_SUBSEQ_CNT,9),0)
        AND  nvl(round(T.UNDUP_CNT,9),0) = nvl(round(S.UNDUP_CNT,9),0)
   )
    when matched then update set
          T.EFFDT_END = TRUNC(dtEFFDT),
          T.LASTUPD_EW_DTTM = SYSDATE
    where S.MAX_EFFDT_START = T.EFFDT_START    -- Only update the latest row
    when not matched then
   insert (
            T.INSTITUTION_CD, 
            T.ACAD_CAR_CD, 
            T.ACAD_PROG_CD, 
            T.ACAD_PLAN_CD, 
            T.ADMIT_TERM_CD, 
            T.PERSON_ID, 
            T.ADM_APPL_NBR, 
            T.SLATE_ID, 
            T.EXT_ADM_APPL_NBR, 
            T.SRC_SYS_ID, 
            T.EFFDT_START, 
            T.EFFDT_END, 
            T.INSTITUTION_LD, 
            T.ACAD_CAR_LD, 
            T.ACAD_PROG_LD, 
            T.ACAD_PLAN_LD, 
            T.ADMIT_TERM_LD, 
            T.REPORTING_TERM_CD, 
            T.REPORTING_TERM_LD, 
            T.ACAD_YR, 
            T.FISCAL_YR, 
            T.ACAD_ORG_CD, 
            T.ACAD_ORG_LD, 
            T.ADMIT_TYPE_ID, 
            T.ADMIT_TYPE_LD, 
            T.ADMIT_TYPE_GRP, 
            T.APPL_CNTR_ID, 
            T.CE_APPL_FLG, 
            T.EDU_LVL_CD, 
            T.EDU_LVL_LD, 
            T.IS_RSDNCY_FLG, 
            T.PLAN_CIP_CD, 
            T.PLAN_CIP_LD, 
            T.RSDNCY_ID, 
            T.RSDNCY_LD, 
            T.APPL_CNT, 
            T.ADMIT_CNT, 
            T.DENY_CNT, 
            T.DEPOSIT_CNT, 
            T.ENROLL_CNT, 
            T.ENROLL_SUBSEQ_CNT, 
            T.UNDUP_CNT, 
            T.CREATED_EW_DTTM,
            T.LASTUPD_EW_DTTM
          )
   values (
            S.INSTITUTION_CD, 
            S.ACAD_CAR_CD, 
            S.ACAD_PROG_CD, 
            S.ACAD_PLAN_CD, 
            S.ADMIT_TERM_CD, 
            S.PERSON_ID, 
            S.ADM_APPL_NBR, 
            S.SLATE_ID, 
            S.EXT_ADM_APPL_NBR, 
            S.SRC_SYS_ID, 
            trunc(dtEFFDT),                    -- EFFDT_START,
            trunc(dtEFFDT),                    -- EFFDT_END,
            S.INSTITUTION_LD, 
            S.ACAD_CAR_LD, 
            S.ACAD_PROG_LD, 
            S.ACAD_PLAN_LD, 
            S.ADMIT_TERM_LD, 
            S.REPORTING_TERM_CD, 
            S.REPORTING_TERM_LD, 
            S.ACAD_YR, 
            S.FISCAL_YR, 
            S.ACAD_ORG_CD, 
            S.ACAD_ORG_LD, 
            S.ADMIT_TYPE_ID, 
            S.ADMIT_TYPE_LD, 
            S.ADMIT_TYPE_GRP, 
            S.APPL_CNTR_ID, 
            S.CE_APPL_FLG, 
            S.EDU_LVL_CD, 
            S.EDU_LVL_LD, 
            S.IS_RSDNCY_FLG, 
            S.PLAN_CIP_CD, 
            S.PLAN_CIP_LD, 
            S.RSDNCY_ID, 
            S.RSDNCY_LD, 
            S.APPL_CNT, 
            S.ADMIT_CNT, 
            S.DENY_CNT, 
            S.DEPOSIT_CNT, 
            S.ENROLL_CNT, 
            S.ENROLL_SUBSEQ_CNT, 
            S.UNDUP_CNT, 
            SYSDATE,                 -- CREATED_EW_DTTM
            SYSDATE                  -- LASTUPD_EW_DTTM
   )
   ;

   strSqlCommand   := 'SET intRowCount';
   intRowCount     := SQL%ROWCOUNT;

   strSqlCommand := 'commit';
   commit;

   strMessage01    := '# of UM_S_AF_STDNT_ADM_AMH rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
           (
                   i_TargetTableName   => 'UM_S_AF_STDNT_ADM_AMH',
                   i_Action            => 'MERGE',
                   i_RowCount          => intRowCount
           );

   strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_S_AF_STDNT_ADM_AMH';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

   strSqlDynamic   := 'alter table CSMRT_OWNER.UM_S_AF_STDNT_ADM_AMH enable constraint PK_UM_S_AF_STDNT_ADM_AMH';
   strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
   COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                   (
                   i_SqlStatement          => strSqlDynamic,
                   i_MaxTries              => 10,
                   i_WaitSeconds           => 10,
                   o_Tries                 => intTries
                   );

   COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_S_AF_STDNT_ADM_AMH');

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
           (
                   i_TargetTableName   => 'UM_S_AF_STDNT_ADM_AMH',
                   i_Action            => 'UPDATE',
                   i_RowCount          => intRowCount
           );

   strMessage01    := 'Updating data in CSMRT_OWNER.UM_S_AF_STDNT_ADM_AMH to prevent gaps';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

   strSqlCommand   := 'update CSMRT_OWNER.UM_S_AF_STDNT_ADM_AMH';
   update CSMRT_OWNER.UM_S_AF_STDNT_ADM_AMH T
      set T.EFFDT_END = (trunc(dtEFFDT-1)),
          T.LASTUPD_EW_DTTM = sysdate
    where T.EFFDT_START < trunc(dtEFFDT)
      and T.EFFDT_END between (select max(EFFDT_END) MAX_EFFDT_END
                                 from CSMRT_OWNER.UM_S_AF_STDNT_ADM_AMH        -- EFFDT_END prior to the preceding merge.
                                where EFFDT_END < trunc(dtEFFDT))
                          and (trunc(dtEFFDT-1))
   ;

   strSqlCommand   := 'SET intRowCount';
   intRowCount     := SQL%ROWCOUNT;

   strSqlCommand := 'commit';
   commit;

   strMessage01    := '# of UM_S_AF_STDNT_ADM_AMH rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
           (
                   i_TargetTableName   => 'UM_S_AF_STDNT_ADM_AMH',
                   i_Action            => 'UPDATE',
                   i_RowCount          => intRowCount
           );

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

   strMessage01    := strProcessName || ' is complete, Processed Status: ' || ProcessedStatus || ' and M Load Status: '|| M_LoadStatus ;
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
  
  
ELSE

   strMessage01    := 'UM_S_AF_STDNT_ADM_AMH process is not eligible to proceed. Processed Status: ' || ProcessedStatus || ' and M Load Status: '|| M_LoadStatus ;
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

End If;

   EXCEPTION
       WHEN OTHERS THEN
           COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_EXCEPTION
                   (
                           i_SqlCommand   => strSqlCommand,
                           i_SqlCode      => SQLCODE,
                           i_SqlErrm      => SQLERRM
                   );

   END UM_S_AF_STDNT_ADM_AMH_P;
/
