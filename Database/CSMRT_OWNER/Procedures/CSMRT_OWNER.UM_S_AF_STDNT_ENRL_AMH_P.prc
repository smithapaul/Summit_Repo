DROP PROCEDURE CSMRT_OWNER.UM_S_AF_STDNT_ENRL_AMH_P
/

--
-- UM_S_AF_STDNT_ENRL_AMH_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_S_AF_STDNT_ENRL_AMH_P"
           (
                   i_EFFDT      in  Varchar2    Default SYSDATE
           )

   IS

   ------------------------------------------------------------------------
   -- James Doucette
   --
   -- Loads table UM_S_AF_STDNT_ENRL_AMH.
   --
    --V01  SMT-xxxx 09/10/2018,    James Doucette
   --                              New Process
   ------------------------------------------------------------------------

           strMartId                       Varchar2(50)    := 'CSW';
           strProcessName                  Varchar2(100)   := 'UM_S_AF_STDNT_ENRL_AMH';
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
    
   --verifying the job is not running twice on the same day and checking whether UM_M_AF_STDNT_ENRL_AMH table loaded with the current day's file 
   
    select decode(count(*),0,'NOTPROCESSED','PROCESSED') into ProcessedStatus from (
    select  max(effdt_start) STARTDATE, max(effdt_end) ENDDATE  from CSMRT_OWNER.UM_S_AF_STDNT_ENRL_AMH
    )
    where (STARTDATE = trunc(sysdate) OR ENDDATE = trunc(sysdate));

    select decode(count(*),0,'SOURCEEMPTY','SOURCELOADED') into M_LoadStatus from UM_M_AF_STDNT_ENRL_AMH  where trunc(created_ew_dttm) = trunc(sysdate);
   
   
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

   strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_S_AF_STDNT_ENRL_AMH';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
   COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_S_AF_STDNT_ENRL_AMH');

   strSqlDynamic   := 'alter table CSMRT_OWNER.UM_S_AF_STDNT_ENRL_AMH disable constraint PK_UM_S_AF_STDNT_ENRL_AMH';
   strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
   COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                   (
                   i_SqlStatement          => strSqlDynamic,
                   i_MaxTries              => 10,
                   i_WaitSeconds           => 10,
                   o_Tries                 => intTries
                   );

   strMessage01    := 'Merging data into CSMRT_OWNER.UM_S_AF_STDNT_ENRL_AMH';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

   strSqlCommand   := 'merge into CSMRT_OWNER.UM_S_AF_STDNT_ENRL_AMH';
   merge into CSMRT_OWNER.UM_S_AF_STDNT_ENRL_AMH T
    using (
     with Q1 as (
   select /*+ inline parallel(8) */
          INSTITUTION_CD, ACAD_CAR_CD, TERM_CD, PERSON_ID, SRC_SYS_ID,
          max(EFFDT_START) MAX_EFFDT_START,
          max(EFFDT_END) MAX_EFFDT_END
     from CSMRT_OWNER.UM_S_AF_STDNT_ENRL_AMH
    group by INSTITUTION_CD, ACAD_CAR_CD, TERM_CD, PERSON_ID, SRC_SYS_ID),
          Q2 as (
   select max(EFFDT_END) MAX_START_DT
     from CSMRT_OWNER.UM_S_AF_STDNT_ENRL_AMH)
   select /*+ inline parallel(8) */
          S.INSTITUTION_CD, S.ACAD_CAR_CD, S.TERM_CD, S.PERSON_ID, S.SRC_SYS_ID,
          nvl(Q1.MAX_EFFDT_START,trunc(SYSDATE)) MAX_EFFDT_START, nvl(Q1.MAX_EFFDT_END,trunc(SYSDATE)) MAX_EFFDT_END, Q2.MAX_START_DT,
          ACAD_YR, AID_YEAR, TERM_LD, ACAD_ORG_CD, ACAD_ORG_LD, ACAD_PROG_CD, ACAD_LEVEL_BOT,      -- Jan 2021
          ACAD_PROG_LD, PROG_CIP_CD, ACAD_PLAN_CD, ACAD_PLAN_LD, PLAN_CIP_CD,
          CE_ONLY_FLG, NEW_CONT_IND, ONLINE_HYBRID_FLG, ONLINE_ONLY_FLG, RSDNCY_ID, RSDNCY_LD, IS_RSDNCY_FLG,
          ONLINE_FTE, TOT_FTE, ONLINE_CREDITS, NON_ONLINE_CREDITS, CE_CREDITS, NON_CE_CREDITS, TOT_CREDITS, ENROLL_CNT, ONLINE_CNT, CE_CNT,
          BIRTHDATE,SEX,ETHNIC_GRP_CD,CITIZENSHIP_STATUS,CITIZENSHIP_STATUS_LD,UM_CITIZENSHIP,UM_CITIZENSHIP_LD,
          MILITARY_STATUS,MILITARY_STATUS_LD,UM_PRIMARY_STATE,UM_PRIMARY_POSTAL
     from CSMRT_OWNER.UM_M_AF_STDNT_ENRL_AMH S
     left outer join Q1
       on S.INSTITUTION_CD = Q1.INSTITUTION_CD
      and S.ACAD_CAR_CD = Q1.ACAD_CAR_CD
      and S.TERM_CD = Q1.TERM_CD
      and S.PERSON_ID = Q1.PERSON_ID
      and S.SRC_SYS_ID = Q1.SRC_SYS_ID
     join Q2
       on 1 = 1) S
      on (nvl(trim(T.INSTITUTION_CD),'-') = nvl(trim(S.INSTITUTION_CD),'-')
     and  nvl(trim(T.ACAD_CAR_CD),'-') = nvl(trim(S.ACAD_CAR_CD),'-')
     and  nvl(trim(T.TERM_CD),'-') = nvl(trim(S.TERM_CD),'-')
     and  nvl(trim(T.PERSON_ID),'-') = nvl(trim(S.PERSON_ID),'-')
     and  nvl(trim(T.SRC_SYS_ID),'-') = nvl(trim(S.SRC_SYS_ID),'-')
     and  nvl(trim(T.EFFDT_START),to_date('01-JAN-1900')) = nvl(trim(S.MAX_EFFDT_START),to_date('01-JAN-1900'))
     and  nvl(trim(S.MAX_EFFDT_END),to_date('01-JAN-1900')) = nvl(trim(S.MAX_START_DT),to_date('01-JAN-1900'))     -- For gap problem
     and  nvl(T.ACAD_YR,0) = nvl(S.ACAD_YR,0)
     and  nvl(trim(T.AID_YEAR),'-') = nvl(trim(S.AID_YEAR),'-')
     and  nvl(trim(T.TERM_LD),'-') = nvl(trim(S.TERM_LD),'-')
     and  nvl(trim(T.ACAD_ORG_CD),'-') = nvl(trim(S.ACAD_ORG_CD),'-')
     and  nvl(trim(T.ACAD_ORG_LD),'-') = nvl(trim(S.ACAD_ORG_LD),'-')
     and  nvl(trim(T.ACAD_LEVEL_BOT),'-') = nvl(trim(S.ACAD_LEVEL_BOT),'-')      -- Jan 2021
     and  nvl(trim(T.ACAD_PROG_CD),'-') = nvl(trim(S.ACAD_PROG_CD),'-')
     and  nvl(trim(T.ACAD_PROG_LD),'-') = nvl(trim(S.ACAD_PROG_LD),'-')
     and  nvl(trim(T.PROG_CIP_CD),'-') = nvl(trim(S.PROG_CIP_CD),'-')
     and  nvl(trim(T.ACAD_PLAN_CD),'-') = nvl(trim(S.ACAD_PLAN_CD),'-')
     and  nvl(trim(T.ACAD_PLAN_LD),'-') = nvl(trim(S.ACAD_PLAN_LD),'-')
     and  nvl(trim(T.PLAN_CIP_CD),'-') = nvl(trim(S.PLAN_CIP_CD),'-')
     and  nvl(trim(T.CE_ONLY_FLG),'-') = nvl(trim(S.CE_ONLY_FLG),'-')
     and  nvl(trim(T.NEW_CONT_IND),'-') = nvl(trim(S.NEW_CONT_IND),'-')
     and  nvl(trim(T.ONLINE_HYBRID_FLG),'-') = nvl(trim(S.ONLINE_HYBRID_FLG),'-')
     and  nvl(trim(T.ONLINE_ONLY_FLG),'-') = nvl(trim(S.ONLINE_ONLY_FLG),'-')
     and  nvl(trim(T.RSDNCY_ID),'-') = nvl(trim(S.RSDNCY_ID),'-')
     and  nvl(trim(T.RSDNCY_LD),'-') = nvl(trim(S.RSDNCY_LD),'-')
     and  nvl(trim(T.IS_RSDNCY_FLG),'-') = nvl(trim(S.IS_RSDNCY_FLG),'-')
     and  nvl(round(T.ONLINE_FTE,9),0) = nvl(round(S.ONLINE_FTE,9),0)
     and  nvl(round(T.TOT_FTE,9),0) = nvl(round(S.TOT_FTE,9),0)
     and  nvl(round(T.ONLINE_CREDITS,9),0) = nvl(round(S.ONLINE_CREDITS,9),0)
     and  nvl(round(T.NON_ONLINE_CREDITS,9),0) = nvl(round(S.NON_ONLINE_CREDITS,9),0)
     and  nvl(round(T.CE_CREDITS,9),0) = nvl(round(S.CE_CREDITS,9),0)
     and  nvl(round(T.NON_CE_CREDITS,9),0) = nvl(round(S.NON_CE_CREDITS,9),0)
     and  nvl(round(T.TOT_CREDITS,9),0) = nvl(round(S.TOT_CREDITS,9),0)
     and  nvl(round(T.ENROLL_CNT,9),0) = nvl(round(S.ENROLL_CNT,9),0)
     and  nvl(round(T.ONLINE_CNT,9),0) = nvl(round(S.ONLINE_CNT,9),0)
     and  nvl(round(T.CE_CNT,9),0) = nvl(round(S.CE_CNT,9),0)
     and  nvl(trim(T.BIRTHDATE),to_date('01-JAN-1900')) = nvl(trim(S.BIRTHDATE),to_date('01-JAN-1900'))
     and  nvl(trim(T.SEX),'-') = nvl(trim(S.SEX),'-')
     and  nvl(trim(T.ETHNIC_GRP_CD),'-') = nvl(trim(S.ETHNIC_GRP_CD),'-')
     and  nvl(trim(T.CITIZENSHIP_STATUS),'-') = nvl(trim(S.CITIZENSHIP_STATUS),'-')
     and  nvl(trim(T.CITIZENSHIP_STATUS_LD),'-') = nvl(trim(S.CITIZENSHIP_STATUS_LD),'-')
     and  nvl(trim(T.UM_CITIZENSHIP),'-') = nvl(trim(S.UM_CITIZENSHIP),'-')
     and  nvl(trim(T.UM_CITIZENSHIP_LD),'-') = nvl(trim(S.UM_CITIZENSHIP_LD),'-')
     and  nvl(trim(T.MILITARY_STATUS),'-') = nvl(trim(S.MILITARY_STATUS),'-')
     and  nvl(trim(T.MILITARY_STATUS_LD),'-') = nvl(trim(S.MILITARY_STATUS_LD),'-')
     and  nvl(trim(T.UM_PRIMARY_STATE),'-') = nvl(trim(S.UM_PRIMARY_STATE),'-')
     and  nvl(trim(T.UM_PRIMARY_POSTAL),'-') = nvl(trim(S.UM_PRIMARY_POSTAL),'-')
   )
    when matched then update set
          T.EFFDT_END = dtEFFDT,
          T.LASTUPD_EW_DTTM = SYSDATE
    where S.MAX_EFFDT_START = T.EFFDT_START    -- Only update the latest row
    when not matched then
   insert (
          T.INSTITUTION_CD,
          T.ACAD_CAR_CD,
          T.TERM_CD,
          T.PERSON_ID,
          T.SRC_SYS_ID,
          T.EFFDT_START,
          T.EFFDT_END,
          T.ACAD_YR,
          T.AID_YEAR,
          T.TERM_LD,
          T.ACAD_ORG_CD,
          T.ACAD_ORG_LD,
          T.ACAD_LEVEL_BOT,    -- Jan 2021
          T.ACAD_PROG_CD,
          T.ACAD_PROG_LD,
          T.PROG_CIP_CD,
          T.ACAD_PLAN_CD,
          T.ACAD_PLAN_LD,
          T.PLAN_CIP_CD,
          T.CE_ONLY_FLG,
          T.NEW_CONT_IND,
          T.ONLINE_HYBRID_FLG,
          T.ONLINE_ONLY_FLG,
          T.RSDNCY_ID,
          T.RSDNCY_LD,
          T.IS_RSDNCY_FLG,
          T.ONLINE_FTE,
          T.TOT_FTE,
          T.ONLINE_CREDITS,
          T.NON_ONLINE_CREDITS,
          T.CE_CREDITS,
          T.NON_CE_CREDITS,
          T.TOT_CREDITS,
          T.ENROLL_CNT,
          T.ONLINE_CNT,
          T.CE_CNT,
          T.CREATED_EW_DTTM,
          T.LASTUPD_EW_DTTM,
          T.BIRTHDATE,
          T.SEX,
          T.ETHNIC_GRP_CD,
          T.CITIZENSHIP_STATUS,
          T.CITIZENSHIP_STATUS_LD,
          T.UM_CITIZENSHIP,
          T.UM_CITIZENSHIP_LD,
          T.MILITARY_STATUS,
          T.MILITARY_STATUS_LD,
          T.UM_PRIMARY_STATE,
          T.UM_PRIMARY_POSTAL)
   values (
          S.INSTITUTION_CD,
          S.ACAD_CAR_CD,
          S.TERM_CD,
          S.PERSON_ID,
          S.SRC_SYS_ID,
          trunc(dtEFFDT),                    -- EFFDT_START,
          trunc(dtEFFDT),                    -- EFFDT_END,
          S.ACAD_YR,
          S.AID_YEAR,
          S.TERM_LD,
          S.ACAD_ORG_CD,
          S.ACAD_ORG_LD,
          S.ACAD_LEVEL_BOT,                  -- Jan 2021
          S.ACAD_PROG_CD,
          S.ACAD_PROG_LD,
          S.PROG_CIP_CD,
          S.ACAD_PLAN_CD,
          S.ACAD_PLAN_LD,
          S.PLAN_CIP_CD,
          S.CE_ONLY_FLG,
          S.NEW_CONT_IND,
          S.ONLINE_HYBRID_FLG,
          S.ONLINE_ONLY_FLG,
          S.RSDNCY_ID,
          S.RSDNCY_LD,
          S.IS_RSDNCY_FLG,
          S.ONLINE_FTE,
          S.TOT_FTE,
          S.ONLINE_CREDITS,
          S.NON_ONLINE_CREDITS,
          S.CE_CREDITS,
          S.NON_CE_CREDITS,
          S.TOT_CREDITS,
          S.ENROLL_CNT,
          S.ONLINE_CNT,
          S.CE_CNT,
          SYSDATE,                 -- CREATED_EW_DTTM
          SYSDATE,                  -- LASTUPD_EW_DTTM
          S.BIRTHDATE,
          S.SEX,
          S.ETHNIC_GRP_CD,
          S.CITIZENSHIP_STATUS,
          S.CITIZENSHIP_STATUS_LD,
          S.UM_CITIZENSHIP,
          S.UM_CITIZENSHIP_LD,
          S.MILITARY_STATUS,
          S.MILITARY_STATUS_LD,
          S.UM_PRIMARY_STATE,
          S.UM_PRIMARY_POSTAL
   )
   ;

   strSqlCommand   := 'SET intRowCount';
   intRowCount     := SQL%ROWCOUNT;

   strSqlCommand := 'commit';
   commit;

   strMessage01    := '# of UM_S_AF_STDNT_ENRL_AMH rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
           (
                   i_TargetTableName   => 'UM_S_AF_STDNT_ENRL_AMH',
                   i_Action            => 'MERGE',
                   i_RowCount          => intRowCount
           );

   strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_S_AF_STDNT_ENRL_AMH';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

   strSqlDynamic   := 'alter table CSMRT_OWNER.UM_S_AF_STDNT_ENRL_AMH enable constraint PK_UM_S_AF_STDNT_ENRL_AMH';
   strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
   COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                   (
                   i_SqlStatement          => strSqlDynamic,
                   i_MaxTries              => 10,
                   i_WaitSeconds           => 10,
                   o_Tries                 => intTries
                   );

   COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_S_AF_STDNT_ENRL_AMH');

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
           (
                   i_TargetTableName   => 'UM_S_AF_STDNT_ENRL_AMH',
                   i_Action            => 'UPDATE',
                   i_RowCount          => intRowCount
           );

   strMessage01    := 'Updating data in CSMRT_OWNER.UM_S_AF_STDNT_ENRL_AMH to prevent gaps';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

   strSqlCommand   := 'update CSMRT_OWNER.UM_S_AF_STDNT_ENRL_AMH';
   update CSMRT_OWNER.UM_S_AF_STDNT_ENRL_AMH T
      set T.EFFDT_END = (trunc(dtEFFDT-1)),
          T.LASTUPD_EW_DTTM = dtEFFDT
    where T.EFFDT_START < trunc(dtEFFDT)
      and T.EFFDT_END between (select max(EFFDT_END) MAX_EFFDT_END
                                 from CSMRT_OWNER.UM_S_AF_STDNT_ENRL_AMH        -- EFFDT_END prior to the preceding merge.
                                where EFFDT_END < trunc(dtEFFDT))
                          and (trunc(dtEFFDT-1))
   ;

   strSqlCommand   := 'SET intRowCount';
   intRowCount     := SQL%ROWCOUNT;

   strSqlCommand := 'commit';
   commit;

   strMessage01    := '# of UM_S_AF_STDNT_ENRL_AMH rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
           (
                   i_TargetTableName   => 'UM_S_AF_STDNT_ENRL_AMH',
                   i_Action            => 'UPDATE',
                   i_RowCount          => intRowCount
           );

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

   strMessage01    := strProcessName || ' is complete, Processed Status: ' || ProcessedStatus || ' and M Load Status: '|| M_LoadStatus ;
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
  
  
ELSE

   strMessage01    := 'UM_S_AF_STDNT_ENRL_AMH_P process is not eligible to proceed. Processed Status: ' || ProcessedStatus || ' and M Load Status: '|| M_LoadStatus ;
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

   END UM_S_AF_STDNT_ENRL_AMH_P;
/
