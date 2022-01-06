CREATE OR REPLACE PROCEDURE             "UM_S_AF_STDNT_ENRL_AMH_FIX_P" 
        (
                i_EFFDT      in  Varchar2    Default SYSDATE
        )

IS

------------------------------------------------------------------------
-- James Doucette
--
-- Loads table UM_S_AF_STDNT_ENRL_AMH_FIX.
--
 --V01  SMT-xxxx 09/10/2018,    James Doucette
--                              New Process
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_S_AF_STDNT_ENRL_AMH_FIX';
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

--strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_S_AF_STDNT_ENRL_AMH_FIX';
--COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
--COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_S_AF_STDNT_ENRL_AMH_FIX');

--alter table UM_S_AF_STDNT_ENRL_AMH_FIX disable constraint PK_UM_S_AF_STDNT_ENRL_AMH;
--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_S_AF_STDNT_ENRL_AMH_FIX disable constraint PK_UM_S_AF_STDNT_ENRL_AMH_FIX';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );
				
strMessage01    := 'Merging data into CSMRT_OWNER.UM_S_AF_STDNT_ENRL_AMH_FIX';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSMRT_OWNER.UM_S_AF_STDNT_ENRL_AMH_FIX';				
merge into CSMRT_OWNER.UM_S_AF_STDNT_ENRL_AMH_FIX T  
using (
select INSTITUTION_CD, ACAD_CAR_CD, TERM_CD, PERSON_ID, 'CS90' SRC_SYS_ID, 
       ACAD_YR, AID_YEAR, TERM_LD, ACAD_ORG_CD, ACAD_ORG_LD, ACAD_PROG_CD, ACAD_PROG_LD, PROG_CIP_CD, ACAD_PLAN_CD, ACAD_PLAN_LD, PLAN_CIP_CD, 
       CE_ONLY_FLG, NEW_CONT_IND, ONLINE_HYBRID_FLG, ONLINE_ONLY_FLG, RSDNCY_ID, RSDNCY_LD, IS_RSDNCY_FLG, 
       ONLINE_FTE, TOT_FTE, ONLINE_CREDITS, NON_ONLINE_CREDITS, CE_CREDITS, NON_CE_CREDITS, TOT_CREDITS, ENROLL_CNT, ONLINE_CNT, CE_CNT
  from CSMRT_OWNER.UM_M_AF_STDNT_ENRL_AMH_FIX) S 
   on (nvl(trim(T.INSTITUTION_CD),'-') = nvl(trim(S.INSTITUTION_CD),'-')
  and  nvl(trim(T.ACAD_CAR_CD),'-') = nvl(trim(S.ACAD_CAR_CD),'-')
  and  nvl(trim(T.TERM_CD),'-') = nvl(trim(S.TERM_CD),'-')
  and  nvl(trim(T.PERSON_ID),'-') = nvl(trim(S.PERSON_ID),'-')
  and  nvl(trim(T.SRC_SYS_ID),'-') = nvl(trim(S.SRC_SYS_ID),'-')
  and  nvl(T.ACAD_YR,0) = nvl(S.ACAD_YR,0)
  and  nvl(trim(T.AID_YEAR),'-') = nvl(trim(S.AID_YEAR),'-')
  and  nvl(trim(T.TERM_LD),'-') = nvl(trim(S.TERM_LD),'-')
  and  nvl(trim(T.ACAD_ORG_CD),'-') = nvl(trim(S.ACAD_ORG_CD),'-')
  and  nvl(trim(T.ACAD_ORG_LD),'-') = nvl(trim(S.ACAD_ORG_LD),'-')
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
)
 when matched then update set 
       T.EFFDT_END = dtEFFDT,
       T.LASTUPD_EW_DTTM = SYSDATE
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
       T.LASTUPD_EW_DTTM)
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
       SYSDATE                  -- LASTUPD_EW_DTTM
)
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_S_AF_STDNT_ENRL_AMH_FIX rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_S_AF_STDNT_ENRL_AMH_FIX',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

--strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_S_AF_STDNT_ENRL_AMH_FIX';
--COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
----alter table UM_S_AF_STDNT_ENRL_AMH_FIX enable constraint PK_UM_S_AF_STDNT_ENRL_AMH_FIX;

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_S_AF_STDNT_ENRL_AMH_FIX enable constraint PK_UM_S_AF_STDNT_ENRL_AMH';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );
				
--COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_S_AF_STDNT_ENRL_AMH_FIX');

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

END UM_S_AF_STDNT_ENRL_AMH_FIX_P;
/
