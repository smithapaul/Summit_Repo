DROP PROCEDURE CSMRT_OWNER.UM_R_STDNT_AGG_P
/

--
-- UM_R_STDNT_AGG_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_R_STDNT_AGG_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--George Adams
--Loads table                -- UM_R_STDNT_AGG
--V01 12/12/2018             -- srikanth ,pabbu converted to proc from sql scripts

------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_R_STDNT_AGG';
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

strMessage01    := 'Truncating table CSMRT_OWNER.UM_R_STDNT_AGG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_R_STDNT_AGG';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_R_STDNT_AGG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_R_STDNT_AGG');

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_R_STDNT_AGG disable constraint PK_UM_R_STDNT_AGG';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_R_STDNT_AGG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_R_STDNT_AGG';
insert /*+ append enable_parallel_dml parallel(8) */ into UM_R_STDNT_AGG
with PROG as (
select /*+ INLINE PARALLEL(8) */
       F.TERM_SID,
       F.PERSON_SID,
       F.STDNT_CAR_NUM,
       F.SRC_SYS_ID,
       F.INSTITUTION_CD,
       F.ACAD_CAR_CD,
       F.TERM_CD,
       F.PERSON_ID,
       F.INSTITUTION_SID,
       F.ACAD_CAR_SID,
       F.ACAD_PROG_SID,
       F.STACK_BEGIN_TERM_SID,
       T.TERM_CD STACK_BEGIN_TERM_CD,
       F.ACAD_PROG_CD,
       F.EFFDT,
       F.PROG_STAT_SID,
       F.PROG_ACN_SID,
       F.PROG_ACN_RSN_SID,
       F.ADMIT_TERM_SID,
       F.EXP_GRAD_TERM_SID,
       F.DEGR_CHKOUT_LAST_EGT,
       F.DEGR_CHKOUT_LAST_EGT_EFFDT,
          CASE WHEN F.STACK_BEGIN_TERM_SID = F.TERM_SID
                AND F.STACK_READMIT_TERM_SID <> F.TERM_SID
               THEN 'Y'
               ELSE 'N'
           END STACK_BEGIN_FLG,     -- Feb 2018
          CASE WHEN F.STACK_BEGIN_TERM_SID <> F.TERM_SID
                AND F.STACK_READMIT_TERM_SID <> F.TERM_SID
               THEN 'Y'
               ELSE 'N'
           END STACK_CONTINUE_FLG,  -- Feb 2018
          CASE WHEN F.STACK_READMIT_TERM_SID = F.TERM_SID
               THEN 'Y'
               ELSE 'N'
           END STACK_READMIT_FLG,   -- Feb 2018
       R.PROG_ACN_CD,
       R.PROG_ACN_RSN_CD,
       S.PROG_STAT_CD,
       max(F.TERM_CD) over (partition by F.INSTITUTION_CD, F.ACAD_CAR_CD, F.STDNT_CAR_NUM, F.PERSON_ID, F.SRC_SYS_ID) MAX_TERM_CD
  from UM_F_ACAD_PROG F
  join PS_D_PROG_ACN_RSN R
    on F.PROG_ACN_RSN_SID = R.PROG_ACN_RSN_SID
  join PS_D_PROG_STAT S
    on F.PROG_STAT_SID = S.PROG_STAT_SID
  join PS_D_TERM T
    on F.STACK_BEGIN_TERM_SID = T.TERM_SID
 where F.TERM_SID <> 2147483646
   and F.PERSON_SID <> 2147483646
--   and F.PERSON_ID = '01097721'         -- Temp!!!
   ),
DEG as (
select /*+ INLINE PARALLEL(8) full(f) */
       distinct T.INSTITUTION_CD, T.ACAD_CAR_CD, T.TERM_CD, F.PERSON_SID, F.SRC_SYS_ID, F.PLAN_DEGR_STATUS
  from PS_F_DEGREES F
  join PS_D_TERM T
    on F.COMPL_TERM_SID = T.TERM_SID
 where F.DATA_ORIGIN <> 'D'
   and F.PLAN_DEGR_STATUS = 'A'
   and ROWNUM < 100000000
),
PROG2 as (
select /*+ INLINE PARALLEL(8) */
       F.TERM_SID,
       F.PERSON_SID,
       F.STDNT_CAR_NUM,
       F.SRC_SYS_ID,
       F.INSTITUTION_CD,
       F.ACAD_CAR_CD,
       F.TERM_CD,
       F.PERSON_ID,
       F.INSTITUTION_SID,
       F.ACAD_CAR_SID,
       F.ACAD_PROG_SID,
       F.STACK_BEGIN_TERM_SID,
       F.STACK_BEGIN_TERM_CD,
       F.ACAD_PROG_CD,
       F.EFFDT,
       F.PROG_STAT_SID,
       F.PROG_ACN_SID,
       F.PROG_ACN_RSN_SID,
       F.ADMIT_TERM_SID,
       F.EXP_GRAD_TERM_SID,
       F.DEGR_CHKOUT_LAST_EGT,
       F.DEGR_CHKOUT_LAST_EGT_EFFDT,
       F.STACK_BEGIN_FLG,       -- Feb 2018
       F.STACK_CONTINUE_FLG,    -- Feb 2018
       F.STACK_READMIT_FLG,     -- Feb 2018
       F.PROG_ACN_CD,
       F.PROG_ACN_RSN_CD,
       F.PROG_STAT_CD,
       F.MAX_TERM_CD,
       case when nvl(D.PLAN_DEGR_STATUS,'N') = 'A' and F.PROG_STAT_CD = 'AC' then 'Y' else 'N' end PREV_DEG_FLG,
       row_number() over (partition by F.TERM_SID, F.PERSON_SID, F.STDNT_CAR_NUM, F.SRC_SYS_ID
                              order by F.PERSON_SID) PROG_ORDER
  from PROG F
  left outer join DEG D
    on F.INSTITUTION_CD = D.INSTITUTION_CD
   and F.ACAD_CAR_CD = D.ACAD_CAR_CD
   and F.PERSON_SID = D.PERSON_SID
   and F.SRC_SYS_ID = D.SRC_SYS_ID
   and F.TERM_CD > D.TERM_CD
   and F.STACK_BEGIN_TERM_CD > D.TERM_CD)
    select /*+ PARALLEL(8) */
    G.TERM_SID,
    G.PERSON_SID,
    G.SRC_SYS_ID,
    G.INSTITUTION_CD,
    G.ACAD_CAR_CD,
    G.TERM_CD,
    G.PERSON_ID,
    G.INSTITUTION_SID,
    G.ACAD_CAR_SID,
    nvl(max(case when P.PRIM_STACK_CAREER_RANK = 1 and P.MAJOR_RANK = 1 then G.STDNT_CAR_NUM else NULL end),0) PS_STDNT_CAR_NUM,
    nvl(max(case when P.PRIM_STACK_CAREER_RANK = 1 and P.MAJOR_RANK = 1 then G.ACAD_PROG_SID else NULL end),2147483646) PS_PROG_SID,
    nvl(max(case when P.PRIM_STACK_CAREER_RANK = 1 and P.MAJOR_RANK = 1 then G.EFFDT else NULL end),NULL) PS_EFFDT,
    nvl(max(case when P.PRIM_STACK_CAREER_RANK = 1 and P.MAJOR_RANK = 1 then G.PROG_STAT_SID else NULL end),2147483646) PS_PROG_STAT_SID,
    nvl(max(case when P.PRIM_STACK_CAREER_RANK = 1 and P.MAJOR_RANK = 1 then G.PROG_ACN_SID else NULL end),2147483646) PS_PROG_ACN_SID,
    nvl(max(case when P.PRIM_STACK_CAREER_RANK = 1 and P.MAJOR_RANK = 1 then G.PROG_ACN_RSN_SID else NULL end),2147483646) PS_PROG_ACN_RSN_SID,
    nvl(max(case when P.PRIM_STACK_CAREER_RANK = 1 and P.MAJOR_RANK = 1 then G.ADMIT_TERM_SID else NULL end),2147483646) PS_ADMIT_TERM_SID,
    nvl(max(case when P.PRIM_STACK_CAREER_RANK = 1 and P.MAJOR_RANK = 1 then G.EXP_GRAD_TERM_SID else NULL end),2147483646) PS_EXP_GRAD_TERM_SID,
    nvl(max(case when P.PRIM_STACK_CAREER_RANK = 1 and P.MAJOR_RANK = 1 then G.DEGR_CHKOUT_LAST_EGT else NULL end),NULL) PS_DEGR_CHKOUT_LAST_EGT,
    nvl(max(case when P.PRIM_STACK_CAREER_RANK = 1 and P.MAJOR_RANK = 1 then G.DEGR_CHKOUT_LAST_EGT_EFFDT else NULL end),NULL) PS_DEGR_CHKOUT_LAST_EGT_EFFDT,
    nvl(max(case when P.PRIM_STACK_CAREER_RANK = 1 and P.MAJOR_RANK = 1 then G.STACK_BEGIN_FLG else NULL end),NULL) STACK_BEGIN_FLG, 		-- Feb 2018
    nvl(max(case when P.PRIM_STACK_CAREER_RANK = 1 and P.MAJOR_RANK = 1 then G.STACK_CONTINUE_FLG else NULL end),NULL) STACK_CONTINUE_FLG, 	-- Feb 2018
    nvl(max(case when P.PRIM_STACK_CAREER_RANK = 1 and P.MAJOR_RANK = 1 then G.STACK_READMIT_FLG else NULL end),NULL) STACK_READMIT_FLG, 	-- Feb 2018
    nvl(max(case when P.MAJOR_RANK_PIVOT = 1 then P.ACAD_PLAN_SID else NULL end),2147483646) MAJ1_ACAD_PLAN_SID,
    nvl(max(case when P.MAJOR_RANK_PIVOT = 1 then P.PLAN_SEQUENCE else NULL end),NULL) MAJ1_PLAN_SEQUENCE,
    nvl(max(case when P.MAJOR_RANK_PIVOT = 1 and P.SPLAN_RANK = 1 then P.ACAD_SPLAN_SID else NULL end),2147483646) MAJ1_SPLAN1_SID,
    nvl(max(case when P.MAJOR_RANK_PIVOT = 1 and P.SPLAN_RANK = 2 then P.ACAD_SPLAN_SID else NULL end),2147483646) MAJ1_SPLAN2_SID,
    nvl(max(case when P.MAJOR_RANK_PIVOT = 1 and P.SPLAN_RANK = 3 then P.ACAD_SPLAN_SID else NULL end),2147483646) MAJ1_SPLAN3_SID,
    nvl(max(case when P.MAJOR_RANK_PIVOT = 1 and P.SPLAN_RANK = 4 then P.ACAD_SPLAN_SID else NULL end),2147483646) MAJ1_SPLAN4_SID,     -- Added Mar 2016
    nvl(max(case when P.MAJOR_RANK_PIVOT = 2 then P.ACAD_PLAN_SID else NULL end),2147483646) MAJ2_ACAD_PLAN_SID,
    nvl(max(case when P.MAJOR_RANK_PIVOT = 2 and P.SPLAN_RANK = 1 then P.ACAD_SPLAN_SID else NULL end),2147483646) MAJ2_SPLAN1_SID,
    nvl(max(case when P.MAJOR_RANK_PIVOT = 3 then P.ACAD_PLAN_SID else NULL end),2147483646) MAJ3_ACAD_PLAN_SID,
    nvl(max(case when P.MAJOR_RANK_PIVOT = 3 and P.SPLAN_RANK = 1 then P.ACAD_SPLAN_SID else NULL end),2147483646) MAJ3_SPLAN1_SID,
    nvl(max(case when P.MAJOR_RANK_PIVOT = 4 then P.ACAD_PLAN_SID else NULL end),2147483646) MAJ4_ACAD_PLAN_SID,
    nvl(max(case when P.MINOR_RANK_PIVOT = 1 then P.ACAD_PLAN_SID else NULL end),2147483646) MIN1_ACAD_PLAN_SID,
    nvl(max(case when P.MINOR_RANK_PIVOT = 2 then P.ACAD_PLAN_SID else NULL end),2147483646) MIN2_ACAD_PLAN_SID,
    nvl(max(case when P.MINOR_RANK_PIVOT = 3 then P.ACAD_PLAN_SID else NULL end),2147483646) MIN3_ACAD_PLAN_SID,
    nvl(max(case when P.MINOR_RANK_PIVOT = 4 then P.ACAD_PLAN_SID else NULL end),2147483646) MIN4_ACAD_PLAN_SID,
--    nvl(max(case when ((G.INSTITUTION_CD = 'UMBOS' and P.POS_RANK_PIVOT = 1) or (G.INSTITUTION_CD <> 'UMBOS' and P.MAJOR_RANK_PIVOT = 5)) then P.ACAD_PLAN_SID else NULL end),2147483646) OTH1_ACAD_PLAN_SID,
    nvl(max(case when P.MAJOR_RANK_PIVOT = 5 then P.ACAD_PLAN_SID
                 when G.INSTITUTION_CD = 'UMBOS' and P.POS_RANK_PIVOT = 1 then P.ACAD_PLAN_SID
                 when G.INSTITUTION_CD = 'UMDAR' and P.POS_RANK_PIVOT = 1 then P.ACAD_PLAN_SID
                 else NULL end),2147483646) OTH1_ACAD_PLAN_SID,     -- June 2016
--    nvl(max(case when ((G.INSTITUTION_CD = 'UMBOS' and P.POS_RANK_PIVOT = 2) or (G.INSTITUTION_CD <> 'UMBOS' and P.MAJOR_RANK_PIVOT = 6)) then P.ACAD_PLAN_SID else NULL end),2147483646) OTH2_ACAD_PLAN_SID,
    nvl(max(case when P.MAJOR_RANK_PIVOT = 6 then P.ACAD_PLAN_SID
                 when G.INSTITUTION_CD = 'UMBOS' and P.POS_RANK_PIVOT = 2 then P.ACAD_PLAN_SID
                 when G.INSTITUTION_CD = 'UMDAR' and P.POS_RANK_PIVOT = 2 then P.ACAD_PLAN_SID
                 else NULL end),2147483646) OTH2_ACAD_PLAN_SID,     -- June 2016
    nvl(max(P.ED_LVL_RANK), 0) ED_LVL_RANK,
    nvl(max(G.PREV_DEG_FLG), 'N') PREV_DEG_FLG,
    nvl(max(case when G.INSTITUTION_CD = 'UMBOS' and G.TERM_CD >= '2330' and P.ACAD_PLAN_CD = 'HONORS-XX' then 'Y' else NULL end), 'N') UMBOS_HON_FLG,
    nvl(max(case when G.INSTITUTION_CD = 'UMDAR' and G.STACK_BEGIN_TERM_SID = G.TERM_SID and G.ACAD_PROG_CD = 'DCE-U' then 'Y' else NULL end), 'N') UMDAR_DCE_FLG,
    nvl(max(case when G.INSTITUTION_CD = 'UMDAR' and G.PROG_ACN_CD = 'ACTV' and G.PROG_ACN_RSN_CD in ('SDEG','TDEG') then 'Y' else NULL end), 'N') UMDAR_UGRD_SECOND_DEGR_FLG,
    nvl(max(case when G.INSTITUTION_CD = 'UMLOW' and G.ACAD_CAR_CD = 'UGRD' and G.PROG_ACN_CD = 'ACTV' and G.PROG_ACN_RSN_CD in ('ITR','SEC') then 'Y'
                 when G.INSTITUTION_CD = 'UMLOW' and G.ACAD_CAR_CD = 'CSCE' and G.PROG_ACN_CD = 'ACTV' and G.PROG_ACN_RSN_CD in ('SDEG') then 'Y'
            else NULL end), 'N') UMLOW_UGRD_SECOND_DEGR_FLG,
    max(case when P.PRIM_STACK_CAREER_RANK = 1 and P.PRIM_STACK_STDNT_RANK = 1 and P.MAJOR_RANK = 1 and P.SPLAN_RANK <= 1
             then 1
             else 0
          end) UNDUP_STDNT_CNT                               -- Added Mar 2016
    from PROG2 G
    join UM_F_ACAD_PLAN P
      on G.TERM_SID = P.TERM_SID
     and G.PERSON_SID = P.PERSON_SID
     and G.STDNT_CAR_NUM = P.STDNT_CAR_NUM
     and G.SRC_SYS_ID = P.SRC_SYS_ID
     and (G.TERM_CD >= '1010' or G.MAX_TERM_CD >= '2010' or G.MAX_TERM_CD = G.TERM_CD)  -- Feb 2018
   where PROG_ORDER = 1
    group by
    G.TERM_SID,
    G.PERSON_SID,
    G.SRC_SYS_ID,
    G.INSTITUTION_CD,
    G.ACAD_CAR_CD,
    G.TERM_CD,
    G.PERSON_ID,
    G.INSTITUTION_SID,
    G.ACAD_CAR_SID
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_R_STDNT_AGG rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_R_STDNT_AGG',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_R_STDNT_AGG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_R_STDNT_AGG enable constraint PK_UM_R_STDNT_AGG';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_R_STDNT_AGG');

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

END UM_R_STDNT_AGG_P;
/
