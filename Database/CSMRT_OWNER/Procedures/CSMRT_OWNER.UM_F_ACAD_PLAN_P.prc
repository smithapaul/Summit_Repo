CREATE OR REPLACE PROCEDURE             "UM_F_ACAD_PLAN_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads mart table UM_F_ACAD_PLAN.
--
 --V01  SMT-xxxx 07/03/2018,    James Doucette
--                              Converted from SQL Script
--
--V02  SMT-7958 09/05/2018,    George Adams
--V03  CASE-47629 08/19/2020,  James Doucette
--                             
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_ACAD_PLAN';
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

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_ACAD_PLAN';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_ACAD_PLAN');

--alter table UM_F_ACAD_PLAN disable constraint PK_UM_F_ACAD_PLAN;
strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_ACAD_PLAN disable constraint PK_UM_F_ACAD_PLAN';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
				
strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_ACAD_PLAN';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_ACAD_PLAN';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );


strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_ACAD_PLAN';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_ACAD_PLAN';				
insert /*+ append */ into UM_F_ACAD_PLAN
with D1 as ( 
select DEGREE, EFFDT, SRC_SYS_ID, 
       EDUCATION_LVL ED_LVL,        -- Remove later??? 
       (case when EDUCATION_LVL not between '01' and '99' then '' else EDUCATION_LVL end) EDU_LVL,
       to_number(100 - (case when trim(EDUCATION_LVL) not between '01' and '99' then 2 else to_number(trim(EDUCATION_LVL)) end)) INV_ED_LVL,    -- Remove later??? 
       ROW_NUMBER() over (partition by DEGREE, SRC_SYS_ID
                              order by EFFDT desc) DEG_ORDER
  from CSSTG_OWNER.PS_DEGREE_TBL T
 where DATA_ORIGIN <> 'D'),
T1 as ( 
select T.INSTITUTION, T.ACAD_PLAN, T.EFFDT, T.SRC_SYS_ID, 
       T.ACAD_PLAN_TYPE,
       (case when T.ACAD_PLAN_TYPE in ('MAJ', 'OBS', 'OBA') then '1'||T.ACAD_PLAN_TYPE
             when T.ACAD_PLAN_TYPE in ('AMJ') then '2'||T.ACAD_PLAN_TYPE
             when T.ACAD_PLAN_TYPE in ('POS', 'OGP') then '3'||T.ACAD_PLAN_TYPE
             when T.ACAD_PLAN_TYPE in ('CRT') then '4'||T.ACAD_PLAN_TYPE
             when T.INSTITUTION = 'UMDAR' and T.ACAD_PLAN_TYPE = 'POS' and T.ACAD_PLAN in ('HONCOLL','COMMHON') then '8'||T.ACAD_PLAN_TYPE   -- Case 47629, August 2020 
             when T.ACAD_PLAN_TYPE in ('MIN') then '9'||T.ACAD_PLAN_TYPE
        else '7'||T.ACAD_PLAN_TYPE end) MAJ_PT_ORDER, 
       (case when D1.EDU_LVL is not null then D1.EDU_LVL 
             when T.INSTITUTION = 'UMBOS' 
              and T.ACAD_PLAN IN ('ENGIN', 'UNDEC-CAS', 'UNDEC-CPCS', 'UNDEC-CSM', 'UNDEC-LA') then '13' 
             when T.INSTITUTION = 'UMDAR' 
--              and T.ACAD_PLAN IN ('ASU-BA', 'BSA-BS', 'BSP-CP', 'EGR-BS', 'EGR-CP', 'FOU-BFA', 'FOUD-BFA') then '13' 
              and T.ACAD_PLAN IN ('ASU-BA', 'BSA-BS', 'BSP-CP', 'EGR-BS', 'EGR-CP', 'FOU-BFA', 'FOUD-BFA', 'LAU-BA', 'ASUP-CP', 'FOUP-CP', 'COLNW', 'START') then '13' -- May 2016 
             when T.INSTITUTION = 'UMLOW' 
              and T.ACAD_PLAN IN ('UB' , 'UE', 'UH', 'UL', 'US') then '13' 
        else D1.EDU_LVL end) EDU_LVL,
       nvl(D1.INV_ED_LVL,98) INV_ED_LVL,        -- Remove later??? 
       T.DEGREE,                                -- Nov 2017 
       case when (max(T.EFFDT) over (partition by T.INSTITUTION, T.ACAD_PLAN, T.SRC_SYS_ID
                                         order by T.EFFDT
                                     rows between unbounded preceding and 1 preceding)) is NULL then to_date('01-JAN-1800') else T.EFFDT end EFFDT_START,      -- Sept 2018  
       nvl(min(T.EFFDT-1) over (partition by T.INSTITUTION, T.ACAD_PLAN, T.SRC_SYS_ID
                                    order by T.EFFDT
                                rows between 1 following and 1 following),to_date('31-DEC-9999')) EFFDT_END,      -- Sept 2018  
       ROW_NUMBER() over (partition by T.INSTITUTION, T.ACAD_PLAN, T.SRC_SYS_ID
                              order by decode(T.EFF_STATUS,'I',9,0), T.EFFDT desc) PT_ORDER
  from CSSTG_OWNER.PS_ACAD_PLAN_TBL T
  left outer join D1
    on T.DEGREE = D1.DEGREE
   and T.SRC_SYS_ID = D1.SRC_SYS_ID
   and D1.DEG_ORDER = 1
   and T.DATA_ORIGIN <> 'D'),
E1 as (
select /*+ parallel(8) inline */ EMPLID, ACAD_CAREER, INSTITUTION, STRM, sum(UNT_TAKEN) ENRL_CREDIT_SUM     -- Added Nov 2015 
  from CSSTG_OWNER.PS_STDNT_ENRL
 where DATA_ORIGIN <> 'D'
   and STDNT_ENRL_STATUS = 'E'
 group by EMPLID, ACAD_CAREER, INSTITUTION, STRM),
L1 as (
select /*+ parallel(8) inline */  
INSTITUTION, ACAD_CAREER, EMPLID, STDNT_CAR_NBR, ACAD_PROG, ACAD_PLAN, ACAD_SUB_PLAN, SRC_SYS_ID,       -- Added Jan 2016 
EFFDT, EFF_STATUS, UM_OVRRIDE_EXTENSN, STRM NEW_LIMIT_TERM_CD, STRM_1 CALC_LIMIT_TERM_CD, YEARS, COMMENTS_MSGS,
ROW_NUMBER() over (partition by INSTITUTION, ACAD_CAREER, EMPLID, STDNT_CAR_NBR, ACAD_PROG, ACAD_PLAN, ACAD_SUB_PLAN, SRC_SYS_ID
                              order by EFFDT desc) DEG_LIMIT_ORDER
  from CSSTG_OWNER.PS_UM_STDNT_DEGLIM
 where DATA_ORIGIN <> 'D'
),
L2 as (
select /*+ parallel(8) inline */  
INSTITUTION, ACAD_CAREER, ACAD_PROG, ACAD_PLAN, ACAD_SUB_PLAN, EFFDT, SRC_SYS_ID, YEARS,
ROW_NUMBER() over (partition by INSTITUTION, ACAD_CAREER, ACAD_PROG, ACAD_PLAN, ACAD_SUB_PLAN, SRC_SYS_ID
                              order by EFFDT desc) DEG_LIMIT_ORDER2
  from CSSTG_OWNER.PS_UM_DEGR_LIM_TBL
 where DATA_ORIGIN <> 'D'
),
P1 as (
select /*+ parallel(8) inline */ 
       G.TERM_SID, 
       G.PERSON_SID, 
       G.STDNT_CAR_NUM, 
       G.INSTITUTION_CD, 
       G.ACAD_CAR_CD ACAD_CAR_CD, 
       G.TERM_CD TERM_CD, 
       G.PERSON_ID PERSON_ID,
       G.ACAD_PROG_CD ACAD_PROG_CD,
       P.ACAD_PLAN ACAD_PLAN_CD,
       nvl(S.ACAD_SUB_PLAN,'-') ACAD_SPLAN_CD,
       G.SRC_SYS_ID,  
       G.EFFDT, 
       G.EFFSEQ,
--       decode(PS.PROG_STAT_CD,'AC','AA','CN','ZZ','DC','ZZ',PS.PROG_STAT_CD) PROG_STAT_CATGRY,      -- Added Oct 2015  
       decode(PS.PROG_STAT_CD,'AC','AA','CM','AA','CN','ZZ','DC','ZZ',PS.PROG_STAT_CD) PROG_STAT_CATGRY,      -- Sept 2018   
       G.MIN_PROG_STAT_CTGRY MIN_PROG_STAT_CATGRY, 
       G.PROG_CNT PROG_CNT, 
       EXP_TERM.TERM_CD EXP_GRAD_TERM,
       decode(trim(G.ADM_APPL_NBR),'-','',G.ADM_APPL_NBR) ADM_APPL_NBR,
       T.TERM_CD ADMIT_TERM_CD,
       P.DECLARE_DT PLAN_DECLARE_DT, 
       P.PLAN_SEQUENCE, 
       P.REQ_TERM PLAN_REQ_TERM, 
       P.COMPLETION_TERM PLAN_COMPL_TERM, 
       P.STDNT_DEGR PLAN_STDNT_DEGR, 
       P.DEGR_CHKOUT_STAT PLAN_DEGR_CHKOUT_STAT, 
       P.ADVIS_STATUS PLAN_ADVIS_STATUS,
       nvl(E1.ENRL_CREDIT_SUM,0) ENRL_CREDIT_SUM,       -- Added Nov 2015 
       L1.EFFDT DEG_LIMIT_EFFDT,                        -- Added Jan 2016  
       L1.UM_OVRRIDE_EXTENSN DEG_LIMIT_UM_OVRRIDE_EXTENSN, 
       L1.NEW_LIMIT_TERM_CD, 
       L1.CALC_LIMIT_TERM_CD, 
       coalesce(L1.YEARS, L2.YEARS) DEG_LIMIT_YEARS, 
       L1.COMMENTS_MSGS DEG_LIMIT_COMMENTS_MSGS,
       T1.ACAD_PLAN_TYPE ACAD_PLAN_TYPE,
       T1.MAJ_PT_ORDER,
       case when G.INSTITUTION_CD = 'UMDAR' and G.ACAD_CAR_CD = 'GRAD' and T1.ACAD_PLAN_TYPE = 'POS'
            then 'MAJ'
       else T1.ACAD_PLAN_TYPE end UM_PLAN_TYPE,      -- Remove later??? 
       S.DECLARE_DT SPLAN_DECLARE_DT, 
       nvl(S.REQ_TERM,'-') SPLAN_REQ_TERM,
       nvl(C.ACAD_LEVEL_BOT,'-') ACAD_LEVEL_BOT,
--       min(case when G.INSTITUTION_CD = 'UMBOS' and T1.ACAD_PLAN_TYPE in ('CRT','PMC') then 'Y'
--                when G.INSTITUTION_CD = 'UMDAR' and T1.ACAD_PLAN_TYPE in ('CRT') and P.ACAD_PLAN <> 'MTLHLT-CRT' then 'Y'
--                when G.INSTITUTION_CD = 'UMDAR' and T1.ACAD_PLAN_TYPE in ('OGC','PBC') then 'Y'
--                when G.INSTITUTION_CD = 'UMLOW' and T1.ACAD_PLAN_TYPE in ('CRT') then 'Y'
--                else 'N' end) 
--                over (partition by G.INSTITUTION_CD, G.ACAD_CAR_CD, G.STDNT_CAR_NUM, G.TERM_CD, G.PERSON_ID, G.SRC_SYS_ID) CERT_ONLY_FLG,       -- Dec 2016 
       min(case when G.INSTITUTION_CD = 'UMBOS' and T1.ACAD_PLAN_TYPE in ('CRT','PMC') then 'Y'
                when G.INSTITUTION_CD = 'UMDAR' and T1.ACAD_PLAN_TYPE in ('CRT') and P.ACAD_PLAN <> 'MTLHLT-CRT' then 'Y'
                when G.INSTITUTION_CD = 'UMDAR' and T1.ACAD_PLAN_TYPE in ('OGC','PBC') then 'Y'
                when G.INSTITUTION_CD = 'UMLOW' and T1.ACAD_PLAN_TYPE in ('CRT') then 'Y'
                else 'N' end) 
                over (partition by G.INSTITUTION_CD, G.ACAD_CAR_CD, G.TERM_CD, G.PERSON_ID, G.SRC_SYS_ID) CERT_ONLY_FLG,       -- Dec 2017 
       min(to_char(P.DECLARE_DT,'YYYYMMDD')||P.PLAN_SEQUENCE) over (partition by G.INSTITUTION_CD, G.ACAD_CAR_CD, G.ACAD_PROG_CD, G.PERSON_ID, G.SRC_SYS_ID) MIN_DECLARE_DT,
       T1.EDU_LVL,
       T1.DEGREE,                                -- Nov 2017 
       case when ACAD_PROG_CD = 'ND-U' then 100
            when ACAD_PROG_CD = 'ND-G' then 99
            when T1.INV_ED_LVL is null and G.ACAD_CAR_CD in ('UGRD', 'CSCE') then 87
            when T1.INV_ED_LVL is null and G.ACAD_CAR_CD in ('GRAD') then 83
       else T1.INV_ED_LVL end ED_LVL_RANK,       -- Remove later??? 
       min(case when ACAD_PROG_CD = 'ND-U' then 100
                when ACAD_PROG_CD = 'ND-G' then 99
                when T1.INV_ED_LVL is null and G.ACAD_CAR_CD in ('UGRD', 'CSCE') then 87
                when T1.INV_ED_LVL is null and G.ACAD_CAR_CD in ('GRAD') then 83
           else T1.INV_ED_LVL end) over (partition by G.INSTITUTION_CD, G.ACAD_CAR_CD, G.ACAD_PROG_CD, G.TERM_CD, G.PERSON_ID, G.SRC_SYS_ID) MIN_ED_LVL_RANK,      -- Remove later???  
--       nvl(max(C.UNT_TAKEN_PRGRSS) over (partition by G.INSTITUTION_CD, G.ACAD_CAR_CD, G.PERSON_ID, G.SRC_SYS_ID),0) MAX_UNT_TAKEN_PRGRSS,
       count(distinct G.ACAD_CAR_CD) over (partition by G.INSTITUTION_CD, G.TERM_CD, G.PERSON_ID, G.STDNT_CAR_NUM, G.SRC_SYS_ID) CAR_CNT,  -- Added Nov 2015 
       count(distinct G.STDNT_CAR_NUM) over (partition by G.INSTITUTION_CD, G.ACAD_CAR_CD, G.TERM_CD, G.PERSON_ID, G.SRC_SYS_ID) STACK_CNT  -- Added Oct 2015 
  from CSMRT_OWNER.UM_F_ACAD_PROG G 
  join CSMRT_OWNER.PS_D_TERM EXP_TERM
    on G.EXP_GRAD_TERM_SID = EXP_TERM.TERM_SID
--   and G.PERSON_ID = '01545694'                     -- Temp!!! 
--   and G.INSTITUTION_CD = 'UMDAR' and G.TERM_CD = '2710'                           -- Temp!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! 
  join CSMRT_OWNER.PS_D_PROG_STAT PS                -- Added Oct 2015 
    on G.PROG_STAT_SID = PS.PROG_STAT_SID           -- Added Oct 2015 
  join CSSTG_OWNER.PS_ACAD_PLAN P 
    on G.PERSON_ID = P.EMPLID
   and G.ACAD_CAR_CD = P.ACAD_CAREER
   and G.STDNT_CAR_NUM = P.STDNT_CAR_NBR
   and G.EFFDT = P.EFFDT
   and G.EFFSEQ = P.EFFSEQ
   and G.SRC_SYS_ID = P.SRC_SYS_ID  
   and P.DATA_ORIGIN <> 'D'
   and P.ACAD_PLAN <> '-' 
  join PS_D_TERM T
    on G.ADMIT_TERM_SID = T.TERM_SID
  join PS_D_TERM T2
    on G.TERM_SID = T2.TERM_SID         -- Sept 2018 
  left outer join CSSTG_OWNER.PS_ACAD_SUBPLAN S
    on P.EMPLID = S.EMPLID
   and P.ACAD_CAREER = S.ACAD_CAREER
   and P.STDNT_CAR_NBR = S.STDNT_CAR_NBR
   and P.EFFDT = S.EFFDT
   and P.EFFSEQ = S.EFFSEQ
   and P.ACAD_PLAN = S.ACAD_PLAN
   and P.SRC_SYS_ID = S.SRC_SYS_ID  
   and S.DATA_ORIGIN <> 'D'
  left outer join CSSTG_OWNER.PS_STDNT_CAR_TERM C
    on G.PERSON_ID = C.EMPLID
   and G.ACAD_CAR_CD = C.ACAD_CAREER
   and G.INSTITUTION_CD = C.INSTITUTION
   and G.TERM_CD = C.STRM
   and G.SRC_SYS_ID = P.SRC_SYS_ID  
   and C.DATA_ORIGIN <> 'D'
  left outer join E1
    on G.INSTITUTION_CD = E1.INSTITUTION
   and G.ACAD_CAR_CD = E1.ACAD_CAREER
   and G.TERM_CD = E1.STRM
   and G.PERSON_ID = E1.EMPLID
  left outer join L1
    on G.INSTITUTION_CD = L1.INSTITUTION 
   and G.ACAD_CAR_CD = L1.ACAD_CAREER 
   and G.PERSON_ID = L1.EMPLID
   and G.STDNT_CAR_NUM = L1.STDNT_CAR_NBR 
   and G.ACAD_PROG_CD = L1.ACAD_PROG
   and P.ACAD_PLAN = L1.ACAD_PLAN 
   and nvl(S.ACAD_SUB_PLAN,'-') = L1.ACAD_SUB_PLAN 
   and G.SRC_SYS_ID = L1.SRC_SYS_ID
   and L1.DEG_LIMIT_ORDER = 1
   and L1.EFF_STATUS = 'A'
  left outer join L2
    on G.INSTITUTION_CD = L2.INSTITUTION 
   and G.ACAD_CAR_CD = L2.ACAD_CAREER 
   and G.ACAD_PROG_CD = L2.ACAD_PROG
   and P.ACAD_PLAN = L2.ACAD_PLAN 
   and nvl(S.ACAD_SUB_PLAN,'-') = L2.ACAD_SUB_PLAN 
   and G.SRC_SYS_ID = L2.SRC_SYS_ID
   and L2.DEG_LIMIT_ORDER2 = 1
  left outer join T1
    on G.INSTITUTION_CD = T1.INSTITUTION 
   and P.ACAD_PLAN = T1.ACAD_PLAN
   and P.SRC_SYS_ID = T1.SRC_SYS_ID
--   and T1.PT_ORDER = 1
   and T2.TERM_END_DT between T1.EFFDT_START and T1.EFFDT_END      -- Sept 2018  
   ),
P2 as (
select /*+ parallel(8) inline */ 
       P1.INSTITUTION_CD, 
       P1.ACAD_CAR_CD, 
       P1.TERM_CD, 
       P1.STDNT_CAR_NUM, 
       P1.PERSON_ID, 
       P1.ACAD_PROG_CD, 
       P1.ACAD_PLAN_CD, 
       P1.ACAD_SPLAN_CD, 
       P1.SRC_SYS_ID,
       P1.EFFDT, 
       P1.EFFSEQ, 
       P1.PERSON_SID, 
       P1.MIN_PROG_STAT_CATGRY, 
       P1.PROG_STAT_CATGRY, 
       P1.PROG_CNT, 
       P1.EXP_GRAD_TERM, 
       P1.PLAN_DECLARE_DT, 
       P1.EDU_LVL,
       P1.DEGREE,                                -- Nov 2017 
       P1.PLAN_SEQUENCE, 
       P1.PLAN_REQ_TERM, 
       P1.PLAN_COMPL_TERM, 
       P1.PLAN_STDNT_DEGR, 
       P1.PLAN_DEGR_CHKOUT_STAT, 
       P1.PLAN_ADVIS_STATUS, 
       P1.ACAD_PLAN_TYPE,
       P1.UM_PLAN_TYPE, 
       P1.SPLAN_DECLARE_DT, 
       P1.SPLAN_REQ_TERM, 
       P1.DEG_LIMIT_EFFDT, 
       P1.DEG_LIMIT_UM_OVRRIDE_EXTENSN, 
       P1.NEW_LIMIT_TERM_CD, 
--       P1.CALC_LIMIT_TERM_CD,
trim(to_char(
CASE 
WHEN P1.ADMIT_TERM_CD not between '0000' and '9999' 
THEN NULL
WHEN P1.DEG_LIMIT_YEARS is NULL
THEN NULL
WHEN trim(P1.CALC_LIMIT_TERM_CD) is not NULL
THEN to_number(trim(P1.CALC_LIMIT_TERM_CD),'9999')
WHEN(trim(P1.CALC_LIMIT_TERM_CD) IS NULL AND TO_NUMBER(P1.ADMIT_TERM_CD, '9999') > 1005 AND SUBSTR(P1.ADMIT_TERM_CD,3,2) = '10') 
THEN (TO_NUMBER(P1.ADMIT_TERM_CD, '9999') + ((NVL(P1.DEG_LIMIT_YEARS, 1) * 100) - 80)) 
WHEN(trim(P1.CALC_LIMIT_TERM_CD) IS NULL AND TO_NUMBER(P1.ADMIT_TERM_CD, '9999') > 1005 AND SUBSTR(P1.ADMIT_TERM_CD,3,2) = '20') 
THEN (TO_NUMBER(P1.ADMIT_TERM_CD, '9999') + ((NVL(P1.DEG_LIMIT_YEARS, 1) * 100) - 10)) 
WHEN(trim(P1.CALC_LIMIT_TERM_CD) IS NULL AND TO_NUMBER(P1.ADMIT_TERM_CD, '9999') > 1005 AND SUBSTR(P1.ADMIT_TERM_CD,3,2) = '30') 
THEN (TO_NUMBER(P1.ADMIT_TERM_CD, '9999') + ((NVL(P1.DEG_LIMIT_YEARS, 1) * 100) - 20)) 
WHEN(trim(P1.CALC_LIMIT_TERM_CD) IS NULL AND TO_NUMBER(P1.ADMIT_TERM_CD, '9999') > 1005 AND SUBSTR(P1.ADMIT_TERM_CD,3,2) = '40') 
THEN (TO_NUMBER(P1.ADMIT_TERM_CD, '9999') + ((NVL(P1.DEG_LIMIT_YEARS, 1) * 100) - 10)) 
WHEN(trim(P1.CALC_LIMIT_TERM_CD) IS NULL AND TO_NUMBER(P1.ADMIT_TERM_CD, '9999') > 1005 AND SUBSTR(P1.ADMIT_TERM_CD,3,2) = '50') 
THEN (TO_NUMBER(P1.ADMIT_TERM_CD, '9999') + ((NVL(P1.DEG_LIMIT_YEARS, 1) * 100) - 20)) 
WHEN(trim(P1.CALC_LIMIT_TERM_CD) IS NULL AND TO_NUMBER(P1.ADMIT_TERM_CD, '9999') <= 1005 AND SUBSTR(P1.ADMIT_TERM_CD,4,1) = '1') 
THEN (TO_NUMBER(P1.ADMIT_TERM_CD, '9999') + ((NVL(P1.DEG_LIMIT_YEARS, 1) * 10) + 5)) 
WHEN(trim(P1.CALC_LIMIT_TERM_CD) IS NULL AND TO_NUMBER(P1.ADMIT_TERM_CD, '9999') <= 1005 AND SUBSTR(P1.ADMIT_TERM_CD,4,1) = '2') 
THEN (TO_NUMBER(P1.ADMIT_TERM_CD, '9999') + ((NVL(P1.DEG_LIMIT_YEARS, 1) * 10) + 4)) 
WHEN(trim(P1.CALC_LIMIT_TERM_CD) IS NULL AND TO_NUMBER(P1.ADMIT_TERM_CD, '9999') <= 1005 AND SUBSTR(P1.ADMIT_TERM_CD,4,1) = '4') 
THEN (TO_NUMBER(P1.ADMIT_TERM_CD, '9999') + ((NVL(P1.DEG_LIMIT_YEARS, 1) * 10) + 8)) 
WHEN(trim(P1.CALC_LIMIT_TERM_CD) IS NULL AND TO_NUMBER(P1.ADMIT_TERM_CD, '9999') <= 1005 AND SUBSTR(P1.ADMIT_TERM_CD,4,1) = '5') 
THEN (TO_NUMBER(P1.ADMIT_TERM_CD, '9999') + ((NVL(P1.DEG_LIMIT_YEARS, 1) * 10) + 7)) 
WHEN(trim(P1.CALC_LIMIT_TERM_CD) IS NULL AND TO_NUMBER(P1.ADMIT_TERM_CD, '9999') <= 1005 AND SUBSTR(P1.ADMIT_TERM_CD,4,1) = '6') 
THEN (TO_NUMBER(P1.ADMIT_TERM_CD, '9999') + ((NVL(P1.DEG_LIMIT_YEARS, 1) * 10) + 6))
ELSE NULL END 
,'0999')) CALC_LIMIT_TERM_CD,
       P1.DEG_LIMIT_YEARS, 
       P1.DEG_LIMIT_COMMENTS_MSGS, 
       P1.ACAD_LEVEL_BOT, 
       P1.CERT_ONLY_FLG, 
       P1.CAR_CNT, 
       P1.STACK_CNT,
--       max(case when P1.UM_PLAN_TYPE = 'MAJ' 
--                then 'Y'
--           else 'N' end) over (partition by P1.INSTITUTION_CD, P1.ACAD_CAR_CD, P1.ACAD_PROG_CD, P1.TERM_CD, P1.PERSON_ID, P1.SRC_SYS_ID) DEGR_SEEK_FLG,
       max(case when P1.INSTITUTION_CD = 'UMBOS' and P1.UM_PLAN_TYPE = 'MAJ' then 'Y'
                when P1.INSTITUTION_CD = 'UMDAR' and P1.UM_PLAN_TYPE in ('MAJ','OBA','OBS','OGP','POS') then 'Y'
                when P1.INSTITUTION_CD = 'UMLOW' and P1.UM_PLAN_TYPE = 'MAJ' then 'Y'
                else 'N' end) 
                over (partition by P1.INSTITUTION_CD, P1.ACAD_CAR_CD, P1.ACAD_PROG_CD, P1.TERM_CD, P1.PERSON_ID, P1.SRC_SYS_ID) DEGR_SEEK_FLG,      -- Dec 2016 
       P1.MIN_DECLARE_DT,
       P1.ED_LVL_RANK,
       (case when P1.ACAD_PLAN_TYPE = 'MIN' then NULL else
       dense_rank() over (partition by P1.INSTITUTION_CD, P1.ACAD_CAR_CD, P1.TERM_CD, P1.PERSON_ID, P1.STDNT_CAR_NUM, P1.SRC_SYS_ID
                              order by P1.PROG_STAT_CATGRY,         -- Changed Oct 2015  
                                       P1.EDU_LVL desc nulls last, 
                                       P1.MAJ_PT_ORDER, 
                                       P1.PLAN_DECLARE_DT nulls last, 
                                       P1.PLAN_SEQUENCE nulls last, 
                                       P1.ADM_APPL_NBR nulls last,          -- May 2016  
                                       P1.ACAD_PLAN_CD) end) MAJOR_RANK,
       (case when P1.ACAD_PLAN_TYPE <> 'MIN' then NULL else
       dense_rank() over (partition by P1.INSTITUTION_CD, P1.ACAD_CAR_CD, P1.TERM_CD, P1.PERSON_ID, P1.STDNT_CAR_NUM, P1.SRC_SYS_ID
                              order by case when P1.ACAD_PLAN_TYPE = 'MIN' then 0 else 9 end, 
                                       P1.PROG_STAT_CATGRY,         -- Changed Oct 2015  
                                       P1.EDU_LVL desc nulls last, 
                                       P1.PLAN_DECLARE_DT nulls last, 
                                       P1.PLAN_SEQUENCE nulls last, 
                                       P1.ADM_APPL_NBR nulls last,          -- May 2016  
                                       P1.ACAD_PLAN_CD) end) MINOR_RANK,
       (case when P1.ACAD_PLAN_TYPE = 'MIN' then NULL
             when P1.ACAD_SPLAN_CD = '-' then NULL 
        else
       dense_rank() over (partition by P1.INSTITUTION_CD, P1.ACAD_CAR_CD, P1.TERM_CD, P1.PERSON_ID, P1.STDNT_CAR_NUM, P1.ACAD_PLAN_CD, P1.SRC_SYS_ID
                              order by (case when P1.INSTITUTION_CD = 'UMBOS' and P1.ACAD_SPLAN_CD in ('CAPE COD', 'HARBOR', 'MGT3YD') then 8
                                             when P1.INSTITUTION_CD = 'UMDAR' and (P1.ACAD_SPLAN_CD in ('CAPE') or P1.ACAD_SPLAN_CD like '%MAT') then 8
                                             when P1.ACAD_SPLAN_CD = '-' or P1.ACAD_SPLAN_CD is null then 9 
                                        else 1 end), 
                                       P1.SPLAN_DECLARE_DT nulls last,
                                       P1.ACAD_SPLAN_CD nulls last) end) SPLAN_RANK,
       dense_rank() over (partition by P1.INSTITUTION_CD, P1.ACAD_CAR_CD, P1.TERM_CD, P1.PERSON_ID, P1.SRC_SYS_ID
                              order by P1.PROG_STAT_CATGRY,         -- Changed Oct 2015  
                                       P1.EDU_LVL desc nulls last, 
                                       P1.MAJ_PT_ORDER, 
                                       P1.PLAN_DECLARE_DT nulls last, 
                                       P1.PLAN_SEQUENCE nulls last, 
                                       P1.ADM_APPL_NBR nulls last,          -- May 2016  
                                       P1.ACAD_PLAN_CD nulls last,
                                       P1.STDNT_CAR_NUM desc nulls last     -- Mar 2018 
                                       ) PRIM_STACK_CAREER_RANK,
       dense_rank() over (partition by P1.INSTITUTION_CD, P1.TERM_CD, P1.PERSON_ID, P1.SRC_SYS_ID
                              order by P1.PROG_STAT_CATGRY,         -- Changed Oct 2015  
                                       (case when P1.ENRL_CREDIT_SUM > 0 then 1 else 0 end) desc, 
                                       P1.EDU_LVL desc nulls last, 
                                       P1.MAJ_PT_ORDER, 
                                       P1.PLAN_DECLARE_DT nulls last, 
                                       P1.PLAN_SEQUENCE nulls last, 
                                       P1.ADM_APPL_NBR nulls last,          -- May 2016  
                                       P1.ACAD_PLAN_CD) PRIM_STACK_STDNT_RANK,
       (case when (P1.ACAD_PLAN_TYPE = 'MIN' or (P1.INSTITUTION_CD = 'UMBOS' and P1.ACAD_PLAN_TYPE = 'POS')
                                             or (P1.INSTITUTION_CD = 'UMDAR' and P1.ACAD_PLAN_TYPE = 'POS' and P1.ACAD_PLAN_CD in ('HONCOLL','COMMHON'))) then NULL else  -- Case 47629, August 2020
       dense_rank() over (partition by P1.INSTITUTION_CD, P1.ACAD_CAR_CD, P1.TERM_CD, P1.PERSON_ID, P1.SRC_SYS_ID     -- Added Jan 2016 
                              order by P1.PROG_STAT_CATGRY, 
                                       P1.EDU_LVL desc nulls last, 
                                       P1.MAJ_PT_ORDER, 
                                       P1.PLAN_DECLARE_DT nulls last, 
                                       P1.PLAN_SEQUENCE nulls last, 
                                       P1.ADM_APPL_NBR nulls last,          -- Sept 2018  
                                       P1.ACAD_PLAN_CD) end) MAJOR_RANK_PIVOT,
       (case when (P1.ACAD_PLAN_TYPE <> 'MIN' or (P1.INSTITUTION_CD = 'UMBOS' and P1.ACAD_PLAN_TYPE = 'POS')
                                              or (P1.INSTITUTION_CD = 'UMDAR' and P1.ACAD_PLAN_TYPE = 'POS' and P1.ACAD_PLAN_CD in ('HONCOLL','COMMHON'))) then NULL else   -- Case 47629, August 2020
       dense_rank() over (partition by P1.INSTITUTION_CD, P1.ACAD_CAR_CD, P1.TERM_CD, P1.PERSON_ID, P1.SRC_SYS_ID
                              order by case when P1.ACAD_PLAN_TYPE = 'MIN' then 0 else 9 end, 
                                       P1.PROG_STAT_CATGRY, 
                                       P1.EDU_LVL desc nulls last, 
                                       P1.PLAN_DECLARE_DT nulls last, 
                                       P1.PLAN_SEQUENCE nulls last, 
                                       P1.ADM_APPL_NBR nulls last,          -- Sept 2018  
                                       P1.ACAD_PLAN_CD) end) MINOR_RANK_PIVOT,
       (case when not ((P1.INSTITUTION_CD = 'UMBOS' and P1.ACAD_PLAN_TYPE = 'POS') or 
                       (P1.INSTITUTION_CD = 'UMDAR' and P1.ACAD_PLAN_TYPE = 'POS' and P1.ACAD_PLAN_CD in ('HONCOLL','COMMHON'))) then NULL else  -- Case 47629, August 2020 
       dense_rank() over (partition by P1.INSTITUTION_CD, P1.ACAD_CAR_CD, P1.TERM_CD, P1.PERSON_ID, P1.SRC_SYS_ID
                              order by case when P1.ACAD_PLAN_TYPE = 'POS' then 0 else 9 end, 
                                       P1.PROG_STAT_CATGRY, 
                                       P1.EDU_LVL desc nulls last, 
                                       P1.PLAN_DECLARE_DT nulls last, 
                                       P1.PLAN_SEQUENCE nulls last, 
                                       P1.ADM_APPL_NBR nulls last,          -- Sept 2018  
                                       P1.ACAD_PLAN_CD) end) POS_RANK_PIVOT,
       P1.MIN_ED_LVL_RANK,             -- Remove later??? 
       case when P1.PROG_CNT = 1
            then dense_rank() over (partition by P1.INSTITUTION_CD, P1.ACAD_CAR_CD, P1.TERM_CD, P1.PERSON_ID, P1.SRC_SYS_ID
                                        order by P1.ACAD_PROG_CD)
            when P1.PROG_CNT > 1
            then dense_rank() over (partition by P1.INSTITUTION_CD, P1.ACAD_CAR_CD, P1.TERM_CD, P1.PERSON_ID, P1.SRC_SYS_ID
                                        order by P1.MIN_PROG_STAT_CATGRY, P1.MIN_ED_LVL_RANK, P1.MIN_DECLARE_DT, P1.ACAD_PROG_CD) end D_RANK,               -- Remove later??? 
       dense_rank() over (partition by P1.INSTITUTION_CD, P1.ACAD_CAR_CD, P1.TERM_CD, P1.PERSON_ID, P1.ACAD_PROG_CD, P1.SRC_SYS_ID, P1.UM_PLAN_TYPE
                                        order by P1.ED_LVL_RANK, to_char(P1.PLAN_DECLARE_DT,'YYYYMMDD')||P1.PLAN_SEQUENCE, P1.ACAD_PLAN_CD) D_RANK_PTYPE,   -- Remove later??? 
       dense_rank() over (partition by P1.INSTITUTION_CD, P1.ACAD_CAR_CD, P1.TERM_CD, P1.PERSON_ID, P1.ACAD_PROG_CD, P1.ACAD_PLAN_CD, P1.SRC_SYS_ID
                                        order by P1.ACAD_SPLAN_CD) D_RANK_SPLAN                                                                             -- Remove later??? 
from P1),
P3 as (
select /*+ parallel(8) inline */ 
       P2.INSTITUTION_CD, P2.ACAD_CAR_CD, P2.TERM_CD, P2.STDNT_CAR_NUM, P2.PERSON_ID, P2.ACAD_PROG_CD, P2.ACAD_PLAN_CD, P2.ACAD_SPLAN_CD, P2.SRC_SYS_ID,
       P2.EFFDT, P2.EFFSEQ, 
       P2.PERSON_SID, 
       P2.MIN_PROG_STAT_CATGRY, P2.PROG_CNT, P2.EXP_GRAD_TERM,  
       P2.PLAN_DECLARE_DT, P2.PLAN_SEQUENCE, P2.PLAN_REQ_TERM, P2.PLAN_COMPL_TERM, P2.PLAN_STDNT_DEGR, P2.PLAN_DEGR_CHKOUT_STAT, P2.PLAN_ADVIS_STATUS,
       P2.ACAD_PLAN_TYPE, P2.UM_PLAN_TYPE, P2.SPLAN_DECLARE_DT, P2.SPLAN_REQ_TERM,
       P2.DEG_LIMIT_EFFDT, P2.DEG_LIMIT_UM_OVRRIDE_EXTENSN, P2.NEW_LIMIT_TERM_CD, P2.CALC_LIMIT_TERM_CD, P2.DEG_LIMIT_YEARS, P2.DEG_LIMIT_COMMENTS_MSGS,
       P2.ACAD_LEVEL_BOT, P2.CERT_ONLY_FLG, P2.DEGR_SEEK_FLG, P2.MIN_DECLARE_DT,
       nvl(P2.MAJOR_RANK,0) MAJOR_RANK, nvl(P2.MINOR_RANK,0) MINOR_RANK, nvl(P2.SPLAN_RANK,0) SPLAN_RANK,
--       case when P2.STACK_CNT = 1 then 1 else nvl(P2.PRIM_STACK_CAREER_RANK,0) end PRIM_STACK_CAREER_RANK,
       min(P2.PRIM_STACK_CAREER_RANK) over (partition by P2.INSTITUTION_CD, P2.ACAD_CAR_CD, P2.TERM_CD, P2.STDNT_CAR_NUM, P2.PERSON_ID, P2.SRC_SYS_ID) PRIM_STACK_CAREER_RANK,      -- Changed Mar 2016 
--       case when P2.STACK_CNT = 1 and P2.CAR_CNT = 1 then 1 else nvl(P2.PRIM_STACK_STDNT_RANK,0) end PRIM_STACK_STDNT_RANK,
       min(P2.PRIM_STACK_STDNT_RANK) over (partition by P2.INSTITUTION_CD, P2.ACAD_CAR_CD, P2.TERM_CD, P2.PERSON_ID, P2.SRC_SYS_ID) PRIM_STACK_STDNT_RANK,      -- Changed Mar 2016 
       nvl(P2.MAJOR_RANK_PIVOT,0) MAJOR_RANK_PIVOT,
       nvl(P2.MINOR_RANK_PIVOT,0) MINOR_RANK_PIVOT,
       nvl(P2.POS_RANK_PIVOT,0) POS_RANK_PIVOT,
       P2.ED_LVL_RANK, P2.MIN_ED_LVL_RANK, P2.D_RANK, P2.D_RANK_PTYPE, P2.D_RANK_SPLAN, 
       P2.DEGREE,                                -- Nov 2017 
       case when P2.PROG_CNT = 1
            then 'PRIMARY'
            when P2.PROG_CNT >= 2 and P2.INSTITUTION_CD = 'UMBOS' 
             and P2.ACAD_PROG_CD in ('MGT-U', 'MGT-G', 'CPCSU', 'CPCSG', 'CCDE', 'NUR-U', 'NUR-G', 'CSM-U', 'CSM-G')
            then 'PRIMARY'
            when P2.PROG_CNT >= 2 and P2.INSTITUTION_CD = 'UMBOS' and P2.D_RANK = 1
            then 'PRIMARY'
            when P2.PROG_CNT >= 2 and P2.INSTITUTION_CD = 'UMBOS' and P2.D_RANK = 2
            then 'SECONDARY'
            when P2.PROG_CNT >= 2 and P2.INSTITUTION_CD = 'UMDAR' and P2.D_RANK = 1
            then 'PRIMARY'
            when P2.PROG_CNT >= 2 and P2.INSTITUTION_CD = 'UMDAR' and P2.D_RANK = 2
            then 'SECONDARY'
            when P2.PROG_CNT >= 2 and P2.INSTITUTION_CD = 'UMLOW' 
             and P2.ED_LVL_RANK = 87
             and (P2.ACAD_PROG_CD in ('EN-U', 'EN-C', 'MG-U', 'MG-C')
              or (P2.ACAD_PROG_CD = 'HP-U' and P2.ACAD_PLAN_CD in ('NU-BS', 'CL-BS', 'EP-BS', 'HE-BS'))
              or (P2.ACAD_PROG_CD = 'HP-C' and P2.ACAD_PLAN_CD in ('NU-BS-CE'))
              or (P2.ACAD_PROG_CD like 'A%S-U' and P2.ACAD_PLAN_CD in ('MP-BM','MU-BM')))
            then 'PRIMARY'
            when P2.PROG_CNT >= 2 and P2.INSTITUTION_CD = 'UMLOW' and P2.D_RANK = 1 
             and not (P2.ED_LVL_RANK = 87
             and (P2.ACAD_PROG_CD in ('EN-U', 'EN-C', 'MG-U', 'MG-C')
              or (P2.ACAD_PROG_CD = 'HP-U' and P2.ACAD_PLAN_CD in ('NU-BS', 'CL-BS', 'EP-BS', 'HE-BS'))
              or (P2.ACAD_PROG_CD = 'HP-C' and P2.ACAD_PLAN_CD in ('NU-BS-CE'))
              or (P2.ACAD_PROG_CD like 'A%S-U' and P2.ACAD_PLAN_CD in ('MP-BM','MU-BM'))))
            then 'PRIMARY'
            when P2.PROG_CNT >= 2 and P2.INSTITUTION_CD = 'UMLOW' and P2.D_RANK = 2 
             and not (P2.ED_LVL_RANK = 87
             and (P2.ACAD_PROG_CD in ('EN-U', 'EN-C', 'MG-U', 'MG-C')
              or (P2.ACAD_PROG_CD = 'HP-U' and P2.ACAD_PLAN_CD in ('NU-BS', 'CL-BS', 'EP-BS', 'HE-BS'))
              or (P2.ACAD_PROG_CD = 'HP-C' and P2.ACAD_PLAN_CD in ('NU-BS-CE'))
              or (P2.ACAD_PROG_CD like 'A%S-U' and P2.ACAD_PLAN_CD in ('MP-BM','MU-BM'))))
            then 'SECONDARY'
       else 'UNKNOWN' end PROG_CATGRY  
from P2),
P4 as (
select /*+ parallel(8) inline */ 
 P3.INSTITUTION_CD, P3.ACAD_CAR_CD, P3.TERM_CD, P3.STDNT_CAR_NUM, P3.PERSON_ID, P3.ACAD_PROG_CD, P3.ACAD_PLAN_CD, P3.ACAD_SPLAN_CD, P3.SRC_SYS_ID,
 P3.EFFDT, P3.EFFSEQ, 
 P3.PERSON_SID, 
 P3.MIN_PROG_STAT_CATGRY, P3.PROG_CNT, P3.EXP_GRAD_TERM, P3.PLAN_DECLARE_DT, P3.PLAN_SEQUENCE, 
 P3.PLAN_REQ_TERM, P3.PLAN_COMPL_TERM, P3.PLAN_STDNT_DEGR, P3.PLAN_DEGR_CHKOUT_STAT, P3.PLAN_ADVIS_STATUS,
 P3.ACAD_PLAN_TYPE, P3.UM_PLAN_TYPE, P3.SPLAN_DECLARE_DT, P3.SPLAN_REQ_TERM,
 P3.DEG_LIMIT_EFFDT, P3.DEG_LIMIT_UM_OVRRIDE_EXTENSN, P3.NEW_LIMIT_TERM_CD, P3.CALC_LIMIT_TERM_CD, P3.DEG_LIMIT_YEARS, P3.DEG_LIMIT_COMMENTS_MSGS,
 P3.CERT_ONLY_FLG, P3.DEGR_SEEK_FLG, P3.MIN_DECLARE_DT,
 P3.MAJOR_RANK, P3.MINOR_RANK, P3.SPLAN_RANK,
-- P3.PRIM_STACK_CAREER_RANK, 
 dense_rank() over (partition by P3.INSTITUTION_CD, P3.ACAD_CAR_CD, P3.TERM_CD, P3.PERSON_ID, P3.SRC_SYS_ID
                        order by P3.PRIM_STACK_CAREER_RANK) PRIM_STACK_CAREER_RANK,      -- Changed Mar 2016 
-- P3.PRIM_STACK_STDNT_RANK, 
 dense_rank() over (partition by P3.INSTITUTION_CD, P3.TERM_CD, P3.PERSON_ID, P3.SRC_SYS_ID
                        order by P3.PRIM_STACK_STDNT_RANK) PRIM_STACK_STDNT_RANK,      -- Changed Mar 2016 
 P3.MAJOR_RANK_PIVOT, P3.MINOR_RANK_PIVOT, P3.POS_RANK_PIVOT,
 P3.ED_LVL_RANK, P3.MIN_ED_LVL_RANK, P3.D_RANK, P3.D_RANK_PTYPE, P3.D_RANK_SPLAN, P3.PROG_CATGRY,
 P3.DEGREE,                                -- Nov 2017 
-- case when P3.INSTITUTION_CD in ('UMBOS','UMLOW') 
--then ''
--when P3.ACAD_CAR_CD = 'GRAD' and P3.ACAD_PLAN_CD <> 'ND-G' 
--then 'GM'
--when P3.ACAD_CAR_CD = 'GRAD' and P3.ACAD_PLAN_CD =  'ND-G' 
--then 'SPG'
--when P3.ACAD_PLAN_CD =  'ND-U' 
--then 'SPU'
--when P3.EXP_GRAD_TERM not between '1010' and '5050' or P3.TERM_CD not between '1010' and '5050' 
--then ''
--when (to_number(P3.EXP_GRAD_TERM) - to_number(P3.TERM_CD)) <= 99  
--then 'SR'
--when (to_number(P3.EXP_GRAD_TERM) - to_number(P3.TERM_CD)) between 100 and 199  
--then 'JR'
--when (to_number(P3.EXP_GRAD_TERM) - to_number(P3.TERM_CD)) between 200 and 299  
--then 'SO'
--when (to_number(P3.EXP_GRAD_TERM) - to_number(P3.TERM_CD)) between 300 and 399  
--then 'FR'
--when P3.ACAD_CAR_CD = 'LAW' 
--then P3.ACAD_LEVEL_BOT  
-- else '-' end UMDAR_EDUCATION_LEVEL_IND,    -- Not finshed 
 case when P3.INSTITUTION_CD in ('UMBOS','UMLOW') 
      then ''
--      when P3.EXP_GRAD_TERM not between '1010' and '5050' or P3.TERM_CD not between '1010' and '5050' 
--      then ''
      when P3.ACAD_PROG_CD = 'ND-G' 
      then 'SPG'
      when P3.ACAD_PROG_CD =  'ND-U' 
      then 'SPU'
      when P3.ACAD_PROG_CD =  'ND-L' 
      then 'SPL'
      when P3.DEGREE in ('PHD','EDD','DNP') 
      then 'DR'
      when P3.DEGREE =  'JD' 
      then 'JD'
      when P3.DEGREE in ('GCT','PMC','PBC','OGC') 
      then 'GCT'
      when P3.DEGREE in ('UCT','CRT','OLCT') 
      then 'UCT'
      else P3.ACAD_LEVEL_BOT 
  end UMDAR_ED_LVL,                 -- Nov 2017  
 case when P3.PROG_CATGRY = 'PRIMARY' and P3.CERT_ONLY_FLG = 'Y' and P3.ACAD_PLAN_TYPE||to_char(P3.D_RANK_PTYPE) = 'CRT1'
then 1
when P3.PROG_CATGRY = 'PRIMARY' and P3.INSTITUTION_CD = 'UMBOS' and P3.ACAD_PLAN_TYPE||to_char(P3.D_RANK_PTYPE) in ('COS1', 'PMC1',  'PDV1','OUC1','OBA1','OGC1','OGP1','OBS1', 'HON1')
then 1
when P3.PROG_CATGRY = 'PRIMARY' and P3.INSTITUTION_CD = 'UMDAR' and P3.ACAD_PLAN_TYPE||to_char(P3.D_RANK_PTYPE) in ('POS1','OUC1','OBA1','OGC1','OGP1','OBS1')
then 1
when P3.PROG_CATGRY = 'PRIMARY' and P3.INSTITUTION_CD in ('UMDAR','UMLOW') and P3.ACAD_PLAN_TYPE||to_char(P3.D_RANK_PTYPE) in ('NC1')
then 1
when P3.PROG_CATGRY = 'PRIMARY' and P3.ACAD_PLAN_TYPE||to_char(P3.D_RANK_PTYPE) in ('MAJ1','ND1')
then 1
 else 0 end PRIM_PROG_MAJOR_1_CNT, 
 case when P3.PROG_CATGRY = 'PRIMARY' and P3.CERT_ONLY_FLG = 'Y' and P3.ACAD_PLAN_TYPE||to_char(P3.D_RANK_PTYPE) = 'CRT2'
then 1
when P3.PROG_CATGRY = 'PRIMARY' and P3.INSTITUTION_CD = 'UMDAR' and P3.ACAD_PLAN_TYPE||to_char(P3.D_RANK_PTYPE) in ('POS2')
then 1
when P3.PROG_CATGRY = 'PRIMARY' and P3.INSTITUTION_CD in ('UMDAR','UMLOW') and P3.ACAD_PLAN_TYPE||to_char(P3.D_RANK_PTYPE) in ('NC1')
then 1
when P3.PROG_CATGRY = 'PRIMARY' and P3.ACAD_PLAN_TYPE||to_char(P3.D_RANK_PTYPE) in ('AMJ1','MAJ2','ND2')
then 1
 else 0 end PRIM_PROG_MAJOR_2_CNT, 
 case when P3.PROG_CATGRY = 'PRIMARY' and P3.ACAD_PLAN_TYPE||to_char(P3.D_RANK_PTYPE) = 'MIN1'
then 1
 else 0 end PRIM_PROG_MINOR_1_CNT, 
 case when P3.PROG_CATGRY = 'PRIMARY' and P3.ACAD_PLAN_TYPE||to_char(P3.D_RANK_PTYPE) = 'MIN2'
then 1
 else 0 end PRIM_PROG_MINOR_2_CNT, 
 case when P3.PROG_CATGRY = 'PRIMARY' and P3.CERT_ONLY_FLG = 'N' and P3.ACAD_PLAN_TYPE||to_char(P3.D_RANK_PTYPE) = 'CRT1'
then 1
when P3.PROG_CATGRY = 'PRIMARY' and P3.INSTITUTION_CD <> 'UMDAR' and P3.ACAD_PLAN_TYPE = 'POS'
then 1
when P3.PROG_CATGRY = 'PRIMARY' and P3.ACAD_PLAN_TYPE in ('MAJ','MIN') and P3.D_RANK_PTYPE > 2
then 1
when P3.PROG_CATGRY = 'PRIMARY' and P3.ACAD_PLAN_TYPE = 'AMJ' and P3.D_RANK_PTYPE > 1
then 1
when P3.PROG_CATGRY = 'PRIMARY' and P3.ACAD_PLAN_TYPE = 'MIN' and P3.D_RANK_PTYPE > 2
then 1
when P3.PROG_CATGRY = 'PRIMARY' and P3.ACAD_PLAN_TYPE = 'CON' 
then 1
when P3.PROG_CATGRY = 'PRIMARY' and P3.INSTITUTION_CD = 'UMBOS' and P3.ACAD_PLAN_TYPE = 'NC'
then 1
when P3.PROG_CATGRY = 'PRIMARY' and P3.INSTITUTION_CD in ('UMDAR','UMLOW') and P3.ACAD_PLAN_TYPE = 'NC' and P3.D_RANK_PTYPE > 1
then 1
 else 0 end PRIM_PROG_OTHER_PLAN_CNT,
 case when P3.PROG_CATGRY = 'SECONDARY' and P3.CERT_ONLY_FLG = 'Y' and P3.ACAD_PLAN_TYPE||to_char(P3.D_RANK_PTYPE) = 'CRT1'
then 1
when P3.PROG_CATGRY = 'SECONDARY' and P3.INSTITUTION_CD = 'UMDAR' and P3.ACAD_PLAN_TYPE||to_char(P3.D_RANK_PTYPE) in ('POS1')
then 1
when P3.PROG_CATGRY = 'SECONDARY' and P3.ACAD_PLAN_TYPE||to_char(P3.D_RANK_PTYPE) in ('MAJ1','ND1')
then 1
 else 0 end SEC_PROG_MAJOR_1_CNT,
 case when P3.PROG_CATGRY = 'SECONDARY' and P3.CERT_ONLY_FLG = 'Y' and P3.ACAD_PLAN_TYPE||to_char(P3.D_RANK_PTYPE) = 'CRT2'
then 1
when P3.PROG_CATGRY = 'SECONDARY' and P3.INSTITUTION_CD = 'UMDAR' and P3.ACAD_PLAN_TYPE||to_char(P3.D_RANK_PTYPE) in ('POS2')
then 1
when P3.PROG_CATGRY = 'SECONDARY' and P3.ACAD_PLAN_TYPE||to_char(P3.D_RANK_PTYPE) in ('AMJ1','MAJ2','ND2')
then 1
 else 0 end SEC_PROG_MAJOR_2_CNT, 
 case when P3.PROG_CATGRY = 'SECONDARY' and P3.ACAD_PLAN_TYPE||to_char(P3.D_RANK_PTYPE) = 'MIN1'
then 1
 else 0 end SEC_PROG_MINOR_1_CNT, 
 case when P3.PROG_CATGRY = 'SECONDARY' and P3.ACAD_PLAN_TYPE||to_char(P3.D_RANK_PTYPE) = 'MIN2'
then 1
 else 0 end SEC_PROG_MINOR_2_CNT, 
 case when P3.PROG_CATGRY = 'SECONDARY' and P3.CERT_ONLY_FLG = 'N' and P3.ACAD_PLAN_TYPE||to_char(P3.D_RANK_PTYPE) = 'CRT1'
then 1
when P3.PROG_CATGRY = 'SECONDARY' and P3.INSTITUTION_CD <> 'UMDAR' and P3.ACAD_PLAN_TYPE = 'POS'
then 1
when P3.PROG_CATGRY = 'SECONDARY' and P3.ACAD_PLAN_TYPE in ('MAJ','MIN') and P3.D_RANK_PTYPE > 2
then 1
when P3.PROG_CATGRY = 'SECONDARY' and P3.ACAD_PLAN_TYPE = 'AMJ' and P3.D_RANK_PTYPE > 1
then 1
when P3.PROG_CATGRY = 'SECONDARY' and P3.ACAD_PLAN_TYPE in ('CON','NC') 
then 1
 else 0 end SEC_PROG_OTHER_PLAN_CNT,
       (case when P3.PRIM_STACK_CAREER_RANK = 1 and P3.PRIM_STACK_STDNT_RANK = 1 and P3.MAJOR_RANK = 1 and P3.SPLAN_RANK <= 1
             then 1 
             else 0
         end) UNDUP_STDNT_CNT                               -- Dec 2017  
from P3)
select /*+ parallel(8) */ 
 P4.INSTITUTION_CD, 
 P4.ACAD_CAR_CD, 
 P4.TERM_CD, 
 P4.PERSON_ID, 
 P4.STDNT_CAR_NUM, 
 P4.ACAD_PROG_CD, 
 P4.ACAD_PLAN_CD, 
 P4.ACAD_SPLAN_CD, 
 P4.SRC_SYS_ID,
 P4.EFFDT, 
 P4.EFFSEQ, 
 nvl(T.INSTITUTION_SID,2147483646) INSTITUTION_SID, 
 nvl(T.ACAD_CAR_SID,2147483646) ACAD_CAR_SID, 
 nvl(T.TERM_SID,2147483646) TERM_SID, 
 nvl(P4.PERSON_SID,2147483646) PERSON_SID, 
 nvl(G.ACAD_PROG_SID,2147483646) ACAD_PROG_SID, 
 nvl(P.ACAD_PLAN_SID,2147483646) ACAD_PLAN_SID, 
 nvl(S.ACAD_SPLAN_SID,2147483646) ACAD_SPLAN_SID,
 nvl(T5.TERM_SID,2147483646) CALC_LIMIT_TERM_SID,
 nvl(T6.TERM_SID,2147483646) NEW_LIMIT_TERM_SID,
 nvl(T2.TERM_SID,2147483646) PLAN_COMPL_TERM_SID,
 nvl(T3.TERM_SID,2147483646) PLAN_REQ_TERM_SID,
 nvl(T4.TERM_SID,2147483646) SPLAN_REQ_TERM_SID,
 P4.CERT_ONLY_FLG, 
 P4.MAJOR_RANK, 
 P4.MINOR_RANK, 
 P4.SPLAN_RANK, 
 P4.PRIM_STACK_CAREER_RANK, 
 P4.PRIM_STACK_STDNT_RANK, 
 P4.MAJOR_RANK_PIVOT, 
 P4.MINOR_RANK_PIVOT, 
 P4.POS_RANK_PIVOT, 
 P4.D_RANK, 
 P4.D_RANK_PTYPE, 
 P4.D_RANK_SPLAN, 
 P4.DEGR_SEEK_FLG, 
 nvl(P4.ED_LVL_RANK, 0), 
 (case when P4.ACAD_PLAN_TYPE is null then 'Y' else 'N' end) NULL_PLAN_TYPE_FLG, 
 P4.PROG_CATGRY, 
 P4.PLAN_ADVIS_STATUS, 
 P4.PLAN_DECLARE_DT, 
 P4.PLAN_SEQUENCE, 
 P4.PLAN_DEGR_CHKOUT_STAT, 
 P4.PLAN_STDNT_DEGR,
 P4.SPLAN_DECLARE_DT, 
 P4.DEG_LIMIT_EFFDT, 
 P4.NEW_LIMIT_TERM_CD, 
 P4.CALC_LIMIT_TERM_CD, 
 P4.DEG_LIMIT_UM_OVRRIDE_EXTENSN, 
 P4.DEG_LIMIT_YEARS, 
 P4.DEG_LIMIT_COMMENTS_MSGS,
-- nvl(P4.UMDAR_ED_LVL, '-') UMDAR_ED_LVL, 
 nvl(max(decode(trim(to_char(P4.UNDUP_STDNT_CNT)),'1',P4.UMDAR_ED_LVL,'-')) over (partition by P4.INSTITUTION_CD, P4.TERM_CD, P4.PERSON_ID, P4.SRC_SYS_ID
                                                                                      order by P4.UNDUP_STDNT_CNT desc
                                                                                  rows between unbounded preceding and unbounded following),'-') UMDAR_ED_LVL,    -- Dec 2017 
 P4.PRIM_PROG_MAJOR_1_CNT, 
 P4.PRIM_PROG_MAJOR_2_CNT, 
 P4.PRIM_PROG_MINOR_1_CNT, 
 P4.PRIM_PROG_MINOR_2_CNT, 
 P4.PRIM_PROG_OTHER_PLAN_CNT, 
 P4.SEC_PROG_MAJOR_1_CNT, 
 P4.SEC_PROG_MAJOR_2_CNT, 
 P4.SEC_PROG_MINOR_1_CNT, 
 P4.SEC_PROG_MINOR_2_CNT, 
 P4.SEC_PROG_OTHER_PLAN_CNT,
 case when P4.ACAD_SPLAN_CD <> '-' and P4.PRIM_PROG_MAJOR_1_CNT = 1 and P4.D_RANK_SPLAN = 1 
then 1
 else 0 end PP_SUB_PLAN_11_CNT, 
 case when P4.ACAD_SPLAN_CD <> '-' and P4.PRIM_PROG_MAJOR_1_CNT = 1 and P4.D_RANK_SPLAN = 2 
then 1
 else 0 end PP_SUB_PLAN_12_CNT, 
 case when P4.ACAD_SPLAN_CD <> '-' and P4.PRIM_PROG_MAJOR_2_CNT = 1 and P4.D_RANK_SPLAN = 1 
then 1
 else 0 end PP_SUB_PLAN_21_CNT, 
 case when P4.ACAD_SPLAN_CD <> '-' and P4.PRIM_PROG_MAJOR_2_CNT = 1 and P4.D_RANK_SPLAN = 2 
then 1
 else 0 end PP_SUB_PLAN_22_CNT, 
 case when P4.ACAD_SPLAN_CD <> '-' and P4.SEC_PROG_MAJOR_1_CNT = 1 and P4.D_RANK_SPLAN = 1 
then 1
 else 0 end SP_SUB_PLAN_11_CNT, 
 case when P4.ACAD_SPLAN_CD <> '-' and P4.SEC_PROG_MAJOR_1_CNT = 1 and P4.D_RANK_SPLAN = 2 
then 1
 else 0 end SP_SUB_PLAN_12_CNT, 
 case when P4.ACAD_SPLAN_CD <> '-' and P4.SEC_PROG_MAJOR_2_CNT = 1 and P4.D_RANK_SPLAN = 1 
then 1
 else 0 end SP_SUB_PLAN_21_CNT, 
 case when P4.ACAD_SPLAN_CD <> '-' and P4.SEC_PROG_MAJOR_2_CNT = 1 and P4.D_RANK_SPLAN = 2 
then 1
 else 0 end SP_SUB_PLAN_22_CNT,
 'N','S',sysdate,sysdate,1234 
from P4
left outer join PS_D_TERM T
  on P4.INSTITUTION_CD = T.INSTITUTION_CD
 and P4.ACAD_CAR_CD = T.ACAD_CAR_CD
 and P4.TERM_CD = T.TERM_CD 
 and P4.SRC_SYS_ID = T.SRC_SYS_ID
left outer join UM_D_ACAD_PROG G
  on P4.INSTITUTION_CD = G.INSTITUTION_CD
 and P4.ACAD_PROG_CD = G.ACAD_PROG_CD
 and P4.SRC_SYS_ID = G.SRC_SYS_ID
 and G.EFFDT_ORDER = 1 
left outer join UM_D_ACAD_PLAN P
  on P4.INSTITUTION_CD = P.INSTITUTION_CD
 and P4.ACAD_PLAN_CD = P.ACAD_PLAN_CD
 and P4.SRC_SYS_ID = P.SRC_SYS_ID
 and P.EFFDT_ORDER = 1 
left outer join UM_D_ACAD_SPLAN S
  on P4.INSTITUTION_CD = S.INSTITUTION_CD
 and P4.ACAD_PLAN_CD = S.ACAD_PLAN_CD
 and P4.ACAD_SPLAN_CD = S.ACAD_SPLAN_CD
 and P4.SRC_SYS_ID = S.SRC_SYS_ID
 and S.EFFDT_ORDER = 1 
left outer join PS_D_TERM T2
  on P4.INSTITUTION_CD = T2.INSTITUTION_CD
 and P4.ACAD_CAR_CD = T2.ACAD_CAR_CD
 and P4.PLAN_COMPL_TERM = T2.TERM_CD 
 and P4.SRC_SYS_ID = T2.SRC_SYS_ID
left outer join PS_D_TERM T3
  on P4.INSTITUTION_CD = T3.INSTITUTION_CD
 and P4.ACAD_CAR_CD = T3.ACAD_CAR_CD
 and P4.PLAN_REQ_TERM = T3.TERM_CD 
 and P4.SRC_SYS_ID = T3.SRC_SYS_ID
left outer join PS_D_TERM T4
  on P4.INSTITUTION_CD = T4.INSTITUTION_CD
 and P4.ACAD_CAR_CD = T4.ACAD_CAR_CD
 and P4.SPLAN_REQ_TERM = T4.TERM_CD 
 and P4.SRC_SYS_ID = T4.SRC_SYS_ID
left outer join PS_D_TERM T5
  on P4.INSTITUTION_CD = T5.INSTITUTION_CD
 and P4.ACAD_CAR_CD = T5.ACAD_CAR_CD
 and P4.CALC_LIMIT_TERM_CD = T5.TERM_CD 
 and P4.SRC_SYS_ID = T5.SRC_SYS_ID
left outer join PS_D_TERM T6
  on P4.INSTITUTION_CD = T6.INSTITUTION_CD
 and P4.ACAD_CAR_CD = T6.ACAD_CAR_CD
 and P4.NEW_LIMIT_TERM_CD = T6.TERM_CD 
 and P4.SRC_SYS_ID = T6.SRC_SYS_ID
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_ACAD_PLAN rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_ACAD_PLAN',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );


strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_ACAD_PLAN',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_ACAD_PLAN';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
--alter table UM_F_ACAD_PLAN enable constraint PK_UM_F_ACAD_PLAN;

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_ACAD_PLAN enable constraint PK_UM_F_ACAD_PLAN';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_ACAD_PLAN');

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

END UM_F_ACAD_PLAN_P;
/
