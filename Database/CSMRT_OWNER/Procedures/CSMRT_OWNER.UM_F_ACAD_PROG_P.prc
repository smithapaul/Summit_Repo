DROP PROCEDURE CSMRT_OWNER.UM_F_ACAD_PROG_P
/

--
-- UM_F_ACAD_PROG_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_F_ACAD_PROG_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads table UM_F_ACAD_PROG.
--
 --V01  SMT-xxxx 07/02/2018,    James Doucette
--                              Converted from SQL Script
--
 --V02  SMT-7958 09/05/2018,    George Adams
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_ACAD_PROG';
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

strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_ACAD_PROG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_ACAD_PROG';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_ACAD_PROG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_ACAD_PROG');

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_ACAD_PROG disable constraint PK_UM_F_ACAD_PROG';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_ACAD_PROG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_ACAD_PROG';
insert /*+ append enable_parallel_dml parallel(8) */ into CSMRT_OWNER.UM_F_ACAD_PROG
with G1 as (
select /*+ parallel(8) inline no_merge */
       G.EMPLID,
       G.ACAD_CAREER,
       G.STDNT_CAR_NBR,
       G.EFFDT,
       G.EFFSEQ,
       G.SRC_SYS_ID,
       G.INSTITUTION,
       G.ACAD_PROG,
       G.PROG_STATUS,
       G.PROG_ACTION,
       G.ACTION_DT,
       G.PROG_REASON,
       G.ADMIT_TERM,
       G.EXP_GRAD_TERM,
       G.REQ_TERM,
--       G.ACAD_LOAD_APPR,
       G.CAMPUS,
       G.DEGR_CHKOUT_STAT,
       G.COMPLETION_TERM,
--       G.ACAD_PROG_DUAL,
--       G.JOINT_PROG_APPR,
       G.ADM_APPL_NBR,
--       G.APPL_PROG_NBR,
       G.DATA_FROM_ADM_APPL,
       max(decode(PROG_ACTION,'MATR',to_char(G.EFFDT,'YYYYMMDD')||trim(to_char(G.EFFSEQ,'00009')),'1900010100000'))
             over(partition by G.EMPLID, G.ACAD_CAREER, G.STDNT_CAR_NBR, G.SRC_SYS_ID) MATR_MAX_EFFDT_SEQ    -- Removed ACAD_PROG from stack key
  from CSSTG_OWNER.PS_ACAD_PROG G
 where G.DATA_ORIGIN <> 'D'
   and G.ACAD_PROG <> '-'
--   and G.EMPLID = '01328482' and G.STDNT_CAR_NBR = 0
),
G2 as (
select /*+ parallel(8) inline no_merge */
       G1.EMPLID,
       G1.ACAD_CAREER,
       G1.STDNT_CAR_NBR,
       G1.EFFDT,
       G1.EFFSEQ,
       G1.SRC_SYS_ID,
       G1.INSTITUTION,
       G1.ACAD_PROG,
       G1.PROG_STATUS,
       G1.PROG_ACTION,
       G1.ACTION_DT,
       G1.PROG_REASON,
       G1.ADMIT_TERM,
       G1.EXP_GRAD_TERM,
       G1.REQ_TERM,
       G1.CAMPUS,
       G1.DEGR_CHKOUT_STAT,
       max(G1.DEGR_CHKOUT_STAT) over (partition by G1.EMPLID, G1.ACAD_CAREER, G1.STDNT_CAR_NBR, G1.EFFDT, G1.SRC_SYS_ID
                                          order by decode(G1.DEGR_CHKOUT_STAT,'-',9,0), G1.EFFSEQ desc) DEGR_CHKOUT_LAST,
       max(decode(G1.DEGR_CHKOUT_STAT,'-','1900010100001-',to_char(G1.EFFDT,'YYYYMMDD')||trim(to_char(G1.EFFSEQ,'00009'))||G1.DEGR_CHKOUT_STAT))
       over (partition by G1.EMPLID, G1.ACAD_CAREER, G1.STDNT_CAR_NBR, G1.EXP_GRAD_TERM, G1.SRC_SYS_ID
                 order by decode(G1.DEGR_CHKOUT_STAT,'-',0,9), G1.EFFDT, G1.EFFSEQ
             rows between unbounded preceding and unbounded following) DEGR_CHKOUT_LAST_EGT,    -- Added Dec 2015
       G1.COMPLETION_TERM,
       G1.ADM_APPL_NBR,
       G1.DATA_FROM_ADM_APPL,
       ROW_NUMBER() over (partition by G1.EMPLID, G1.ACAD_CAREER, G1.STDNT_CAR_NBR, G1.SRC_SYS_ID
                              order by G1.EFFDT desc) EFFDT_ORDER,
       ROW_NUMBER() over (partition by G1.EMPLID, G1.ACAD_CAREER, G1.STDNT_CAR_NBR, G1.EFFDT, G1.SRC_SYS_ID
                              order by G1.EFFSEQ desc) PROG_ORDER,
       max(case when G1.INSTITUTION = 'UMBOS' and G1.PROG_ACTION = 'RADM' and G1.PROG_ACTION <> 'ACTG' then G1.EFFDT
                when G1.INSTITUTION = 'UMDAR' and (G1.PROG_ACTION in ('RADM','RLOA') or (G1.PROG_ACTION = 'ACTV' and G1.PROG_REASON in ('SDEG','TDEG'))) then G1.EFFDT
                when G1.INSTITUTION = 'UMLOW' and (G1.PROG_ACTION in ('RADM') or (G1.PROG_ACTION = 'ACTV' and G1.PROG_REASON in ('FRST', 'PRG2', 'PROB', 'RADM'))) then G1.EFFDT
                else to_date('01-JAN-1900') end)
                   over (partition by G1.EMPLID, G1.ACAD_CAREER, G1.STDNT_CAR_NBR, G1.SRC_SYS_ID
                             order by G1.EFFDT, G1.EFFSEQ
                         rows between unbounded preceding and current row) RADM_EFFDT,
       max(case when G1.INSTITUTION = 'UMBOS' and G1.PROG_ACTION = 'RADM' and G1.PROG_ACTION <> 'ACTG' then G1.ADMIT_TERM
                when G1.INSTITUTION = 'UMDAR' and (G1.PROG_ACTION in ('RADM','RLOA') or (G1.PROG_ACTION = 'ACTV' and G1.PROG_REASON in ('SDEG','TDEG'))) then G1.ADMIT_TERM
                when G1.INSTITUTION = 'UMLOW' and (G1.PROG_ACTION in ('RADM') or (G1.PROG_ACTION = 'ACTV' and G1.PROG_REASON in ('FRST', 'PRG2', 'PROB', 'RADM'))) then G1.ADMIT_TERM
                else '0000' end)
                   over (partition by G1.EMPLID, G1.ACAD_CAREER, G1.STDNT_CAR_NBR, G1.SRC_SYS_ID
                             order by G1.EFFDT, G1.EFFSEQ
                         rows between unbounded preceding and current row) RADM_ADMIT_TERM,
       replace(max(case when G1.PROG_STATUS = 'CM' and G1.COMPLETION_TERM <> '-' then G1.COMPLETION_TERM else '0000' end)
                   over (partition by G1.EMPLID, G1.ACAD_CAREER, G1.STDNT_CAR_NBR, G1.SRC_SYS_ID, G1.ACAD_PROG),'0000','9998') COMP_TERM,   -- Remove ACAG_PROG from stack key???
       min(G1.ADMIT_TERM) over (partition by G1.EMPLID, G1.ACAD_CAREER, G1.STDNT_CAR_NBR, G1.SRC_SYS_ID
                                    order by G1.EFFDT, G1.EFFSEQ
                          rows between current row and unbounded following) MIN_ADMIT_TERM,         -- Lower ADMIT_TERM fix
       case when MATR_MAX_EFFDT_SEQ <> '1900010100000'
            then min(decode(G1.PROG_ACTION,'MATR',G1.ADMIT_TERM,'9998')) over (partition by G1.EMPLID, G1.ACAD_CAREER, G1.STDNT_CAR_NBR, G1.SRC_SYS_ID)        -- STACK_BEGIN_TERM fix!!!
--            then min(decode(G1.PROG_STATUS,'AC',G1.ADMIT_TERM,'9998')) over (partition by G1.EMPLID, G1.ACAD_CAREER, G1.STDNT_CAR_NBR, G1.SRC_SYS_ID)
            else min(decode(G1.PROG_ACTION,'ACTV',G1.ADMIT_TERM,'9998')) over (partition by G1.EMPLID, G1.ACAD_CAREER, G1.STDNT_CAR_NBR, G1.SRC_SYS_ID)
--            else min(decode(G1.PROG_STATUS,'AC',G1.ADMIT_TERM,'9998')) over (partition by G1.EMPLID, G1.ACAD_CAREER, G1.STDNT_CAR_NBR, G1.SRC_SYS_ID)
             end STACK_BEGIN_TERM
  from G1
 where to_char(G1.EFFDT,'YYYYMMDD')||trim(to_char(G1.EFFSEQ,'00009')) >= G1.MATR_MAX_EFFDT_SEQ    -- Does not filter any careers that never had MATR??? CSCE???
),
G3 as (
select /*+ parallel(8) inline no_merge */
       G2.EMPLID,
       G2.ACAD_CAREER,
       G2.STDNT_CAR_NBR,
       G2.EFFDT,
       G2.EFFSEQ,
       G2.SRC_SYS_ID,
       G2.INSTITUTION,
       G2.ACAD_PROG,
       G2.PROG_STATUS,
       G2.PROG_ACTION,
       G2.ACTION_DT,
       G2.PROG_REASON,
       G2.ADMIT_TERM,
       G2.EXP_GRAD_TERM,
       G2.REQ_TERM,
       G2.CAMPUS,
       G2.DEGR_CHKOUT_STAT,
       G2.DEGR_CHKOUT_LAST,
       substr(G2.DEGR_CHKOUT_LAST_EGT,14,2) DEGR_CHKOUT_LAST_EGT,                           -- Added Dec 2015
       to_date(substr(G2.DEGR_CHKOUT_LAST_EGT,1,8),'YYYYMMDD') DEGR_CHKOUT_LAST_EGT_EFFDT,  -- Added Dec 2015
       G2.COMPLETION_TERM,
       G2.ADM_APPL_NBR,
       G2.DATA_FROM_ADM_APPL,
       G2.EFFDT_ORDER,
       G2.RADM_EFFDT,
       T1.FULL_TERM_FLAG,
       max(G2.RADM_EFFDT) over (partition by G2.EMPLID, G2.ACAD_CAREER, G2.STDNT_CAR_NBR, G2.SRC_SYS_ID, G2.ADMIT_TERM) ADMIT_RADM_EFFDT,   -- For backwards RADMs
       max(G2.RADM_ADMIT_TERM) over (partition by G2.EMPLID, G2.ACAD_CAREER, G2.STDNT_CAR_NBR, G2.SRC_SYS_ID, G2.ADMIT_TERM) ADMIT_RADM_TERM,   -- For backwards RADMs
       min(G2.ADMIT_TERM) over (partition by G2.EMPLID, G2.ACAD_CAREER, G2.STDNT_CAR_NBR, G2.SRC_SYS_ID) MIN_ADMIT_TERM,    -- STACK_BEGIN_TERM fix!!!
--       max(G2.EFFDT) over (partition by G2.EMPLID, G2.ACAD_CAREER, G2.STDNT_CAR_NBR, G2.SRC_SYS_ID, T1.STRM) MAX_TERM_EFFDT,   -- Lower ADMIT_TERM fix
       G2.STACK_BEGIN_TERM,
--       greatest(T1.STRM, G2.ADMIT_TERM, G2.STACK_BEGIN_TERM) ACAD_TERM,
--       greatest(T1.STRM, G2.ADMIT_TERM) ACAD_TERM,        -- STACK_BEGIN_TERM fix
       greatest(T1.STRM, G2.MIN_ADMIT_TERM) ACAD_TERM,        -- Lower ADMIT_TERM fix
       G2.COMP_TERM
  from G2
  join CSSTG_OWNER.UM_TERM_TBL_VW T1
    on G2.INSTITUTION = T1.INSTITUTION
   and G2.ACAD_CAREER = T1.ACAD_CAREER
   and G2.SRC_SYS_ID = T1.SRC_SYS_ID
   and G2.PROG_ORDER = 1      -- Keep all rows???
   and (((G2.EFFDT between T1.EARLY_BEGIN_DT and T1.TERM_END_DT) and G2.PROG_STATUS IN ('AC', 'AD', 'AP', 'PM', 'WT'))
    or  ((G2.EFFDT between T1.TERM_BEGIN_DT  and T1.LATE_END_DT) and G2.PROG_STATUS IN ('CM', 'CN', 'DC', 'DE', 'DM', 'SP', 'LA')))
--   and (((G2.EFFDT between T1.EARLY_BEGIN_DT and T1.TERM_END_DT) and G2.PROG_STATUS IN ('AC', 'AD', 'AP', 'PM', 'WT') and G2.DEGR_CHKOUT_STAT = '-')
--    or  ((G2.EFFDT between T1.TERM_BEGIN_DT  and T1.LATE_END_DT) and (G2.PROG_STATUS IN ('CM', 'CN', 'DC', 'DE', 'DM', 'SP', 'LA') or G2.DEGR_CHKOUT_STAT <> '-')))
   and T1.STRM <= '9000'
   and substr(T1.STRM,-1,1) not in ('7','9')   -- Initial, etc!!!
),
T5 as (
select /*+ parallel(8) inline no_merge */ EMPLID, ACAD_CAREER, STDNT_CAR_NBR, SRC_SYS_ID, ACAD_TERM,
       nvl(min(ACAD_TERM) over (partition by EMPLID, ACAD_CAREER, STDNT_CAR_NBR, SRC_SYS_ID
                                    order by ACAD_TERM
                                rows between 1 following and unbounded following),trim(to_char(ACAD_TERM+1,'0009'))) END_TERM
  from (select /*+ parallel(8) inline no_merge */ distinct G3.EMPLID, G3.ACAD_CAREER, G3.STDNT_CAR_NBR, G3.SRC_SYS_ID, G3.ACAD_TERM
          from G3
--         where G3.EFFDT = G3.MAX_TERM_EFFDT     -- Lower ADMIT_TERM fix
--           and not (G3.INSTITUTION = 'UMLOW' and substr(G3.ACAD_TERM,-2,2) = '50'))
           where not (G3.INSTITUTION = 'UMLOW' and substr(G3.ACAD_TERM,-2,2) = '50'))
union
select /*+ parallel(8) inline no_merge */ EMPLID, ACAD_CAREER, STDNT_CAR_NBR, SRC_SYS_ID, ACAD_TERM,
       nvl(min(ACAD_TERM) over (partition by EMPLID, ACAD_CAREER, STDNT_CAR_NBR, SRC_SYS_ID
                                    order by ACAD_TERM
                                rows between 1 following and unbounded following),trim(to_char(ACAD_TERM+1,'0009'))) END_TERM
  from (select /*+ parallel(8) inline no_merge */ distinct G3.EMPLID, G3.ACAD_CAREER, G3.STDNT_CAR_NBR, G3.SRC_SYS_ID, G3.ACAD_TERM
          from G3
--         where G3.EFFDT = G3.MAX_TERM_EFFDT     -- Lower ADMIT_TERM fix
--           and G3.INSTITUTION = 'UMLOW')
           where G3.INSTITUTION = 'UMLOW')
),
G4 as (
select /*+ parallel(8) inline no_merge */
       G3.EMPLID,
       G3.ACAD_CAREER,
       G3.STDNT_CAR_NBR,
       G3.EFFDT,
       G3.EFFSEQ,
       G3.SRC_SYS_ID,
       G3.INSTITUTION,
       G3.ACAD_PROG,
       G3.PROG_STATUS,
       G3.PROG_ACTION,
       G3.ACTION_DT,
       G3.PROG_REASON,
       G3.ADMIT_TERM,
       G3.EXP_GRAD_TERM,
       G3.REQ_TERM,
       G3.CAMPUS,
       G3.DEGR_CHKOUT_STAT,
       max(G3.DEGR_CHKOUT_LAST) over (partition by G3.EMPLID, G3.ACAD_CAREER, G3.STDNT_CAR_NBR,
                                       (case when G3.PROG_STATUS = 'CM' and G3.COMPLETION_TERM <> '-' then G3.COMPLETION_TERM else least(G3.ACAD_TERM,G3.COMP_TERM) end), G3.SRC_SYS_ID
                                          order by decode(G3.DEGR_CHKOUT_STAT,'-',9,0), G3.EFFDT desc, T5.END_TERM desc) DEGR_CHKOUT_LAST,
       G3.DEGR_CHKOUT_LAST_EGT,         -- Added Dec 2015
       G3.DEGR_CHKOUT_LAST_EGT_EFFDT,   -- Added Dec 2015
       G3.COMPLETION_TERM,
       G3.ADM_APPL_NBR,
       G3.DATA_FROM_ADM_APPL,
       G3.RADM_EFFDT,
       G3.ADMIT_RADM_EFFDT,
       G3.ADMIT_RADM_TERM,
       G3.MIN_ADMIT_TERM,        -- STACK_BEGIN_TERM fix!!!
       G3.STACK_BEGIN_TERM,
       case when G3.PROG_STATUS = 'CM' and G3.COMPLETION_TERM <> '-' then G3.COMPLETION_TERM else least(G3.ACAD_TERM,G3.COMP_TERM) end ACAD_TERM,   -- COMP_TERM always less than ACAD_TERM???
       case when G3.PROG_STATUS = 'CM' and G3.COMPLETION_TERM <> '-'
            then trim(to_char(G3.COMP_TERM+1,'0009'))
--            when G3.PROG_STATUS not in ('AC','LA')
            when G3.PROG_STATUS not in ('AC')
            then trim(to_char((case when G3.PROG_STATUS = 'CM' and G3.COMPLETION_TERM <> '-' then G3.COMPLETION_TERM else least(G3.ACAD_TERM,G3.COMP_TERM) end)+1,'0009'))
--            when G3.PROG_STATUS in ('AC','LA') and G3.EFFDT_ORDER = 1
            when G3.PROG_STATUS in ('AC') and G3.EFFDT_ORDER = 1
            then '5000'
            else least(G3.COMP_TERM,trim(to_char(T5.END_TERM,'0009')))
        end END_TERM,
       ROW_NUMBER() over (partition by G3.EMPLID, G3.ACAD_CAREER, G3.STDNT_CAR_NBR,
                                       (case when G3.PROG_STATUS = 'CM' and G3.COMPLETION_TERM <> '-' then G3.COMPLETION_TERM else least(G3.ACAD_TERM,G3.COMP_TERM) end),
                                       G3.SRC_SYS_ID
                              order by G3.EFFDT desc, T5.END_TERM desc) TERM_ORDER
  from G3
  join T5
    on G3.EMPLID = T5.EMPLID
   and G3.ACAD_CAREER = T5.ACAD_CAREER
   and G3.STDNT_CAR_NBR = T5.STDNT_CAR_NBR
   and G3.SRC_SYS_ID = T5.SRC_SYS_ID
   and G3.ACAD_TERM = T5.ACAD_TERM
),
G5 as (
select /*+ parallel(8) inline no_merge */
       G4.EMPLID,
       G4.ACAD_CAREER,
       G4.STDNT_CAR_NBR,
       G4.EFFDT,
       G4.EFFSEQ,
       G4.SRC_SYS_ID,
       G4.INSTITUTION,
       G4.ACAD_PROG,
       G4.PROG_STATUS,
       G4.PROG_ACTION,
       G4.ACTION_DT,
       G4.PROG_REASON,
       G4.ADMIT_TERM,
       G4.EXP_GRAD_TERM,
       G4.REQ_TERM,
       G4.CAMPUS,
       G4.DEGR_CHKOUT_STAT,
       G4.DEGR_CHKOUT_LAST,
       G4.DEGR_CHKOUT_LAST_EGT,         -- Added Dec 2015
       G4.DEGR_CHKOUT_LAST_EGT_EFFDT,   -- Added Dec 2015
       G4.COMPLETION_TERM,
       G4.ADM_APPL_NBR,
       G4.DATA_FROM_ADM_APPL,
       (case when G4.INSTITUTION =  'UMBOS' and T2.STRM < '2510' and G4.RADM_EFFDT > to_date('01-JAN-1900') and G4.RADM_EFFDT <= T2.TERM_BEGIN_DT then G4.RADM_EFFDT
             when (G4.INSTITUTION <> 'UMBOS' or (G4.INSTITUTION = 'UMBOS' and T2.STRM >= '2510')) and G4.ADMIT_RADM_EFFDT > to_date('01-JAN-1900') then G4.ADMIT_RADM_EFFDT
             else NULL end) RADM_EFFDT,
       T2.FULL_TERM_FLAG,
       G4.MIN_ADMIT_TERM,        -- STACK_BEGIN_TERM fix!!!
       decode(G4.STACK_BEGIN_TERM,'9998',G4.MIN_ADMIT_TERM,G4.STACK_BEGIN_TERM) STACK_BEGIN_TERM,        -- STACK_BEGIN_TERM fix!!!
       G4.ADMIT_RADM_TERM,
       T2.STRM ACAD_TERM,
       G4.ACAD_TERM BEGIN_TERM,
       G4.END_TERM,
--       case when PROG_STATUS = 'AC'
       case when PROG_STATUS in ('AC','CM')     -- Sept 2018
            then 'AA'
--            when PROG_STATUS in ('CN','DC')
            when PROG_STATUS in ('CN','DC','LA')
            then 'ZZ'
            else PROG_STATUS
        end PROG_STAT_CATGRY,
       nvl(MAX(to_char(G4.EFFDT,'YYYYMMDD')||trim(to_char(G4.EFFSEQ,'00009')))
           over (partition by G4.EMPLID, G4.ACAD_CAREER, G4.STDNT_CAR_NBR, G4.SRC_SYS_ID
                              order by T2.STRM --, G4.EFFDT, G4.EFFSEQ   -- Why not same as ACAD_TERM???
                              rows between unbounded preceding and 1 preceding),'0000000000000') PREV_EFFDT_EFFSEQ,  -- Fix for too many rows???
       row_number() over (partition by G4.EMPLID, G4.ACAD_CAREER, G4.STDNT_CAR_NBR, T2.STRM, G4.SRC_SYS_ID     -- Fix for too many rows???
                              order by G4.EFFDT desc, G4.ACAD_TERM desc) TERM_ORDER                             -- Fix Mar 2016
  from G4
  join CSSTG_OWNER.UM_TERM_TBL_VW T2
    on G4.INSTITUTION = T2.INSTITUTION
   and G4.ACAD_CAREER = T2.ACAD_CAREER
   and (T2.STRM >= G4.ACAD_TERM and T2.STRM < G4.END_TERM)
   and G4.SRC_SYS_ID = T2.SRC_SYS_ID
--   and G4.TERM_ORDER = 1
--   and T2.STRM >= G4.ADMIT_TERM   -- Temp fix for CM row??????????????
--   and T2.STRM >= least(decode(G4.COMPLETION_TERM,'-',G4.ADMIT_TERM,G4.COMPLETION_TERM),G4.ADMIT_TERM)   -- Temp fix for CM row??????????????
   and (G4.TERM_ORDER = 1 or
        T2.STRM >= least(decode(G4.COMPLETION_TERM,'-',G4.ADMIT_TERM,G4.COMPLETION_TERM),G4.ADMIT_TERM))   -- Fix for missing RADM rows???
   and T2.STRM <= '9000'
   and substr(T2.STRM,-1,1) not in ('7','9')
),
G6 as (
select /*+ parallel(8) inline no_merge */
       G5.EMPLID,
       G5.ACAD_CAREER,
       G5.STDNT_CAR_NBR,
       G5.EFFDT,
       G5.EFFSEQ,
       G5.SRC_SYS_ID,
       G5.INSTITUTION,
       G5.ACAD_PROG,
       G5.PROG_STATUS,
       G5.PROG_ACTION,
       G5.ACTION_DT,
       G5.PROG_REASON,
       G5.ADMIT_TERM,
       G5.EXP_GRAD_TERM,
       G5.REQ_TERM,
       G5.CAMPUS,
       G5.DEGR_CHKOUT_STAT,
       case when G5.ACAD_TERM = G5.BEGIN_TERM then G5.DEGR_CHKOUT_LAST else '-' end DEGR_CHKOUT_LAST,        -- Fix Oct 2015
       G5.DEGR_CHKOUT_LAST_EGT,         -- Added Dec 2015
       G5.DEGR_CHKOUT_LAST_EGT_EFFDT,   -- Added Dec 2015
       G5.COMPLETION_TERM,
       G5.ADM_APPL_NBR,
       G5.DATA_FROM_ADM_APPL,
       G5.ACAD_TERM,
       G5.STACK_BEGIN_TERM,
       G5.RADM_EFFDT,       -- Added Dec 2015
      min((case when RADM_EFFDT is null then ''
                 when ACAD_TERM < ADMIT_RADM_TERM then ''
                 when FULL_TERM_FLAG = 'Y' then ACAD_TERM else '9999' end)) over (partition by INSTITUTION, ACAD_CAREER, EMPLID, SRC_SYS_ID, RADM_EFFDT) RADM_FULL_TERM,
       min((case when RADM_EFFDT is null then ''
                 when ACAD_TERM < ADMIT_RADM_TERM then ''
                 when FULL_TERM_FLAG = 'N' then ACAD_TERM else '9999' end)) over (partition by INSTITUTION, ACAD_CAREER, EMPLID, SRC_SYS_ID, RADM_EFFDT) RADM_OTHER_TERM,
       min(PROG_STAT_CATGRY) over (partition by INSTITUTION, ACAD_CAREER, ACAD_TERM, EMPLID, ACAD_PROG, SRC_SYS_ID) MIN_PROG_STAT_CATGRY,
       count(distinct ACAD_PROG) over (partition by INSTITUTION, ACAD_CAREER, ACAD_TERM, EMPLID, SRC_SYS_ID) PROG_COUNT
  from G5
 where TERM_ORDER = 1
--   and ACAD_TERM >= least(ADMIT_TERM,STACK_BEGIN_TERM)        -- STACK_BEGIN_TERM fix!!!
   and ACAD_TERM >= MIN_ADMIT_TERM                            -- Lower ADMIT_TERM fix
   and ACAD_TERM <= (select max(STRM) from CSSTG_OWNER.PS_STDNT_CAR_TERM)
   and to_char(G5.EFFDT,'YYYYMMDD')||trim(to_char(G5.EFFSEQ,'00009')) >= PREV_EFFDT_EFFSEQ
   and not(G5.PROG_ACTION = 'RADM' and G5.RADM_EFFDT is NULL)       -- Nov 2015
),
G7 as (
select /*+ parallel(8) inline no_merge */
       G6.EMPLID,
       G6.ACAD_CAREER,
       G6.STDNT_CAR_NBR,
       G6.EFFDT,
       G6.EFFSEQ,
       G6.SRC_SYS_ID,
       G6.INSTITUTION,
       G6.ACAD_PROG,
       G6.PROG_STATUS,
       G6.PROG_ACTION,
       G6.ACTION_DT,
       G6.PROG_REASON,
       G6.ADMIT_TERM,
       G6.EXP_GRAD_TERM,
       G6.REQ_TERM,
       G6.CAMPUS,
       G6.DEGR_CHKOUT_STAT,
       G6.DEGR_CHKOUT_LAST,
       G6.DEGR_CHKOUT_LAST_EGT,         -- Added Dec 2015
       decode(G6.DEGR_CHKOUT_LAST_EGT,'-','',G6.DEGR_CHKOUT_LAST_EGT_EFFDT) DEGR_CHKOUT_LAST_EGT_EFFDT,
       decode(G6.DEGR_CHKOUT_LAST_EGT,'-',0,row_number() over (partition by G6.EMPLID, G6.ACAD_CAREER, G6.STDNT_CAR_NBR, G6.EXP_GRAD_TERM, G6.SRC_SYS_ID
                                                                      order by G6.ACAD_TERM desc, G6.EFFDT desc)) DEGR_CHKOUT_LAST_EGT_ORDER,    -- Added Dec 2015
       G6.COMPLETION_TERM,
       G6.ADM_APPL_NBR,
       G6.DATA_FROM_ADM_APPL,
       G6.ACAD_TERM,
       G6.STACK_BEGIN_TERM,
       (case when RADM_OTHER_TERM is not null and RADM_OTHER_TERM < RADM_FULL_TERM and (RADM_OTHER_TERM = ACAD_TERM or substr(RADM_OTHER_TERM,1,2)||'50' = ACAD_TERM) then G6.RADM_EFFDT
             when RADM_FULL_TERM is not null and RADM_FULL_TERM = ACAD_TERM then G6.RADM_EFFDT
             else NULL end) STACK_READMIT_EFFDT,      -- Added Dec 2015
       (case when RADM_OTHER_TERM is not null and RADM_OTHER_TERM < RADM_FULL_TERM and (RADM_OTHER_TERM = ACAD_TERM or substr(RADM_OTHER_TERM,1,2)||'50' = ACAD_TERM) then ACAD_TERM
             when RADM_FULL_TERM is not null and RADM_FULL_TERM = ACAD_TERM then ACAD_TERM
             else '' end) STACK_READMIT_TERM,
       G6.MIN_PROG_STAT_CATGRY,
       G6.PROG_COUNT
from G6
)
--select /*+ parallel(8) inline use_hash(P1) */     -- Feb 2022 
select /*+ parallel(8) inline use_hash(P1) no_use_nl(C1 C2 C3 I1 P1 P2 P3 P4 P5 T1 T2 T3 T4 T5 T6 T7) */
       G7.INSTITUTION,
       G7.ACAD_CAREER,
       G7.ACAD_TERM,
       G7.EMPLID,
       G7.STDNT_CAR_NBR,
       G7.SRC_SYS_ID,
nvl(I1.INSTITUTION_SID,2147483646) INSTITUTION_SID,
nvl(C1.ACAD_CAR_SID,2147483646) ACAD_CAR_SID,
nvl(T1.TERM_SID,2147483646) TERM_SID,
nvl(P1.PERSON_SID,2147483646) PERSON_SID,
G7.ACAD_PROG,
nvl(P2.ACAD_PROG_SID,2147483646) ACAD_PROG_SID,
nvl(T2.TERM_SID,2147483646) ADMIT_TERM_SID,
nvl(C2.CAMPUS_SID,2147483646) CAMPUS_SID,
nvl(T3.TERM_SID,2147483646) COMPL_TERM_SID,
nvl(T4.TERM_SID,2147483646) EXP_GRAD_TERM_SID,
nvl(P3.PROG_STAT_SID,2147483646) PROG_STAT_SID,
nvl(P4.PROG_ACN_SID,2147483646) PROG_ACN_SID,
nvl(P5.PROG_ACN_RSN_SID,2147483646) PROG_ACN_RSN_SID,
nvl(T5.TERM_SID,2147483646) REQ_TERM_SID,
nvl(T6.TERM_SID,2147483646) STACK_BEGIN_TERM_SID,
nvl(T7.TERM_SID,2147483646) STACK_READMIT_TERM_SID,
       G7.EFFDT,
       G7.EFFSEQ,
       G7.ACTION_DT,
       G7.ADM_APPL_NBR,
       G7.DATA_FROM_ADM_APPL,
       G7.DEGR_CHKOUT_STAT,
       G7.DEGR_CHKOUT_LAST,
       G7.DEGR_CHKOUT_LAST_EGT,
       DEGR_CHKOUT_LAST_EGT_EFFDT,
       DEGR_CHKOUT_LAST_EGT_ORDER,
       G7.MIN_PROG_STAT_CATGRY,
       STACK_READMIT_EFFDT,
       G7.PROG_COUNT,
       'N' LOAD_ERROR,
       'S' DATA_ORIGIN,
       sysdate CREATED_EW_DTTM,
       sysdate LASTUPD_EW_DTTM,
       1234 BATCH_SID
from G7
left outer join PS_D_TERM T1
  on G7.INSTITUTION = T1.INSTITUTION_CD
 and G7.ACAD_CAREER = T1.ACAD_CAR_CD
 and G7.ACAD_TERM = T1.TERM_CD
 and G7.SRC_SYS_ID = T1.SRC_SYS_ID
left outer join PS_D_PERSON P1
  on G7.EMPLID = P1.PERSON_ID
 and G7.SRC_SYS_ID = P1.SRC_SYS_ID
left outer join PS_D_ACAD_CAR C1
  on G7.INSTITUTION = C1.INSTITUTION_CD
 and G7.ACAD_CAREER = C1.ACAD_CAR_CD
 and G7.SRC_SYS_ID = C1.SRC_SYS_ID
--left outer join PS_D_ACAD_PROG P2
--  on G7.INSTITUTION = P2.INSTITUTION_CD
-- and G7.ACAD_PROG = P2.ACAD_PROG_CD
-- and G7.SRC_SYS_ID = P2.SRC_SYS_ID
left outer join UM_D_ACAD_PROG P2
  on G7.INSTITUTION = P2.INSTITUTION_CD
 and G7.ACAD_PROG = P2.ACAD_PROG_CD
 and G7.SRC_SYS_ID = P2.SRC_SYS_ID
 and P2.EFFDT_ORDER = 1
left outer join PS_D_TERM T2
  on G7.INSTITUTION = T2.INSTITUTION_CD
 and G7.ACAD_CAREER = T2.ACAD_CAR_CD
 and G7.ADMIT_TERM = T2.TERM_CD
 and G7.SRC_SYS_ID = T2.SRC_SYS_ID
left outer join PS_D_CAMPUS C2
  on G7.INSTITUTION = C2.INSTITUTION_CD
 and G7.CAMPUS = C2.CAMPUS_CD
 and G7.SRC_SYS_ID = C2.SRC_SYS_ID
left outer join PS_D_TERM T3
  on G7.INSTITUTION = T3.INSTITUTION_CD
 and G7.ACAD_CAREER = T3.ACAD_CAR_CD
 and G7.COMPLETION_TERM = T3.TERM_CD
 and G7.SRC_SYS_ID = T3.SRC_SYS_ID
left outer join PS_D_TERM T4
  on G7.INSTITUTION = T4.INSTITUTION_CD
 and G7.ACAD_CAREER = T4.ACAD_CAR_CD
 and G7.EXP_GRAD_TERM = T4.TERM_CD
 and G7.SRC_SYS_ID = T4.SRC_SYS_ID
left outer join PS_D_INSTITUTION I1
  on G7.INSTITUTION = I1.INSTITUTION_CD
 and G7.SRC_SYS_ID = I1.SRC_SYS_ID
left outer join PS_D_PROG_STAT P3
  on G7.PROG_STATUS = P3.PROG_STAT_CD
 and G7.SRC_SYS_ID = P3.SRC_SYS_ID
left outer join PS_D_PROG_ACN P4
  on G7.INSTITUTION = P4.SETID                -- SETID added to key for PS_D_PROG_ACN, Dec 2018
 and G7.PROG_ACTION = P4.PROG_ACN_CD
 and G7.SRC_SYS_ID = P4.SRC_SYS_ID
left outer join PS_D_PROG_ACN_RSN P5
  on G7.INSTITUTION = P5.SETID				  -- SRC_SETID renamed SETID for PS_D_PROG_ACN_RSN, Dec 2018
 and G7.PROG_ACTION = P5.PROG_ACN_CD
 and G7.PROG_REASON = P5.PROG_ACN_RSN_CD
 and G7.SRC_SYS_ID = P5.SRC_SYS_ID
left outer join PS_D_TERM T5
  on G7.INSTITUTION = T5.INSTITUTION_CD
 and G7.ACAD_CAREER = T5.ACAD_CAR_CD
 and G7.REQ_TERM = T5.TERM_CD
 and G7.SRC_SYS_ID = T5.SRC_SYS_ID
left outer join PS_D_TERM T6
  on G7.INSTITUTION = T6.INSTITUTION_CD
 and G7.ACAD_CAREER = T6.ACAD_CAR_CD
 and G7.STACK_BEGIN_TERM = T6.TERM_CD
 and G7.SRC_SYS_ID = T6.SRC_SYS_ID
left outer join PS_D_TERM T7
  on G7.INSTITUTION = T7.INSTITUTION_CD
 and G7.ACAD_CAREER = T7.ACAD_CAR_CD
 and G7.STACK_READMIT_TERM = T7.TERM_CD
 and G7.SRC_SYS_ID = T7.SRC_SYS_ID
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_ACAD_PROG rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_ACAD_PROG',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_ACAD_PROG',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_ACAD_PROG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_ACAD_PROG enable constraint PK_UM_F_ACAD_PROG';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_ACAD_PROG');

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

END UM_F_ACAD_PROG_P;
/
