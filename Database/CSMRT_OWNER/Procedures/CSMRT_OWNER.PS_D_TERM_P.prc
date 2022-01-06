CREATE OR REPLACE PROCEDURE             "PS_D_TERM_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--George Adams
--OLD tables               -- PS_D_TERM
--Loads target table       -- PS_D_TERM
--PS_D_TERM            -- PS_D_INSTITUTION-100, PS_D_ACAD_CAR -100
--V01 4/16/2018           -- srikanth ,pabbu converted to proc from sql
--V02 2/12/2021           -- Srikanth,Pabbu made changes to TERM_SID field
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_D_TERM';
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

strMessage01    := 'Merging data into CSMRT_OWNER.PS_D_TERM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSMRT_OWNER.PS_D_TERM';

merge /*+ use_hash(S,T) */ into CSMRT_OWNER.PS_D_TERM T	 
using (
with TERM as (
select INSTITUTION, ACAD_CAREER, STRM, SRC_SYS_ID, 
nvl(max(trunc(TERM_END_DT)+1) 
    over (partition by INSTITUTION, ACAD_CAREER, SRC_SYS_ID
              order by STRM
          rows between unbounded preceding and 1 preceding),trunc(to_date('01-JAN-1900'))) UM_TERM_BEG_DT,
nvl(min(trunc(TERM_BEGIN_DT)-1) 
    over (partition by INSTITUTION, ACAD_CAREER, SRC_SYS_ID
              order by STRM
          rows between 1 following and unbounded following),trunc(to_date('31-DEC-8999'))) UM_TERM_END_DT
from CSSTG_OWNER.PS_TERM_TBL
where DATA_ORIGIN <> 'D'
  and STRM between '1010' and '9000'
  and substr(STRM,3,2) not in ('50','90') 
union
select INSTITUTION, ACAD_CAREER, STRM, SRC_SYS_ID, UM_TERM_BEG_DT, UM_TERM_END_DT
from (
select INSTITUTION, ACAD_CAREER, STRM, SRC_SYS_ID, 
nvl(max(trunc(TERM_END_DT)+1) 
    over (partition by INSTITUTION, ACAD_CAREER, SRC_SYS_ID
              order by STRM
          rows between unbounded preceding and 1 preceding),trunc(to_date('01-JAN-1900'))) UM_TERM_BEG_DT,
nvl(min(trunc(TERM_BEGIN_DT)-1) 
    over (partition by INSTITUTION, ACAD_CAREER, SRC_SYS_ID
              order by STRM
          rows between 1 following and unbounded following),trunc(to_date('31-DEC-8999'))) UM_TERM_END_DT
from CSSTG_OWNER.PS_TERM_TBL
where DATA_ORIGIN <> 'D'
  and STRM between '1010' and '9000'
  and substr(STRM,3,2) not in ('40','90') 
)
where substr(STRM,3,2) in ('50')
union
select INSTITUTION, ACAD_CAREER, STRM, SRC_SYS_ID, 
trunc(TERM_BEGIN_DT) UM_TERM_BEG_DT,
trunc(TERM_END_DT) UM_TERM_END_DT
from CSSTG_OWNER.PS_TERM_TBL
where DATA_ORIGIN <> 'D'
  and (substr(STRM,3,2) in ('90') 
   or STRM < '1010'
   or STRM > '9000')),
CAR as (
select INSTITUTION, ACAD_CAREER, EFFDT, SRC_SYS_ID,
       ROW_NUMBER() over (partition by INSTITUTION, ACAD_CAREER, SRC_SYS_ID
                              order by EFFDT desc) CAR_ORDER
  from CSSTG_OWNER.PS_ACAD_CAR_TBL
 where DATA_ORIGIN <> 'D'),
MTERM as (
select distinct INSTITUTION, ACAD_CAREER, SRC_SYS_ID,
       max(STRM) over (partition by INSTITUTION, ACAD_CAREER, SRC_SYS_ID) MAX_STRM
  from CSSTG_OWNER.PS_TERM_TBL
 where DATA_ORIGIN <> 'D'
   and STRM < '9900'),
VAL as (
select CAR.INSTITUTION, CAR.ACAD_CAREER, V.STRM, V.SRC_SYS_ID, V.DESCR, V.DESCRSHORT
  from CSSTG_OWNER.PS_TERM_VAL_TBL V
  join CAR
    on CAR.CAR_ORDER = 1
   and not (V.STRM like '%50' and CAR.INSTITUTION <> 'UMLOW')   -- July 2019 
 where V.DATA_ORIGIN <> 'D'),
TERM2 as (
select 
T.INSTITUTION, 
T.ACAD_CAREER, 
T.STRM, 
T.SRC_SYS_ID,
DESCR, 
DESCRSHORT, 
TERM_BEGIN_DT, 
TERM_END_DT, 
WEEKS_OF_INSTRUCT, 
ACAD_YEAR, 
SIXTY_PCT_DT, 
TERM.UM_TERM_BEG_DT, 
TERM.UM_TERM_END_DT, 
DATA_ORIGIN
from CSSTG_OWNER.PS_TERM_TBL T
join TERM 
  on T.INSTITUTION = TERM.INSTITUTION
 and T.ACAD_CAREER = TERM.ACAD_CAREER
 and T.STRM = TERM.STRM
 and T.SRC_SYS_ID = TERM.SRC_SYS_ID
union all
select VAL.INSTITUTION, 
       VAL.ACAD_CAREER, 
       VAL.STRM, 
       VAL.SRC_SYS_ID, 
       VAL.DESCR, 
       VAL.DESCRSHORT,
       to_date('01-JAN-9000') TERM_BEGIN_DT, 
       to_date('31-DEC-9000') TERM_END_DT,
       0 WEEKS_OF_INSTRUCT, 
       '9000' ACAD_YEAR, 
       to_date('31-DEC-9000') SIXTY_PCT_DT,
       to_date('31-DEC-9000') UM_TERM_BEG_DT, 
       to_date('31-DEC-9000') UM_TERM_END_DT, 
       'S' DATA_ORIGIN
  from VAL
  join MTERM
    on VAL.INSTITUTION = MTERM.INSTITUTION
   and VAL.ACAD_CAREER = MTERM.ACAD_CAREER
   and VAL.STRM > MTERM.MAX_STRM
   and VAL.SRC_SYS_ID = MTERM.SRC_SYS_ID
   and VAL.STRM < '9900'),
TERM3 as (
select INSTITUTION, 
       ACAD_CAREER, 
       STRM, 
       SRC_SYS_ID,
       DESCR, 
       DESCRSHORT, 
       TERM_BEGIN_DT, 
       TERM_END_DT, 
       WEEKS_OF_INSTRUCT, 
       ACAD_YEAR, 
       SIXTY_PCT_DT, 
       UM_TERM_BEG_DT, 
       UM_TERM_END_DT, 
       DATA_ORIGIN,
       case when INSTITUTION = 'UMBOS' and ACAD_CAREER = 'CENC' then 'Y' else 'N' end UMBOS_CENC_FLG,   -- July 2019 
       max(STRM) over (partition by INSTITUTION, ACAD_CAREER, SRC_SYS_ID
                           order by STRM
                       rows between 3 preceding and 3 preceding) TERM_MINUS_3,      -- July 2019 
       max(STRM) over (partition by INSTITUTION, ACAD_CAREER, SRC_SYS_ID
                           order by STRM
                       rows between 2 preceding and 2 preceding) TERM_MINUS_2,      -- July 2019 
       max(STRM) over (partition by INSTITUTION, ACAD_CAREER, SRC_SYS_ID
                           order by STRM
                       rows between 1 preceding and 1 preceding) TERM_MINUS_1,      -- July 2019 
       max(STRM) over (partition by INSTITUTION, ACAD_CAREER, SRC_SYS_ID
                           order by STRM
                       rows between 1 following and 1 following) TERM_PLUS_1,       -- July 2019 
       max(STRM) over (partition by INSTITUTION, ACAD_CAREER, SRC_SYS_ID
                           order by STRM
                       rows between 2 following and 2 following) TERM_PLUS_2,       -- July 2019 
       max(STRM) over (partition by INSTITUTION, ACAD_CAREER, SRC_SYS_ID
                           order by STRM
                       rows between 3 following and 3 following) TERM_PLUS_3,       -- Jul 2019 
       max(case when DESCR like '%Fall' 
                then STRM else '-' end) over (partition by INSTITUTION, ACAD_CAREER, SRC_SYS_ID
                                                  order by STRM
                                              rows between unbounded preceding and 1 preceding) PREV_FALL,
       max(case when DESCR like '%Winter' 
                then STRM else '-' end) over (partition by INSTITUTION, ACAD_CAREER, SRC_SYS_ID
                                                  order by STRM
                                              rows between unbounded preceding and 1 preceding) PREV_WINTER,
       max(case when DESCR like '%Spring' 
                then STRM else '-' end) over (partition by INSTITUTION, ACAD_CAREER, SRC_SYS_ID
                                                  order by STRM
                                              rows between unbounded preceding and 1 preceding) PREV_SPRING,
       max(case when DESCR like '%Summer' 
                then STRM else '-' end) over (partition by INSTITUTION, ACAD_CAREER, SRC_SYS_ID
                                                  order by STRM
                                              rows between unbounded preceding and (case when INSTITUTION = 'UMLOW' and DESCR like '%Trimester' then 2 else 1 end) preceding) PREV_SUMMER,
       max(case when INSTITUTION = 'UMLOW' and DESCR like '%Trimester' 
                then STRM else '-' end) over (partition by INSTITUTION, ACAD_CAREER, SRC_SYS_ID
                                                  order by STRM
                                              rows between unbounded preceding and (case when INSTITUTION = 'UMLOW' and DESCR like '%Trimester' then 2 else 1 end) preceding) PREV_SUMMER_2,
       min(case when DESCR like '%Fall' 
                then STRM else '9999' end) over (partition by INSTITUTION, ACAD_CAREER, SRC_SYS_ID
                                                     order by STRM
                                                 rows between 1 following and unbounded following) NEXT_FALL,
       min(case when DESCR like '%Winter' 
                then STRM else '9999' end) over (partition by INSTITUTION, ACAD_CAREER, SRC_SYS_ID
                                                     order by STRM
                                                 rows between 1 following and unbounded following) NEXT_WINTER,
       min(case when DESCR like '%Spring' 
                then STRM else '9999' end) over (partition by INSTITUTION, ACAD_CAREER, SRC_SYS_ID
                                                     order by STRM
                                                 rows between 1 following and unbounded following) NEXT_SPRING,
       min(case when DESCR like '%Summer' 
                then STRM else '9999' end) over (partition by INSTITUTION, ACAD_CAREER, SRC_SYS_ID
                                                     order by STRM
                                                 rows between (case when INSTITUTION = 'UMLOW' and DESCR like '%Summer' then 2 else 1 end) following and unbounded following) NEXT_SUMMER,
       min(case when INSTITUTION = 'UMLOW' and DESCR like '%Trimester' 
                then STRM else '9999' end) over (partition by INSTITUTION, ACAD_CAREER, SRC_SYS_ID
                                                     order by STRM
                                                 rows between (case when INSTITUTION = 'UMLOW' and DESCR like '%Summer' then 2 else 1 end) following and unbounded following) NEXT_SUMMER_2
  from TERM2),
S as (   
select TERM3.INSTITUTION INSTITUTION_CD, 
       TERM3.ACAD_CAREER ACAD_CAR_CD, 
       TERM3.STRM TERM_CD, 
       TERM3.SRC_SYS_ID, 
       TERM3.DESCRSHORT TERM_SD, 
       TERM3.DESCR TERM_LD, 
       TERM3.STRM||' ('||TERM3.DESCR||')' TERM_CD_DESC,
       nvl(I.INSTITUTION_SID, 2147483646) INSTITUTION_SID, 
       nvl(C.ACAD_CAR_SID, 2147483646) ACAD_CAR_SID, 
       TERM3.ACAD_YEAR ACAD_YR_SID,
       TERM3.TERM_BEGIN_DT, 
       TERM3.TERM_END_DT, 
       TERM3.TERM_BEGIN_DT EFF_START_DT,    -- Overlaps!!!  
       TERM3.UM_TERM_END_DT EFF_END_DT,
       CASE WHEN TERM3.INSTITUTION = '-'
            THEN '-'
            WHEN (case when to_char(SYSDATE,'HH24') >= '21' 
                       then TRUNC(SYSDATE+.13) 
                       else TRUNC(SYSDATE+.13) 
                   end) BETWEEN TERM3.TERM_BEGIN_DT AND TERM3.UM_TERM_END_DT
             AND ADD_MONTHS (TERM3.TERM_BEGIN_DT, 12) > TRUNC (SYSDATE) -- Jan 2017
            THEN 'Y'
            ELSE 'N'
        END CURRENT_TERM_FLG,
       CASE WHEN TERM3.STRM >= '1010'
            THEN '20'||TRIM(TO_CHAR((TO_NUMBER(SUBSTR(TERM3.STRM,1,2),'99') - 9),'09'))
            ELSE ''
        END AID_YEAR,
       TERM3.WEEKS_OF_INSTRUCT INSTRCTN_WEEK_NUM, 
       TERM3.SIXTY_PCT_DT, 
       case when (nvl(TERM_MINUS_1,'-') not like '%90' and (UMBOS_CENC_FLG = 'N' and nvl(TERM_MINUS_1,'-') not like '%50') and STRM not like '%50')      -- Sept 2019  
            then decode(nvl(TERM_MINUS_1,'-'),'9999','-',nvl(TERM_MINUS_1,'-')) 
            when (nvl(TERM_MINUS_2,'-') not like '%90' and (UMBOS_CENC_FLG = 'N' and nvl(TERM_MINUS_2,'-') not like '%50'))      -- July 2019  
            then decode(nvl(TERM_MINUS_2,'-'),'9999','-',nvl(TERM_MINUS_2,'-'))
            when (nvl(TERM_MINUS_3,'-') not like '%90' and (UMBOS_CENC_FLG = 'N' and nvl(TERM_MINUS_3,'-') not like '%50'))      -- July 2019  
            then decode(nvl(TERM_MINUS_3,'-'),'9999','-',nvl(TERM_MINUS_3,'-'))
            else decode(nvl(TERM_MINUS_1,'-'),'9999','-',nvl(TERM_MINUS_1,'-')) 
        end PREV_TERM,   
       case when TERM3.INSTITUTION = 'UMLOW' and ((STRM like '%50' and nvl(TERM_MINUS_1,'-') like '%40') or (STRM like '%10' and nvl(TERM_MINUS_1,'-') like '%50'))     -- July 2019   
            then decode(nvl(TERM_MINUS_1,'-'),'9999','-',nvl(TERM_MINUS_1,'-'))
            else '-' 
        end PREV_TERM_2,   
       case when (UMBOS_CENC_FLG = 'N' and STRM like '%40' and nvl(TERM_PLUS_1,'-') like '%50') and nvl(TERM_PLUS_2,'-') like '%90'     -- July 2019   
            then decode(nvl(TERM_PLUS_3,'-'),'9999','-',nvl(TERM_PLUS_3,'-'))
            when (UMBOS_CENC_FLG = 'N' and STRM like '%40' and (nvl(TERM_PLUS_1,'-') like '%50' or nvl(TERM_PLUS_1,'-') like '%90')) 
            then decode(nvl(TERM_PLUS_2,'-'),'9999','-',nvl(TERM_PLUS_2,'-'))
            when (UMBOS_CENC_FLG = 'N' and STRM like '%50' and nvl(TERM_PLUS_1,'-') like '%90') 
            then decode(nvl(TERM_PLUS_2,'-'),'9999','-',nvl(TERM_PLUS_2,'-'))
            else decode(nvl(TERM_PLUS_1,'-'),'9999','-',nvl(TERM_PLUS_1,'-')) 
        end NEXT_TERM,   
       case when TERM3.INSTITUTION = 'UMLOW' and (STRM like '%30' and nvl(TERM_PLUS_2,'-') like '%50')      -- July 2019    
            then decode(nvl(TERM_PLUS_2,'-'),'9999','-',nvl(TERM_PLUS_2,'-'))
            when TERM3.INSTITUTION = 'UMLOW' and (STRM like '%40' and nvl(TERM_PLUS_1,'-') like '%50')  
            then decode(nvl(TERM_PLUS_1,'-'),'9999','-',nvl(TERM_PLUS_1,'-'))
            else '-' 
        end NEXT_TERM_2,   
       decode(nvl(PREV_FALL,'-'),'9999','-',nvl(PREV_FALL,'-')) PREV_FALL,
       decode(nvl(PREV_WINTER,'-'),'9999','-',nvl(PREV_WINTER,'-')) PREV_WINTER,
       decode(nvl(PREV_SPRING,'-'),'9999','-',nvl(PREV_SPRING,'-')) PREV_SPRING,
       decode(nvl(PREV_SUMMER,'-'),'9999','-',nvl(PREV_SUMMER,'-')) PREV_SUMMER,
       case when nvl(PREV_SUMMER_2,'-') = '9999'
            then '-'
            when nvl(PREV_SUMMER,'-') = nvl(PREV_SUMMER_2,'-')
            then '-'
            else nvl(PREV_SUMMER_2,'-')
        end PREV_SUMMER_2,
       decode(nvl(NEXT_FALL,'-'),'9999','-',nvl(NEXT_FALL,'-')) NEXT_FALL,
       decode(nvl(NEXT_WINTER,'-'),'9999','-',nvl(NEXT_WINTER,'-')) NEXT_WINTER,
       decode(nvl(NEXT_SPRING,'-'),'9999','-',nvl(NEXT_SPRING,'-')) NEXT_SPRING,
       decode(nvl(NEXT_SUMMER,'-'),'9999','-',nvl(NEXT_SUMMER,'-')) NEXT_SUMMER,
       case when nvl(NEXT_SUMMER_2,'-') = '9999'
            then '-'
            when nvl(NEXT_SUMMER,'-') = nvl(NEXT_SUMMER_2,'-')
            then '-'
            else nvl(NEXT_SUMMER_2,'-')
        end NEXT_SUMMER_2,
       TERM3.DATA_ORIGIN, 
       SYSDATE CREATED_EW_DTTM, 
       SYSDATE LASTUPD_EW_DTTM
  from TERM3
  left outer join CSMRT_OWNER.PS_D_INSTITUTION I
    on TERM3.INSTITUTION = I.INSTITUTION_CD
   and TERM3.SRC_SYS_ID = I.SRC_SYS_ID
   and I.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.PS_D_ACAD_CAR C
    on TERM3.INSTITUTION = C.INSTITUTION_CD
   and TERM3.ACAD_CAREER = C.ACAD_CAR_CD
   and TERM3.SRC_SYS_ID = C.SRC_SYS_ID
   and C.DATA_ORIGIN <> 'D')
select nvl(D.TERM_SID,  --max(D.TERM_SID) over (partition by 1) + This code does not ignore SID 2147483646 and below line will fix the issue and is added on 2/12/2021
(select nvl(max(TERM_SID),0) from CSMRT_OWNER.PS_D_TERM where TERM_SID <> 2147483646) + 
              row_number() over (partition by 1 order by D.TERM_SID nulls first)) TERM_SID, 
       nvl(D.INSTITUTION_CD, S.INSTITUTION_CD) INSTITUTION_CD, 
       nvl(D.ACAD_CAR_CD, S.ACAD_CAR_CD) ACAD_CAR_CD, 
       nvl(D.TERM_CD, S.TERM_CD) TERM_CD, 
       nvl(D.SRC_SYS_ID, S.SRC_SYS_ID) SRC_SYS_ID, 
       decode(D.TERM_SD,S.TERM_SD,D.TERM_SD,S.TERM_SD) TERM_SD,
       decode(D.TERM_LD,S.TERM_LD,D.TERM_LD,S.TERM_LD) TERM_LD,
       decode(D.TERM_CD_DESC,S.TERM_CD_DESC,D.TERM_CD_DESC,S.TERM_CD_DESC) TERM_CD_DESC,
       decode(D.INSTITUTION_SID,S.INSTITUTION_SID,D.INSTITUTION_SID,S.INSTITUTION_SID) INSTITUTION_SID,
       decode(D.ACAD_CAR_SID,S.ACAD_CAR_SID,D.ACAD_CAR_SID,S.ACAD_CAR_SID) ACAD_CAR_SID,
       decode(D.ACAD_YR_SID,S.ACAD_YR_SID,D.ACAD_YR_SID,S.ACAD_YR_SID) ACAD_YR_SID,
       decode(D.TERM_BEGIN_DT,S.TERM_BEGIN_DT,D.TERM_BEGIN_DT,S.TERM_BEGIN_DT) TERM_BEGIN_DT,
       decode(D.TERM_END_DT,S.TERM_END_DT,D.TERM_END_DT,S.TERM_END_DT) TERM_END_DT,
       decode(D.EFF_START_DT,S.EFF_START_DT,D.EFF_START_DT,S.EFF_START_DT) EFF_START_DT,
       decode(D.EFF_END_DT,S.EFF_END_DT,D.EFF_END_DT,S.EFF_END_DT) EFF_END_DT,
       decode(D.CURRENT_TERM_FLG,S.CURRENT_TERM_FLG,D.CURRENT_TERM_FLG,S.CURRENT_TERM_FLG) CURRENT_TERM_FLG,
       decode(D.AID_YEAR,S.AID_YEAR,D.AID_YEAR,S.AID_YEAR) AID_YEAR,
       decode(D.INSTRCTN_WEEK_NUM,S.INSTRCTN_WEEK_NUM,D.INSTRCTN_WEEK_NUM,S.INSTRCTN_WEEK_NUM) INSTRCTN_WEEK_NUM,
       decode(D.SIXTY_PCT_DT,S.SIXTY_PCT_DT,D.SIXTY_PCT_DT,S.SIXTY_PCT_DT) SIXTY_PCT_DT,
       decode(D.PREV_TERM,S.PREV_TERM,D.PREV_TERM,S.PREV_TERM) PREV_TERM,
       decode(D.PREV_TERM_2,S.PREV_TERM_2,D.PREV_TERM_2,S.PREV_TERM_2) PREV_TERM_2,
       decode(D.NEXT_TERM,S.NEXT_TERM,D.NEXT_TERM,S.NEXT_TERM) NEXT_TERM,
       decode(D.NEXT_TERM_2,S.NEXT_TERM_2,D.NEXT_TERM_2,S.NEXT_TERM_2) NEXT_TERM_2,
       decode(D.PREV_FALL,S.PREV_FALL,D.PREV_FALL,S.PREV_FALL) PREV_FALL,
       decode(D.PREV_WINTER,S.PREV_WINTER,D.PREV_WINTER,S.PREV_WINTER) PREV_WINTER,
       decode(D.PREV_SPRING,S.PREV_SPRING,D.PREV_SPRING,S.PREV_SPRING) PREV_SPRING,
       decode(D.PREV_SUMMER,S.PREV_SUMMER,D.PREV_SUMMER,S.PREV_SUMMER) PREV_SUMMER,
       decode(D.PREV_SUMMER_2,S.PREV_SUMMER_2,D.PREV_SUMMER_2,S.PREV_SUMMER_2) PREV_SUMMER_2,
       decode(D.NEXT_FALL,S.NEXT_FALL,D.NEXT_FALL,S.NEXT_FALL) NEXT_FALL,
       decode(D.NEXT_WINTER,S.NEXT_WINTER,D.NEXT_WINTER,S.NEXT_WINTER) NEXT_WINTER,
       decode(D.NEXT_SPRING,S.NEXT_SPRING,D.NEXT_SPRING,S.NEXT_SPRING) NEXT_SPRING,
       decode(D.NEXT_SUMMER,S.NEXT_SUMMER,D.NEXT_SUMMER,S.NEXT_SUMMER) NEXT_SUMMER,
       decode(D.NEXT_SUMMER_2,S.NEXT_SUMMER_2,D.NEXT_SUMMER_2,S.NEXT_SUMMER_2) NEXT_SUMMER_2,
       decode(D.DATA_ORIGIN,S.DATA_ORIGIN,D.DATA_ORIGIN,S.DATA_ORIGIN) DATA_ORIGIN,
       nvl(D.CREATED_EW_DTTM, SYSDATE) CREATED_EW_DTTM, 
       nvl(D.LASTUPD_EW_DTTM, SYSDATE) LASTUPD_EW_DTTM
  from S
  left outer join CSMRT_OWNER.PS_D_TERM D  
    on D.INSTITUTION_CD = S.INSTITUTION_CD 
   and D.ACAD_CAR_CD = S.ACAD_CAR_CD 
   and D.TERM_CD = S.TERM_CD 
   and D.SRC_SYS_ID = S.SRC_SYS_ID 
   and D.TERM_SID < 2147483646) S 
   on (T.INSTITUTION_CD = S.INSTITUTION_CD
  and  T.ACAD_CAR_CD = S.ACAD_CAR_CD 
  and  T.TERM_CD = S.TERM_CD 
  and  T.SRC_SYS_ID = S.SRC_SYS_ID) 
 when matched then update set 
T.TERM_SD = S.TERM_SD,
T.TERM_LD = S.TERM_LD,
T.TERM_CD_DESC = S.TERM_CD_DESC,
T.INSTITUTION_SID = S.INSTITUTION_SID,
T.ACAD_CAR_SID = S.ACAD_CAR_SID,
T.ACAD_YR_SID = S.ACAD_YR_SID,
T.TERM_BEGIN_DT = S.TERM_BEGIN_DT,
T.TERM_END_DT = S.TERM_END_DT,
T.EFF_START_DT = S.EFF_START_DT,
T.EFF_END_DT = S.EFF_END_DT,
T.CURRENT_TERM_FLG = S.CURRENT_TERM_FLG,
T.AID_YEAR = S.AID_YEAR,
T.INSTRCTN_WEEK_NUM = S.INSTRCTN_WEEK_NUM,
T.SIXTY_PCT_DT = S.SIXTY_PCT_DT,
T.PREV_TERM = S.PREV_TERM,
T.PREV_TERM_2 = S.PREV_TERM_2,
T.NEXT_TERM = S.NEXT_TERM,
T.NEXT_TERM_2 = S.NEXT_TERM_2,
T.PREV_FALL = S.PREV_FALL,
T.PREV_WINTER = S.PREV_WINTER,
T.PREV_SPRING = S.PREV_SPRING,
T.PREV_SUMMER = S.PREV_SUMMER,
T.PREV_SUMMER_2 = S.PREV_SUMMER_2,
T.NEXT_FALL = S.NEXT_FALL,
T.NEXT_WINTER = S.NEXT_WINTER,
T.NEXT_SPRING = S.NEXT_SPRING,
T.NEXT_SUMMER = S.NEXT_SUMMER,
T.NEXT_SUMMER_2 = S.NEXT_SUMMER_2,
T.DATA_ORIGIN = S.DATA_ORIGIN,
T.LASTUPD_EW_DTTM = SYSDATE
where 
decode(T.TERM_SD,S.TERM_SD,0,1) = 1 or
decode(T.TERM_LD,S.TERM_LD,0,1) = 1 or
decode(T.TERM_CD_DESC,S.TERM_CD_DESC,0,1) = 1 or
decode(T.INSTITUTION_SID,S.INSTITUTION_SID,0,1) = 1 or
decode(T.ACAD_CAR_SID,S.ACAD_CAR_SID,0,1) = 1 or
decode(T.ACAD_YR_SID,S.ACAD_YR_SID,0,1) = 1 or
decode(T.TERM_BEGIN_DT,S.TERM_BEGIN_DT,0,1) = 1 or
decode(T.TERM_END_DT,S.TERM_END_DT,0,1) = 1 or
decode(T.EFF_START_DT,S.EFF_START_DT,0,1) = 1 or
decode(T.EFF_END_DT,S.EFF_END_DT,0,1) = 1 or
decode(T.CURRENT_TERM_FLG,S.CURRENT_TERM_FLG,0,1) = 1 or
decode(T.AID_YEAR,S.AID_YEAR,0,1) = 1 or
decode(T.INSTRCTN_WEEK_NUM,S.INSTRCTN_WEEK_NUM,0,1) = 1 or
decode(T.SIXTY_PCT_DT,S.SIXTY_PCT_DT,0,1) = 1 or
decode(T.PREV_TERM,S.PREV_TERM,0,1) = 1 or
decode(T.PREV_TERM_2,S.PREV_TERM_2,0,1) = 1 or
decode(T.NEXT_TERM,S.NEXT_TERM,0,1) = 1 or
decode(T.NEXT_TERM_2,S.NEXT_TERM_2,0,1) = 1 or
decode(T.PREV_FALL,S.PREV_FALL,0,1) = 1 or
decode(T.PREV_WINTER,S.PREV_WINTER,0,1) = 1 or
decode(T.PREV_SPRING,S.PREV_SPRING,0,1) = 1 or
decode(T.PREV_SUMMER,S.PREV_SUMMER,0,1) = 1 or
decode(T.PREV_SUMMER_2,S.PREV_SUMMER_2,0,1) = 1 or
decode(T.NEXT_FALL,S.NEXT_FALL,0,1) = 1 or
decode(T.NEXT_WINTER,S.NEXT_WINTER,0,1) = 1 or
decode(T.NEXT_SPRING,S.NEXT_SPRING,0,1) = 1 or
decode(T.NEXT_SUMMER,S.NEXT_SUMMER,0,1) = 1 or
decode(T.NEXT_SUMMER_2,S.NEXT_SUMMER_2,0,1) = 1 or
decode(T.DATA_ORIGIN,S.DATA_ORIGIN,0,1) = 1 
 when not matched then
insert (
T.TERM_SID, 
T.INSTITUTION_CD, 
T.ACAD_CAR_CD, 
T.TERM_CD, 
T.SRC_SYS_ID, 
T.TERM_SD, 
T.TERM_LD, 
T.TERM_CD_DESC, 
T.INSTITUTION_SID, 
T.ACAD_CAR_SID, 
T.ACAD_YR_SID, 
T.TERM_BEGIN_DT, 
T.TERM_END_DT, 
T.EFF_START_DT, 
T.EFF_END_DT, 
T.CURRENT_TERM_FLG,
T.AID_YEAR, 
T.INSTRCTN_WEEK_NUM, 
T.SIXTY_PCT_DT, 
T.PREV_TERM, 
T.PREV_TERM_2, 
T.NEXT_TERM, 
T.NEXT_TERM_2, 
T.PREV_FALL, 
T.PREV_WINTER, 
T.PREV_SPRING, 
T.PREV_SUMMER, 
T.PREV_SUMMER_2, 
T.NEXT_FALL, 
T.NEXT_WINTER, 
T.NEXT_SPRING, 
T.NEXT_SUMMER, 
T.NEXT_SUMMER_2, 
T.DATA_ORIGIN,
T.CREATED_EW_DTTM,
T.LASTUPD_EW_DTTM)
values (
S.TERM_SID, 
S.INSTITUTION_CD, 
S.ACAD_CAR_CD, 
S.TERM_CD, 
S.SRC_SYS_ID, 
S.TERM_SD, 
S.TERM_LD, 
S.TERM_CD_DESC, 
S.INSTITUTION_SID, 
S.ACAD_CAR_SID, 
S.ACAD_YR_SID, 
S.TERM_BEGIN_DT, 
S.TERM_END_DT, 
S.EFF_START_DT, 
S.EFF_END_DT, 
S.CURRENT_TERM_FLG,
S.AID_YEAR, 
S.INSTRCTN_WEEK_NUM, 
S.SIXTY_PCT_DT, 
S.PREV_TERM, 
S.PREV_TERM_2, 
S.NEXT_TERM, 
S.NEXT_TERM_2, 
S.PREV_FALL, 
S.PREV_WINTER, 
S.PREV_SPRING, 
S.PREV_SUMMER, 
S.PREV_SUMMER_2, 
S.NEXT_FALL, 
S.NEXT_WINTER, 
S.NEXT_SPRING, 
S.NEXT_SUMMER, 
S.NEXT_SUMMER_2, 
S.DATA_ORIGIN,
SYSDATE,
SYSDATE)
;

strMessage01    := 'Updating DATA_ORIGIN on CSMRT_OWNER.PS_D_TERM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSMRT_OWNER.PS_D_TERM';

update CSMRT_OWNER.PS_D_TERM T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.TERM_SID <> 2147483646
   and T.DATA_ORIGIN <> 'D'
   and not exists (select 1 
                     from CSSTG_OWNER.PS_TERM_TBL S
                    where T.INSTITUTION_CD = S.INSTITUTION
                      and T.ACAD_CAR_CD = S.ACAD_CAREER
                      and T.TERM_CD = S.STRM
                      and T.SRC_SYS_ID = S.SRC_SYS_ID
					  and S.DATA_ORIGIN <> 'D')
   and not exists (select 1 
                     from (select VAL.INSTITUTION, VAL.ACAD_CAREER, VAL.STRM, VAL.SRC_SYS_ID 
                             from (select CAR.INSTITUTION, CAR.ACAD_CAREER, V.STRM, V.SRC_SYS_ID
                                     from CSSTG_OWNER.PS_TERM_VAL_TBL V
                             join (select distinct INSTITUTION, ACAD_CAREER, SRC_SYS_ID 
                                     from CSSTG_OWNER.PS_ACAD_CAR_TBL 
                                    where DATA_ORIGIN <> 'D') CAR
                               on 1 = 1
                            where V.DATA_ORIGIN <> 'D'
                              and V.STRM < '9900') VAL
                     join (select INSTITUTION, ACAD_CAREER, SRC_SYS_ID, max(STRM) MAX_STRM 
                             from CSSTG_OWNER.PS_TERM_TBL 
                            where DATA_ORIGIN <> 'D' 
                              and STRM < '9900' 
                            group by INSTITUTION, ACAD_CAREER, SRC_SYS_ID) MTERM
                       on VAL.INSTITUTION = MTERM.INSTITUTION
                      and VAL.ACAD_CAREER = MTERM.ACAD_CAREER
                      and VAL.STRM > MTERM.MAX_STRM
                      and VAL.SRC_SYS_ID = MTERM.SRC_SYS_ID) S2
                    where T.INSTITUTION_CD = S2.INSTITUTION
                      and T.ACAD_CAR_CD = S2.ACAD_CAREER
                      and T.TERM_CD = S2.STRM
                      and T.SRC_SYS_ID = S2.SRC_SYS_ID)
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_D_TERM rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_D_TERM',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

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

END PS_D_TERM_P;
/
