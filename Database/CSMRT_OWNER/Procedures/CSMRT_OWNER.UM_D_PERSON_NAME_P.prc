DROP PROCEDURE CSMRT_OWNER.UM_D_PERSON_NAME_P
/

--
-- UM_D_PERSON_NAME_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_D_PERSON_NAME_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- 
-- Loads target table      -- UM_D_PERSON_NAME
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_D_PERSON_NAME';
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

strMessage01    := 'Truncating table CSMRT_OWNER.UM_D_PERSON_NAME';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_D_PERSON_NAME';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_D_PERSON_NAME';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_D_PERSON_NAME');

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_D_PERSON_NAME disable constraint PK_UM_D_PERSON_NAME';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_D_PERSON_NAME';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_D_PERSON_NAME';
insert /*+ append parallel(8) enable_parallel_dml */ into UM_D_PERSON_NAME
  with Q1 as (
select /*+ inline parallel(8) */
       N.EMPLID PERSON_ID, N.NAME_TYPE, N.SRC_SYS_ID, N.EFFDT, N.EFF_STATUS,
       decode(NAME,'-','',NAME) NAME, 
       decode(NAME_INITIALS,'-','',NAME_INITIALS) NAME_INITIALS, 
       decode(NAME_PREFIX,'-','',NAME_PREFIX) NAME_PREFIX, 
       decode(NAME_SUFFIX,'-','',NAME_SUFFIX) NAME_SUFFIX, 
       decode(NAME_TITLE,'-','',NAME_TITLE) NAME_TITLE,
       decode(LAST_NAME_SRCH,'-','',LAST_NAME_SRCH) LAST_NAME_SRCH, 
       decode(FIRST_NAME_SRCH,'-','',FIRST_NAME_SRCH) FIRST_NAME_SRCH, 
       decode(LAST_NAME,'-','',LAST_NAME) LAST_NAME, 
       decode(FIRST_NAME,'-','',FIRST_NAME) FIRST_NAME, 
       decode(MIDDLE_NAME,'-','',MIDDLE_NAME) MIDDLE_NAME, 
       decode(PREF_FIRST_NAME,'-','',PREF_FIRST_NAME) PREF_FIRST_NAME, 
       decode(NAME_DISPLAY,'-','',NAME_DISPLAY) NAME_DISPLAY, 
       decode(NAME_FORMAL,'-','',NAME_FORMAL) NAME_FORMAL, 
       LASTUPDDTTM, 
       LASTUPDOPRID, 
       DATA_ORIGIN,
       row_number() over (partition by N.EMPLID, N.NAME_TYPE, N.SRC_SYS_ID
                              order by (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) Q_ORDER
  from CSSTG_OWNER.PS_NAMES N
 where DATA_ORIGIN <> 'D'),    -- Sept 2022 
       Q2 as (
select /*+ inline parallel(8) */
       PERSON_ID, NAME_TYPE, SRC_SYS_ID, EFFDT, EFF_STATUS,
       NAME, NAME_INITIALS, NAME_PREFIX, NAME_SUFFIX, NAME_TITLE,
       LAST_NAME_SRCH, FIRST_NAME_SRCH, LAST_NAME, FIRST_NAME, MIDDLE_NAME, PREF_FIRST_NAME,
       NAME_DISPLAY, NAME_FORMAL, LASTUPDDTTM, LASTUPDOPRID,
       row_number() over (partition by PERSON_ID, SRC_SYS_ID
                              order by (case when NAME_TYPE  = 'PRF' then 0
                                             when NAME_TYPE  = 'PRI' then 1
                                             when NAME_TYPE  = 'LEG' then 2
                                             else 9 end), NAME_TYPE) NAME_ORDER,
       row_number() over (partition by PERSON_ID, SRC_SYS_ID
                              order by (case when NAME_TYPE  = 'AK1' then 0
                                             when NAME_TYPE  = 'AK2' then 1
                                             when NAME_TYPE  = 'AK3' then 2
                                             when NAME_TYPE  = 'AK4' then 3
                                             when NAME_TYPE  = 'PRF' then 4
                                             when NAME_TYPE  = 'PRI' then 5
                                             else 9 end), NAME_TYPE) AKA_ORDER,
       row_number() over (partition by PERSON_ID, SRC_SYS_ID
                              order by (case when NAME_TYPE  = 'CPS' then 0
                                             when NAME_TYPE  = 'PRF' then 1
                                             when NAME_TYPE  = 'PRI' then 2
                                             else 9 end), NAME_TYPE) CPS_ORDER, 
       row_number() over (partition by PERSON_ID, SRC_SYS_ID
                              order by (case when NAME_TYPE  = 'DEG' then 0
                                             when NAME_TYPE  = 'PRI' then 1
                                             else 9 end), NAME_TYPE) DEG_ORDER,
       row_number() over (partition by PERSON_ID, SRC_SYS_ID
                              order by (case when NAME_TYPE  = 'PRF' then 0
                                             when NAME_TYPE  = 'PRI' then 1
                                             else 9 end), NAME_TYPE) PRF_ORDER,
       row_number() over (partition by PERSON_ID, SRC_SYS_ID
                              order by (case when NAME_TYPE  = 'PRI' then 0
                                             when NAME_TYPE  = 'PRF' then 1
                                             else 9 end), NAME_TYPE) PRI_ORDER
  from Q1
 where Q_ORDER = 1
   and EFF_STATUS = 'A'), 
       Q3 as (
select /*+ inline parallel(8) */
       PERSON_ID, NAME_TYPE, SRC_SYS_ID, EFFDT, EFF_STATUS,
       NAME, NAME_INITIALS, NAME_PREFIX, NAME_SUFFIX, NAME_TITLE,
       LAST_NAME_SRCH, FIRST_NAME_SRCH, LAST_NAME, FIRST_NAME, MIDDLE_NAME, 
       PREF_FIRST_NAME, NAME_DISPLAY, NAME_FORMAL,
       max(case when AKA_ORDER = 1 and NAME_TYPE like 'AK%' then LAST_NAME else '' end) over (partition by PERSON_ID, SRC_SYS_ID) LAST_NAME_FORMER,
       max(case when AKA_ORDER = 1 and NAME_TYPE like 'AK%' then NAME else '' end) over (partition by PERSON_ID, SRC_SYS_ID) NAME_FORMER,
       max(case when PRI_ORDER = 1 and NAME_TYPE = 'PRI' and (upper(nvl(LAST_NAME,'-')) like 'X%DUP%' 
                                                          or  upper(nvl(LAST_NAME,'-')) like '%XXX%') then 'Y' else 'N' end) over (partition by PERSON_ID, SRC_SYS_ID) DEL_FLAG,     -- Sept 2022 
       LASTUPDDTTM, LASTUPDOPRID,
       NAME_ORDER, AKA_ORDER, CPS_ORDER, DEG_ORDER, PRF_ORDER, PRI_ORDER
  from Q2)
select /*+ inline parallel(8) */
       P.PERSON_ID, nvl(Q3.NAME_TYPE,'-') NAME_TYPE, P.SRC_SYS_ID, Q3.EFFDT, nvl(Q3.EFF_STATUS,'-') EFF_STATUS,
       P.PERSON_SID,
       Q3.NAME, Q3.NAME_INITIALS, Q3.NAME_PREFIX, Q3.NAME_SUFFIX, Q3.NAME_TITLE, 
       Q3.LAST_NAME_SRCH, Q3.FIRST_NAME_SRCH, Q3.LAST_NAME, Q3.FIRST_NAME, Q3.MIDDLE_NAME,
       Q3.PREF_FIRST_NAME, Q3.NAME_DISPLAY, Q3.NAME_FORMAL, Q3.LAST_NAME_FORMER, Q3.NAME_FORMER, 
       Q3.LASTUPDDTTM, Q3.LASTUPDOPRID,
       nvl(Q3.NAME_ORDER,1) NAME_ORDER, nvl(Q3.AKA_ORDER,1) AKA_ORDER, nvl(Q3.CPS_ORDER,1) CPS_ORDER, nvl(Q3.DEG_ORDER,1) DEG_ORDER, nvl(Q3.PRF_ORDER,1) PRF_ORDER, nvl(Q3.PRI_ORDER,1) PRI_ORDER,
       'S' DATA_ORIGIN, SYSDATE CREATED_EW_DTTM, SYSDATE LASTUPD_EW_DTTM
         from PS_D_PERSON P
  left outer join Q3
    on P.PERSON_ID = Q3.PERSON_ID
   and P.SRC_SYS_ID = Q3.SRC_SYS_ID
 where P.DATA_ORIGIN <> 'D'
   and nvl(Q3.DEL_FLAG,'N') <> 'Y'      -- Sept 2022   
--   and not (upper(nvl(Q3.LAST_NAME,'-')) like 'X%DUP%' 
--        or  upper(nvl(Q3.LAST_NAME,'-')) like '%XXX%') 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_D_PERSON_NAME rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);


strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_D_PERSON_NAME';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_D_PERSON_NAME enable constraint PK_UM_D_PERSON_NAME';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_D_PERSON_NAME');

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

END UM_D_PERSON_NAME_P;
/
