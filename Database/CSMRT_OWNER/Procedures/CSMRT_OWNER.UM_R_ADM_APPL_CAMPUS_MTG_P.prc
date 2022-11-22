DROP PROCEDURE CSMRT_OWNER.UM_R_ADM_APPL_CAMPUS_MTG_P
/

--
-- UM_R_ADM_APPL_CAMPUS_MTG_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_R_ADM_APPL_CAMPUS_MTG_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--George Adams
--Loads table                -- UM_R_ADM_APPL_CAMPUS_MTG
--V01 01/31/2019             -- Jim Doucette converted to proc from sql scripts

------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_R_ADM_APPL_CAMPUS_MTG';
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

strMessage01    := 'Truncating table CSMRT_OWNER.UM_R_ADM_APPL_CAMPUS_MTG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_R_ADM_APPL_CAMPUS_MTG';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_R_ADM_APPL_CAMPUS_MTG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_R_ADM_APPL_CAMPUS_MTG');

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_R_ADM_APPL_CAMPUS_MTG disable constraint PK_UM_R_ADM_APPL_CAMPUS_MTG';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_R_ADM_APPL_CAMPUS_MTG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_R_ADM_APPL_CAMPUS_MTG';
insert /*+ append parallel(16) ENABLE_PARALLEL_DML */ into UM_R_ADM_APPL_CAMPUS_MTG
  with X as (
select /*+ parallel(16) inline */ FIELDNAME, FIELDVALUE, EFFDT, SRC_SYS_ID,
       XLATLONGNAME, XLATSHORTNAME, DATA_ORIGIN,
       row_number() over (partition by FIELDNAME, FIELDVALUE, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1900') else EFFDT end) desc) X_ORDER
  from CSSTG_OWNER.PSXLATITEM
 where DATA_ORIGIN <> 'D'),
       Q1 as (
select /*+ parallel(16) inline */ distinct INSTITUTION_CD, PERSON_ID, SRC_SYS_ID,
       INSTITUTION_SID, APPLCNT_SID PERSON_SID
  from CSMRT_OWNER.PS_F_ADM_APPL_STAT),
       Q2 as (
select /*+ parallel(16) inline */
       AT.CAMPUS_EVENT_NBR, AT.CAMPUS_EVENT_ATND, nvl(S.EVENT_MTG_NBR,0) EVENT_MTG_NBR, AT.SRC_SYS_ID,
       CE.INSTITUTION INSTITUTION_CD, AT.EMPLID PERSON_ID, nvl(S.EVNT_ATND_STAT,'-') EVNT_ATND_STAT
  from CSSTG_OWNER.PS_CAMPUS_EVNT_ATT AT
  join CSSTG_OWNER.PS_CAMPUS_EVENT CE
    on AT.CAMPUS_EVENT_NBR = CE.CAMPUS_EVENT_NBR
   and AT.SRC_SYS_ID = CE.SRC_SYS_ID
   and CE.DATA_ORIGIN <> 'D'
  left outer join CSSTG_OWNER.PS_CAMPUS_MTG_SEL S
    on AT.CAMPUS_EVENT_NBR = S.CAMPUS_EVENT_NBR
   and AT.CAMPUS_EVENT_ATND = S.CAMPUS_EVENT_ATND
   and AT.SRC_SYS_ID = S.SRC_SYS_ID
   and S.DATA_ORIGIN <> 'D'
 where AT.DATA_ORIGIN <> 'D'),
       S as (
select /*+ parallel(16) inline */ Q1.INSTITUTION_CD, Q1.PERSON_ID, nvl(Q2.CAMPUS_EVENT_NBR,'-') CAMPUS_EVENT_NBR, nvl(Q2.CAMPUS_EVENT_ATND,'-') CAMPUS_EVENT_ATND, nvl(Q2.EVENT_MTG_NBR,0) EVENT_MTG_NBR, Q1.SRC_SYS_ID,
       Q1.INSTITUTION_SID, Q1.PERSON_SID, nvl(E.CAMPUS_EVENT_SID,2147483646) CAMPUS_EVENT_SID, nvl(M.EVENT_MTG_SID,2147483646) EVENT_MTG_SID,
       nvl(Q2.EVNT_ATND_STAT,'-') EVNT_ATND_STAT, nvl(X1.XLATSHORTNAME,'-') EVNT_ATND_STAT_SD, nvl(X1.XLATLONGNAME,'-') EVNT_ATND_STAT_LD,
       'S' DATA_ORIGIN, SYSDATE CREATED_EW_DTTM, SYSDATE LASTUPD_EW_DTTM
  from Q1
  left outer join Q2
    on Q1.INSTITUTION_CD = Q2.INSTITUTION_CD
   and Q1.PERSON_ID = Q2.PERSON_ID
   and Q1.SRC_SYS_ID = Q2.SRC_SYS_ID
  left outer join UM_D_CAMPUS_EVENT E
    on Q2.CAMPUS_EVENT_NBR = E.CAMPUS_EVENT_NBR
   and Q2.SRC_SYS_ID = E.SRC_SYS_ID
   and E.DATA_ORIGIN <> 'D'
  left outer join UM_D_EVENT_MTG M
    on Q2.CAMPUS_EVENT_NBR = M.CAMPUS_EVENT_NBR
   and Q2.EVENT_MTG_NBR = M.EVENT_MTG_NBR
   and Q2.SRC_SYS_ID = M.SRC_SYS_ID
   and M.DATA_ORIGIN <> 'D'
  left outer join X X1
    on Q2.EVNT_ATND_STAT = X1.FIELDVALUE
   and Q2.SRC_SYS_ID = X1.SRC_SYS_ID
   and X1.FIELDNAME = 'EVNT_ATND_STAT'
   and X1.X_ORDER = 1)
select /*+ parallel(16) */
       INSTITUTION_CD, PERSON_ID, CAMPUS_EVENT_NBR, CAMPUS_EVENT_ATND, EVENT_MTG_NBR, SRC_SYS_ID,
       INSTITUTION_SID, PERSON_SID, CAMPUS_EVENT_SID, EVENT_MTG_SID,
       EVNT_ATND_STAT, EVNT_ATND_STAT_SD, EVNT_ATND_STAT_LD,
       DATA_ORIGIN, CREATED_EW_DTTM, LASTUPD_EW_DTTM
  from S
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_R_ADM_APPL_CAMPUS_MTG rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_R_ADM_APPL_CAMPUS_MTG',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_R_ADM_APPL_CAMPUS_MTG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_R_ADM_APPL_CAMPUS_MTG enable constraint PK_UM_R_ADM_APPL_CAMPUS_MTG';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_R_ADM_APPL_CAMPUS_MTG');

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

END UM_R_ADM_APPL_CAMPUS_MTG_P;
/
