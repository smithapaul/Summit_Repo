DROP PROCEDURE CSMRT_OWNER.PS_NAMES_P
/

--
-- PS_NAMES_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_NAMES_P" AUTHID CURRENT_USER IS

/*
-- Run before the first time
DELETE
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_NAMES'

INSERT INTO CSSTG_OWNER.UM_STAGE_JOBS
(TABLE_NAME, DELETE_FLG)
VALUES
('PS_NAMES', 'Y')

SELECT *
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_NAMES'
*/


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_NAMES from PeopleSoft table PS_NAMES.
--
-- V01  SMT-xxxx 05/30/2017,    Jim Doucette
--                              Converted from PS_NAMES.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_NAMES';
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

strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);


strSqlCommand   := 'update START_DT on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Reading',
       START_DT = sysdate,
       END_DT = NULL
 where TABLE_NAME = 'PS_NAMES'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_NAMES@SASOURCE S)
 where TABLE_NAME = 'PS_NAMES'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_NAMES';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_NAMES';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_NAMES T
using (select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(NAME_TYPE),'-') NAME_TYPE, 
    to_date(to_char(case when EFFDT < '01-JAN-1800' then NULL 
                    else EFFDT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EFFDT, 
    nvl(trim(EFF_STATUS),'-') EFF_STATUS, 
    nvl(trim(COUNTRY_NM_FORMAT),'-') COUNTRY_NM_FORMAT, 
    nvl(trim(NAME),'-') NAME, 
    nvl(trim(NAME_INITIALS),'-') NAME_INITIALS, 
    nvl(trim(NAME_PREFIX),'-') NAME_PREFIX, 
    nvl(trim(NAME_SUFFIX),'-') NAME_SUFFIX, 
    nvl(trim(NAME_ROYAL_PREFIX),'-') NAME_ROYAL_PREFIX, 
    nvl(trim(NAME_ROYAL_SUFFIX),'-') NAME_ROYAL_SUFFIX, 
    nvl(trim(NAME_TITLE),'-') NAME_TITLE, 
    nvl(trim(LAST_NAME_SRCH),'-') LAST_NAME_SRCH, 
    nvl(trim(FIRST_NAME_SRCH),'-') FIRST_NAME_SRCH, 
    nvl(trim(LAST_NAME),'-') LAST_NAME, 
    nvl(trim(FIRST_NAME),'-') FIRST_NAME, 
    nvl(trim(MIDDLE_NAME),'-') MIDDLE_NAME, 
    nvl(trim(SECOND_LAST_NAME),'-') SECOND_LAST_NAME, 
    nvl(trim(SECOND_LAST_SRCH),'-') SECOND_LAST_SRCH, 
    nvl(trim(NAME_AC),'-') NAME_AC, 
    nvl(trim(PREF_FIRST_NAME),'-') PREF_FIRST_NAME, 
    nvl(trim(PARTNER_LAST_NAME),'-') PARTNER_LAST_NAME, 
    nvl(trim(PARTNER_ROY_PREFIX),'-') PARTNER_ROY_PREFIX, 
    nvl(trim(LAST_NAME_PREF_NLD),'-') LAST_NAME_PREF_NLD, 
    nvl(trim(NAME_DISPLAY),'-') NAME_DISPLAY, 
    nvl(trim(NAME_FORMAL),'-') NAME_FORMAL, 
    nvl(trim(NAME_DISPLAY_SRCH),'-') NAME_DISPLAY_SRCH, 
    to_date(to_char(case when LASTUPDDTTM < '01-JAN-1800' then NULL 
                    else LASTUPDDTTM end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') LASTUPDDTTM, 
    nvl(trim(LASTUPDOPRID),'-') LASTUPDOPRID
  from SYSADM.PS_NAMES@SASOURCE S 
 where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_NAMES')
   and EMPLID between '00000000' and '99999999'
   and length(EMPLID) = 8 ) S
 on ( 
    T.EMPLID = S.EMPLID and 
    T.NAME_TYPE = S.NAME_TYPE and 
    T.EFFDT = S.EFFDT and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.EFF_STATUS = S.EFF_STATUS,
    T.COUNTRY_NM_FORMAT = S.COUNTRY_NM_FORMAT,
    T.NAME = S.NAME,
    T.NAME_INITIALS = S.NAME_INITIALS,
    T.NAME_PREFIX = S.NAME_PREFIX,
    T.NAME_SUFFIX = S.NAME_SUFFIX,
    T.NAME_ROYAL_PREFIX = S.NAME_ROYAL_PREFIX,
    T.NAME_ROYAL_SUFFIX = S.NAME_ROYAL_SUFFIX,
    T.NAME_TITLE = S.NAME_TITLE,
    T.LAST_NAME_SRCH = S.LAST_NAME_SRCH,
    T.FIRST_NAME_SRCH = S.FIRST_NAME_SRCH,
    T.LAST_NAME = S.LAST_NAME,
    T.FIRST_NAME = S.FIRST_NAME,
    T.MIDDLE_NAME = S.MIDDLE_NAME,
    T.SECOND_LAST_NAME = S.SECOND_LAST_NAME,
    T.SECOND_LAST_SRCH = S.SECOND_LAST_SRCH,
    T.NAME_AC = S.NAME_AC,
    T.PREF_FIRST_NAME = S.PREF_FIRST_NAME,
    T.PARTNER_LAST_NAME = S.PARTNER_LAST_NAME,
    T.PARTNER_ROY_PREFIX = S.PARTNER_ROY_PREFIX,
    T.LAST_NAME_PREF_NLD = S.LAST_NAME_PREF_NLD,
    T.NAME_DISPLAY = S.NAME_DISPLAY,
    T.NAME_FORMAL = S.NAME_FORMAL,
    T.NAME_DISPLAY_SRCH = S.NAME_DISPLAY_SRCH,
    T.LASTUPDDTTM = S.LASTUPDDTTM,
    T.LASTUPDOPRID = S.LASTUPDOPRID,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.EFF_STATUS <> S.EFF_STATUS or 
    T.COUNTRY_NM_FORMAT <> S.COUNTRY_NM_FORMAT or 
    T.NAME <> S.NAME or 
    T.NAME_INITIALS <> S.NAME_INITIALS or 
    T.NAME_PREFIX <> S.NAME_PREFIX or 
    T.NAME_SUFFIX <> S.NAME_SUFFIX or 
    T.NAME_ROYAL_PREFIX <> S.NAME_ROYAL_PREFIX or 
    T.NAME_ROYAL_SUFFIX <> S.NAME_ROYAL_SUFFIX or 
    T.NAME_TITLE <> S.NAME_TITLE or 
    T.LAST_NAME_SRCH <> S.LAST_NAME_SRCH or 
    T.FIRST_NAME_SRCH <> S.FIRST_NAME_SRCH or 
    T.LAST_NAME <> S.LAST_NAME or 
    T.FIRST_NAME <> S.FIRST_NAME or 
    T.MIDDLE_NAME <> S.MIDDLE_NAME or 
    T.SECOND_LAST_NAME <> S.SECOND_LAST_NAME or 
    T.SECOND_LAST_SRCH <> S.SECOND_LAST_SRCH or 
    T.NAME_AC <> S.NAME_AC or 
    T.PREF_FIRST_NAME <> S.PREF_FIRST_NAME or 
    T.PARTNER_LAST_NAME <> S.PARTNER_LAST_NAME or 
    T.PARTNER_ROY_PREFIX <> S.PARTNER_ROY_PREFIX or 
    T.LAST_NAME_PREF_NLD <> S.LAST_NAME_PREF_NLD or 
    T.NAME_DISPLAY <> S.NAME_DISPLAY or 
    T.NAME_FORMAL <> S.NAME_FORMAL or 
    T.NAME_DISPLAY_SRCH <> S.NAME_DISPLAY_SRCH or 
    nvl(trim(T.LASTUPDDTTM),0) <> nvl(trim(S.LASTUPDDTTM),0) or 
    T.LASTUPDOPRID <> S.LASTUPDOPRID or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.EMPLID, 
    T.NAME_TYPE,
    T.EFFDT,
    T.SRC_SYS_ID, 
    T.EFF_STATUS, 
    T.COUNTRY_NM_FORMAT,
    T.NAME, 
    T.NAME_INITIALS,
    T.NAME_PREFIX,
    T.NAME_SUFFIX,
    T.NAME_ROYAL_PREFIX,
    T.NAME_ROYAL_SUFFIX,
    T.NAME_TITLE, 
    T.LAST_NAME_SRCH, 
    T.FIRST_NAME_SRCH,
    T.LAST_NAME,
    T.FIRST_NAME, 
    T.MIDDLE_NAME,
    T.SECOND_LAST_NAME, 
    T.SECOND_LAST_SRCH, 
    T.NAME_AC,
    T.PREF_FIRST_NAME,
    T.PARTNER_LAST_NAME,
    T.PARTNER_ROY_PREFIX, 
    T.LAST_NAME_PREF_NLD, 
    T.NAME_DISPLAY, 
    T.NAME_FORMAL,
    T.NAME_DISPLAY_SRCH,
    T.LASTUPDDTTM,
    T.LASTUPDOPRID, 
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
) 
values (
    S.EMPLID, 
    S.NAME_TYPE,
    S.EFFDT,
    'CS90', 
    S.EFF_STATUS, 
    S.COUNTRY_NM_FORMAT,
    S.NAME, 
    S.NAME_INITIALS,
    S.NAME_PREFIX,
    S.NAME_SUFFIX,
    S.NAME_ROYAL_PREFIX,
    S.NAME_ROYAL_SUFFIX,
    S.NAME_TITLE, 
    S.LAST_NAME_SRCH, 
    S.FIRST_NAME_SRCH,
    S.LAST_NAME,
    S.FIRST_NAME, 
    S.MIDDLE_NAME,
    S.SECOND_LAST_NAME, 
    S.SECOND_LAST_SRCH, 
    S.NAME_AC,
    S.PREF_FIRST_NAME,
    S.PARTNER_LAST_NAME,
    S.PARTNER_ROY_PREFIX, 
    S.LAST_NAME_PREF_NLD, 
    S.NAME_DISPLAY, 
    S.NAME_FORMAL,
    S.NAME_DISPLAY_SRCH,
    S.LASTUPDDTTM,
    S.LASTUPDOPRID, 
    'N',
    'S',
    sysdate,
    sysdate,
    1234);


strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_NAMES rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_NAMES',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_NAMES';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_NAMES';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_NAMES';
update CSSTG_OWNER.PS_NAMES T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, NAME_TYPE, EFFDT
   from CSSTG_OWNER.PS_NAMES T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_NAMES') = 'Y'
  minus
 select EMPLID, NAME_TYPE, EFFDT
   from SYSADM.PS_NAMES@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_NAMES') = 'Y'
   ) S
 where T.EMPLID = S.EMPLID
   and T.NAME_TYPE = S.NAME_TYPE
   and T.EFFDT = S.EFFDT
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_NAMES rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_NAMES',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_NAMES'
;

strSqlCommand := 'commit';
commit;


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

END PS_NAMES_P;
/
