DROP PROCEDURE CSMRT_OWNER.PSOPRDEFN_P
/

--
-- PSOPRDEFN_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PSOPRDEFN_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PSOPRDEFN from PeopleSoft table PSOPRDEFN.
--
 --V01  SMT-xxxx 10/11/2017,    James Doucette
--                              Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PSOPRDEFN';
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
 where TABLE_NAME = 'PSOPRDEFN'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PSOPRDEFN@SASOURCE S)
 where TABLE_NAME = 'PSOPRDEFN'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PSOPRDEFN';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PSOPRDEFN';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PSOPRDEFN T 
using (select /*+ full(S) */
    nvl(trim(OPRID),'-') OPRID, 
    nvl(VERSION,0) VERSION, 
    nvl(trim(OPRDEFNDESC),'-') OPRDEFNDESC, 
    nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(EMAILID),'-') EMAILID, 
    nvl(trim(OPRCLASS),'-') OPRCLASS, 
    nvl(trim(ROWSECCLASS),'-') ROWSECCLASS, 
    nvl(trim(OPERPSWD),'-') OPERPSWD, 
    nvl(ENCRYPTED,0) ENCRYPTED, 
    nvl(trim(SYMBOLICID),'-') SYMBOLICID, 
    nvl(trim(LANGUAGE_CD),'-') LANGUAGE_CD, 
    nvl(MULTILANG,0) MULTILANG, 
    nvl(trim(CURRENCY_CD),'-') CURRENCY_CD, 
    LASTPSWDCHANGE,
    nvl(ACCTLOCK,0) ACCTLOCK, 
    nvl(trim(PRCSPRFLCLS),'-') PRCSPRFLCLS, 
    nvl(trim(DEFAULTNAVHP),'-') DEFAULTNAVHP, 
    nvl(FAILEDLOGINS,0) FAILEDLOGINS, 
    nvl(EXPENT,0) EXPENT, 
    nvl(OPRTYPE,0) OPRTYPE, 
    nvl(trim(USERIDALIAS),'-') USERIDALIAS, 
    NVL(LASTSIGNONDTTM, to_date(to_char('01/01/1900'), 'MM/DD/YYYY' )) LASTSIGNONDTTM,
    NVL(LASTUPDDTTM, to_date(to_char('01/01/1900'), 'MM/DD/YYYY' )) LASTUPDDTTM, 
    nvl(trim(LASTUPDOPRID),'-') LASTUPDOPRID, 
    nvl(PTALLOWSWITCHUSER,0) PTALLOWSWITCHUSER
from SYSADM.PSOPRDEFN@SASOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PSOPRDEFN') ) S 
 on ( 
    T.OPRID = S.OPRID and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.VERSION = S.VERSION,
    T.OPRDEFNDESC = S.OPRDEFNDESC,
    T.EMPLID = S.EMPLID,
    T.EMAILID = S.EMAILID,
    T.OPRCLASS = S.OPRCLASS,
    T.ROWSECCLASS = S.ROWSECCLASS,
    T.OPERPSWD = S.OPERPSWD,
    T.ENCRYPTED = S.ENCRYPTED,
    T.SYMBOLICID = S.SYMBOLICID,
    T.LANGUAGE_CD = S.LANGUAGE_CD,
    T.MULTILANG = S.MULTILANG,
    T.CURRENCY_CD = S.CURRENCY_CD,
    T.LASTPSWDCHANGE = S.LASTPSWDCHANGE,
    T.ACCTLOCK = S.ACCTLOCK,
    T.PRCSPRFLCLS = S.PRCSPRFLCLS,
    T.DEFAULTNAVHP = S.DEFAULTNAVHP,
    T.FAILEDLOGINS = S.FAILEDLOGINS,
    T.EXPENT = S.EXPENT,
    T.OPRTYPE = S.OPRTYPE,
    T.USERIDALIAS = S.USERIDALIAS,
    T.LASTSIGNONDTTM = S.LASTSIGNONDTTM,
    T.LASTUPDDTTM = S.LASTUPDDTTM,
    T.LASTUPDOPRID = S.LASTUPDOPRID,
    T.PTALLOWSWITCHUSER = S.PTALLOWSWITCHUSER,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.VERSION <> S.VERSION or 
    T.OPRDEFNDESC <> S.OPRDEFNDESC or 
    T.EMPLID <> S.EMPLID or 
    T.EMAILID <> S.EMAILID or 
    T.OPRCLASS <> S.OPRCLASS or 
    T.ROWSECCLASS <> S.ROWSECCLASS or 
    T.OPERPSWD <> S.OPERPSWD or 
    T.ENCRYPTED <> S.ENCRYPTED or 
    T.SYMBOLICID <> S.SYMBOLICID or 
    T.LANGUAGE_CD <> S.LANGUAGE_CD or 
    T.MULTILANG <> S.MULTILANG or 
    T.CURRENCY_CD <> S.CURRENCY_CD or 
    T.LASTPSWDCHANGE <> S.LASTPSWDCHANGE or 
    T.ACCTLOCK <> S.ACCTLOCK or 
    T.PRCSPRFLCLS <> S.PRCSPRFLCLS or 
    T.DEFAULTNAVHP <> S.DEFAULTNAVHP or 
    T.FAILEDLOGINS <> S.FAILEDLOGINS or 
    T.EXPENT <> S.EXPENT or 
    T.OPRTYPE <> S.OPRTYPE or 
    T.USERIDALIAS <> S.USERIDALIAS or 
    nvl(trim(T.LASTSIGNONDTTM),0) <> nvl(trim(S.LASTSIGNONDTTM),0) or 
    nvl(trim(T.LASTUPDDTTM),0) <> nvl(trim(S.LASTUPDDTTM),0) or 
    T.LASTUPDOPRID <> S.LASTUPDOPRID or 
    T.PTALLOWSWITCHUSER <> S.PTALLOWSWITCHUSER or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.OPRID,
    T.SRC_SYS_ID, 
    T.VERSION,
    T.OPRDEFNDESC,
    T.EMPLID, 
    T.EMAILID,
    T.OPRCLASS, 
    T.ROWSECCLASS,
    T.OPERPSWD, 
    T.ENCRYPTED,
    T.SYMBOLICID, 
    T.LANGUAGE_CD,
    T.MULTILANG,
    T.CURRENCY_CD,
    T.LASTPSWDCHANGE, 
    T.ACCTLOCK, 
    T.PRCSPRFLCLS,
    T.DEFAULTNAVHP, 
    T.FAILEDLOGINS, 
    T.EXPENT, 
    T.OPRTYPE,
    T.USERIDALIAS,
    T.LASTSIGNONDTTM, 
    T.LASTUPDDTTM,
    T.LASTUPDOPRID, 
    T.PTALLOWSWITCHUSER,
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
    ) 
values (
    S.OPRID,
    'CS90', 
    S.VERSION,
    S.OPRDEFNDESC,
    S.EMPLID, 
    S.EMAILID,
    S.OPRCLASS, 
    S.ROWSECCLASS,
    S.OPERPSWD, 
    S.ENCRYPTED,
    S.SYMBOLICID, 
    S.LANGUAGE_CD,
    S.MULTILANG,
    S.CURRENCY_CD,
    S.LASTPSWDCHANGE, 
    S.ACCTLOCK, 
    S.PRCSPRFLCLS,
    S.DEFAULTNAVHP, 
    S.FAILEDLOGINS, 
    S.EXPENT, 
    S.OPRTYPE,
    S.USERIDALIAS,
    S.LASTSIGNONDTTM, 
    S.LASTUPDDTTM,
    S.LASTUPDOPRID, 
    S.PTALLOWSWITCHUSER,
    'N',
    'S',
    sysdate,
    sysdate,
    1234)
;

commit;


strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PSOPRDEFN rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PSOPRDEFN',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PSOPRDEFN';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PSOPRDEFN';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PSOPRDEFN';
update CSSTG_OWNER.PSOPRDEFN T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select OPRID
   from CSSTG_OWNER.PSOPRDEFN T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PSOPRDEFN') = 'Y'
  minus
 select OPRID
   from SYSADM.PSOPRDEFN@SASOURCE
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PSOPRDEFN') = 'Y' 
   ) S
 where T.OPRID = S.OPRID
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PSOPRDEFN rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PSOPRDEFN',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PSOPRDEFN'
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

END PSOPRDEFN_P;
/
