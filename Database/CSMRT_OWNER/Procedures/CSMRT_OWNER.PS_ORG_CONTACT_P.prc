DROP PROCEDURE CSMRT_OWNER.PS_ORG_CONTACT_P
/

--
-- PS_ORG_CONTACT_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_ORG_CONTACT_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_ORG_CONTACT from PeopleSoft table PS_ORG_CONTACT.
--
 --V01  SMT-xxxx 08/16/2017,    James Doucette
--                              Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_ORG_CONTACT';
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
 where TABLE_NAME = 'PS_ORG_CONTACT'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_ORG_CONTACT@SASOURCE S)
 where TABLE_NAME = 'PS_ORG_CONTACT'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_ORG_CONTACT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_ORG_CONTACT';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_ORG_CONTACT T
using (select /*+ full(S) */
nvl(trim(EXT_ORG_ID),'-') EXT_ORG_ID,
nvl(ORG_CONTACT,0) ORG_CONTACT,
to_date(to_char(case when EFFDT < '01-JAN-1800' then NULL else EFFDT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EFFDT,
nvl(trim(EFF_STATUS),'-') EFF_STATUS,
nvl(trim(EMPLID),'-') EMPLID,
nvl(trim(CONTACT_NAME),'-') CONTACT_NAME,
nvl(trim(JOBTITLE),'-') JOBTITLE,
nvl(ORG_LOCATION,0) ORG_LOCATION,
nvl(ORG_DEPARTMENT,0) ORG_DEPARTMENT,
nvl(trim(EMAILID),'-') EMAILID,
nvl(trim(ORG_CONTACT_TYPE),'-') ORG_CONTACT_TYPE,
nvl(trim(URL_ADDRESS),'-') URL_ADDRESS,
nvl(trim(SCC_CNTC_ADDR_TYPE),'-') SCC_CNTC_ADDR_TYPE,
nvl(trim(ADDRESS_TYPE),'-') ADDRESS_TYPE,
to_date(to_char(case when LASTUPDDTTM < '01-JAN-1800' then NULL else LASTUPDDTTM end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') LASTUPDDTTM,
nvl(trim(LASTUPDOPRID),'-') LASTUPDOPRID
from SYSADM.PS_ORG_CONTACT@SASOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ORG_CONTACT') ) S
   on (
T.EXT_ORG_ID = S.EXT_ORG_ID and
T.ORG_CONTACT = S.ORG_CONTACT and
T.EFFDT = S.EFFDT and
T.SRC_SYS_ID = 'CS90')
when matched then update set
T.EFF_STATUS = S.EFF_STATUS,
T.EMPLID = S.EMPLID,
T.CONTACT_NAME = S.CONTACT_NAME,
T.JOBTITLE = S.JOBTITLE,
T.ORG_LOCATION = S.ORG_LOCATION,
T.ORG_DEPARTMENT = S.ORG_DEPARTMENT,
T.EMAILID = S.EMAILID,
T.ORG_CONTACT_TYPE = S.ORG_CONTACT_TYPE,
T.URL_ADDRESS = S.URL_ADDRESS,
T.SCC_CNTC_ADDR_TYPE = S.SCC_CNTC_ADDR_TYPE,
T.ADDRESS_TYPE = S.ADDRESS_TYPE,
T.LASTUPDDTTM = S.LASTUPDDTTM,
T.LASTUPDOPRID = S.LASTUPDOPRID,
T.DATA_ORIGIN = 'S',
T.LASTUPD_EW_DTTM = sysdate,
T.BATCH_SID   = 1234
where
T.EFF_STATUS <> S.EFF_STATUS or
T.EMPLID <> S.EMPLID or
T.CONTACT_NAME <> S.CONTACT_NAME or
T.JOBTITLE <> S.JOBTITLE or
T.ORG_LOCATION <> S.ORG_LOCATION or
T.ORG_DEPARTMENT <> S.ORG_DEPARTMENT or
T.EMAILID <> S.EMAILID or
T.ORG_CONTACT_TYPE <> S.ORG_CONTACT_TYPE or
T.URL_ADDRESS <> S.URL_ADDRESS or
T.SCC_CNTC_ADDR_TYPE <> S.SCC_CNTC_ADDR_TYPE or
T.ADDRESS_TYPE <> S.ADDRESS_TYPE or
nvl(trim(T.LASTUPDDTTM),0) <> nvl(trim(S.LASTUPDDTTM),0) or
T.LASTUPDOPRID <> S.LASTUPDOPRID or
T.DATA_ORIGIN = 'D'
when not matched then
insert (
T.EXT_ORG_ID,
T.ORG_CONTACT,
T.EFFDT,
T.SRC_SYS_ID,
T.EFF_STATUS,
T.EMPLID,
T.CONTACT_NAME,
T.JOBTITLE,
T.ORG_LOCATION,
T.ORG_DEPARTMENT,
T.EMAILID,
T.ORG_CONTACT_TYPE,
T.URL_ADDRESS,
T.SCC_CNTC_ADDR_TYPE,
T.ADDRESS_TYPE,
T.LASTUPDDTTM,
T.LASTUPDOPRID,
T.LOAD_ERROR,
T.DATA_ORIGIN,
T.CREATED_EW_DTTM,
T.LASTUPD_EW_DTTM,
T.BATCH_SID
)
values (
S.EXT_ORG_ID,
S.ORG_CONTACT,
S.EFFDT,
'CS90',
S.EFF_STATUS,
S.EMPLID,
S.CONTACT_NAME,
S.JOBTITLE,
S.ORG_LOCATION,
S.ORG_DEPARTMENT,
S.EMAILID,
S.ORG_CONTACT_TYPE,
S.URL_ADDRESS,
S.SCC_CNTC_ADDR_TYPE,
S.ADDRESS_TYPE,
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

strMessage01    := '# of PS_ORG_CONTACT rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ORG_CONTACT',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_ORG_CONTACT';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_ORG_CONTACT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_ORG_CONTACT';
update CSSTG_OWNER.PS_ORG_CONTACT T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EXT_ORG_ID, ORG_CONTACT, EFFDT
   from CSSTG_OWNER.PS_ORG_CONTACT T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ORG_CONTACT') = 'Y'
  minus
 select EXT_ORG_ID, ORG_CONTACT, EFFDT
   from SYSADM.PS_ORG_CONTACT@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ORG_CONTACT') = 'Y'
   ) S
 where T.EXT_ORG_ID = S.EXT_ORG_ID
  and T.ORG_CONTACT = S.ORG_CONTACT
  and T.EFFDT = S.EFFDT
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;
strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_ORG_CONTACT rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ORG_CONTACT',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_ORG_CONTACT'
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

END PS_ORG_CONTACT_P;
/
