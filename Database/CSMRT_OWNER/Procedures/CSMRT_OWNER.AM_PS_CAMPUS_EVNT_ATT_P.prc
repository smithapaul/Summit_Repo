DROP PROCEDURE CSMRT_OWNER.AM_PS_CAMPUS_EVNT_ATT_P
/

--
-- AM_PS_CAMPUS_EVNT_ATT_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."AM_PS_CAMPUS_EVNT_ATT_P" IS

------------------------------------------------------------------------
--Preethi Lodha
--
-- Loads stage table PS_CAMPUS_EVNT_ATT from PeopleSoft table PS_CAMPUS_EVNT_ATT.
--
-- V01  SMT-xxxx 08/30/2017,    James Doucette
--                              Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'AM_PS_CAMPUS_EVNT_ATT';
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

strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);


strSqlCommand   := 'update START_DT on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Reading',
       START_DT = sysdate,
       END_DT = NULL
 where TABLE_NAME = 'PS_CAMPUS_EVNT_ATT'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_CAMPUS_EVNT_ATT@AMSOURCE S)
 where TABLE_NAME = 'PS_CAMPUS_EVNT_ATT'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into AMSTG_OWNER.PS_CAMPUS_EVNT_ATT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into AMSTG_OWNER.PS_CAMPUS_EVNT_ATT';
merge /*+ use_hash(S,T) */ into AMSTG_OWNER.PS_CAMPUS_EVNT_ATT T
using (select /*+ full(S) */
    nvl(trim(CAMPUS_EVENT_NBR),'-') CAMPUS_EVENT_NBR, 
    nvl(trim(CAMPUS_EVENT_ATND),'-') CAMPUS_EVENT_ATND, 
    nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(NAME),'-') NAME, 
    nvl(trim(COUNTRY),'-') COUNTRY, 
    nvl(trim(ADDRESS1),'-') ADDRESS1, 
    nvl(trim(ADDRESS2),'-') ADDRESS2, 
    nvl(trim(ADDRESS3),'-') ADDRESS3, 
    nvl(trim(ADDRESS4),'-') ADDRESS4, 
    nvl(trim(CITY),'-') CITY, 
    nvl(trim(NUM1),'-') NUM1, 
    nvl(trim(NUM2),'-') NUM2, 
    nvl(trim(HOUSE_TYPE),'-') HOUSE_TYPE, 
    nvl(trim(ADDR_FIELD1),'-') ADDR_FIELD1, 
    nvl(trim(ADDR_FIELD2),'-') ADDR_FIELD2, 
    nvl(trim(ADDR_FIELD3),'-') ADDR_FIELD3, 
    nvl(trim(COUNTY),'-') COUNTY, 
    nvl(trim(STATE),'-') STATE, 
    nvl(trim(POSTAL),'-') POSTAL, 
    nvl(trim(GEO_CODE),'-') GEO_CODE, 
    nvl(trim(IN_CITY_LIMIT),'-') IN_CITY_LIMIT, 
    nvl(trim(COUNTRY_CODE),'-') COUNTRY_CODE, 
    nvl(trim(PHONE),'-') PHONE, 
    nvl(trim(EXTENSION),'-') EXTENSION, 
    nvl(trim(EXT_ORG_ID),'-') EXT_ORG_ID, 
    nvl(trim(DESCR),'-') DESCR, 
    nvl(trim(PHONE_TYPE),'-') PHONE_TYPE, 
    nvl(trim(ADDRESS_TYPE),'-') ADDRESS_TYPE, 
    nvl(trim(GUEST_ATTENDEE),'-') GUEST_ATTENDEE, 
    nvl(trim(GUEST_RELATIONSHIP),'-') GUEST_RELATIONSHIP
from SYSADM.PS_CAMPUS_EVNT_ATT@AMSOURCE S 
where ORA_ROWSCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CAMPUS_EVNT_ATT') ) S
 on ( 
    T.CAMPUS_EVENT_NBR = S.CAMPUS_EVENT_NBR and 
    T.CAMPUS_EVENT_ATND = S.CAMPUS_EVENT_ATND and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.EMPLID = S.EMPLID,
    T.NAME = S.NAME,
    T.COUNTRY = S.COUNTRY,
    T.ADDRESS1 = S.ADDRESS1,
    T.ADDRESS2 = S.ADDRESS2,
    T.ADDRESS3 = S.ADDRESS3,
    T.ADDRESS4 = S.ADDRESS4,
    T.CITY = S.CITY,
    T.NUM1 = S.NUM1,
    T.NUM2 = S.NUM2,
    T.HOUSE_TYPE = S.HOUSE_TYPE,
    T.ADDR_FIELD1 = S.ADDR_FIELD1,
    T.ADDR_FIELD2 = S.ADDR_FIELD2,
    T.ADDR_FIELD3 = S.ADDR_FIELD3,
    T.COUNTY = S.COUNTY,
    T.STATE = S.STATE,
    T.POSTAL = S.POSTAL,
    T.GEO_CODE = S.GEO_CODE,
    T.IN_CITY_LIMIT = S.IN_CITY_LIMIT,
    T.COUNTRY_CODE = S.COUNTRY_CODE,
    T.PHONE = S.PHONE,
    T.EXTENSION = S.EXTENSION,
    T.EXT_ORG_ID = S.EXT_ORG_ID,
    T.DESCR = S.DESCR,
    T.PHONE_TYPE = S.PHONE_TYPE,
    T.ADDRESS_TYPE = S.ADDRESS_TYPE,
    T.GUEST_ATTENDEE = S.GUEST_ATTENDEE,
    T.GUEST_RELATIONSHIP = S.GUEST_RELATIONSHIP,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.EMPLID <> S.EMPLID or 
    T.NAME <> S.NAME or 
    T.COUNTRY <> S.COUNTRY or 
    T.ADDRESS1 <> S.ADDRESS1 or 
    T.ADDRESS2 <> S.ADDRESS2 or 
    T.ADDRESS3 <> S.ADDRESS3 or 
    T.ADDRESS4 <> S.ADDRESS4 or 
    T.CITY <> S.CITY or 
    T.NUM1 <> S.NUM1 or 
    T.NUM2 <> S.NUM2 or 
    T.HOUSE_TYPE <> S.HOUSE_TYPE or 
    T.ADDR_FIELD1 <> S.ADDR_FIELD1 or 
    T.ADDR_FIELD2 <> S.ADDR_FIELD2 or 
    T.ADDR_FIELD3 <> S.ADDR_FIELD3 or 
    T.COUNTY <> S.COUNTY or 
    T.STATE <> S.STATE or 
    T.POSTAL <> S.POSTAL or 
    T.GEO_CODE <> S.GEO_CODE or 
    T.IN_CITY_LIMIT <> S.IN_CITY_LIMIT or 
    T.COUNTRY_CODE <> S.COUNTRY_CODE or 
    T.PHONE <> S.PHONE or 
    T.EXTENSION <> S.EXTENSION or 
    T.EXT_ORG_ID <> S.EXT_ORG_ID or 
    T.DESCR <> S.DESCR or 
    T.PHONE_TYPE <> S.PHONE_TYPE or 
    T.ADDRESS_TYPE <> S.ADDRESS_TYPE or 
    T.GUEST_ATTENDEE <> S.GUEST_ATTENDEE or 
    T.GUEST_RELATIONSHIP <> S.GUEST_RELATIONSHIP or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.CAMPUS_EVENT_NBR, 
    T.CAMPUS_EVENT_ATND,
    T.SRC_SYS_ID, 
    T.EMPLID, 
    T.NAME, 
    T.COUNTRY,
    T.ADDRESS1, 
    T.ADDRESS2, 
    T.ADDRESS3, 
    T.ADDRESS4, 
    T.CITY, 
    T.NUM1, 
    T.NUM2, 
    T.HOUSE_TYPE, 
    T.ADDR_FIELD1,
    T.ADDR_FIELD2,
    T.ADDR_FIELD3,
    T.COUNTY, 
    T.STATE,
    T.POSTAL, 
    T.GEO_CODE, 
    T.IN_CITY_LIMIT,
    T.COUNTRY_CODE, 
    T.PHONE,
    T.EXTENSION,
    T.EXT_ORG_ID, 
    T.DESCR,
    T.PHONE_TYPE, 
    T.ADDRESS_TYPE, 
    T.GUEST_ATTENDEE, 
    T.GUEST_RELATIONSHIP, 
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
    ) 
values (
    S.CAMPUS_EVENT_NBR, 
    S.CAMPUS_EVENT_ATND,
    'CS90', 
    S.EMPLID, 
    S.NAME, 
    S.COUNTRY,
    S.ADDRESS1, 
    S.ADDRESS2, 
    S.ADDRESS3, 
    S.ADDRESS4, 
    S.CITY, 
    S.NUM1, 
    S.NUM2, 
    S.HOUSE_TYPE, 
    S.ADDR_FIELD1,
    S.ADDR_FIELD2,
    S.ADDR_FIELD3,
    S.COUNTY, 
    S.STATE,
    S.POSTAL, 
    S.GEO_CODE, 
    S.IN_CITY_LIMIT,
    S.COUNTRY_CODE, 
    S.PHONE,
    S.EXTENSION,
    S.EXT_ORG_ID, 
    S.DESCR,
    S.PHONE_TYPE, 
    S.ADDRESS_TYPE, 
    S.GUEST_ATTENDEE, 
    S.GUEST_RELATIONSHIP, 
    'N',
    'S',
    sysdate,
    sysdate,
    1234)
;


strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_CAMPUS_EVNT_ATT rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_CAMPUS_EVNT_ATT',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_CAMPUS_EVNT_ATT';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_CAMPUS_EVNT_ATT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on AMSTG_OWNER.PS_CAMPUS_EVNT_ATT';
update AMSTG_OWNER.PS_CAMPUS_EVNT_ATT T
        set T.DATA_ORIGIN = 'D',
               T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select CAMPUS_EVENT_NBR, CAMPUS_EVENT_ATND
   from AMSTG_OWNER.PS_CAMPUS_EVNT_ATT T2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CAMPUS_EVNT_ATT') = 'Y'
  minus
 select CAMPUS_EVENT_NBR, CAMPUS_EVENT_ATND
   from SYSADM.PS_CAMPUS_EVNT_ATT@AMSOURCE
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CAMPUS_EVNT_ATT') = 'Y' 
   ) S
 where T.CAMPUS_EVENT_NBR = S.CAMPUS_EVENT_NBR   
   AND T.CAMPUS_EVENT_ATND = S.CAMPUS_EVENT_ATND
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_CAMPUS_EVNT_ATT rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_CAMPUS_EVNT_ATT',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_CAMPUS_EVNT_ATT'
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

END AM_PS_CAMPUS_EVNT_ATT_P;
/
