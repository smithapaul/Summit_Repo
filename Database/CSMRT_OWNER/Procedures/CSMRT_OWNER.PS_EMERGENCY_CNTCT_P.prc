DROP PROCEDURE CSMRT_OWNER.PS_EMERGENCY_CNTCT_P
/

--
-- PS_EMERGENCY_CNTCT_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_EMERGENCY_CNTCT_P" AUTHID CURRENT_USER IS

/*
-- Run before the first time
DELETE
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_EMERGENCY_CNTCT'

INSERT INTO CSSTG_OWNER.UM_STAGE_JOBS
(TABLE_NAME, DELETE_FLG)
VALUES
('PS_EMERGENCY_CNTCT', 'Y')

SELECT *
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_EMERGENCY_CNTCT'
*/


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_EMERGENCY_CNTCT from PeopleSoft table PS_EMERGENCY_CNTCT.
--
-- V01  SMT-xxxx 05/30/2017,    Jim Doucette
--                              Converted from PS_EMERGENCY_CNTCT.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_EMERGENCY_CNTCT';
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
 where TABLE_NAME = 'PS_EMERGENCY_CNTCT'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_EMERGENCY_CNTCT@SASOURCE S)
 where TABLE_NAME = 'PS_EMERGENCY_CNTCT'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_EMERGENCY_CNTCT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_EMERGENCY_CNTCT';
    merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_EMERGENCY_CNTCT T
    using (select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID,
    nvl(trim(CONTACT_NAME),'-') CONTACT_NAME,
    nvl(trim(SAME_ADDRESS_EMPL),'-') SAME_ADDRESS_EMPL,
    nvl(trim(PRIMARY_CONTACT),'-') PRIMARY_CONTACT,
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
    nvl(trim(RELATIONSHIP),'-') RELATIONSHIP,
    nvl(trim(SAME_PHONE_EMPL),'-') SAME_PHONE_EMPL,
    nvl(trim(ADDRESS_TYPE),'-') ADDRESS_TYPE,
    nvl(trim(PHONE_TYPE),'-') PHONE_TYPE
from SYSADM.PS_EMERGENCY_CNTCT@SASOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_EMERGENCY_CNTCT') 
  and length(EMPLID) = 8
  and EMPLID between '00000000' and '99999999') S
   on (
    T.EMPLID = S.EMPLID and
    T.CONTACT_NAME = S.CONTACT_NAME and
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.SAME_ADDRESS_EMPL = S.SAME_ADDRESS_EMPL,
    T.PRIMARY_CONTACT = S.PRIMARY_CONTACT,
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
    T.RELATIONSHIP = S.RELATIONSHIP,
    T.SAME_PHONE_EMPL = S.SAME_PHONE_EMPL,
    T.ADDRESS_TYPE = S.ADDRESS_TYPE,
    T.PHONE_TYPE = S.PHONE_TYPE,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID   = 1234
where
    T.SAME_ADDRESS_EMPL <> S.SAME_ADDRESS_EMPL or
    T.PRIMARY_CONTACT <> S.PRIMARY_CONTACT or
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
    T.RELATIONSHIP <> S.RELATIONSHIP or
    T.SAME_PHONE_EMPL <> S.SAME_PHONE_EMPL or
    T.ADDRESS_TYPE <> S.ADDRESS_TYPE or
    T.PHONE_TYPE <> S.PHONE_TYPE or
    T.DATA_ORIGIN = 'D'
when not matched then
insert (
    T.EMPLID,
    T.CONTACT_NAME,
    T.SRC_SYS_ID,
    T.SAME_ADDRESS_EMPL,
    T.PRIMARY_CONTACT,
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
    T.RELATIONSHIP,
    T.SAME_PHONE_EMPL,
    T.ADDRESS_TYPE,
    T.PHONE_TYPE,
    T.LOAD_ERROR,
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
    )
values (
    S.EMPLID,
    S.CONTACT_NAME,
    'CS90',
    S.SAME_ADDRESS_EMPL,
    S.PRIMARY_CONTACT,
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
    S.RELATIONSHIP,
    S.SAME_PHONE_EMPL,
    S.ADDRESS_TYPE,
    S.PHONE_TYPE,
    'N',
    'S',
    sysdate,
    sysdate,
    1234);


strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_EMERGENCY_CNTCT rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_EMERGENCY_CNTCT',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_EMERGENCY_CNTCT';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_EMERGENCY_CNTCT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_EMERGENCY_CNTCT';
update CSSTG_OWNER.PS_EMERGENCY_CNTCT T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, CONTACT_NAME
   from CSSTG_OWNER.PS_EMERGENCY_CNTCT T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_EMERGENCY_CNTCT') = 'Y'
  minus
 select nvl(trim(EMPLID),'-') EMPLID, nvl(trim(CONTACT_NAME),'-') CONTACT_NAME
   from SYSADM.PS_EMERGENCY_CNTCT@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_EMERGENCY_CNTCT') = 'Y'
   ) S
 where T.EMPLID = S.EMPLID
   and T.CONTACT_NAME = S.CONTACT_NAME
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_EMERGENCY_CNTCT rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_EMERGENCY_CNTCT',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_EMERGENCY_CNTCT'
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

END PS_EMERGENCY_CNTCT_P;
/
