DROP PROCEDURE CSMRT_OWNER.PS_ADDRESSES_P
/

--
-- PS_ADDRESSES_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_ADDRESSES_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--Preethi Lodha
--
-- Loads stage table PS_ADDRESSES from PeopleSoft table PS_ADDRESSES.
--
-- V01  SMT-xxxx 08/30/2017,    James Doucette
--                              Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_ADDRESSES';
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
 where TABLE_NAME = 'PS_ADDRESSES'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_ADDRESSES@SASOURCE S)
 where TABLE_NAME = 'PS_ADDRESSES'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_ADDRESSES';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_ADDRESSES';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_ADDRESSES T
using (select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(ADDRESS_TYPE),'-') ADDRESS_TYPE, 
    EFFDT, 
    nvl(trim(EFF_STATUS),'-') EFF_STATUS, 
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
    nvl(trim(ADDRESS1_AC),'-') ADDRESS1_AC, 
    nvl(trim(ADDRESS2_AC),'-') ADDRESS2_AC, 
    nvl(trim(ADDRESS3_AC),'-') ADDRESS3_AC, 
    nvl(trim(CITY_AC),'-') CITY_AC, 
    nvl(trim(REG_REGION),'-') REG_REGION, 
    NVL(S.LASTUPDDTTM, to_date(to_char('01/01/1900'), 'MM/DD/YYYY' )) LASTUPDDTTM, 
    nvl(trim(LASTUPDOPRID),'-') LASTUPDOPRID
from SYSADM.PS_ADDRESSES@SASOURCE S 
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ADDRESSES') 
  and EMPLID between '00000000' and '99999999'
  and length(EMPLID) = 8 ) S
 on ( 
    T.EMPLID = S.EMPLID and 
    T.ADDRESS_TYPE = S.ADDRESS_TYPE and 
    T.EFFDT = S.EFFDT and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.EFF_STATUS = S.EFF_STATUS,
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
    T.ADDRESS1_AC = S.ADDRESS1_AC,
    T.ADDRESS2_AC = S.ADDRESS2_AC,
    T.ADDRESS3_AC = S.ADDRESS3_AC,
    T.CITY_AC = S.CITY_AC,
    T.REG_REGION = S.REG_REGION,
    T.LASTUPDDTTM = S.LASTUPDDTTM,
    T.LASTUPDOPRID = S.LASTUPDOPRID,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.EFF_STATUS <> S.EFF_STATUS or 
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
    T.ADDRESS1_AC <> S.ADDRESS1_AC or 
    T.ADDRESS2_AC <> S.ADDRESS2_AC or 
    T.ADDRESS3_AC <> S.ADDRESS3_AC or 
    T.CITY_AC <> S.CITY_AC or 
    T.REG_REGION <> S.REG_REGION or 
    nvl(trim(T.LASTUPDDTTM),0) <> nvl(trim(S.LASTUPDDTTM),0) or 
    T.LASTUPDOPRID <> S.LASTUPDOPRID or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.EMPLID, 
    T.ADDRESS_TYPE, 
    T.EFFDT,
    T.SRC_SYS_ID, 
    T.EFF_STATUS, 
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
    T.ADDRESS1_AC,
    T.ADDRESS2_AC,
    T.ADDRESS3_AC,
    T.CITY_AC,
    T.REG_REGION, 
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
    S.ADDRESS_TYPE, 
    S.EFFDT,
    'CS90', 
    S.EFF_STATUS, 
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
    S.ADDRESS1_AC,
    S.ADDRESS2_AC,
    S.ADDRESS3_AC,
    S.CITY_AC,
    S.REG_REGION, 
    S.LASTUPDDTTM,
    S.LASTUPDOPRID, 
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

strMessage01    := '# of PS_ADDRESSES rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ADDRESSES',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_ADDRESSES';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_ADDRESSES';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_ADDRESSES';
update CSSTG_OWNER.PS_ADDRESSES T
        set T.DATA_ORIGIN = 'D',
               T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, ADDRESS_TYPE, EFFDT
   from CSSTG_OWNER.PS_ADDRESSES T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ADDRESSES') = 'Y'
  minus
 select EMPLID, ADDRESS_TYPE, EFFDT
   from SYSADM.PS_ADDRESSES@SASOURCE
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ADDRESSES') = 'Y' 
   ) S
 where T.EMPLID = S.EMPLID   
   AND T.ADDRESS_TYPE = S.ADDRESS_TYPE 
   AND T.EFFDT = S.EFFDT
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_ADDRESSES rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ADDRESSES',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_ADDRESSES'
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

END PS_ADDRESSES_P;
/
