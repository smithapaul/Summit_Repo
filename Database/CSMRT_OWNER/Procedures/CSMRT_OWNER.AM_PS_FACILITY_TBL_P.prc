DROP PROCEDURE CSMRT_OWNER.AM_PS_FACILITY_TBL_P
/

--
-- AM_PS_FACILITY_TBL_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."AM_PS_FACILITY_TBL_P" IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_FACILITY_TBL from PeopleSoft table PS_FACILITY_TBL.
--
 --V01  SMT-xxxx 09/05/2017,    James Doucette
--                              Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'AM_PS_FACILITY_TBL';
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
 where TABLE_NAME = 'PS_FACILITY_TBL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_FACILITY_TBL@AMSOURCE S)
 where TABLE_NAME = 'PS_FACILITY_TBL'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into AMSTG_OWNER.PS_FACILITY_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into AMSTG_OWNER.PS_FACILITY_TBL';
merge /*+ use_hash(S,T) */ into AMSTG_OWNER.PS_FACILITY_TBL T 
using (select /*+ full(S) */
    nvl(trim(SETID),'-') SETID, 
    nvl(trim(FACILITY_ID),'-') FACILITY_ID, 
    NVL(EFFDT, to_date(to_char('01/01/1900'), 'MM/DD/YYYY' )) EFFDT, 
    nvl(trim(EFF_STATUS),'-') EFF_STATUS, 
    nvl(trim(BLDG_CD),'-') BLDG_CD, 
    nvl(trim(ROOM),'-') ROOM, 
    nvl(trim(replace(DESCR, '  ', ' ')),'-') DESCR,
    nvl(trim(DESCRSHORT),'-') DESCRSHORT, 
    nvl(trim(FACILITY_TYPE),'-') FACILITY_TYPE, 
    nvl(trim(FACILITY_GROUP),'-') FACILITY_GROUP, 
    nvl(trim(LOCATION),'-') LOCATION, 
    nvl(ROOM_CAPACITY,0) ROOM_CAPACITY, 
    nvl(trim(GENERL_ASSIGN),'-') GENERL_ASSIGN, 
    nvl(trim(ACAD_ORG),'-') ACAD_ORG, 
    nvl(trim(FACILITY_PARTITION),'-') FACILITY_PARTITION, 
    nvl(MIN_UTLZN_PCT,0) MIN_UTLZN_PCT, 
    nvl(trim(FACILITY_CONFLICT),'-') FACILITY_CONFLICT, 
    nvl(trim(EXT_SA_FACILITY_ID),'-') EXT_SA_FACILITY_ID
from SYSADM.PS_FACILITY_TBL@AMSOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_FACILITY_TBL') ) S 
 on ( 
    T.SETID = S.SETID and 
    T.FACILITY_ID = S.FACILITY_ID and 
    T.EFFDT = S.EFFDT and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.EFF_STATUS = S.EFF_STATUS,
    T.BLDG_CD = S.BLDG_CD,
    T.ROOM = S.ROOM,
    T.DESCR = S.DESCR,
    T.DESCRSHORT = S.DESCRSHORT,
    T.FACILITY_TYPE = S.FACILITY_TYPE,
    T.FACILITY_GROUP = S.FACILITY_GROUP,
    T.LOCATION = S.LOCATION,
    T.ROOM_CAPACITY = S.ROOM_CAPACITY,
    T.GENERL_ASSIGN = S.GENERL_ASSIGN,
    T.ACAD_ORG = S.ACAD_ORG,
    T.FACILITY_PARTITION = S.FACILITY_PARTITION,
    T.MIN_UTLZN_PCT = S.MIN_UTLZN_PCT,
    T.FACILITY_CONFLICT = S.FACILITY_CONFLICT,
    T.EXT_SA_FACILITY_ID = S.EXT_SA_FACILITY_ID,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.EFF_STATUS <> S.EFF_STATUS or 
    T.BLDG_CD <> S.BLDG_CD or 
    T.ROOM <> S.ROOM or 
    T.DESCR <> S.DESCR or 
    T.DESCRSHORT <> S.DESCRSHORT or 
    T.FACILITY_TYPE <> S.FACILITY_TYPE or 
    T.FACILITY_GROUP <> S.FACILITY_GROUP or 
    T.LOCATION <> S.LOCATION or 
    T.ROOM_CAPACITY <> S.ROOM_CAPACITY or 
    T.GENERL_ASSIGN <> S.GENERL_ASSIGN or 
    T.ACAD_ORG <> S.ACAD_ORG or 
    T.FACILITY_PARTITION <> S.FACILITY_PARTITION or 
    T.MIN_UTLZN_PCT <> S.MIN_UTLZN_PCT or 
    T.FACILITY_CONFLICT <> S.FACILITY_CONFLICT or 
    T.EXT_SA_FACILITY_ID <> S.EXT_SA_FACILITY_ID or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.SETID,
    T.FACILITY_ID,
    T.EFFDT,
    T.SRC_SYS_ID, 
    T.EFF_STATUS, 
    T.BLDG_CD,
    T.ROOM, 
    T.DESCR,
    T.DESCRSHORT, 
    T.FACILITY_TYPE,
    T.FACILITY_GROUP, 
    T.LOCATION, 
    T.ROOM_CAPACITY,
    T.GENERL_ASSIGN,
    T.ACAD_ORG, 
    T.FACILITY_PARTITION, 
    T.MIN_UTLZN_PCT,
    T.FACILITY_CONFLICT,
    T.EXT_SA_FACILITY_ID, 
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
    ) 
values (
    S.SETID,
    S.FACILITY_ID,
    S.EFFDT,
    'CS90', 
    S.EFF_STATUS, 
    S.BLDG_CD,
    S.ROOM, 
    S.DESCR,
    S.DESCRSHORT, 
    S.FACILITY_TYPE,
    S.FACILITY_GROUP, 
    S.LOCATION, 
    S.ROOM_CAPACITY,
    S.GENERL_ASSIGN,
    S.ACAD_ORG, 
    S.FACILITY_PARTITION, 
    S.MIN_UTLZN_PCT,
    S.FACILITY_CONFLICT,
    S.EXT_SA_FACILITY_ID, 
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

strMessage01    := '# of PS_FACILITY_TBL rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_FACILITY_TBL',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_FACILITY_TBL';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_FACILITY_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on AMSTG_OWNER.PS_FACILITY_TBL';
update AMSTG_OWNER.PS_FACILITY_TBL T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select SETID, FACILITY_ID, NVL(EFFDT, to_date(to_char('01/01/1900'), 'MM/DD/YYYY' )) EFFDT
   from AMSTG_OWNER.PS_FACILITY_TBL T2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_FACILITY_TBL') = 'Y'
  minus
 select SETID, FACILITY_ID, EFFDT
   from SYSADM.PS_FACILITY_TBL@AMSOURCE
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_FACILITY_TBL') = 'Y' 
   ) S
 where T.SETID = S.SETID   
   and T.FACILITY_ID = S.FACILITY_ID
   and T.EFFDT = S.EFFDT
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_FACILITY_TBL rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_FACILITY_TBL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_FACILITY_TBL'
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

END AM_PS_FACILITY_TBL_P;
/
