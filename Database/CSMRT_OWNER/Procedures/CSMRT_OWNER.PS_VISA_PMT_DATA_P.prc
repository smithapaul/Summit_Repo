DROP PROCEDURE CSMRT_OWNER.PS_VISA_PMT_DATA_P
/

--
-- PS_VISA_PMT_DATA_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_VISA_PMT_DATA_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--
-- Loads stage table PS_VISA_PMT_DATA from PeopleSoft table PS_VISA_PMT_DATA.
--
-- V01  SMT-xxxx 09/13/2017,    James Doucette
--                              Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_VISA_PMT_DATA';
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
 where TABLE_NAME = 'PS_VISA_PMT_DATA'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_VISA_PMT_DATA@SASOURCE S)
 where TABLE_NAME = 'PS_VISA_PMT_DATA'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_VISA_PMT_DATA';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_VISA_PMT_DATA';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_VISA_PMT_DATA T
using (select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(DEPENDENT_ID),'-') DEPENDENT_ID, 
    nvl(trim(COUNTRY),'-') COUNTRY, 
    nvl(trim(VISA_PERMIT_TYPE),'-') VISA_PERMIT_TYPE, 
    EFFDT, 
    nvl(trim(VISA_WRKPMT_NBR),'-') VISA_WRKPMT_NBR, 
    nvl(trim(VISA_WRKPMT_STATUS),'-') VISA_WRKPMT_STATUS, 
    STATUS_DT, 
    DT_ISSUED, 
    nvl(trim(PLACE_ISSUED),'-') PLACE_ISSUED, 
    nvl(DURATION_TIME,0) DURATION_TIME, 
    nvl(trim(DURATION_TYPE),'-') DURATION_TYPE, 
    ENTRY_DT,
    EXPIRATN_DT, 
    nvl(trim(ISSUING_AUTHORITY),'-') ISSUING_AUTHORITY
from SYSADM.PS_VISA_PMT_DATA@SASOURCE S 
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_VISA_PMT_DATA')
  and EMPLID between '00000000' and '99999999'
  and length(EMPLID) = 8 ) S
 on ( 
    T.EMPLID = S.EMPLID and 
    T.DEPENDENT_ID = S.DEPENDENT_ID and 
    T.COUNTRY = S.COUNTRY and 
    T.VISA_PERMIT_TYPE = S.VISA_PERMIT_TYPE and 
    T.EFFDT = S.EFFDT and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.VISA_WRKPMT_NBR = S.VISA_WRKPMT_NBR,
    T.VISA_WRKPMT_STATUS = S.VISA_WRKPMT_STATUS,
    T.STATUS_DT = S.STATUS_DT,
    T.DT_ISSUED = S.DT_ISSUED,
    T.PLACE_ISSUED = S.PLACE_ISSUED,
    T.DURATION_TIME = S.DURATION_TIME,
    T.DURATION_TYPE = S.DURATION_TYPE,
    T.ENTRY_DT = S.ENTRY_DT,
    T.EXPIRATN_DT = S.EXPIRATN_DT,
    T.ISSUING_AUTHORITY = S.ISSUING_AUTHORITY,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.VISA_WRKPMT_NBR <> S.VISA_WRKPMT_NBR or 
    T.VISA_WRKPMT_STATUS <> S.VISA_WRKPMT_STATUS or 
    T.STATUS_DT <> S.STATUS_DT or 
    nvl(trim(T.DT_ISSUED),0) <> nvl(trim(S.DT_ISSUED),0) or 
    T.PLACE_ISSUED <> S.PLACE_ISSUED or 
    T.DURATION_TIME <> S.DURATION_TIME or 
    T.DURATION_TYPE <> S.DURATION_TYPE or 
    nvl(trim(T.ENTRY_DT),0) <> nvl(trim(S.ENTRY_DT),0) or 
    nvl(trim(T.EXPIRATN_DT),0) <> nvl(trim(S.EXPIRATN_DT),0) or 
    T.ISSUING_AUTHORITY <> S.ISSUING_AUTHORITY or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.EMPLID, 
    T.DEPENDENT_ID, 
    T.COUNTRY,
    T.VISA_PERMIT_TYPE, 
    T.EFFDT,
    T.SRC_SYS_ID, 
    T.VISA_WRKPMT_NBR,
    T.VISA_WRKPMT_STATUS, 
    T.STATUS_DT,
    T.DT_ISSUED,
    T.PLACE_ISSUED, 
    T.DURATION_TIME,
    T.DURATION_TYPE,
    T.ENTRY_DT, 
    T.EXPIRATN_DT,
    T.ISSUING_AUTHORITY,
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
    ) 
values (
    S.EMPLID, 
    S.DEPENDENT_ID, 
    S.COUNTRY,
    S.VISA_PERMIT_TYPE, 
    S.EFFDT,
    'CS90', 
    S.VISA_WRKPMT_NBR,
    S.VISA_WRKPMT_STATUS, 
    S.STATUS_DT,
    S.DT_ISSUED,
    S.PLACE_ISSUED, 
    S.DURATION_TIME,
    S.DURATION_TYPE,
    S.ENTRY_DT, 
    S.EXPIRATN_DT,
    S.ISSUING_AUTHORITY,
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

strMessage01    := '# of PS_VISA_PMT_DATA rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_VISA_PMT_DATA',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_VISA_PMT_DATA';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_VISA_PMT_DATA';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_VISA_PMT_DATA';
update CSSTG_OWNER.PS_VISA_PMT_DATA T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, DEPENDENT_ID, COUNTRY, VISA_PERMIT_TYPE, EFFDT
   from CSSTG_OWNER.PS_VISA_PMT_DATA T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_VISA_PMT_DATA') = 'Y'
  minus
 select nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(DEPENDENT_ID),'-') DEPENDENT_ID, 
    nvl(trim(COUNTRY),'-') COUNTRY, 
    nvl(trim(VISA_PERMIT_TYPE),'-') VISA_PERMIT_TYPE,
    EFFDT
   from SYSADM.PS_VISA_PMT_DATA@SASOURCE
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_VISA_PMT_DATA') = 'Y' 
   ) S
 where T.EMPLID = S.EMPLID   
   AND T.DEPENDENT_ID = S.DEPENDENT_ID
   AND T.COUNTRY = S.COUNTRY
   AND T.VISA_PERMIT_TYPE = S.VISA_PERMIT_TYPE
   AND T.EFFDT = S.EFFDT
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_VISA_PMT_DATA rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_VISA_PMT_DATA',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_VISA_PMT_DATA'
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

END PS_VISA_PMT_DATA_P;
/
