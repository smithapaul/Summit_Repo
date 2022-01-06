CREATE OR REPLACE PROCEDURE             "PS_PERS_DATA_USA_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_PERS_DATA_USA from PeopleSoft table PS_PERS_DATA_USA.
--
 --V01  SMT-xxxx 08/17/2017,    James Doucette
--                              Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_PERS_DATA_USA';
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
 where TABLE_NAME = 'PS_PERS_DATA_USA'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_PERS_DATA_USA@SASOURCE S)
 where TABLE_NAME = 'PS_PERS_DATA_USA'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_PERS_DATA_USA';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_PERS_DATA_USA';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_PERS_DATA_USA T
using (select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID, 
    to_date(to_char(case when EFFDT < '01-JAN-1900' then NULL else EFFDT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EFFDT, 
    nvl(trim(US_WORK_ELIGIBILTY),'-') US_WORK_ELIGIBILTY, 
    nvl(trim(MILITARY_STATUS),'-') MILITARY_STATUS, 
    nvl(trim(CITIZEN_PROOF1),'-') CITIZEN_PROOF1, 
    nvl(trim(CITIZEN_PROOF2),'-') CITIZEN_PROOF2, 
    NVL(S.MEDICARE_ENTLD_DT, to_date(to_char('01/01/1900'), 'MM/DD/YYYY' )) MEDICARE_ENTLD_DT
from SYSADM.PS_PERS_DATA_USA@SASOURCE S 
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_PERS_DATA_USA') 
  and EMPLID between '00000000' and '99999999'
  and length(EMPLID) = 8) S
 on ( 
    T.EMPLID = S.EMPLID and 
    T.EFFDT = S.EFFDT and 
    T.SRC_SYS_ID = 'CS90')
    when matched then update set
    T.US_WORK_ELIGIBILTY = S.US_WORK_ELIGIBILTY,
    T.MILITARY_STATUS = S.MILITARY_STATUS,
    T.CITIZEN_PROOF1 = S.CITIZEN_PROOF1,
    T.CITIZEN_PROOF2 = S.CITIZEN_PROOF2,
    T.MEDICARE_ENTLD_DT = S.MEDICARE_ENTLD_DT,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.US_WORK_ELIGIBILTY <> S.US_WORK_ELIGIBILTY or 
    T.MILITARY_STATUS <> S.MILITARY_STATUS or 
    T.CITIZEN_PROOF1 <> S.CITIZEN_PROOF1 or 
    T.CITIZEN_PROOF2 <> S.CITIZEN_PROOF2 or 
    nvl(trim(T.MEDICARE_ENTLD_DT),0) <> nvl(trim(S.MEDICARE_ENTLD_DT),0) or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.EMPLID, 
    T.EFFDT,
    T.SRC_SYS_ID, 
    T.US_WORK_ELIGIBILTY, 
    T.MILITARY_STATUS,
    T.CITIZEN_PROOF1, 
    T.CITIZEN_PROOF2, 
    T.MEDICARE_ENTLD_DT,
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
    ) 
values (
    S.EMPLID, 
    S.EFFDT,
    'CS90', 
    S.US_WORK_ELIGIBILTY, 
    S.MILITARY_STATUS,
    S.CITIZEN_PROOF1, 
    S.CITIZEN_PROOF2, 
    S.MEDICARE_ENTLD_DT,
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

strMessage01    := '# of PS_PERS_DATA_USA rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_PERS_DATA_USA',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_PERS_DATA_USA';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_PERS_DATA_USA';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_PERS_DATA_USA';
update CSSTG_OWNER.PS_PERS_DATA_USA T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, EFFDT
   from CSSTG_OWNER.PS_PERS_DATA_USA T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_PERS_DATA_USA') = 'Y'
  minus
 select EMPLID, EFFDT
   from SYSADM.PS_PERS_DATA_USA@SASOURCE
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_PERS_DATA_USA') = 'Y'
   ) S
 where T.EMPLID = S.EMPLID
   and T.EFFDT = S.EFFDT
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_PERS_DATA_USA rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_PERS_DATA_USA',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_PERS_DATA_USA'
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

END PS_PERS_DATA_USA_P;
/
