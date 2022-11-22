DROP PROCEDURE CSMRT_OWNER.AM_PS_UM_RPT_NODES_P
/

--
-- AM_PS_UM_RPT_NODES_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER.AM_PS_UM_RPT_NODES_P IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_UM_RPT_NODES.
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'AM_PS_UM_RPT_NODES';
        strTableName                    Varchar2(100)   := 'PS_UM_RPT_NODES';
        intProcessSid                   Integer;
        dtProcessStart                  Date            := SYSDATE;
        strMessage01                    Varchar2(4000);
        strMessage02                    Varchar2(512);
        strMessage03                    Varchar2(512)   :='';
        strNewLine                      Varchar2(2)     := chr(13) || chr(10);
        strSqlCommand                   Varchar2(32767) :='';
        strSqlDynamic                   Varchar2(32767) :='';
        strClientInfo                   Varchar2(100);
        strDELETE_FLG                   Varchar2(1);
        intRowCount                     Integer;
        intTotalRowCount                Integer         := 0;
        intOLD_MAX_SCN                  Integer         := 0;
        intNEW_MAX_SCN                  Integer         := 0;
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
       START_DT = SYSDATE,
       END_DT = NULL
 where TABLE_NAME = strTableName
;

strSqlCommand := 'commit';
commit;

strSqlCommand   := 'update NEW_MAX_SCN on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_UM_RPT_NODES@AMSOURCE S)
 where TABLE_NAME = strTableName
;

strSqlCommand := 'commit';
commit;

strMessage01    := 'Selecting variables from AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

select DELETE_FLG,
       OLD_MAX_SCN,
       NEW_MAX_SCN
  into strDELETE_FLG,
       intOLD_MAX_SCN,
       intNEW_MAX_SCN
  from AMSTG_OWNER.UM_STAGE_JOBS
 where TABLE_NAME = strTableName
;

strMessage01    := 'Merging data into AMSTG_OWNER.PS_UM_RPT_NODES';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into AMSTG_OWNER.PS_UM_RPT_NODES';
merge /*+ use_hash(S,T) */ into AMSTG_OWNER.PS_UM_RPT_NODES T
using (select /*+ full(S) */
    nvl(trim(SETID),'-') SETID, 
    nvl(trim(ITEM_TYPE),'-') ITEM_TYPE, 
    UM_KEY_NODE1,
    UM_KEY_NODE2,
    UM_KEY_NODE3,
    UM_KEY_NODE4,
    UM_KEY_NODE5,
    UM_KEY_NODE6
  from SYSADM.PS_UM_RPT_NODES@AMSOURCE S 
 where ORA_ROWSCN > intOLD_MAX_SCN) S 
    on ( 
    T.SETID = S.SETID and 
    T.ITEM_TYPE = S.ITEM_TYPE and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.UM_KEY_NODE1 = S.UM_KEY_NODE1,
    T.UM_KEY_NODE2 = S.UM_KEY_NODE2,
    T.UM_KEY_NODE3 = S.UM_KEY_NODE3,
    T.UM_KEY_NODE4 = S.UM_KEY_NODE4,
    T.UM_KEY_NODE5 = S.UM_KEY_NODE5,
    T.UM_KEY_NODE6 = S.UM_KEY_NODE6,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.UM_KEY_NODE1 <> S.UM_KEY_NODE1 or 
    T.UM_KEY_NODE2 <> S.UM_KEY_NODE2 or 
    T.UM_KEY_NODE3 <> S.UM_KEY_NODE3 or 
    T.UM_KEY_NODE4 <> S.UM_KEY_NODE4 or 
    T.UM_KEY_NODE5 <> S.UM_KEY_NODE5 or 
    T.UM_KEY_NODE6 <> S.UM_KEY_NODE6 or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.SETID,
    T.ITEM_TYPE,
    T.SRC_SYS_ID, 
    T.UM_KEY_NODE1, 
    T.UM_KEY_NODE2, 
    T.UM_KEY_NODE3, 
    T.UM_KEY_NODE4, 
    T.UM_KEY_NODE5, 
    T.UM_KEY_NODE6, 
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
) 
values (
    S.SETID,
    S.ITEM_TYPE,
    'CS90', 
    S.UM_KEY_NODE1, 
    S.UM_KEY_NODE2, 
    S.UM_KEY_NODE3, 
    S.UM_KEY_NODE4, 
    S.UM_KEY_NODE5, 
    S.UM_KEY_NODE6, 
    'N',
    'S',
    sysdate,
    sysdate,
    1234);

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of '||strTableName||' rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => strTableName,
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

If strDELETE_FLG = 'Y' then

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = strTableName;

strSqlCommand := 'commit';
commit;

strMessage01    := 'Updating DATA_ORIGIN on '||strTableName;
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on AMSTG_OWNER.PS_UM_RPT_NODES';
update AMSTG_OWNER.PS_UM_RPT_NODES T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select SETID, ITEM_TYPE
   from AMSTG_OWNER.PS_UM_RPT_NODES T2
  minus
 select /*+ full(S2) */ SETID, ITEM_TYPE
   from SYSADM.PS_UM_RPT_NODES@AMSOURCE S2
   ) S
 where T.SETID = S.SETID
   and T.ITEM_TYPE = S.ITEM_TYPE
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of '||strTableName||' rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => strTableName,
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

End if;

strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = strTableName
;

strSqlCommand := 'commit';
commit;

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := strTableName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

EXCEPTION
    WHEN OTHERS THEN
        COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_EXCEPTION
                (
                        i_SqlCommand   => strSqlCommand,
                        i_SqlCode      => SQLCODE,
                        i_SqlErrm      => SQLERRM
                );

END AM_PS_UM_RPT_NODES_P;
/
