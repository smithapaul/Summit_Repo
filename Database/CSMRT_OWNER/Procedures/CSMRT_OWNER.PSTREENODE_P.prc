DROP PROCEDURE CSMRT_OWNER.PSTREENODE_P
/

--
-- PSTREENODE_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PSTREENODE_P" AUTHID CURRENT_USER IS


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PSTREENODE from PeopleSoft table PSTREENODE.
--
 --V01  SMT-xxxx 09/14/2017,    James Doucette
--                              Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PSTREENODE';
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
 where TABLE_NAME = 'PSTREENODE'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PSTREENODE@SASOURCE S)
 where TABLE_NAME = 'PSTREENODE'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PSTREENODE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PSTREENODE';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PSTREENODE T
using (select /*+ full(S) */
    nvl(trim(SETID),'-') SETID, 
    nvl(trim(SETCNTRLVALUE),'-') SETCNTRLVALUE, 
    nvl(trim(TREE_NAME),'-') TREE_NAME, 
    EFFDT, 
    nvl(TREE_NODE_NUM,0) TREE_NODE_NUM, 
    nvl(trim(TREE_NODE),'-') TREE_NODE, 
    nvl(trim(TREE_BRANCH),'-') TREE_BRANCH, 
    nvl(TREE_NODE_NUM_END,0) TREE_NODE_NUM_END, 
    nvl(TREE_LEVEL_NUM,0) TREE_LEVEL_NUM, 
    nvl(trim(TREE_NODE_TYPE),'-') TREE_NODE_TYPE, 
    nvl(PARENT_NODE_NUM,0) PARENT_NODE_NUM, 
    nvl(trim(PARENT_NODE_NAME),'-') PARENT_NODE_NAME, 
    nvl(trim(OLD_TREE_NODE_NUM),'-') OLD_TREE_NODE_NUM, 
    nvl(trim(NODECOL_IMAGE),'-') NODECOL_IMAGE, 
    nvl(trim(NODEEXP_IMAGE),'-') NODEEXP_IMAGE
from SYSADM.PSTREENODE@SASOURCE S 
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PSTREENODE') ) S
 on ( 
    T.SETID = S.SETID and 
    T.SETCNTRLVALUE = S.SETCNTRLVALUE and 
    T.TREE_NAME = S.TREE_NAME and 
    T.EFFDT = S.EFFDT and 
    T.TREE_NODE_NUM = S.TREE_NODE_NUM and 
    T.TREE_NODE = S.TREE_NODE and 
    T.TREE_BRANCH = S.TREE_BRANCH and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.TREE_NODE_NUM_END = S.TREE_NODE_NUM_END,
    T.TREE_LEVEL_NUM = S.TREE_LEVEL_NUM,
    T.TREE_NODE_TYPE = S.TREE_NODE_TYPE,
    T.PARENT_NODE_NUM = S.PARENT_NODE_NUM,
    T.PARENT_NODE_NAME = S.PARENT_NODE_NAME,
    T.OLD_TREE_NODE_NUM = S.OLD_TREE_NODE_NUM,
    T.NODECOL_IMAGE = S.NODECOL_IMAGE,
    T.NODEEXP_IMAGE = S.NODEEXP_IMAGE,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.TREE_NODE_NUM_END <> S.TREE_NODE_NUM_END or 
    T.TREE_LEVEL_NUM <> S.TREE_LEVEL_NUM or 
    T.TREE_NODE_TYPE <> S.TREE_NODE_TYPE or 
    T.PARENT_NODE_NUM <> S.PARENT_NODE_NUM or 
    T.PARENT_NODE_NAME <> S.PARENT_NODE_NAME or 
    T.OLD_TREE_NODE_NUM <> S.OLD_TREE_NODE_NUM or 
    T.NODECOL_IMAGE <> S.NODECOL_IMAGE or 
    T.NODEEXP_IMAGE <> S.NODEEXP_IMAGE or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.SETID,
    T.SETCNTRLVALUE,
    T.TREE_NAME,
    T.EFFDT,
    T.TREE_NODE_NUM,
    T.TREE_NODE,
    T.TREE_BRANCH,
    T.SRC_SYS_ID, 
    T.TREE_NODE_NUM_END,
    T.TREE_LEVEL_NUM, 
    T.TREE_NODE_TYPE, 
    T.PARENT_NODE_NUM,
    T.PARENT_NODE_NAME, 
    T.OLD_TREE_NODE_NUM,
    T.NODECOL_IMAGE,
    T.NODEEXP_IMAGE,
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
    ) 
values (
    S.SETID,
    S.SETCNTRLVALUE,
    S.TREE_NAME,
    S.EFFDT,
    S.TREE_NODE_NUM,
    S.TREE_NODE,
    S.TREE_BRANCH,
    'CS90', 
    S.TREE_NODE_NUM_END,
    S.TREE_LEVEL_NUM, 
    S.TREE_NODE_TYPE, 
    S.PARENT_NODE_NUM,
    S.PARENT_NODE_NAME, 
    S.OLD_TREE_NODE_NUM,
    S.NODECOL_IMAGE,
    S.NODEEXP_IMAGE,
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

strMessage01    := '# of PSTREENODE rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PSTREENODE',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PSTREENODE';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PSTREENODE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PSTREENODE';
update CSSTG_OWNER.PSTREENODE T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select SETID, SETCNTRLVALUE, TREE_NAME, EFFDT, TREE_NODE_NUM, TREE_NODE, TREE_BRANCH
   from CSSTG_OWNER.PSTREENODE T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PSTREENODE') = 'Y'
  minus
 select nvl(trim(SETID),'-') SETID, 
    nvl(trim(SETCNTRLVALUE),'-') SETCNTRLVALUE, 
    nvl(trim(TREE_NAME),'-') TREE_NAME, 
    EFFDT, 
    nvl(TREE_NODE_NUM,0) TREE_NODE_NUM, 
    nvl(trim(TREE_NODE),'-') TREE_NODE, 
    nvl(trim(TREE_BRANCH),'-') TREE_BRANCH
   from SYSADM.PSTREENODE@SASOURCE
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PSTREENODE') = 'Y'
   ) S
 where T.SETID = S.SETID
   and T.SETCNTRLVALUE = S.SETCNTRLVALUE
   and T.TREE_NAME = S.TREE_NAME
   and T.EFFDT = S.EFFDT
   and T.TREE_NODE_NUM = S.TREE_NODE_NUM
   and T.TREE_NODE = S.TREE_NODE
   and T.TREE_BRANCH = S.TREE_BRANCH
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PSTREENODE rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PSTREENODE',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PSTREENODE'
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

END PSTREENODE_P;
/
