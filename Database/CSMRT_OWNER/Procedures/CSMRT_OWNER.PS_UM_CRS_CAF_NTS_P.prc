DROP PROCEDURE CSMRT_OWNER.PS_UM_CRS_CAF_NTS_P
/

--
-- PS_UM_CRS_CAF_NTS_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_UM_CRS_CAF_NTS_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--
-- Loads stage table PS_UM_CRS_CAF_NTS from PeopleSoft table PS_UM_CRS_CAF_NTS.
--
 --V01  SMT-xxxx 11/1/2022,    Steve Celi
--                              
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_UM_CRS_CAF_NTS';
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
 where TABLE_NAME = 'PS_UM_CRS_CAF_NTS'       
;

strSqlCommand := 'commit';
commit;

strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_UM_CRS_CAF_NTS@SASOURCE S)
 where TABLE_NAME = 'PS_UM_CRS_CAF_NTS'
;

strSqlCommand := 'commit';
commit;

strMessage01    := 'Merging data into CSSTG_OWNER.PS_UM_CRS_CAF_NTS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_UM_CRS_CAF_NTS';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_UM_CRS_CAF_NTS T
using (select /*+ full(S) */
 CRSE_ID          ,
 EFFDT            ,
 SCC_CAF_ATTR_SEQ ,
 SCC_CAF_ATTRIB_NM,
 SCC_CAF_ATTR_VAL ,
 SCC_CAF_ATTR_NVAL,
 SCC_CAF_ATTR_DVAL,
 SCC_CAF_ATTR_TVAL,
 SCC_CAF_ATTR_TIME,
 SCC_CAF_ATTR_YNO ,
 SCC_CAF_ATTR_LVAL 
from SYSADM.PS_UM_CRS_CAF_NTS@SASOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_UM_CRS_CAF_NTS') 
) S
   on (
T.CRSE_ID = S.CRSE_ID and
T.EFFDT=S.EFFDT and     
T.SRC_SYS_ID = 'CS90')
when matched then update set
 T.SCC_CAF_ATTR_SEQ =S.SCC_CAF_ATTR_SEQ ,
 T.SCC_CAF_ATTRIB_NM=S.SCC_CAF_ATTRIB_NM,
 T.SCC_CAF_ATTR_VAL =S.SCC_CAF_ATTR_VAL ,
 T.SCC_CAF_ATTR_NVAL=S.SCC_CAF_ATTR_NVAL,
 T.SCC_CAF_ATTR_DVAL=S.SCC_CAF_ATTR_DVAL,
 T.SCC_CAF_ATTR_TVAL=S.SCC_CAF_ATTR_TVAL,
 T.SCC_CAF_ATTR_TIME=S.SCC_CAF_ATTR_TIME,
 T.SCC_CAF_ATTR_YNO =S.SCC_CAF_ATTR_YNO ,
 T.SCC_CAF_ATTR_LVAL=S.SCC_CAF_ATTR_LVAL ,
 T.DATA_ORIGIN='S',
T.LASTUPD_EW_DTTM=SYSDATE
where
DECODE(T.SCC_CAF_ATTR_SEQ ,S.SCC_CAF_ATTR_SEQ ,0,1) = 1 OR
DECODE(T.SCC_CAF_ATTRIB_NM,S.SCC_CAF_ATTRIB_NM,0,1) = 1 OR
DECODE(T.SCC_CAF_ATTR_VAL ,S.SCC_CAF_ATTR_VAL ,0,1) = 1 OR
DECODE(T.SCC_CAF_ATTR_NVAL,S.SCC_CAF_ATTR_NVAL,0,1) = 1 OR
DECODE(T.SCC_CAF_ATTR_DVAL,S.SCC_CAF_ATTR_DVAL,0,1) = 1 OR
DECODE(T.SCC_CAF_ATTR_TVAL,S.SCC_CAF_ATTR_TVAL,0,1) = 1 OR
DECODE(T.SCC_CAF_ATTR_TIME,S.SCC_CAF_ATTR_TIME,0,1) = 1 OR
DECODE(T.SCC_CAF_ATTR_YNO ,S.SCC_CAF_ATTR_YNO ,0,1) = 1 OR
DECODE(TO_CHAR(T.SCC_CAF_ATTR_LVAL),TO_CHAR(S.SCC_CAF_ATTR_LVAL),0,1) = 1 OR
T.DATA_ORIGIN = 'D'
when not matched then
insert (
T.CRSE_ID            ,
T.EFFDT              ,
T.SRC_SYS_ID         ,
T.SCC_CAF_ATTR_SEQ   ,
T.SCC_CAF_ATTRIB_NM  ,
T.SCC_CAF_ATTR_VAL   ,
T.SCC_CAF_ATTR_NVAL  ,
T.SCC_CAF_ATTR_DVAL  ,
T.SCC_CAF_ATTR_TVAL  ,
T.SCC_CAF_ATTR_TIME  ,
T.SCC_CAF_ATTR_YNO   ,
T.SCC_CAF_ATTR_LVAL  ,
T.DATA_ORIGIN        ,
T.CREATED_EW_DTTM    ,
T.LASTUPD_EW_DTTM    
)
values (
S.CRSE_ID            ,
S.EFFDT              ,
'CS90'               ,
S.SCC_CAF_ATTR_SEQ   ,
S.SCC_CAF_ATTRIB_NM  ,
S.SCC_CAF_ATTR_VAL   ,
S.SCC_CAF_ATTR_NVAL  ,
S.SCC_CAF_ATTR_DVAL  ,
S.SCC_CAF_ATTR_TVAL  ,
S.SCC_CAF_ATTR_TIME  ,
S.SCC_CAF_ATTR_YNO   ,
S.SCC_CAF_ATTR_LVAL  ,
'S'                  ,
SYSDATE              ,
SYSDATE              );

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_UM_CRS_CAF_NTS rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_UM_CRS_CAF_NTS',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_UM_CRS_CAF_NTS';

strSqlCommand := 'commit';
commit;

strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_UM_CRS_CAF_NTS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_UM_CRS_CAF_NTS';     
update CSSTG_OWNER.PS_UM_CRS_CAF_NTS T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select CRSE_ID, EFFDT
   from CSSTG_OWNER.PS_UM_CRS_CAF_NTS T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_UM_CRS_CAF_NTS') = 'Y'
  minus
 select CRSE_ID, EFFDT
   from SYSADM.PS_UM_CRS_CAF_NTS@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_UM_CRS_CAF_NTS') = 'Y'
   ) S
 where T.CRSE_ID = S.CRSE_ID
   and T.EFFDT = S.EFFDT
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_UM_CRS_CAF_NTS rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_UM_CRS_CAF_NTS',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_UM_CRS_CAF_NTS'
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

END PS_UM_CRS_CAF_NTS_P;
/
