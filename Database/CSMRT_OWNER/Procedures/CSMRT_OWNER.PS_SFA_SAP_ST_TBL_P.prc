CREATE OR REPLACE PROCEDURE             "PS_SFA_SAP_ST_TBL_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_SFA_SAP_ST_TBL from PeopleSoft table PS_SFA_SAP_ST_TBL.
--
-- V01  SMT-xxxx 04/04/2017,    Jim Doucette
--                              Converted from PS_SFA_SAP_ST_TBL.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_SFA_SAP_ST_TBL';
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
 where TABLE_NAME = 'PS_SFA_SAP_ST_TBL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_SFA_SAP_ST_TBL@SASOURCE S)
 where TABLE_NAME = 'PS_SFA_SAP_ST_TBL'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_SFA_SAP_ST_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_SFA_SAP_ST_TBL';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_SFA_SAP_ST_TBL T
using (select /*+ full(S) */
nvl(trim(INSTITUTION),'-') INSTITUTION,
nvl(trim(ACAD_CAREER),'-') ACAD_CAREER,
to_date(to_char(EFFDT,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EFFDT,
nvl(trim(SFA_SAP_STATUS),'-') SFA_SAP_STATUS,
nvl(trim(EFF_STATUS),'-') EFF_STATUS,
nvl(SFA_SAP_SEVERITY,0) SFA_SAP_SEVERITY,
nvl(trim(DESCRSHORT),'-') DESCRSHORT,
nvl(trim(DESCR),'-') DESCR,
nvl(trim(SAT_ACADEMIC_PRG),'-') SAT_ACADEMIC_PRG
from SYSADM.PS_SFA_SAP_ST_TBL@SASOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_SFA_SAP_ST_TBL') ) S
   on (
T.INSTITUTION = S.INSTITUTION and
T.ACAD_CAREER = S.ACAD_CAREER and
T.EFFDT = S.EFFDT and
T.SFA_SAP_STATUS = S.SFA_SAP_STATUS and
T.SRC_SYS_ID = 'CS90')
when matched then update set
T.EFF_STATUS = S.EFF_STATUS,
T.SFA_SAP_SEVERITY = S.SFA_SAP_SEVERITY,
T.DESCRSHORT = S.DESCRSHORT,
T.DESCR = S.DESCR,
T.SAT_ACADEMIC_PRG = S.SAT_ACADEMIC_PRG,
T.DATA_ORIGIN = 'S',
T.LASTUPD_EW_DTTM = sysdate,
T.BATCH_SID   = 1234
where
T.EFF_STATUS <> S.EFF_STATUS or
T.SFA_SAP_SEVERITY <> S.SFA_SAP_SEVERITY or
T.DESCRSHORT <> S.DESCRSHORT or
T.DESCR <> S.DESCR or
T.SAT_ACADEMIC_PRG <> S.SAT_ACADEMIC_PRG or
T.DATA_ORIGIN = 'D'
when not matched then
insert (
T.INSTITUTION,
T.ACAD_CAREER,
T.EFFDT,
T.SFA_SAP_STATUS,
T.SRC_SYS_ID,
T.EFF_STATUS,
T.SFA_SAP_SEVERITY,
T.DESCRSHORT,
T.DESCR,
T.SAT_ACADEMIC_PRG,
T.LOAD_ERROR,
T.DATA_ORIGIN,
T.CREATED_EW_DTTM,
T.LASTUPD_EW_DTTM,
T.BATCH_SID
)
values (
S.INSTITUTION,
S.ACAD_CAREER,
S.EFFDT,
S.SFA_SAP_STATUS,
'CS90',
S.EFF_STATUS,
S.SFA_SAP_SEVERITY,
S.DESCRSHORT,
S.DESCR,
S.SAT_ACADEMIC_PRG,
'N',
'S',
sysdate,
sysdate,
1234);


strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_SFA_SAP_ST_TBL rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_SFA_SAP_ST_TBL',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_SFA_SAP_ST_TBL';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_SFA_SAP_ST_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_SFA_SAP_ST_TBL';
update CSSTG_OWNER.PS_SFA_SAP_ST_TBL T
        set T.DATA_ORIGIN = 'D',
               T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select INSTITUTION, ACAD_CAREER, EFFDT, SFA_SAP_STATUS
   from CSSTG_OWNER.PS_SFA_SAP_ST_TBL T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_SFA_SAP_ST_TBL') = 'Y'
  minus
 select INSTITUTION, ACAD_CAREER, EFFDT, SFA_SAP_STATUS
   from SYSADM.PS_SFA_SAP_ST_TBL@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_SFA_SAP_ST_TBL') = 'Y'
   ) S
 where T.INSTITUTION = S.INSTITUTION
   and T.ACAD_CAREER = S.ACAD_CAREER
   and T.EFFDT = S.EFFDT
   and T.SFA_SAP_STATUS = S.SFA_SAP_STATUS
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_SFA_SAP_ST_TBL rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_SFA_SAP_ST_TBL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_SFA_SAP_ST_TBL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := strProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);


EXCEPTION
    WHEN OTHERS THEN
        numSqlCode := SQLCODE;
        strSqlErrm := SQLERRM;

        ROLLBACK;
  
        strMessage01 := 'Error code: ' || TO_CHAR(SQLCODE) || ' Error Message: ' || SQLERRM;
        strMessage02 := TO_CHAR(SQLCODE);
  
        COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_FAILURE
                       (i_SqlCommand    => strSqlCommand,
                        i_ErrorText     => strMessage01,
                        i_ErrorCode     => strMessage02,
                        i_ErrorMessage  => strSqlErrm
                       );
               
        strMessage01 := 'Error...'
                        || strNewLine   || 'SQL Command:   ' || strSqlCommand
                        || strNewLine   || 'Error code:    ' || numSqlCode
                        || strNewLine   || 'Error Message: ' || strSqlErrm;

        COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
        RAISE_APPLICATION_ERROR( -20001, strMessage01);

END PS_SFA_SAP_ST_TBL_P;
/
